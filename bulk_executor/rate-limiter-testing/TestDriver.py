import random
import string
import boto3
import threading
from boto3.dynamodb.conditions import Key

from server.src.python_modules.shared.rate_limiter.DynamoDBMonitor import DynamoDBMonitor
from server.src.python_modules.shared.rate_limiter.DistributedDynamoDBMonitorAggregator import DistributedDynamoDBMonitorAggregator
from server.src.python_modules.shared.rate_limiter.DistributedDynamoDBMonitorWorker import DistributedDynamoDBMonitorWorker

class TestDriver:
    def __init__(self, session, table_name: str, pk_name='pk'):
        self.resource = session.resource('dynamodb')
        self.client = self.resource.meta.client
        self.table = self.resource.Table(table_name)
        self.pk_name = pk_name

    def random_string(self, size):
        return ''.join(random.choices(string.ascii_letters + string.digits, k=size))

    def main_loop(self):
        while True:
            op = random.choice([
                'put', 'get', 'update', 'delete',
                'query', 'scan', 'batch_write', 'batch_get',
                'transact_write_and_get'
            ])

            pk = f"item-{random.randint(1, 1000000)}"

            if op == 'put':
                payload = self.random_string(random.randint(0, 10 * 1024))
                self.table.put_item(Item={self.pk_name: pk, 'somedata': payload})

            elif op == 'get':
                self.table.get_item(Key={self.pk_name: pk})

            elif op == 'update':
                self.table.update_item(
                    Key={self.pk_name: pk},
                    UpdateExpression='SET somedata = :val',
                    ExpressionAttributeValues={':val': self.random_string(random.randint(0, 10 * 1024))}
                )

            elif op == 'delete':
                self.table.delete_item(Key={self.pk_name: pk})

            elif op == 'query':
                self.table.query(KeyConditionExpression=Key(self.pk_name).eq(pk))

            elif op == 'scan':
                self.table.scan(Limit=10)

            elif op == 'batch_write':
                with self.table.batch_writer() as writer:
                    for _ in range(random.randint(1, 5)):
                        pk_batch = f"item-{random.randint(1, 1000000)}"
                        writer.put_item(Item={self.pk_name: pk_batch, 'somedata': self.random_string(random.randint(0, 10 * 1024))})

            elif op == 'batch_get':
                keys = [{self.pk_name: f"item-{random.randint(1, 1000000)}"} for _ in range(5)]
                self.client.batch_get_item(RequestItems={self.table.name: {'Keys': keys}})

            elif op == 'transact_write_and_get':
                key1 = f"item-{random.randint(1, 1000000)}"
                key2 = f"item-{random.randint(1, 1000000)}!"

                transact_write_items = [
                    {'Put': {'TableName': self.table.name, 'Item': {self.pk_name: key1, 'somedata': self.random_string(100)}}},
                    {'Put': {'TableName': self.table.name, 'Item': {self.pk_name: key2, 'somedata': self.random_string(100)}}}
                ]
                try:
                    self.client.transact_write_items(TransactItems=transact_write_items)
                except self.client.exceptions.TransactionCanceledException as e:
                    print("Transaction canceled. Reasons:")
                    for i, reason in enumerate(e.response.get('CancellationReasons', [])):
                        print(f"  [{i}] Code: {reason.get('Code')}, Message: {reason.get('Message')}, Item: {reason.get('Item')}")
                    raise

                transact_get_items = [
                    {'Get': {'TableName': self.table.name, 'Key': {self.pk_name: key1}}},
                    {'Get': {'TableName': self.table.name, 'Key': {self.pk_name: key2}}}
                ]
                try:
                    self.client.transact_get_items(TransactItems=transact_get_items)
                except self.client.exceptions.TransactionCanceledException as e:
                    print("Transaction canceled. Reasons:")
                    for i, reason in enumerate(e.response.get('CancellationReasons', [])):
                        print(f"  [{i}] Code: {reason.get('Code')}, Message: {reason.get('Message')}, Item: {reason.get('Item')}")
                    raise

def local_main():
    # The session events are where the monitoring happens, so make that first
    session = boto3.Session()

    # instantiate the monitor (registers hooks & starts reporting)
    DynamoDBMonitor(session, max_read_rate=10, max_write_rate=10, reset_interval=5)

    # pass the monitor to the test driver, along with table name
    driver = TestDriver(session, table_name='throttles', pk_name='pk')

    # start main loop
    driver.main_loop()


def distributed_main(bucket, prefix=None, table_name=None, 
                     aggregate_max_read_rate=100000, aggregate_max_write_rate=50000,
                     worker_max_read_rate=1500, worker_max_write_rate=500,
                     num_workers=2):

    # Create separate boto3 sessions for aggregator + workers
    session_agg = boto3.Session()
    worker_sessions = [boto3.Session() for _ in range(num_workers)]

    # Start aggregator
    aggregator = DistributedDynamoDBMonitorAggregator(
        session=session_agg,
        bucket=bucket,
        prefix=prefix
    )

    # Start workers
    workers = []
    for i, session in enumerate(worker_sessions):
        worker = DistributedDynamoDBMonitorWorker(
            session=session,
            bucket=bucket,
            prefix=prefix,
            aggregate_max_read_rate=aggregate_max_read_rate,
            aggregate_max_write_rate=aggregate_max_write_rate,
            worker_max_read_rate=worker_max_read_rate,
            worker_max_write_rate=worker_max_write_rate
        )
        workers.append(worker)

    # Define a function to run a driver per worker
    def run_driver(session, name):
        driver = TestDriver(session, table_name=table_name)
        #print(f"[{name}] starting main loop")
        driver.main_loop()

    # Launch driver threads
    driver_threads = []
    for i, session in enumerate(worker_sessions):
        t = threading.Thread(target=run_driver, args=(session, f"Worker-{i+1}"), daemon=True)
        t.start()
        driver_threads.append(t)

    try:
        # Wait for drivers to run (or you could sleep X seconds here if temporary test)
        for t in driver_threads:
            t.join()
    finally:
        print("Shutting down... cleaning up")
        for worker in workers:
            worker.cleanup()
        aggregator.stop()


if __name__ == "__main__":
    BUCKET='aws-glue-bulk-dynamodb-us-east-1-654654401288-kudksyvw'
    PREFIX='throttling'
    TABLE='throttles'

    distributed_main(bucket=BUCKET, prefix=PREFIX, table_name=TABLE, 
                     aggregate_max_read_rate=100, aggregate_max_write_rate=100,
                     worker_max_read_rate=80, worker_max_write_rate=80,
                     num_workers=5)
