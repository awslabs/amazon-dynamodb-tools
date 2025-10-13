import threading
import time
from collections import defaultdict

class DynamoDBMonitor:
    def __init__(self, session, max_read_rate=1500, max_write_rate=500, reset_interval=10, enable_reporting=True):
        self.max_read_rate = max_read_rate
        self.max_write_rate = max_write_rate
        self.reset_interval = reset_interval

        self.metrics = defaultdict(float)
        self.metrics_lock = threading.Lock()
        self.stop_event = threading.Event()

        self.rate_limit_lock = threading.Lock()
        self.rate_limit_state = {
            'checkpoint_time': time.monotonic(),
            'read_so_far': 0,
            'write_so_far': 0
        }

        # register hooks
        events = session.events
        events.register('provide-client-params.dynamodb.*', self._add_return_consumed_capacity)
        events.register('before-call.dynamodb.*', self._enforce_rate_limit)
        events.register('after-call.dynamodb.*', self._track_consumed_capacity)

        # start background reporter, if desired
        if enable_reporting:
            self._report_thread = threading.Thread(
                    target=self._report_metrics,
                    daemon=True
                )
            self._report_thread.start()

    def _add_return_consumed_capacity(self, params, **kwargs):
        if 'ReturnConsumedCapacity' not in params:
            params['ReturnConsumedCapacity'] = 'TOTAL'

    def _enforce_rate_limit(self, params, model, **kwargs):
        operation = model.name
        is_read = operation in ('GetItem', 'BatchGetItem', 'Query', 'Scan', 'TransactGetItems')
        is_write = operation in ('PutItem', 'UpdateItem', 'DeleteItem', 'BatchWriteItem', 'TransactWriteItems')

        now = time.monotonic()
        with self.rate_limit_lock:
            elapsed = now - self.rate_limit_state['checkpoint_time']
            if elapsed >= self.reset_interval:
                self.rate_limit_state['checkpoint_time'] = now
                self.rate_limit_state['read_so_far'] = 0
                self.rate_limit_state['write_so_far'] = 0
                elapsed = 0

            allowed_reads = self.max_read_rate * elapsed
            allowed_writes = self.max_write_rate * elapsed

            sleep_needed = 0
            if is_read:
                over_reads = self.rate_limit_state['read_so_far'] - allowed_reads
                if over_reads > 0:
                    sleep_needed = over_reads / self.max_read_rate
            elif is_write:
                over_writes = self.rate_limit_state['write_so_far'] - allowed_writes
                if over_writes > 0:
                    sleep_needed = over_writes / self.max_write_rate

            if sleep_needed > 0:
                time.sleep(sleep_needed)

    def _track_consumed_capacity(self, http_response, parsed, model, **kwargs):
        consumed = parsed.get('ConsumedCapacity')
        if consumed:
            if isinstance(consumed, list):
                for entry in consumed:
                    self._process_entry(entry, model.name)
            elif isinstance(consumed, dict):
                self._process_entry(consumed, model.name)

    def _process_entry(self, entry, model_name):
        read = entry.get('ReadCapacityUnits')
        write = entry.get('WriteCapacityUnits')
        if read is None and write is None:
            if model_name in ('GetItem', 'BatchGetItem', 'Query', 'Scan', 'TransactGetItems'):
                read = entry.get('CapacityUnits', 0.0)
                write = 0.0
            elif model_name in ('PutItem', 'UpdateItem', 'DeleteItem', 'BatchWriteItem', 'TransactWriteItems'):
                read = 0.0
                write = entry.get('CapacityUnits', 0.0)
            else:
                raise Exception(f"Unknown model_name {model_name} with ambiguous capacity: {entry}")
        else:
            read = read or 0.0
            write = write or 0.0

        capacity = entry.get('CapacityUnits', 0.0)
        with self.metrics_lock:
            self.metrics['read_capacity'] += read
            self.metrics['write_capacity'] += write
            self.metrics['total_capacity'] += capacity
            self.metrics['calls'] += 1

        with self.rate_limit_lock:
            self.rate_limit_state['read_so_far'] += read
            self.rate_limit_state['write_so_far'] += write

    def _report_metrics(self):
        while not self.stop_event.is_set():
            time.sleep(1)
            with self.metrics_lock, self.rate_limit_lock:
                print(
                    f"WCU: {self.metrics['write_capacity']:.2f}, RCU: {self.metrics['read_capacity']:.2f}, "
                    f"Total CU: {self.metrics['total_capacity']:.2f}, Calls: {self.metrics['calls']} | "
                    f"Accumulated W: {self.rate_limit_state['write_so_far']:.2f}, "
                    f"Accumulated R: {self.rate_limit_state['read_so_far']:.2f}, "
                    f"Since: {time.monotonic() - self.rate_limit_state['checkpoint_time']:.2f}s"
                )

    def stop(self):
        self.stop_event.set()
        if hasattr(self, '_report_thread') and self._report_thread.is_alive():
            self._report_thread.join(timeout=2)
