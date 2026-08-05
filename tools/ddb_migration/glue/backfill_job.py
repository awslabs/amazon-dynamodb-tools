"""
AWS Glue job for parallel bulk import during DynamoDB zero-downtime migration.

Reads DynamoDB export files from S3 and writes to the target table with
_migration_ts conditional writes. Designed to run as a Glue Python Shell job
for massive parallelism on large tables (100+ GiB).

For tables under 100 GiB, use scripts/backfill.py instead (simpler, no Glue setup).

Glue job parameters (passed via --additional-python-modules or job args):
    --TARGET_TABLE      Target DynamoDB table name
    --PARTITION_KEY     Partition key attribute name
    --EXPORT_BUCKET     S3 bucket containing the export
    --EXPORT_PREFIX     S3 prefix for the export
    --TARGET_REGION     Region of the target table
    --MAX_WRITE_RATE    Max WCU/s to consume (default: unlimited)
    --LAMBDA_FUNCTION   Stream replay Lambda name (for circuit breaker)
"""

import sys
import boto3
import json
import gzip
import random
import time
from decimal import Decimal
from concurrent.futures import ThreadPoolExecutor, as_completed
from awsglue.utils import getResolvedOptions
from botocore.exceptions import ClientError
from boto3.dynamodb.types import TypeDeserializer

# Parse Glue job arguments
args = getResolvedOptions(sys.argv, [
    'TARGET_TABLE', 'PARTITION_KEY', 'EXPORT_BUCKET', 'EXPORT_PREFIX',
    'TARGET_REGION', 'MAX_WRITE_RATE', 'LAMBDA_FUNCTION'
])

TARGET_TABLE = args['TARGET_TABLE']
PARTITION_KEY = args['PARTITION_KEY']
BUCKET = args['EXPORT_BUCKET']
PREFIX = args['EXPORT_PREFIX']
REGION = args.get('TARGET_REGION', 'us-east-1')
MAX_WRITE_RATE = int(args.get('MAX_WRITE_RATE', '0'))  # 0 = unlimited
LAMBDA_FUNCTION = args.get('LAMBDA_FUNCTION', 'migration-stream-replay')
ITERATOR_AGE_PAUSE_HOURS = 18

s3 = boto3.client('s3', region_name=REGION)
dynamodb = boto3.resource('dynamodb', region_name=REGION)
cloudwatch = boto3.client('cloudwatch', region_name=REGION)
target_table = dynamodb.Table(TARGET_TABLE)
deserializer = TypeDeserializer()

# Import transform (bundled with the job)
try:
    from transform import transform
except ImportError:
    def transform(item, source_event=None):
        return item


def get_data_files():
    """Parse export manifest to get data file S3 keys."""
    paginator = s3.get_paginator('list_objects_v2')
    data_files = []
    for page in paginator.paginate(Bucket=BUCKET, Prefix=PREFIX):
        for obj in page.get('Contents', []):
            if obj['Key'].endswith('.json.gz'):
                data_files.append(obj['Key'])
    return data_files


def should_pause():
    """Circuit breaker: pause if stream replay iterator age is too high."""
    try:
        resp = cloudwatch.get_metric_statistics(
            Namespace='AWS/Lambda',
            MetricName='IteratorAge',
            Dimensions=[{'Name': 'FunctionName', 'Value': LAMBDA_FUNCTION}],
            StartTime=time.time() - 300,
            EndTime=time.time(),
            Period=60,
            Statistics=['Maximum']
        )
        if resp['Datapoints']:
            max_age_h = max(dp['Maximum'] for dp in resp['Datapoints']) / 3_600_000
            if max_age_h > ITERATOR_AGE_PAUSE_HOURS:
                print(f"PAUSING: Iterator age {max_age_h:.1f}h > {ITERATOR_AGE_PAUSE_HOURS}h")
                return True
    except Exception as e:
        print(f"Warning: iterator age check failed: {e}")
    return False


def put_item_conditional(item, max_retries=8):
    """PutItem with _migration_ts=0 conditional write and backoff."""
    item = transform(item, source_event=None)
    if item is None:
        return 'skipped'

    item['_migration_ts'] = 0
    for attempt in range(max_retries):
        try:
            target_table.put_item(
                Item=item,
                ConditionExpression='attribute_not_exists(#pk) OR #ts < :ts',
                ExpressionAttributeNames={'#pk': PARTITION_KEY, '#ts': '_migration_ts'},
                ExpressionAttributeValues={':ts': 0}
            )
            return 'written'
        except ClientError as e:
            code = e.response['Error']['Code']
            if code == 'ConditionalCheckFailedException':
                return 'skipped'
            elif code in ('ProvisionedThroughputExceededException', 'ThrottlingException'):
                time.sleep(min(2 ** attempt * 0.1, 30) + random.uniform(0, 0.5))
            else:
                raise
    raise RuntimeError(f"Failed after {max_retries} retries")


def process_file(s3_key):
    """Process one export data file."""
    obj = s3.get_object(Bucket=BUCKET, Key=s3_key)
    content = gzip.decompress(obj['Body'].read()).decode('utf-8')

    items = []
    for line in content.strip().split('\n'):
        if line:
            record = json.loads(line)
            item = {k: deserializer.deserialize(v) for k, v in record['Item'].items()}
            items.append(item)

    random.shuffle(items)
    written, skipped, errors = 0, 0, 0

    for i, item in enumerate(items):
        if i % 5000 == 0 and i > 0:
            while should_pause():
                time.sleep(60)
        try:
            result = put_item_conditional(item)
            if result == 'written':
                written += 1
            else:
                skipped += 1
        except Exception as e:
            errors += 1
            print(f"ERROR: {e}")

    return len(items), written, skipped, errors


# Main execution
data_files = get_data_files()
print(f"Found {len(data_files)} data files")

total_items, total_written, total_skipped, total_errors = 0, 0, 0, 0
start = time.time()

with ThreadPoolExecutor(max_workers=16) as executor:
    futures = {executor.submit(process_file, f): f for f in data_files}
    for future in as_completed(futures):
        key = futures[future]
        try:
            count, written, skipped, errors = future.result()
            total_items += count
            total_written += written
            total_skipped += skipped
            total_errors += errors
            rate = total_items / (time.time() - start)
            print(f"Done {key}: {count} items ({written}w/{skipped}s/{errors}e) "
                  f"[total: {total_items:,} @ {rate:.0f}/s]")
        except Exception as e:
            print(f"FAILED {key}: {e}")

elapsed = time.time() - start
print(f"\nBackfill complete: {total_items:,} items in {elapsed/60:.1f}min "
      f"({total_written:,} written, {total_skipped:,} skipped, {total_errors:,} errors)")
