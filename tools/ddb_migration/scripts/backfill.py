"""
Parallel bulk import for DynamoDB MRSC migration.

Reads exported data files from S3, writes items to the MRSC target table
with _migration_ts conditional writes to avoid overwriting stream-replicated data.

Includes:
- Manifest parsing to discover data files
- Parallel file processing with concurrent.futures
- Conditional writes (_migration_ts = 0, lowest priority) for conflict safety
- Exponential backoff + jitter for throttling
- Circuit breaker that pauses if stream replay iterator age grows too high
- Randomized write order to avoid hot partitions

Usage:
    export SOURCE_TABLE=SourceTable
    export TARGET_TABLE=TargetTable-MRSC
    export PARTITION_KEY=pk
    export REGION=us-east-1
    export EXPORT_BUCKET=my-migration-bucket
    export EXPORT_PREFIX=exports/
    export MAX_WORKERS=16
    python backfill.py
"""

import boto3
import json
import gzip
import random
import time
import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from botocore.exceptions import ClientError
from boto3.dynamodb.types import TypeDeserializer

# --- Configuration ---
BUCKET = os.environ['EXPORT_BUCKET']
EXPORT_PREFIX = os.environ.get('EXPORT_PREFIX', 'exports/')
TARGET_TABLE_NAME = os.environ['TARGET_TABLE']
PARTITION_KEY = os.environ.get('PARTITION_KEY', 'pk')
MAX_WORKERS = int(os.environ.get('MAX_WORKERS', '16'))
REGION = os.environ.get('REGION', 'us-east-1')
LAMBDA_FUNCTION_NAME = os.environ.get('LAMBDA_FUNCTION_NAME', 'MRSCStreamReplay')

# Iterator age threshold (in hours) to pause backfill
ITERATOR_AGE_PAUSE_THRESHOLD_HOURS = 18

s3 = boto3.client('s3', region_name=REGION)
dynamodb = boto3.resource('dynamodb', region_name=REGION)
cloudwatch = boto3.client('cloudwatch', region_name=REGION)
target_table = dynamodb.Table(TARGET_TABLE_NAME)
deserializer = TypeDeserializer()


def get_data_file_keys_from_manifest(bucket, prefix):
    """
    Parse the DynamoDB export manifest to get the list of data file S3 keys.

    The export creates a timestamped subdirectory containing:
    - manifest-summary.json (points to the manifest file)
    - manifest file (JSONL with one entry per data file)
    - data/ directory with .json.gz files
    """
    # Find the export directory (DynamoDB creates a timestamped subfolder)
    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, Delimiter='/')
    export_dirs = [p['Prefix'] for p in response.get('CommonPrefixes', [])]

    if not export_dirs:
        raise ValueError(f"No export directories found under s3://{bucket}/{prefix}")

    # Use the most recent export directory
    export_dir = sorted(export_dirs)[-1]
    print(f"Using export directory: s3://{bucket}/{export_dir}")

    # Read manifest-summary.json
    summary_key = f"{export_dir}manifest-summary.json"
    summary = json.loads(
        s3.get_object(Bucket=bucket, Key=summary_key)['Body'].read()
    )
    print(f"Export status: {summary.get('exportStatus', 'unknown')}")
    print(f"Item count: {summary.get('itemCount', 'unknown')}")

    # Read the manifest files list
    manifest_key = summary['manifestFilesS3Key']
    manifest_content = s3.get_object(Bucket=bucket, Key=manifest_key)['Body'].read()

    data_files = []
    for line in manifest_content.decode('utf-8').strip().split('\n'):
        entry = json.loads(line)
        data_files.append(entry['dataFileS3Key'])

    print(f"Found {len(data_files)} data files in export manifest")
    return data_files


def deserialize_dynamodb_json(dynamo_item):
    """Convert DynamoDB JSON format to Python-native types."""
    return {k: deserializer.deserialize(v) for k, v in dynamo_item.items()}


def put_item_with_retry(item, max_retries=8):
    """
    PutItem with _migration_ts conditional write and exponential backoff.

    Backfill always writes _migration_ts = 0 (lowest priority). Stream replay
    writes with ApproximateCreationDateTime (always > 0), so stream data
    always wins over backfill data.

    Returns:
        'written' if the item was written
        'skipped' if a newer version already exists (written by stream replay)
    """
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
                # A newer version exists (stream replay wrote it) — skip safely
                return 'skipped'
            elif code in ('ProvisionedThroughputExceededException',
                          'ThrottlingException'):
                wait = min(2 ** attempt * 0.1, 30) + random.uniform(0, 0.5)
                time.sleep(wait)
            else:
                raise

    raise RuntimeError(
        f"Failed after {max_retries} retries for item "
        f"{item.get(PARTITION_KEY, 'unknown_key')}"
    )


def should_pause_backfill():
    """
    Circuit breaker: returns True if stream replay iterator age is dangerously high.

    Checks the IteratorAge CloudWatch metric for the stream replay Lambda.
    If the Lambda is more than ITERATOR_AGE_PAUSE_THRESHOLD_HOURS behind,
    we pause backfill to free write capacity.
    """
    try:
        response = cloudwatch.get_metric_statistics(
            Namespace='AWS/Lambda',
            MetricName='IteratorAge',
            Dimensions=[{
                'Name': 'FunctionName',
                'Value': LAMBDA_FUNCTION_NAME
            }],
            StartTime=time.time() - 300,
            EndTime=time.time(),
            Period=60,
            Statistics=['Maximum']
        )

        if response['Datapoints']:
            max_age_ms = max(dp['Maximum'] for dp in response['Datapoints'])
            max_age_hours = max_age_ms / 3_600_000

            if max_age_hours > ITERATOR_AGE_PAUSE_THRESHOLD_HOURS:
                print(f"⚠️  PAUSING: Stream replay iterator age is {max_age_hours:.1f}h "
                      f"(threshold: {ITERATOR_AGE_PAUSE_THRESHOLD_HOURS}h)")
                return True
            elif max_age_hours > 12:
                print(f"⚠️  WARNING: Stream replay iterator age is {max_age_hours:.1f}h")

    except Exception as e:
        print(f"Warning: could not check iterator age: {e}")

    return False


def process_data_file(s3_key):
    """Process a single export data file — read items and write to target table."""
    response = s3.get_object(Bucket=BUCKET, Key=s3_key)

    if s3_key.endswith('.gz'):
        content = gzip.decompress(response['Body'].read()).decode('utf-8')
    else:
        content = response['Body'].read().decode('utf-8')

    items = []
    for line in content.strip().split('\n'):
        if not line:
            continue
        record = json.loads(line)
        item = deserialize_dynamodb_json(record['Item'])
        items.append(item)

    # Randomize to distribute writes across partitions
    random.shuffle(items)

    written = 0
    skipped = 0
    errors = 0

    for i, item in enumerate(items):
        # Circuit breaker check every 1000 items
        if i % 1000 == 0 and i > 0:
            while should_pause_backfill():
                time.sleep(60)

        try:
            result = put_item_with_retry(item)
            if result == 'written':
                written += 1
            else:
                skipped += 1
        except Exception as e:
            errors += 1
            print(f"  ERROR in {s3_key}: {e}")

    return len(items), written, skipped, errors


def run_parallel_import():
    """Import all data files in parallel."""
    manifest_files = get_data_file_keys_from_manifest(BUCKET, EXPORT_PREFIX)

    total_items = 0
    total_written = 0
    total_skipped = 0
    total_errors = 0
    failed_files = []

    start_time = time.time()

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_file = {
            executor.submit(process_data_file, f): f
            for f in manifest_files
        }

        for future in as_completed(future_to_file):
            file_key = future_to_file[future]
            try:
                count, written, skipped, errors = future.result()
                total_items += count
                total_written += written
                total_skipped += skipped
                total_errors += errors
                elapsed = time.time() - start_time
                rate = total_items / elapsed if elapsed > 0 else 0
                print(f"✓ {file_key}: {count} items "
                      f"({written} written, {skipped} skipped, {errors} errors) "
                      f"[total: {total_items:,} @ {rate:.0f} items/sec]")
            except Exception as e:
                print(f"✗ FAILED {file_key}: {e}")
                failed_files.append(file_key)

    elapsed = time.time() - start_time
    print(f"\n{'='*60}")
    print(f"Backfill complete in {elapsed/60:.1f} minutes")
    print(f"  Total items:   {total_items:,}")
    print(f"  Written:       {total_written:,}")
    print(f"  Skipped:       {total_skipped:,} (already existed via stream replay)")
    print(f"  Errors:        {total_errors:,}")
    print(f"  Failed files:  {len(failed_files)}")
    print(f"  Avg rate:      {total_items/elapsed:.0f} items/sec")

    if failed_files:
        print(f"\nFAILED files — retry these manually:")
        for f in failed_files:
            print(f"  {f}")
        sys.exit(1)


if __name__ == '__main__':
    run_parallel_import()
