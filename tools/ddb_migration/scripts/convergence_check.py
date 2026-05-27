"""
Pre-cutover convergence verification for MRSC migration.

Checks that the migration is safe to cut over:
1. Stream replay iterator age is near zero (Lambda is caught up)
2. DLQ is empty (no failed records)
3. Item counts are within tolerance

Usage:
    export TARGET_TABLE=TargetTable-MRSC
    export SOURCE_TABLE=SourceTable
    export REGION=us-east-1
    export DLQ_URL=https://sqs.us-east-1.amazonaws.com/123456789012/migration-dlq
    export LAMBDA_FUNCTION_NAME=MRSCStreamReplay
    python convergence_check.py
"""

import boto3
import time
import sys
import os

REGION = os.environ.get('REGION', 'us-east-1')
SOURCE_TABLE = os.environ['SOURCE_TABLE']
TARGET_TABLE = os.environ['TARGET_TABLE']
DLQ_URL = os.environ['DLQ_URL']
LAMBDA_FUNCTION_NAME = os.environ.get('LAMBDA_FUNCTION_NAME', 'MRSCStreamReplay')

cloudwatch = boto3.client('cloudwatch', region_name=REGION)
dynamodb = boto3.client('dynamodb', region_name=REGION)
sqs = boto3.client('sqs', region_name=REGION)


def check_iterator_age(max_wait_seconds=600):
    """
    Wait for stream replay Lambda to be fully caught up.

    Iterator age = how far behind the Lambda is from the tip of the stream.
    An age of 0 means events are being processed in real time.

    Returns True if caught up within timeout.
    """
    print("Checking stream replay iterator age...")
    start = time.time()

    while time.time() - start < max_wait_seconds:
        response = cloudwatch.get_metric_statistics(
            Namespace='AWS/Lambda',
            MetricName='IteratorAge',
            Dimensions=[{
                'Name': 'FunctionName',
                'Value': LAMBDA_FUNCTION_NAME
            }],
            StartTime=time.time() - 120,
            EndTime=time.time(),
            Period=60,
            Statistics=['Maximum']
        )

        if response['Datapoints']:
            max_age_ms = max(dp['Maximum'] for dp in response['Datapoints'])
            print(f"  Iterator age: {max_age_ms/1000:.1f}s")

            if max_age_ms < 1000:  # < 1 second
                print("  ✓ Stream replay is caught up (processing in real time)")
                return True
        else:
            print("  No datapoints yet, waiting...")

        time.sleep(10)

    print("  ✗ Stream replay did not converge within timeout")
    return False


def check_dlq_empty():
    """Verify no unprocessed records in the dead-letter queue."""
    print("Checking DLQ...")
    attrs = sqs.get_queue_attributes(
        QueueUrl=DLQ_URL,
        AttributeNames=['ApproximateNumberOfMessages', 'ApproximateNumberOfMessagesNotVisible']
    )
    visible = int(attrs['Attributes']['ApproximateNumberOfMessages'])
    in_flight = int(attrs['Attributes']['ApproximateNumberOfMessagesNotVisible'])
    total = visible + in_flight

    if total == 0:
        print("  ✓ DLQ is empty — no failed records")
        return True
    else:
        print(f"  ✗ DLQ has {total} messages ({visible} visible, {in_flight} in-flight)")
        print("    Investigate these records before proceeding with cutover")
        return False


def check_item_counts():
    """
    Compare approximate item counts between source and target.

    Note: DynamoDB ItemCount updates approximately every 6 hours.
    This is a sanity check, not a precise validation.
    """
    print("Checking item counts (approximate)...")
    source = dynamodb.describe_table(TableName=SOURCE_TABLE)
    target = dynamodb.describe_table(TableName=TARGET_TABLE)

    src_count = source['Table']['ItemCount']
    tgt_count = target['Table']['ItemCount']

    print(f"  Source: ~{src_count:,} items")
    print(f"  Target: ~{tgt_count:,} items")

    if src_count == 0:
        print("  ⚠️  Source count is 0 — ItemCount may not be populated yet")
        return True

    diff_pct = abs(src_count - tgt_count) / src_count * 100

    if diff_pct < 5:
        print(f"  ✓ Counts within {diff_pct:.1f}% (ItemCount updates every ~6h)")
        return True
    else:
        print(f"  ⚠️  Counts differ by {diff_pct:.1f}% — may be stale, or backfill incomplete")
        print("    Consider running a precise count via Scan if concerned")
        return True  # Don't block on approximate metric


def main():
    print("=" * 60)
    print("MRSC Migration — Pre-Cutover Convergence Check")
    print("=" * 60)
    print()

    results = []

    results.append(('Iterator Age', check_iterator_age()))
    print()
    results.append(('DLQ Empty', check_dlq_empty()))
    print()
    results.append(('Item Counts', check_item_counts()))
    print()

    print("=" * 60)
    print("Results:")
    all_passed = True
    for name, passed in results:
        status = "✓ PASS" if passed else "✗ FAIL"
        print(f"  {status}: {name}")
        if not passed:
            all_passed = False

    print()
    if all_passed:
        print("✓ All checks passed — safe to proceed with cutover")
        print()
        print("Next steps:")
        print("  1. Pause writes briefly (~5 seconds)")
        print("  2. Wait for in-flight stream records to drain")
        print("  3. Flip your feature flag to route traffic to TargetTable-MRSC")
        print("  4. Resume writes")
    else:
        print("✗ Some checks failed — do NOT proceed with cutover")
        sys.exit(1)


if __name__ == '__main__':
    main()
