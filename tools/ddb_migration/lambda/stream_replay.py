"""
DynamoDB Stream Replay Lambda for zero-downtime table migration.

Processes stream records from the source table and writes to the target
using _migration_ts conditional writes for conflict resolution.

Behaviors:
- INSERT/MODIFY: conditional PutItem with _migration_ts ordering
- REMOVE: writes a tombstone (prevents backfill from re-inserting)
- ConditionalCheckFailedException: safe to skip (newer version exists)
- Other errors: reports as batchItemFailure for Lambda retry

Environment variables:
    TARGET_TABLE    - Name of the target table
    PARTITION_KEY   - Partition key attribute name (default: pk)
    TARGET_REGION   - Region of target table (default: same as Lambda)
    TRANSFORM_MODULE - Optional path to custom transform module

Deploy with:
    --function-response-types ReportBatchItemFailures
    --bisect-batch-on-function-error
"""

import boto3
import os
import logging
import importlib.util
from decimal import Decimal
from botocore.exceptions import ClientError
from boto3.dynamodb.types import TypeDeserializer

logger = logging.getLogger()
logger.setLevel(logging.INFO)

TARGET_TABLE_NAME = os.environ['TARGET_TABLE']
PARTITION_KEY = os.environ.get('PARTITION_KEY', 'pk')
TARGET_REGION = os.environ.get('TARGET_REGION', os.environ.get('AWS_REGION', 'us-east-1'))

dynamodb = boto3.resource('dynamodb', region_name=TARGET_REGION)
target_table = dynamodb.Table(TARGET_TABLE_NAME)
deserializer = TypeDeserializer()

# Load transform module
try:
    from transform import transform
except ImportError:
    def transform(item, source_event=None):
        return item


def handler(event, context):
    """Process DynamoDB Stream records with conditional writes."""
    failed_records = []

    for record in event['Records']:
        try:
            process_record(record)
        except ClientError as e:
            if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
                pass  # Newer version exists -- safe to skip
            else:
                logger.error(f"ClientError: {e}")
                failed_records.append(record)
        except Exception as e:
            logger.error(f"Error: {e}", exc_info=True)
            failed_records.append(record)

    if failed_records:
        return {
            'batchItemFailures': [
                {'itemIdentifier': r['eventID']} for r in failed_records
            ]
        }


def process_record(record):
    """Apply a single stream record to the target table."""
    event_name = record['eventName']
    event_ts = Decimal(str(record['dynamodb']['ApproximateCreationDateTime']))
    keys_raw = record['dynamodb']['Keys']
    key = {k: deserializer.deserialize(v) for k, v in keys_raw.items()}

    if event_name in ('INSERT', 'MODIFY'):
        new_image_raw = record['dynamodb']['NewImage']
        item = {k: deserializer.deserialize(v) for k, v in new_image_raw.items()}

        # Apply transform
        item = transform(item, source_event=event_name)
        if item is None:
            return  # Transform says skip

        item['_migration_ts'] = event_ts
        target_table.put_item(
            Item=item,
            ConditionExpression='attribute_not_exists(#pk) OR #ts < :ts',
            ExpressionAttributeNames={'#pk': PARTITION_KEY, '#ts': '_migration_ts'},
            ExpressionAttributeValues={':ts': event_ts}
        )

    elif event_name == 'REMOVE':
        tombstone = {**key, '_tombstone': True, '_migration_ts': event_ts}
        target_table.put_item(
            Item=tombstone,
            ConditionExpression='attribute_not_exists(#pk) OR #ts < :ts',
            ExpressionAttributeNames={'#pk': PARTITION_KEY, '#ts': '_migration_ts'},
            ExpressionAttributeValues={':ts': event_ts}
        )
