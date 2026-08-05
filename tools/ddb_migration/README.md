# DynamoDB Zero-Downtime Migration

Zero-downtime migration between DynamoDB tables using DynamoDB Streams, Export to S3, and conditional writes for conflict resolution.

## When to use this

- Migrating to Global Tables with Multi-Region Strong Consistency (MRSC)
- Moving a table to a different AWS account
- Restructuring key schema or adding GSIs that require a new table
- Consolidating multiple tables into a single-table design
- Any scenario where you need a new table but can't stop writes

## How it works

Three overlapping phases:

1. **Stream Capture & Bulk Copy** -- Enable streams, export to S3, backfill into target (parallel with stream replay)
2. **Stream Catch-up** -- Lambda replays live changes with conditional writes
3. **Convergence & Switchover** -- Verify consistency, pause briefly, flip routing

Conflict resolution uses a `_migration_ts` attribute with conditional writes. Backfill writes `_migration_ts = 0` (lowest priority). Stream replay writes `_migration_ts = ApproximateCreationDateTime` (always wins over backfill). Later timestamps always win over earlier ones.

## Package structure

```
ddb_migration/
  lambda/
    stream_replay.py    # Lambda handler for DynamoDB Streams -> target table
  glue/
    backfill_job.py     # AWS Glue job for large tables (100+ GiB)
  scripts/
    backfill.py         # Standalone backfill script (tables < 100 GiB)
    convergence_check.py # Pre-cutover verification
  iam/
    policies.json       # IAM policy templates for each component
  transform.py          # Shared item transformation (customize for schema changes)
  deploy.sh             # One-command infrastructure setup
  requirements.txt
```

## Quick start

```bash
# 1. Configure
export SOURCE_TABLE=OrdersV1
export TARGET_TABLE=OrdersV2
export PARTITION_KEY=pk
export SORT_KEY=sk
export REGION=us-east-1

# 2. Deploy infrastructure (creates target table, Lambda, alarms)
./deploy.sh

# 3. Export source table
aws dynamodb export-table-to-point-in-time \
  --table-arn arn:aws:dynamodb:$REGION:$(aws sts get-caller-identity --query Account --output text):table/$SOURCE_TABLE \
  --s3-bucket $EXPORT_BUCKET --s3-prefix exports/ \
  --export-format DYNAMODB_JSON --region $REGION

# 4. Run backfill (while stream replay Lambda is already processing)
export EXPORT_BUCKET=ddb-migration-$(aws sts get-caller-identity --query Account --output text)-$REGION
export EXPORT_PREFIX=exports/
python scripts/backfill.py

# 5. Verify convergence
python scripts/convergence_check.py

# 6. Cutover: flip your application routing to TARGET_TABLE
```

## For large tables (100+ GiB)

Use the Glue job instead of the standalone script:

```bash
aws glue create-job \
  --name migration-backfill \
  --role AWSGlueServiceRole-Migration \
  --command '{"Name":"pythonshell","ScriptLocation":"s3://bucket/glue/backfill_job.py","PythonVersion":"3.9"}' \
  --default-arguments '{
    "--TARGET_TABLE":"OrdersV2",
    "--PARTITION_KEY":"pk",
    "--EXPORT_BUCKET":"my-bucket",
    "--EXPORT_PREFIX":"exports/",
    "--TARGET_REGION":"us-east-1",
    "--LAMBDA_FUNCTION":"migration-stream-replay",
    "--additional-python-modules":"boto3"
  }' \
  --max-capacity 1.0

aws glue start-job-run --job-name migration-backfill
```

Or use [Bulk Executor](../bulk_executor) for the simple path (maintenance window, no conditional writes needed):
```bash
./bulk load-export --table OrdersV2 --s3-path "s3://bucket/exports/AWSDynamoDB/export-id"
```

## Schema changes

If your target table has a different schema, edit `transform.py`:

```python
def transform(item, source_event=None):
    # Example: rename attribute
    item['order_id'] = item.pop('orderId', item.get('order_id'))
    # Example: add computed field
    item['gsi1pk'] = f"TENANT#{item['tenant_id']}"
    return item
```

Both the Lambda and backfill import this module, ensuring identical transformations.

## Cross-account migration

1. Add a resource-based policy on the target table (see `iam/policies.json` -> `CrossAccountTarget`)
2. Set `TARGET_REGION` and configure the Lambda to assume a role in the target account
3. The S3 export bucket needs a bucket policy allowing the target account to read

## Prerequisites

- Python 3.9+
- boto3
- Source table with PITR enabled
- AWS CLI v2

## Related

- [Zero-Downtime Migration to MRSC](https://quip-amazon.com/4fhmAPBfxor6) -- MRSC-specific walkthrough
- [Bulk Executor](../bulk_executor) -- for simple-path migrations (maintenance window)
- [AWS Glue DynamoDB export blog](https://aws.amazon.com/blogs/database/filter-transform-and-load-your-dynamodb-table-exports-using-aws-glue/)
