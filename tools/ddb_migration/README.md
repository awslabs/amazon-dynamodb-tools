# DynamoDB Zero-Downtime Migration Toolkit

General-purpose, production-grade tooling for migrating an Amazon DynamoDB
table to a new table without taking writes offline. Built on three native
DynamoDB features — Export to S3, Streams, and conditional writes — and a
single-attribute conflict-resolution scheme.

Use cases:

* Adopting Multi-Region Strong Consistency (MRSC) on Global Tables (requires
  starting from an empty replica).
* Cross-account migrations (workload consolidation, account splits, compliance).
* Schema changes — partition→composite key, attribute renames, new GSI keys.
* Billing-mode transitions on large tables without surprise throttling.
* Single-table consolidation across small tables.

## Architecture

Three overlapping phases:

1. **Capture & bulk copy** — enable Streams on the source, export it to S3,
   start the stream-replay Lambda, run `backfill.py` to load the export into
   the target. Backfill writes go in with `_migration_ts=0`.
2. **Catch-up** — the Lambda replays every live source-table change to the
   target, with `_migration_ts = ApproximateCreationDateTime` (always > 0).
   The conditional expression `attribute_not_exists(#pk) OR #ts < :ts` ensures
   newer timestamps always win, so backfill writes never overwrite live writes
   and stale events never overwrite newer ones.
3. **Convergence & cutover** — `convergence_check.py` blocks until iterator age
   is near zero, the DLQ is empty, and Scan COUNT on both tables agrees. Then
   you flip routing.

REMOVE events are written as `_tombstone=True` items (not deletes) so the
in-flight backfill cannot resurrect them. Post-cutover, `cleanup.py` enables
DynamoDB TTL to expire them automatically.

## Layout

```
tools/ddb_migration/
├── deploy.sh / teardown.sh         One-command provision / clean-up
├── transform.py                    Shared per-item transform (customize here)
├── lambda/stream_replay.py         Streams → target, conditional writes
├── scripts/
│   ├── backfill.py                 S3 export → target, parallel + throttled
│   ├── convergence_check.py        Pre-cutover gate (exit 0 / 1)
│   ├── cleanup.py                  Post-cutover removal of migration metadata
│   └── verify_cutover.py           Sample-based source ↔ target verifier
├── iam/policies.json               Reference IAM policy templates
├── demo/                           One-click demo (real AWS resources)
└── tests/                          pytest unit tests with moto
```

## Prerequisites

* Python 3.10+ and `pip install -r requirements.txt`.
* AWS CLI v2, configured for the source-table account.
* PITR enabled on the source table (for the export). `deploy.sh` does not
  enable this for you on production tables — verify before running.
* Permissions to create IAM roles, Lambda functions, S3 buckets, SQS queues,
  SNS topics, and CloudWatch alarms.

## Quick start (same account)

```sh
cd tools/ddb_migration
make install
source .venv/bin/activate

export SOURCE_TABLE=my-prod-table
export TARGET_TABLE=my-prod-table-v2
export PARTITION_KEY=customer_id
export SORT_KEY=order_id          # optional
export REGION=us-east-1

# 1. Provision Lambda, IAM role, DLQ, SNS topic, alarms, target table.
./deploy.sh

# 2. Trigger an S3 export (the deploy.sh output prints the exact command).

# 3. Run the backfill once the export completes.
EXPORT_BUCKET=ddb-migration-<account>-us-east-1 \
  python scripts/backfill.py

# 4. Wait for stream replay to drain. Run the gate.
DLQ_URL=<from deploy.sh output> python scripts/convergence_check.py

# 5. Sample-verify before flipping app routing.
python scripts/verify_cutover.py --sample-size 1000

# 6. Flip your application's table reference. Resume traffic.

# 7. After 7-14 days of validation:
python scripts/cleanup.py
./teardown.sh CONFIRM=yes        # removes Lambda/role/DLQ/SNS/alarms/bucket
```

## Configuration

`deploy.sh` reads from environment variables or from `./config.env` if present.
A non-exhaustive list:

| Var | Default | Notes |
|-----|---------|-------|
| `SOURCE_TABLE` | required | Existing table |
| `TARGET_TABLE` | required | Created by deploy.sh unless cross-account |
| `PARTITION_KEY` | `pk` | Source-table partition-key attribute name |
| `PARTITION_KEY_TYPE` | `S` | `S`, `N`, or `B` |
| `SORT_KEY` | (unset) | Leave empty for hash-only schema |
| `SORT_KEY_TYPE` | `S` | |
| `REGION` | `us-east-1` | |
| `LAMBDA_FUNCTION_NAME` | `ddb-migration-stream-replay` | |
| `LAMBDA_ROLE_NAME` | `ddb-migration-stream-replay-role` | |
| `DLQ_NAME` | `ddb-migration-dlq` | |
| `SNS_TOPIC_NAME` | `ddb-migration-alerts` | Subscribe an endpoint after deploy |
| `ITERATOR_AGE_WARN_MS` | `43200000` (12 h) | Warning alarm threshold |
| `ITERATOR_AGE_CRIT_MS` | `72000000` (20 h) | Critical alarm threshold |
| `TARGET_ACCOUNT` | (unset) | Set for cross-account; switches mode |
| `TARGET_ROLE_ARN` | (unset) | Required when `TARGET_ACCOUNT` differs |
| `TRANSFORM_MODULE` | (unset) | Custom Python module path; falls back to bundled `transform.py` |

`backfill.py` and `convergence_check.py` accept the same env vars plus their
own CLI flags (`--dry-run`, `--ignore-count-drift`, `--max-iterator-age-ms`,
etc.). Run any of them with `--help` for the full list.

## Customizing the transform

Edit `transform.py` (or set `TRANSFORM_MODULE` to a different module). The
function runs in the Lambda, in `backfill.py`, and in `verify_cutover.py` —
all three must share the same logic, which is why it is one file.

```python
def transform(item, source_event=None):
    # Rename a column.
    if "user_id" in item:
        item["customer_id"] = item.pop("user_id")
    # Compute a new GSI key.
    item["status_idx"] = f"{item['status']}#{item['created_at']}"
    return item   # or return None to skip the item entirely
```

## Cross-account migrations

When the target table lives in a different account:

1. In the **target** account, create a role `ddb-migration-target-writer` that
   trusts the source-account stream-replay role and grants
   `dynamodb:PutItem` + `dynamodb:UpdateItem` on the target table. The
   resource-based policy template is in `iam/policies.json` →
   `CrossAccountTargetTablePolicy`.
2. In the **source** account, set `TARGET_ACCOUNT` and `TARGET_ROLE_ARN`
   before running `deploy.sh`. The Lambda's inline policy will include
   `sts:AssumeRole` for that role.
3. To run `backfill.py` from the target account against the source-account
   export bucket, attach `iam/policies.json:CrossAccountExportBucketPolicy` to
   the bucket.

Test IAM end-to-end before you start the migration. Permission failures
mid-stream waste the 24-hour stream-retention window.

## Convergence gates

`convergence_check.py` runs three checks in sequence:

1. **Iterator age** is below `--max-iterator-age-ms` (default 1000 ms).
2. **DLQ** is empty (visible + in-flight).
3. **Scan COUNT** on source vs. target is within `--count-drift-pct`
   (default 0.5%). The target scan excludes tombstones, so deleted-and-replayed
   items don't inflate the count. Use `--ignore-count-drift` to skip this check
   on very large tables where the Scan would be expensive.

The script exits non-zero on any failure, so you can use it in CI:

```sh
python scripts/convergence_check.py || { echo "not ready"; exit 1; }
```

## Demo

`demo/run_demo.sh` provisions a tiny source/target pair, seeds 10K items,
drives live writes during the migration, and verifies the cutover. End-to-end
runtime ~12 min, cost <$1. See `demo/README.md`.

## Limitations

* Streams retain records for 24 h. If the Lambda falls behind by that long,
  data is lost — alarms fire at 12 h (warning) and 20 h (critical).
* `Scan COUNT` for very large tables is expensive and time-consuming. Allow
  several minutes for tables over 100 GiB.
* Glue-based backfill for tables larger than ~100 GiB is not bundled;
  `backfill.py` parallelizes within one host. For very large tables, fan it
  out across multiple hosts or write a small Glue wrapper.
* Tombstones live for `--tombstone-ttl-days` (default 7) after `cleanup.py`
  runs. They are eventually deleted by DynamoDB TTL.
* Rollback is *not* a flag flip back. Once you cut over, returning to the
  source requires deploying a reverse-replay Lambda before the cutover so the
  source stays current.

## Tests

`make test` runs the suite with moto-backed mocks; no AWS calls. `make
coverage` for a per-line report. The integration path (`demo/run_demo.sh`) is
gated behind `DDB_MIGRATION_DEMO_CONFIRM=yes` and requires real AWS creds.

## References

* [AWS docs — Export to S3](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/S3DataExport.html)
* [AWS docs — DynamoDB Streams](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Streams.html)
* [AWS docs — Conditional writes](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.ConditionExpressions.html)
* [Bulk Executor for DynamoDB](https://github.com/awslabs/amazon-dynamodb-tools/tree/main/tools/bulk_executor) — sibling tool for non-zero-downtime bulk operations.
