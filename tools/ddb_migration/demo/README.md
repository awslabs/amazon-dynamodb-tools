# `tools/ddb_migration/demo` — One-click migration demo

Provisions a small source + target pair, seeds 10,000 items, drives live writes
during the migration, runs the convergence gate, and verifies the cutover.

End-to-end: ~12–15 minutes. Cost: <$1 in a US region.

## Prerequisites

* An AWS account with admin-equivalent permissions (the demo creates IAM roles,
  Lambda, DynamoDB tables, S3 bucket, SNS topic, SQS queue, CloudWatch alarms).
* `aws` CLI v2 configured (`AWS_PROFILE` set).
* Python 3.10+ with the parent tool's dependencies installed:
  ```sh
  cd tools/ddb_migration
  make install
  source .venv/bin/activate
  ```

## Run it

```sh
cp demo/config.example.env demo/config.env
# edit demo/config.env: set AWS_PROFILE and REGION
DDB_MIGRATION_DEMO_CONFIRM=yes ./demo/run_demo.sh
```

## What you'll see

```
========== 1/9  Creating source table ddb-migration-demo-source ==========
========== 2/9  Provisioning migration infrastructure (deploy.sh) ==========
[deploy] Ensuring target table ddb-migration-demo-target exists in us-east-1
...
========== 9/9  Verifying sample of items ==========
  matched=500  missing=0  diverged=0  total=500
VERIFY OK

DEMO PASSED
```

## Clean up

```sh
CONFIRM=yes ./teardown.sh
aws dynamodb delete-table --table-name ddb-migration-demo-source --region us-east-1
aws dynamodb delete-table --table-name ddb-migration-demo-target --region us-east-1
```

## What it actually demonstrates

1. Source table receives live writes throughout the migration window.
2. The S3 export captures the source at a point in time.
3. `backfill.py` loads the export into the target with `_migration_ts=0`.
4. The Lambda is replaying live writes (with newer `_migration_ts`) in parallel.
5. Conflict resolution: backfill writes never overwrite live updates.
6. Tombstones prevent the backfill from resurrecting deleted items.
7. The convergence gate blocks cutover until iterator age, DLQ, and counts agree.
8. The verifier samples 500 items and proves source ↔ target parity.
