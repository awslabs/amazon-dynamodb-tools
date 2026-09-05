# Agents Guide — `tools/ddb_migration`

Conventions for future contributors (human or AI) extending this toolkit.

## Module boundaries

- **`transform.py`** — single source of truth for any per-item transform. Both `lambda/stream_replay.py` and `scripts/backfill.py` import it. Keep it free of AWS SDK calls.
- **`lambda/stream_replay.py`** — runs in Lambda. No filesystem writes, no `print()` for structured data (use the `_log()` helper). Every record write must be conditional on `_migration_ts`.
- **`scripts/backfill.py`** — runs locally or in EC2/CodeBuild. Same conditional-write contract as the Lambda. Always sets `_migration_ts = 0`.
- **`scripts/convergence_check.py`** — gate (exits 0 / 1). Do not add interactive prompts.
- **`scripts/cleanup.py`** — idempotent post-cutover. Safe to re-run.
- **`scripts/verify_cutover.py`** — read-only. Never writes to source or target.

## Conditional-write contract (load-bearing)

Every write to the target table goes through:

```python
table.put_item(
    Item=item,
    ConditionExpression='attribute_not_exists(#pk) OR #ts < :ts',
    ExpressionAttributeNames={'#pk': partition_key, '#ts': '_migration_ts'},
    ExpressionAttributeValues={':ts': migration_ts},
)
```

`migration_ts` rules:
- backfill writes: `0`
- stream replay `INSERT`/`MODIFY`: `event['dynamodb']['ApproximateCreationDateTime']`
- stream replay `REMOVE` (tombstone): `event['dynamodb']['ApproximateCreationDateTime']`

Do not bypass this. Do not introduce a third writer with a different timestamp source.

## Adding a new script

1. Add `scripts/your_script.py` with a `main()` that returns an `int` exit code.
2. Add `tests/test_your_script.py` using moto fixtures from `conftest.py`.
3. Document in `README.md` under "Scripts."
4. If it provisions AWS resources, hook it into `deploy.sh` AND `teardown.sh`.

## Testing

- Unit tests use `moto`. No real AWS calls.
- DynamoDB Streams events are constructed as fixtures (moto's stream support is partial); we mock the Lambda handler's boto3 client when needed.
- `make test` must stay green before any commit.
