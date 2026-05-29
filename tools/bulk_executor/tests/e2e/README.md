# bulk_executor end-to-end test suite

These tests run **real Glue jobs against real DynamoDB tables** in your AWS
account. They are opt-in. `make test` will never invoke them.

## When to run

- Before merging a source-side PR that touches the Glue connector wrapper, the
  verb dispatchers, or anything that affects how `bulk` produces Spark jobs.
- When verifying that a new bootstrap of `bulk` deploys correctly to a new
  account.
- When investigating a suspected regression in connector behavior.

## Cost

Each `make test-e2e-connector` run launches one Glue job per verb (4 total).
On the smallest Glue capacity, that's a few dollars and ~5-10 minutes of wall
time. **Don't run it in tight loops.**

## First run

```sh
cd tools/bulk_executor
make install                  # if you haven't already
./bulk bootstrap              # if your account doesn't have the Glue job yet
make test-e2e-connector       # answers your config prompts on first run
```

The first invocation prompts for:

- AWS account ID
- AWS region
- DynamoDB read-only test table (used by `count`, `find`, `sql`)
- DynamoDB writable test table (used by the `load` smoke step)
- Confirmation that you've run `bulk bootstrap` on this account+region

Answers persist to `tests/e2e/.e2e-config` (gitignored, per-developer).
Delete that file to be re-prompted.

## What gets verified

| Verb    | Coverage                                                              |
|---------|-----------------------------------------------------------------------|
| `count` | Run on read table; assert returned count is non-negative.             |
| `find`  | Run with `--limit 100`; assert at least one item came back inline.    |
| `sql`   | Run `SELECT * LIMIT 100`; assert at least one row came back inline.   |
| `load`  | Load a 10-row CSV into the writable table; assert exit 0; cleanup.    |

Each run captures wall-time (from the `[connector] took Xs` log line) and
DPU-seconds (from `glue.get_job_run`). A Connector Smoke Report appears at the
end of the run and also lands in
`tests/e2e/results/connector-smoke-<timestamp>.md`.

### Coverage gaps (followup PR)

The connector wrapper is exercised end-to-end on every code path it exposes
(read, write, count). What's **not** covered yet is verb-specific
orchestration on top of the wrapper:

- `delete` — read + scoped DDB writes (delete-by-key, delete-by-where, delete-by-orderby+limit)
- `copy` — cross-region/cross-account read+write
- `fill` — pure-write via generators (write_dynamodb_dataframe path)
- `update` — read + transformed-write via generators
- `load-export` — DynamoDB-export S3 prefix parsing + write
- `diff` — segmented reads across two tables

Each of these is a *combination* of the three primitives we already smoke-test,
so the wrapper's correctness against the new DataFrame source is verified
transitively. The gap is regression coverage for the verb-specific Spark
orchestration, particularly important after the Glue 4.0→5.0 jump (PR #162).
Tracked as a followup suite (`make test-e2e-verbs`) in a separate PR.

## Cleanup

The load smoke step writes 10 items per run with a unique partition-key
prefix (`e2e-load-smoke-<run_id>`) and deletes them at the end of the test.
If a test crashes before cleanup runs, sweep orphans manually:

```sh
make test-e2e-cleanup
```

This scans the writable table for `begins_with(pk, 'e2e-load-smoke-')` items
and deletes them. Safe to run any time.

## Failure modes

When a Glue job fails, the test fails with the relevant Glue stderr in the
assertion. AWS console links for deeper digging:

- Glue console → Jobs → bulk_dynamodb → Runs (find the run by ID)
- CloudWatch Logs → `/aws-glue/jobs/output` (and `/error`)

Transient AWS hiccups will surface as failures, not skips. If you suspect a
flake, re-run.
