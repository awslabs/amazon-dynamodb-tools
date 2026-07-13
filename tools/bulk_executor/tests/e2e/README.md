# bulk_executor end-to-end test suite

These tests run **real Glue jobs against real DynamoDB tables** in your AWS
account. They are opt-in. `make test` will never invoke them.

## When to run

- Before merging a source-side PR that touches the Glue connector wrapper, the
  command dispatchers, or anything that affects how `bulk` produces Spark jobs.
- When verifying that a new bootstrap of `bulk` deploys correctly to a new
  account.
- When investigating a suspected regression in connector behavior.

## Cost

Each `make test-e2e-connector` run launches one Glue job per command (4 total).
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

| Command | Coverage                                                              |
|---------|-----------------------------------------------------------------------|
| `count` | Run on read table; assert returned count is non-negative.             |
| `find`  | Run with `--limit 100`; assert at least one item came back inline.    |
| `sql`   | Run `SELECT * LIMIT 100`; assert at least one row came back inline.   |
| `load`  | Load a 10-row CSV into the writable table; assert exit 0; cleanup.    |

Each run captures wall-time (from the `[connector] took Xs` log line) and
DPU-seconds (from `glue.get_job_run`). A Connector Smoke Report appears at the
end of the run and also lands in
`tests/e2e/results/connector-smoke-<timestamp>.md`.

### Command-orchestration coverage

The connector suite exercises the wrapper's read/write/count primitives. The
command suite (`make test-e2e-commands`) covers the command-specific Spark
orchestration layered on top — particularly important after the Glue 4.0→5.0
jump (PR #162):

- `fill` — pure-write via generators (write_dynamodb_dataframe path)
- `update` — read + transformed-write via generators
- `delete` — read + filter + scoped delete via where-predicate
- `copy` — read-A-write-B in one Spark job (same-region)
- `diff` — segmented scans + join across two tables

Each command smoke creates its own transient table, asserts exit 0, and tears
down on exit. See `tests/e2e/commands/README.md` and `specs/e2e-commands.md`.

Still uncovered (followup PRs): `load-export` (needs an export S3 prefix),
cross-region/cross-account `copy`/`diff`, and `scancount` (bypasses the
connector by design).

### Security / bootstrap coverage (`make test-e2e-security`)

The security suite validates the IAM story around `bulk bootstrap`. Its tests
form deliberate **layers** — each proves something the others can't, so read
why all four exist before collapsing them:

| Test | Proves | Touches shared state? |
|------|--------|-----------------------|
| `test_simulator.py` | The documented README policy allows every bootstrap action, and removing any statement denies at least one (via `iam:SimulateCustomPolicy`). Tier-1 oracle, no resources created. | No |
| `test_real_bootstrap.py` | The documented policy *actually* bootstraps a real account (temp IAM user, real `bulk bootstrap`), **and the built-in role is created with the right shape** (not just exit 0 — see invariant #1). Random-negative rotation removes one action per run and asserts bootstrap fails. | Yes — bootstraps/tears-down the shared `bulk_dynamodb` job; guarded by `preserve_shared_glue_job`. |
| `test_real_builtin_role.py` | The **real** `AWSGlueServiceRoleBulkDynamoDB-*` role exists *right now* with the fresh-bootstrap trust policy + required managed policies. Pure read. | No (read-only) |
| `test_real_role_refresh.py` | The version-mismatch **role-refresh logic** converges a stale trust policy to the fresh-bootstrap shape, against real IAM. | No — runs on a **throwaway** role it creates and deletes. |

**Why the split (the key tradeoff):** the refresh test uses a *throwaway* role
so it has zero blast radius (safe under parallel runs and during a live Glue
job — it never mutates the shared role). But a throwaway role proves only the
*logic*; it says nothing about whether the *real* built-in role exists or is
correctly shaped. `test_real_builtin_role.py` (read-only) and the role-creation
assertion inside `test_real_bootstrap.py` close that gap. Existence + shape +
refresh-logic are three separate claims, so they are three separate checks.

The shared assertion `assert_builtin_role_shape(region, access)` lives in
`helpers/assertions.py` and intentionally hardcodes the expected role name /
policies rather than importing them from `client/src` — so if bootstrap's own
constants drift, the test still checks the contract we expect and the mismatch
surfaces as a failure.

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
