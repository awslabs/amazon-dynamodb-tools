# Spec: bulk-command e2e smokes

> Status: implemented in `tests/e2e/commands/`. This spec captures the
> design rationale; the README in `tests/e2e/commands/README.md` documents
> the current shape.

## Why this exists

The connector smoke suite (`tests/e2e/connector/`) verifies the wrapper's
read/write/count primitives against the new DataFrame-based DynamoDB
connector. What it does **not** verify is the command-specific Spark
orchestration that sits on top of the wrapper — `fill` invoking
generators, `delete` filtering scope, `copy` reading-A-writing-B in one
job, `diff` segmenting scans across two tables, `update`'s
read-transform-write loop. Each is a non-trivial pipeline that the
wrapper alone can't catch a regression in.

The suite exists primarily to catch crashes and behavioral changes from
the Glue 4.0 → 5.0 jump introduced in PR #162. We're not chasing
correctness assertions here — the unit suite covers correctness; we're
checking "does this still execute end-to-end without the underlying Spark
runtime breaking us."

## Scope

Five commands covered today, all on a single account, all on
freshly-created transient tables:

| Command | Setup | Coverage |
|---------|---|---|
| `fill`   | empty transient table | wrapper write path with generator-driven items |
| `update` | seeded transient table | read + transform + write via generator |
| `delete` | seeded transient table | read + filter + scoped delete via where-predicate |
| `copy`   | two transient tables, source seeded | read-A-write-B in one Spark job |
| `diff`   | two transient tables, both seeded | segmented scans + join across two tables |

Each test asserts exit 0 (the contract: command didn't crash on the new
runtime) and captures wall-time + DPU-seconds for the perf report.
Correctness-of-output is intentionally not asserted — those are unit-test
concerns.

## Out of scope (followup PRs)

| Command | Why deferred |
|---------|---|
| `load-export`   | Requires a pre-existing DynamoDB export S3 prefix. Either supply via config or trigger inside the test (~10-15 min). Either way, separable from the fast-loop suite. |
| `copy --source <ARN>` (cross-region or cross-account) | Needs IAM role wiring on both accounts plus pre-created target tables in the other region/account. Distinct setup story from the on-account smokes. |
| `diff` cross-region | Same as above. |
| `scancount` | Bypasses the connector by design (direct boto3 scan). Doesn't exercise the path PR #162 changed; lower priority. |

These get their own make targets when implemented:
`make test-e2e-commands-export`, `make test-e2e-commands-multi-account`.
The fast suite (`make test-e2e-commands`) stays scoped to setup-light
single-account verification.

## Constraints

1. **Each test creates its own transient table.** No reliance on
   pre-existing tables in the developer's account beyond what
   `bulk bootstrap` creates. Cleanup is automatic via context manager.
2. **PAY_PER_REQUEST + PITR.** PAY_PER_REQUEST keeps idle cost at zero;
   PITR is required for `bulk` to perform mutations.
3. **Smokes, not correctness tests.** Exit 0 + clean teardown is the
   contract. No item-count assertions, no field-level checks.
4. **Reuses harness components.** `command_runner.py`, `perf.py`,
   `aws_guard.py`, the `e2e_config` fixture all come from
   `tests/e2e/helpers/` and the shared conftest. Only `transient_table.py`
   is new.

## Make targets

```
make test-e2e-commands              # All five command smokes (~10-15 min)
```

Future:
```
make test-e2e-commands-export       # load-export with pre-built or auto-triggered export
make test-e2e-commands-multi-account # copy/diff across accounts/regions
```

`make test` (no `-e2e`) never invokes any of these.

## Layout

```
tests/e2e/
├── commands/
│   ├── conftest.py             # cmd_perf_collector fixture + report renderer
│   ├── report.py               # Command Smoke Report (same shape as connector report)
│   ├── test_fill_smoke.py
│   ├── test_update_smoke.py
│   ├── test_delete_smoke.py
│   ├── test_copy_smoke.py
│   └── test_diff_smoke.py
└── helpers/
    └── transient_table.py      # NEW: context manager for temp PAY_PER_REQUEST + PITR table
```

## Performance capture

Same shape as connector smoke: scrape `[connector] took Xs` from stdout
for wall-time, call `glue.get_job_run` for DPU-seconds, append to a
shared collector, render a Command Smoke Report at suite end and persist to
`tests/e2e/results/command-smoke-<timestamp>.md`.

## Failure modes

When a command fails, the test fails with the relevant Glue stderr in the
assertion. Console links for deeper digging are documented in
`tests/e2e/README.md`. The transient table is destroyed regardless of
test outcome, so a failed test never leaves orphan tables.

The one orphan risk: if the test harness itself dies between table
create and the cleanup `finally` (e.g. `kill -9` mid-test), the table
remains. They're tagged `purpose=bulk_executor e2e command test` and
`ephemeral=true` so they're easy to find: `aws dynamodb list-tables` then
`describe-table` for the tags. A cleanup helper for these would be a
straightforward followup.

## Done definition

The suite is "done" when:

1. Five smokes run green against a freshly-bootstrapped account.
2. Suite teardown leaves zero ephemeral tables.
3. The Glue 4.0→5.0 jump from PR #162 is verified against more than the
   four primitives (count/find/sql/load) the connector smoke covers.

## Naming: "command" throughout (resolved)

Earlier harness drafts (from PR #162) used "verb" for what the rest of
the bulk_executor codebase consistently calls a "command" (see `HELP.md`,
README, `client/src/python_modules/`). That inconsistency has been
resolved — the harness and both suites now use "command" end-to-end:

- `tests/e2e/helpers/command_runner.py` (was `verb_runner.py`)
- `run_command` / `run_command_raw` (was `run_verb` / `run_verb_raw`)
- `CommandResult` (was `VerbResult`)
- `command=` kwarg (was `verb=`)
- `PerfRow.command` field, used by both connector + command report renderers
- all callers in `tests/e2e/connector/test_*.py` and `tests/e2e/commands/test_*.py`
