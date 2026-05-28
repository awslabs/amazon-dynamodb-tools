# Spec: bulk_executor e2e test harness

> Status: harness built, single-path connector smoke suite implemented in `tests/e2e/connector/`.
> This spec captures the original design (which was a two-path parity comparison between the legacy DynamicFrame connector and the new DataFrame connector). After verification revealed the new connector should be the only path, the legacy connector was removed, and the suite was simplified to a single-path **connector smoke** suite. The README in `tests/e2e/README.md` documents the current shape; this spec is preserved for design context.

## Why this exists

Unit tests prove the code is internally consistent. They prove nothing about whether `glue_connector.py` produces the same results as the inline code on `main`, whether the DataFrame path actually works (eca5cac admits it was never run end-to-end), or whether the new path is faster/cheaper as issue #145 claims. We need real Glue jobs hitting real DynamoDB tables to answer those questions.

## Scope

The harness is intentionally extensible. The first suite covers connector parity. Future suites might cover:

- `bulk fill` correctness across generators
- `bulk diff` cross-region / cross-account
- `bulk load-export` against a real DynamoDB export

Each suite is independent and invoked via its own `make test-e2e-<name>` target. `make test-e2e` (no suffix) prints a list of available suites and how to run each.

## Constraints

1. **Mac-only.** Always run from the developer's Mac. No CI integration. Devdesktop is unstable; pipeline is not set up.
2. **Operator-monitored.** This is final verification, not background CI. The developer watches it.
3. **No defaults that bind to one developer.** The original eca5cac was verified against `LatencyTestTable` in account `654654401288` — that's Colin's chaos, not the next developer's reality. Prompt for everything on first run, save per-developer config.
4. **No source modifications during e2e dev.** Same rule that applied to the unit-test push: tests-only work in this PR.
5. **Run before merging the source PR (#162).** This is the gate that says "the refactor doesn't change behavior."

## Make targets

```
make test-e2e              # Lists available e2e suites + invocation
make test-e2e-connector    # Connector parity suite (count/find/sql/load)
make test-e2e-<future>     # New suites added here as the project grows
```

`make test` (no `-e2e`) never invokes e2e tests. The unit suite stays fast and offline.

## First-run config flow

On first invocation of any e2e suite, the harness prompts:

```
=== bulk_executor e2e: connector parity ===

This suite runs real Glue jobs in your AWS account.
Cost: a few dollars per run. Wall time: ~10-15 min.

AWS account ID: _____
AWS region: _____
DynamoDB read-only test table: _____
DynamoDB writable test table (for load-smoke step): _____
Confirm you have run 'bulk bootstrap' on this account+region [y/N]: _____

Saved to tests/e2e/.e2e-config (gitignored).
Delete that file to be re-prompted.
```

**Why every prompt is required:**

- **Account ID** — guards against accidentally running against the wrong account. The harness cross-checks `aws sts get-caller-identity` and refuses if it doesn't match.
- **Region** — same reason; tables are region-scoped.
- **Read-only table** — `count`, `find`, `sql` exercise the read paths. Reuse the same table across runs.
- **Writable table** — `load` writes data. Must be distinct from the read-only table to keep the read suite hermetic.
- **Bootstrap confirmation** — assumes source code changed since the last bootstrap, so the developer must explicitly confirm they re-ran `bulk bootstrap` on this account+region. Test fails fast if the Glue job isn't current.

`.e2e-config` is gitignored. Delete to re-prompt.

## Layout

```
tests/e2e/
├── conftest.py              # First-run interactive prompts; AWS guard
├── README.md                # How to run, how to interpret, how to clean up
├── connector/
│   ├── test_count_parity.py
│   ├── test_find_parity.py
│   ├── test_sql_parity.py
│   ├── test_load_smoke.py
│   └── fixtures/
│       └── load_smoke.csv   # 10 rows, unique PK prefix per run
├── helpers/
│   ├── verb_runner.py       # Shells out to ./bulk, captures stdout, returns structured result
│   ├── perf.py              # CloudWatch '[connector=…] took Xs' scrape + glue.get_job_run DPU-seconds
│   └── config.py            # Load/save .e2e-config; AWS account guard
└── .e2e-config              # gitignored; per-developer
```

## Suite #1: connector parity

For each verb in {`count`, `find`, `sql`, `load`}, run the wrapper through both `--XConnectorVersion=legacy` and `--XConnectorVersion=dataframe` and answer two questions: are the results equivalent, and what was the perf delta?

| Verb | Approach |
|---|---|
| `count` | Run on read table, both paths. Assert `count_legacy == count_dataframe`. Capture perf. |
| `find` | Run with `--limit 100`, both paths. Sort items by primary key, assert deep equal. Capture perf. |
| `sql` | Run `SELECT * FROM t LIMIT 100`, both paths. Sort, deep equal. Capture perf. |
| `load` | One-shot smoke per path. Load `fixtures/load_smoke.csv` into the writable table. Assert exit code 0. **No parity check, no item-level assertions** — the goal is "the wrapper's write_dynamodb_dataframe path doesn't crash." Capture perf. Cleanup: scope-delete by PK prefix at end. |

**Why `load` is smoke-only:** Colin's call. Connector performance can be verified with the read verbs; we don't need to load gigabytes to prove the write wrapper works. Any write-correctness regression would surface in the read suite anyway (you'd see different items between paths).

## Performance capture

For every job run, capture two numbers:

- **Wall-time:** scrape the `[connector=<version>] ... took <X>s` log line that `glue_connector.py` already emits to CloudWatch.
- **DPU-seconds:** call `glue.get_job_run(JobName=…, RunId=…)` after the run completes. The response includes `ExecutionTime` and `MaxCapacity`; DPU-seconds = `ExecutionTime * MaxCapacity`.

Print every run, even on success. The whole point is comparing paths.

## Output format

```
============== Connector Parity Report ==============
verb     legacy_wall  legacy_dpu_s  df_wall  df_dpu_s  delta
count    32.1s        12.8          24.5s    9.6       df 23% faster, 25% cheaper
find     45.3s        18.1          39.8s    14.5      df 12% faster, 20% cheaper
sql      28.7s        11.2          27.1s    10.8      df  6% faster,  4% cheaper
load     22.5s        n/a           24.1s    n/a       smoke only (no parity)
=====================================================
```

The harness writes this to stdout AND appends to `tests/e2e/results/<timestamp>.md` so devs can compare runs over time. Results dir is gitignored.

## Cleanup

The `load` smoke writes items with a unique partition-key prefix per run (e.g. `e2e-load-smoke-<uuid>`). At suite end, the harness scope-deletes all items matching that prefix. If the test crashes before cleanup, an orphan-cleaner CLI exists: `make test-e2e-cleanup` walks the writable table and removes any items with PKs matching `e2e-load-smoke-*`.

## Failure modes

When a Glue job fails, the test fails loudly and prints:

- The CloudWatch log group / stream URL
- The Glue job run ID
- The last 50 lines of the job's CloudWatch log

We do NOT silently skip on AWS hiccups. A throttled `count` looks identical to a real regression to the wrapper logic; the developer needs to read the log to decide. False-failing on a transient hiccup is acceptable; false-passing is not.

## Open questions for build time

- **Parallel vs sequential job runs:** running both paths sequentially is simpler and the default; running in parallel cuts wall-time roughly in half but doubles concurrent DPU usage. Default sequential, expose `--parallel` flag if desired.
- **Result snapshotting:** should the harness compare against a snapshot from a previous run? Probably not — the read table can change between runs and snapshots become stale. Compare-within-run is the contract.
- **Quotas:** verify the Glue job-run concurrency quota in the test account before running parallel mode.

## Done definition

The harness is "done" when:

1. A new developer can clone the repo, `cd tools/bulk_executor`, run `make test-e2e-connector`, answer the prompts, and get a Connector Parity Report or a clear failure with log URLs.
2. The harness refuses to run if `.e2e-config`'s account ID doesn't match `aws sts get-caller-identity`.
3. The output report is reproducible: same table, same code → similar wall-time / DPU-seconds across runs (within the noise floor of Glue cold-start variance).
4. Issue #145's claim about the dataframe path being faster is either confirmed with numbers or refuted with numbers.
