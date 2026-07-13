# AGENTS.md — bulk_executor e2e harness

Operating manual for AI agents working in `tests/e2e/`. These tests run **real Glue jobs against real DynamoDB tables in a real AWS account**. They are not unit tests — the usual "mock awsglue/pyspark" rules from the parent `AGENTS.md` do not apply here. Read this before adding or changing anything under `tests/e2e/`.

## What lives here

```
tests/e2e/
  conftest.py          Shared: e2e_config fixture (prompts once, caches to .e2e-config), account guard
  helpers/
    command_runner.py  Shell out to ./bulk; capture stdout/stderr/exit + scrape job_run_id  (run_command / run_command_raw / CommandResult)
    assertions.py      assert_glue_succeeded / assert_table_has_items / require_write_capable_job  ← the truthful-assertion layer
    transient_table.py Context manager: create+PITR a throwaway table, delete in finally
    capacity.py        fetch_consumed_write_capacity → observed WCU/s from CloudWatch (the rate-enforcement oracle)
    perf.py            fetch_perf(job_run_id) → JobRunPerf (real DPUSeconds + JobRunState)
    glue_bucket.py     discover the bootstrap S3 bucket; cleanup.py: orphan sweeper
  connector/           count/find/sql/load SMOKES against the live DynamoDB DataFrame connector
  commands/            fill/update/delete/copy/diff SMOKES, each on its own transient table
  security/            real-bootstrap IAM tests + job_state_guard.py (shared-job snapshot/restore)
  whole_system/        TRUE end-to-end: real datasets through the full pipeline with behavioral assertions
  results/             generated smoke reports (gitignored)
```

## Smoke vs. true e2e — they prove different things

Both hit real AWS, but do not conflate them:

- **Smoke** (`connector/`, `commands/`): early detection. Runs a command with a *small* input and asserts the Glue job reached SUCCEEDED and *something* landed. It proves the wiring doesn't crash and the connector accepts its options. It does **not** prove behavior — a smoke with 10 items passes whether or not a rate limit, ordering guarantee, or type-preservation actually works, because the dataset is too small to exercise the behavior.
- **True e2e** (`whole_system/`): drives a *realistically sized* dataset through the whole pipeline and asserts the **behavior** the feature promises — round-trip fidelity (every item, exact values, back out), an *observed* rate ceiling from CloudWatch, etc. The fixture must be large enough that the behavior is actually exercised, and the test must **guard against a vacuous pass** (see `test_load_rate_roundtrip.py::_assert_fixture_is_a_real_test`: if the load finished before the rate ceiling could bind, fail loudly instead of green).

When you add coverage for a behavioral guarantee (a rate, a limit, an ordering, a type round-trip), a smoke is not enough — add a `whole_system/` test that observes the guarantee, or you are shipping an assertion with no teeth.

## Running

```sh
make test-e2e-connector      # ~10 min
make test-e2e-commands       # ~15 min
make test-e2e-security       # ~3 min
make test-e2e-whole-system   # ~7 min (60k load: round-trip + observed rate enforcement)
make test-e2e-max-rate       # EXPENSIVE, opt-in: prove load beats the old 60k connector ceiling
make test-e2e-cleanup        # sweep orphaned transient tables
```

Requires AWS creds + a one-time `./bulk bootstrap`. First run prompts for account/region/test tables → cached in `.e2e-config` (gitignored). **Never run these in tight loops** — each Glue job costs real money and ~2 min of cold start.

### The `max_rate` tier is expensive and opt-in

`whole_system/test_load_exceeds_legacy_ceiling.py` proves the DataFrame connector sustains a write rate **above the legacy 60k WCU/s ceiling** (the old connector's 40k-on-demand-assumption × 1.5x percent cap). To *observe* >60k it must write **millions of tiny items** (1 WCU each) across thousands of partition keys into a table **pre-warmed** to the target rate — so it costs real money (order of a few dollars of DynamoDB write + Glue DPU per run) and takes several minutes. It is marked `max_rate` and **excluded from `make test-e2e-whole-system`** (`-m "not max_rate"`); run it deliberately via `make test-e2e-max-rate`. Tune cost/volume with `BULK_E2E_MAXRATE_WCU` / `_ITEMS` / `_PARTITIONS`. It guards against a vacuous pass the same way the round-trip test does: if warm throughput never provisioned or the fixture was too small to fill a hot CloudWatch minute, it fails loudly instead of green.

### Deploying the branch-under-test to Glue first

The e2e suites trigger the **deployed** Glue job — whatever code was last uploaded to S3, *not* your working tree. Before running e2e on a branch that changes `server/src/`, you must push that code to the job, or you are testing stale code and calling it a pass:

- Full deploy: `./bulk bootstrap --XRole READ-WRITE` (rebuilds + uploads the server zip; also repoints the shared job's role — see invariant #3).
- Faster iteration: `./bulk <cmd> --XDev` pushes updated script code into the bootstrapped environment without a full bootstrap. Use it when only `server/src/` changed.

### Running suites in parallel

The shared `bulk_dynamodb` Glue job has `MaxConcurrentRuns=20`, so `whole_system/` and the `connector`/`commands` suites can run **concurrently** — each triggers its own job run, and every test scopes its assertions (item counts, CloudWatch capacity) to its **own transient table**, so one suite's writes cannot pollute another's metric. **The exception is `security/`**: it flips the shared job's *role* mid-run (invariant #3), so it must run alone. Never launch `security` alongside any suite that expects a write-capable job.

## Non-negotiable invariants

These encode bugs we have actually hit. Do not regress them.

1. **Assert the Glue job state, never just the CLI exit code.** `./bulk` exits **0 even when its Glue job FAILS.** A smoke that only checks `result.succeeded` (exit code) is *false-green* — it passes against crashed jobs. Always go through `assert_glue_succeeded(command, result, region)`, which checks `JobRunState == SUCCEEDED` via `glue.get_job_run`. For write commands, also assert real effects (`assert_table_has_items`, post-delete count == 0, copy target count == source).

2. **Every test owns its data via `transient_table`.** Tests must not depend on pre-existing tables (beyond the read-only `read_table`/`write_table` in config) and must tear down what they create. `transient_table` deletes in a `finally`, so a failing test still cleans up. Tables are named `bulk-e2e-<label>-<8hex>` and tagged `ephemeral=true` / `purpose=bulk_executor e2e command test`.

3. **The security suite mutates the SHARED Glue job — guard it.** `test_real_bootstrap.py` bootstraps/tears-down the real `bulk_dynamodb` job (flips its role to READ-ONLY, or deletes it). The autouse `preserve_shared_glue_job` fixture (`job_state_guard.py`) snapshots the job's role before the suite and restores it after. If you add tests that re-bootstrap, keep them inside that guard, or you will silently break a developer's READ-WRITE job that the connector/command write smokes depend on.

4. **A write command needs a write-capable bootstrap.** `require_write_capable_job` (autouse in `commands/conftest.py`) fails fast with a clear message if the deployed job is on the `DdbReadOnly` role. Don't remove it — without it, write smokes fail deep inside Glue with an opaque `BatchWriteItem` denial.

5. **Transient network/AWS failures are expected; they are not regressions.** A DNS/endpoint blip (`Could not resolve glue.us-east-1...`) surfaces as a test failure, not a skip. Before concluding "the code regressed," check whether other tests in the same run hit endpoint errors, and re-run. Distinguish a *connectivity* failure from a *Glue-job* failure (the latter shows a real `JobRunState=FAILED` + a Spark traceback).

## Adding a new command smoke

Mirror `commands/test_fill_smoke.py`:

- `with transient_table(region, label="<cmd>") as table:` for the data.
- `run_command("<cmd>", table=table, extra_args=[...])` — or `run_command_raw` for commands whose args aren't `--table <name>` (e.g. `copy` uses `--source`/`--target`).
- `perf = assert_glue_succeeded("<cmd>", result, region)` — never just `assert result.succeeded`.
- Verify a real effect where cheap (item count, emptiness, target==source).
- Append a `PerfRow(command=..., ...)` to the collector for the smoke report.
- Wire a `make test-e2e-<x>` target if it's a new suite, and document it in `tests/e2e/README.md` **and** the README testing table.

## Terminology

The harness uses **command** (matching the rest of the codebase / `HELP.md`), not "verb". The runner is `command_runner.py` with `run_command` / `CommandResult` / `command=`. Don't reintroduce "verb".

## Glue 5.0 connector notes (why the write path is delicate)

The DynamoDB source is the Glue 5.0 DataFrame connector (`spark.read.format("dynamodb")` / `df.write.format("dynamodb")`), wrapped in `server/src/python_modules/shared/glue_connector.py`. Two migration hazards already bit us and have regression guards in `tests/server/test_glue_connector.py`:

- A Glue `DynamicFrame` exposes `write`+`schema` too, so detect it by `hasattr(toDF)`, not by absence of `write`.
- The connector rejects Spark's default `ErrorIfExists` save mode — writes must use `.mode("append")`.

Only `load` writes through this connector path; `fill`/`copy`/`update` write via boto3 `batch_writer`/`update_item`. That asymmetry is why a connector write bug shows up *only* in the `load` smoke.
