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
    perf.py            fetch_perf(job_run_id) → JobRunPerf (real DPUSeconds + JobRunState)
    glue_bucket.py     discover the bootstrap S3 bucket; cleanup.py: orphan sweeper
  connector/           count/find/sql/load smokes against the live DynamoDB DataFrame connector
  commands/            fill/update/delete/copy/diff smokes, each on its own transient table
  security/            real-bootstrap IAM tests + job_state_guard.py (shared-job snapshot/restore)
  results/             generated smoke reports (gitignored)
```

## Running

```sh
make test-e2e-connector   # ~10 min
make test-e2e-commands    # ~15 min
make test-e2e-security    # ~3 min
make test-e2e-cleanup     # sweep orphaned transient tables
```

Requires AWS creds + a one-time `./bulk bootstrap`. First run prompts for account/region/test tables → cached in `.e2e-config` (gitignored). **Never run these in tight loops** — each Glue job costs real money and ~2 min of cold start.

## Non-negotiable invariants

These encode bugs we have actually hit. Do not regress them.

1. **Assert the Glue job state, never just the CLI exit code.** `./bulk` exits **0 even when its Glue job FAILS.** A smoke that only checks `result.succeeded` (exit code) is *false-green* — it passes against crashed jobs. Always go through `assert_glue_succeeded(command, result, region)`, which checks `JobRunState == SUCCEEDED` via `glue.get_job_run`. For write commands, also assert real effects (`assert_table_has_items`, post-delete count == 0, copy target count == source).

2. **Every test owns its data via `transient_table`.** Tests must not depend on pre-existing tables (beyond the read-only `read_table`/`write_table` in config) and must tear down what they create. `transient_table` deletes in a `finally`, so a failing test still cleans up. Tables are named `bulk-e2e-<label>-<8hex>` and tagged `ephemeral=true` / `purpose=bulk_executor e2e command test`.

3. **The security suite mutates the SHARED Glue job — guard it.** `test_real_bootstrap.py` bootstraps/tears-down the real `bulk_dynamodb` job (flips its role to READ-ONLY, or deletes it). The autouse `preserve_shared_glue_job` fixture (`job_state_guard.py`) snapshots the job's role before the suite and restores it after. If you add tests that re-bootstrap, keep them inside that guard, or you will silently break a developer's READ-WRITE job that the connector/command write smokes depend on.

4. **A write command needs a write-capable bootstrap.** `require_write_capable_job` (autouse in `commands/conftest.py`) fails fast with a clear message if the deployed job is on the `DdbReadOnly` role. Don't remove it — without it, write smokes fail deep inside Glue with an opaque `BatchWriteItem` denial.

5. **Transient network/AWS failures are expected; they are not regressions.** A DNS/endpoint blip (`Could not resolve glue.us-east-1...`) surfaces as a test failure, not a skip. Before concluding "the code regressed," check whether other tests in the same run hit endpoint errors, and re-run. Distinguish a *connectivity* failure from a *Glue-job* failure (the latter shows a real `JobRunState=FAILED` + a Spark traceback).

6. **Prefer a throwaway resource over mutating shared infra — but don't let isolation hide a missing-real-resource bug.** When a test must corrupt/mutate state to exercise a code path (e.g. the role-refresh logic in `security/test_real_role_refresh.py`), create a **throwaway** resource (unique-suffix role/table), drive the real code path against it, and delete it in `finally`. That gives zero blast radius — safe under parallel runs and while a live Glue job is running — instead of flipping the shared `bulk_dynamodb` role out from under other work. To stay isolated you sometimes can't shell out to the top-level CLI (`./bulk bootstrap` always repoints the *shared* job regardless of `--XRole`); drive the real class method in-process and stub only the *trigger/source* (e.g. `_get_glue_job_details`'s version), never the behavior under test (the real `update_assume_role_policy`). **The catch:** a throwaway proves only the *logic*, not that the real built-in role exists or is shaped right. Pair it with a read-only existence/shape oracle (`security/test_real_builtin_role.py` + `assert_builtin_role_shape` in `helpers/assertions.py`) and, where a test already creates the real role, assert its shape (invariant #1 again — not just exit 0). Existence, shape, and logic are separate claims; cover each.

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
