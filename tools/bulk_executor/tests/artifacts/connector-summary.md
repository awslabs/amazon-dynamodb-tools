# Glue → DynamoDB Connector Wrapper: e2e Findings

_Branch: `bulk-executor-glue-connector` (PR #162) · Account: `654654401288` · Region: `us-east-1`_

## TL;DR

The new DataFrame-based DynamoDB connector path (issue #145) ships in PR #162 but **was unverified before this work**. Live e2e testing against AWS surfaced two real blockers that prevented the new path from running at all; both are now fixed in `bulk-executor-glue-connector`. **Once unblocked, the new path is dramatically better for the workload that matters:** `bulk find --limit 100` on a 1.7B-item / 506 GB table consumed **24,747 RRU in 76s** on the dataframe path vs **80,840,486 RRU in 743s** on the legacy path — roughly **3,267× cheaper and ~10× faster**, because the new connector pushes `LIMIT` down into DynamoDB instead of double-scanning the table. Pure `count` (no predicate) is ~25% slower / ~25% more DPU on the new path, so legacy stays a sensible default for that one workload — but `find` / `sql` / future predicate-pushable verbs should default to dataframe once it's mature.

## What was tested

| Surface | Verb | Path | Outcome |
|---|---|---|---|
| `tiny-boat` (7.9M items, on-demand) | `count` | legacy | ✅ 7,885,099 in 77s ExecutionTime / 16,940 DPU-s |
| `tiny-boat` | `count` | dataframe | ✅ 7,885,099 in 96s ExecutionTime / 21,120 DPU-s |
| `another-bigger-boat` (1.7B items) | `count` | legacy | ✅ 1,733,919,618 in 365s ExecutionTime |
| `another-bigger-boat` | `count` | dataframe | ❌ Failed in 49s — `[DATA_SOURCE_NOT_FOUND] dynamodb` (root cause below) |

Find / sql / load were not run end-to-end; harness exists but cost / time was held until count was unblocked.

## Bugs surfaced and fixed in this branch

### 1. Bootstrap shipped Glue 4.0; new connector requires Glue 5.0+

`client/src/infrastructure/constants.py` had `GLUE_VERSION = '4.0'`. The DataFrame-based DynamoDB connector AWS shipped in November 2025 is only available on Glue 5.0+. Any developer running `bulk bootstrap` and then trying `--XConnectorVersion=dataframe` would have hit a hard failure they could not have diagnosed without reading AWS release notes.

**Fix:** bumped `GLUE_VERSION` to `'5.0'`.

### 2. Glue jobs missing the required `DYNAMODB`-type connection

Per AWS docs:

> "In order to load in the DataFrame-based connector library, make sure to attach a DynamoDB connection to the Glue job."

The bootstrap created the Glue job with `Connections: null`. Without a connection of `ConnectionType=DYNAMODB` attached, Spark cannot resolve `spark.read.format("dynamodb")` and dies with `[DATA_SOURCE_NOT_FOUND] Failed to find the data source: dynamodb`.

**Fix:** added `_ensure_dynamodb_glue_connection()` to `bootstrap.py`. It creates a marker connection (`bulk-dynamodb-connection`, `ConnectionType=DYNAMODB`, empty `ConnectionProperties`) idempotently — `get_connection` first, only `create_connection` on `EntityNotFoundException`. The connection is then attached via `Connections={'Connections': [name]}` in both `create_job` and `update_job` paths.

The connection carries no credentials; it exists purely as a marker that tells Glue to load the DynamoDB connector library on the Spark executors.

### 3. (Documented, not fixed) Wrapper logs misleading timing on failure

`glue_connector.py` emits `[connector=dataframe] count of 'X' completed in 1.891s` from a `finally` block — when the underlying `df.count()` raises, the time reflects the failure point, not actual work done. During the first canary run this looked like "absurdly fast success" until the Glue console revealed the run had `JobRunState=FAILED`.

**Recommendation:** future PR to gate the log line on success, or distinguish "completed in" vs "failed after". Filed for follow-up; not blocking #162.

## Performance comparison (same workload, same DPU allocation)

`bulk count --table tiny-boat` (7.9M items, on-demand, G.1X × 220 workers, us-east-1):

| Path | Wall (wrapper-logged) | Glue ExecutionTime | DPU-seconds |
|---|---|---|---|
| `legacy` | 42.6s | 77s | **16,940** |
| `dataframe` | 51.1s | 96s | **21,120** |

**Dataframe is ~25% more expensive than legacy for `count` on a small-medium table.** Plausible explanations (none verified):

- Connection-load overhead on the executor classpath
- Different default throttling: legacy auto-set the read rate to the account quota (240,000 RRU); dataframe used the default `dynamodb.throughput.read.ratio=0.5`, which yields a gentler, slower read
- Legacy's `DynamicFrame.count()` is the optimized count-without-toDF() shortcut (issue #81) — DataFrame's `df.count()` may not enjoy the same pushdown

**Issue #145's promise** ("faster, more capable" connector) is not validated for `count` on small tables. Bigger tables, write-heavy workloads, and complex queries are where the new connector's claimed wins live — those remain to be tested.

**Recommendation:** keep `--XConnectorVersion=legacy` as the default until a workload is found where dataframe wins by a margin that justifies the added bootstrap complexity.

## Find on large tables: dataframe path is dramatically better

When the working set exceeds executor memory, Spark's `cache()` falls back to recomputing the source — and on the legacy `DynamicFrame.toDF()` path, "recompute" means a second full DynamoDB scan. The new DataFrame-based connector exposes a real Spark DataFrame whose query plan can pushdown `LIMIT` to DynamoDB directly.

Test: `bulk find --table another-bigger-boat --limit 100` (1.7B items, 506 GB), both connector paths, immediately back-to-back, us-east-1.

| Path | Glue ExecutionTime | RRU consumed | Equivalent scans |
|---|---|---|---|
| **legacy** | **743s (12.4 min)** | **80,840,486** | ~1.29× full scan |
| **dataframe** | **76s (1.3 min)** | **24,747** | ~0.0004× full scan |

The legacy path's RRU curve was bimodal: peak (~10M RRU/min) → valley (~1.9M RRU/min, partial cache hit) → peak (~10M RRU/min) — the partial-cache-then-rescan signature of issue #81 firing on a too-big-to-cache dataset. Total ~1.3× a single scan, not a clean 2×, because some partitions did stay cached.

The dataframe path consumed only **24,747 RRU total** — roughly the cost of reading ~12,000 items from a 1.7B-item table — meaning the LIMIT 100 was pushed through Spark's logical plan into the DynamoDB connector and translated into a bounded read instead of a full table scan.

**Implication:** for `find --limit N`, the dataframe path isn't merely "faster than legacy" — it's a different algorithm. Roughly **3,267× cheaper** in RRU and **~10× faster** in wall time on this workload. The win scales with table size.

The earlier `count` numbers (dataframe ~25% slower / ~25% more DPU on `tiny-boat`) reflect a different story: count without predicates can't pushdown anything, the dataframe path's gentler default `dynamodb.throughput.read.ratio=0.5` means slower reads, and DPU-seconds includes the connection-load overhead. Count and find live in different perf regimes.

## Double-scan check (issue #81)

Issue #81 documented a hazard where `DynamicFrame.toDF().count()` triggered TWO scans against DynamoDB. The wrapper's `count_dynamodb_table` claims to avoid it on the legacy path by calling `dynamic_frame.count()` directly, and the dataframe path uses `df.count()` as a single Spark action. **Verified against CloudWatch `ConsumedReadCapacityUnits` (us-east-1, AWS/DynamoDB namespace, TableName=tiny-boat).**

| Window (UTC) | RRU Sum | Run active |
|---|---|---|
| 03:15-03:17 | 0 | (idle) |
| 03:18 | 46,322 | dataframe count starting |
| 03:19 | 139,336 | dataframe finishing + legacy starting |
| 03:20 | 186,670 | legacy in full swing |
| 03:21+ | 0 | (idle) |
| **Total across both runs** | **372,328 RRU** | |

A single full scan of `tiny-boat` consumes ~187,365 RRU (per bulk's own cost estimator). Two single-scans → ~374K expected. Observed: **372K**. If either path had double-scanned, total would be ~560K (3 scans) or ~748K (4 scans).

**Conclusion: neither path double-scanned.** The wrapper preserves the issue #81 fix on the legacy path and the dataframe path executes its `df.count()` as a single Spark action.

## Files changed in `bulk-executor-glue-connector` to enable e2e

```
client/src/infrastructure/constants.py     +6  -1  (GLUE_VERSION 4.0→5.0; new GLUE_DYNAMODB_CONNECTION_NAME)
client/src/infrastructure/bootstrap.py     +35 -0  (_ensure_dynamodb_glue_connection + Connections wiring in create/update_job)
```

The `glue_connector.py` wrapper itself was not modified — its API call shape was correct from the start. The blocker was the Glue job's classpath, not the wrapper's code.

## What's next

1. Run pytest e2e suite (count + find + sql + load smoke) against `tiny-boat` to exercise read parity beyond `count` and prove the load-write path doesn't crash. Estimated: ~8 Glue jobs, ~10-15 min wall, ~$0.50.
2. Run count parity against `another-bigger-boat` (1.7B items) once to surface big-table behavior delta. Estimated: ~2 jobs, ~$16, ~10 min.
3. Open a follow-up issue tracking the misleading timing log (#3 above) and the issue #145 perf-claim validation.
4. Land #162 (with the bootstrap changes) and the e2e harness in a separate test-only PR.

## ⚠️ Known gap: no e2e security tests

The IAM policies in `README.md` (the bootstrap-user policy and the runtime-user policy) are documentation, not contract. **Nothing today verifies that those policies are correct in either direction.**

Concretely, this PR introduces a new Glue connection that requires three new IAM permissions (`glue:CreateConnection`, `GetConnection`, `DeleteConnection`). The README has been updated to grant these on the resource ARN `arn:aws:glue:*:*:connection/bulk-dynamodb-connection`. But:

- **No test confirms `bulk bootstrap` actually works with exactly the README policy.** If the policy is too narrow (we forgot a permission), users get a confusing mid-bootstrap AccessDenied. If it's too broad (a wildcard slipped in), users grant more than they need.
- **No test confirms the policy *denies* what it should.** A future refactor that accidentally widens the resource scope (`*` instead of the named connection ARN) would not be caught.
- **No test for the upgrade path.** Existing users on the pre-#162 bootstrap policy who upgrade to this code will hit `AccessDenied` at teardown when `_delete_dynamodb_glue_connection` runs without `glue:DeleteConnection`. The error message is currently a generic Glue exception, not a user-friendly "you need to update your IAM policy" hint.
- **No test for the GlueServiceRole.** The role the Glue *job* assumes (created by bootstrap, distinct from the user's bootstrap-time role) is what reads/writes DynamoDB. If we changed those perms, nothing catches it.

These gaps existed before this PR — `bulk_executor` has never had e2e security tests. But the gap is now *more salient* because we added new IAM perms, and it's worth recording explicitly so it's not lost.

**Tracked as follow-up.** A future PR should add `tests/e2e/security/` with:

1. A test that creates a temp IAM user with exactly the README bootstrap policy attached, runs `bulk bootstrap` with those credentials, and asserts success.
2. Tests that remove one permission at a time and assert the bootstrap fails with a *named, actionable* permission error — not a generic Glue exception.
3. Equivalent coverage for the runtime policy and the GlueServiceRole.
4. Cleanup: remove the temp user and any artifacts at suite end.

This protects users from both directions: under-permissive policies (broken bootstrap) and over-permissive policies (security risk via unintended wildcards).
