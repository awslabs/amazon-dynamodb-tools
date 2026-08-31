# Rule: a DynamoDB failure must reach the user as a sentence, not a traceback

**Why this can't be a unit test:** it needs to know which functions run *in a Spark
worker* — which means following `rdd.map` / `rdd.foreach` / `foreachPartition` /
`mapPartitions` closures to their target — and then judging whether the handler
there is **broad enough** for what the body can raise. Both are reading tasks. The
consequence only appears against real AWS (a denied API call), so a unit test can
assert the handler exists but not that it covers the realistic failure.

Distinct from `permission_failure_handling.md`, which decides *whether* a denial
should be fatal. This rule takes that as settled and governs *how* a failure the
verb has decided is fatal reaches the user.

## Invariant 1 — worker-side work records, never escapes

For every function that executes in a worker:

1. **Catch broadly.** `except Exception`, not just `except ClientError`. A narrow
   handler is the most common way this breaks.
2. **Record through `shared/worker_errors.py`** — `record_worker_failure(acc, e,
   context)`, or `record_understood_failure(acc, message)` when the verb has already
   phrased the failure itself. A direct `error_accumulator.add([...])` skips the
   classification below.
3. **Return something the driver can aggregate** (e.g. `0, []`) rather than raising.
4. **The driver must surface it** with `raise_first_worker_error(acc)` after
   `collect()` / `foreachPartition`. Recording without surfacing is worse than not
   recording: the run reports success having done nothing.

## Why it matters, measured

Under a DynamoDB authorization denial (issue #327), the two shapes diverge sharply:

| shape | console output | closing line |
|---|---|---|
| records on an accumulator | **67–80 lines** | `Error in worker 3: User … is not authorized to perform: dynamodb:Scan …` |
| lets the exception escape | **625–939 lines** | a Py4J wrapper, or text cut at `Traceback (most recent call last):` — the denied action is never named |

The mechanism: a recorded failure lets the Spark task **succeed**, so the driver
raises a plain Python exception whose message Glue records verbatim as the job's
`ErrorMessage` — which the client prints as its closing line. An escaping exception
gets retried `spark.task.maxFailures` (4) times, aborts the job, and reaches the
driver wrapped in Py4J, where the cause is buried or truncated away.

## Scope — find the worker entry points live

Do **not** work from a hard-coded list.

- Grep `server/src/python_modules/` for `rdd.map(`, `rdd.foreach(`, `.foreachPartition(`,
  `.mapPartitions(` and resolve each lambda to the function it calls. That function,
  and everything it calls, is worker code.
- Also treat `spark_context.parallelize(...).map(...)` chains as worker entry points.
- For each, locate the `try` and confirm its `except` clauses, then confirm the
  matching driver-side check.

## Classification decides what the user sees

Recording and surfacing is not enough. `shared/worker_errors.py` splits failures in
two, and the split is what the user experiences:

- **understood** — a permission denial, throttling, a validation rejection, a missing
  table (`UNDERSTOOD_ERROR_CODES`, plus AWS's own "is not authorized to perform"
  wording). The user can act on it and there is nothing to debug, so the one sentence
  is the whole report.
- **unexpected** — a bug of ours, or a user-supplied generator or transform doing
  something we cannot anticipate. The driver prints the worker's traceback to the
  **console**, once, from the first recorded failure however many workers hit it.
  Nobody should have to open CloudWatch to find it.

Either way the raise is `BulkExecutorError`, so `root.py` calls `sys.exit(str(e))` and
the job's `ErrorMessage` — the last line the user sees, and what the Glue console shows
as the failure reason — stays to one line. The traceback goes above it, never into it.
Re-raising a plain exception instead gets the traceback *and* a `GlueExceptionAnalysis`
blob wrapped around the message.

Measured on a denied `diff`: **82 lines with a traceback** when the driver re-raised,
**52 lines and no traceback** through the seam. The message text was byte-identical.
This reads as fine in review and passes a unit test asserting on the message, so check
the path, not just the text.

Code we do not own is the case worth looking for: a user's faker generator or transform
should report with its traceback even when the exception type looks recognisable, which
is what `understood=False` is for at the `Transform function raised an exception` site.

A deterministic guard covers the seam
(`tests/server/test_worker_failures_go_through_worker_errors.py`): no direct adds to an
error accumulator, no raising out of one. Worth knowing that the earlier version of that
guard found a seventh site nobody had noticed (`shared/export/pipeline/writer.py`). Do
not treat the guard as replacing this rule — it checks the plumbing, not whether a
handler exists or is broad enough.

## Two traps worth checking explicitly

- **`batch_writer` flushes on `with` exit.** A per-item `try` inside the loop does
  **not** cover writes: `boto3`'s batch writer buffers 25 items and sends them when
  the `with` block exits, so a denied or throttled write raises *outside* the loop.
  A verb with only a per-item handler is still unprotected. This is exactly how
  `find --delete` produced 939 log lines while looking handled.
- **`except ClientError` is too narrow.** A generator returning items that don't
  match the table's key schema raises `KeyError` inside `batch_writer`, which a
  ClientError-only handler misses. That is how `fill` produced 625 lines.

## Invariant 2 — driver-side work fails politely too

A verb whose DynamoDB access happens on the **driver** (`find`, `count`, `sql` read
through `shared/glue_connector`'s `.load()`) has no worker code to check, but it is
not therefore exempt. A denied `.load()` must still reach the user as one sentence.

The channel is `BulkExecutorError`: `root.py` catches it and calls
`sys.exit(str(e))`, Glue records that as the job's `ErrorMessage`, and the client
prints it as its closing line. The PITR guard already rides this path and produces
exactly one clean sentence. Anything else — a bare Spark/Py4J exception, or a plain
`Exception` — leaves the user a traceback prefixed with Glue's error category, even
when the text underneath is perfectly good.

Two things that look like compliance and are not:

- **Cleaning the message without using the channel.** `sql` builds
  `Exception("SQL query error: " + get_error_message(e))`. The message is cleaned;
  the exception type means `root.py` re-raises it anyway.
- **Relying on AWS's message surviving.** It does survive today, which is why this is
  a presentation finding rather than a data-loss one. Judge on how the user receives
  it, not on whether the text exists somewhere in the log.

## Accepted variants

- A **per-item** reporter (`shared/failure_reporter.BoundedFailureReporter`) is fine
  *in addition* to the worker-level handler — it bounds diagnostic logging, it is not
  the safety net. Do not treat its presence as satisfying invariant 1.
- `foreachPartition` has no return value, so an accumulator is the only channel;
  points 1, 2 and 4 still apply.

## Verifying against real AWS

Reading the code cannot tell you what the user sees, and two things about *setting up*
the test have burned time here:

- **A no-access role does not exercise write paths.** With no `Scan` permission, a
  verb that reads before writing (`find --delete`, `copy`) fails on the *read* first
  and never reaches the write. Exercising a denied write needs a role that **can read
  and cannot write** — which is also the realistic case, since that is what
  `--XRole READ-ONLY` produces.
- **Custom IAM roles are the fast lever.** Identity-based policies take effect
  immediately; a DynamoDB resource-based policy took **5-10 minutes** to become
  visible in testing, which makes an iteration loop painful. Note that bootstrap
  leaves a custom role untouched (#330), so what you grant is what the job gets.
- **A denial is only half the test.** It exercises the understood path. To see the
  unexpected path, break something we do not own — a `fill` generator whose items do
  not match the table's key schema, or a transform that raises — and confirm the
  traceback reaches the console *and* the closing line is still one sentence.
- **Two unit-test traps that made green tests meaningless here.** `pytest.ini` sets
  `testpaths = tests/client tests/server`, so a guard dropped in `tests/` is collected
  by nothing — put source guards under `tests/server/`. And `tests/server/conftest.py`
  mocks `python_modules.shared.errors`, so `get_error_message` returns a `Mock`; a test
  asserting on message text has to patch it on `worker_errors`, not on the verb's
  module.

Judge the result on three things, not one: the **line count** (a conforming denial is
tens of lines, not hundreds), whether any of `Traceback`, `py4j`,
`GlueExceptionAnalysis` or `at org.apache.spark` appears at all, and the **closing
line**.

## How to report

For each worker entry point, state one of:

- `conforms — <function>: except Exception -> <accumulator>, driver raises
  BulkExecutorError at <line>`
- a **finding**: name the function, say which point fails (no handler / handler too
  narrow / records but the driver never checks / raises instead of returning), and
  give the shape of the consequence (log volume, closing line).

And for each verb's driver-side DynamoDB access:

- `conforms — raises BulkExecutorError, so the closing line is one sentence`
- a **finding**: name the call, say what the user gets instead (bare Py4J exception,
  or a plain `Exception` that `root.py` re-raises), and quote the closing line.

State what you verified even when clean, so a pass is trustworthy.

**Baseline as of this rule's writing** — re-derive rather than trusting it:

- Conforms: `copy` (`_copy_data`), `update` (`_update_data`), `scancount`
  (`_count_data`), `diff` (`diff_segment`), `fill` (`_fill_data`), `find --delete`
  (`delete_partition`), and `shared/export/pipeline/writer.py`. The last three verbs
  were fixed in the PR that added this rule; the first three were the pattern it
  copies. All seven record through `worker_errors` and surface with
  `raise_first_worker_error` — verified live for `diff`, `fill` and `find --delete`: 41-52 lines, no traceback, closing line naming the principal,
  action and resource.
- **Does not conform to invariant 2, tracked as #332:** `find`, `count` and `sql`.
  A table the role cannot `Scan` gives 314-324 lines closing on
  `Error Category: UNCLASSIFIED_ERROR; Failed Line Number: 1362; An error occurred
  while calling o304.load. User: ... is not authorized ...`. AWS's sentence is in
  there, but it arrives as an unhandled Py4J exception. Compare the worker-side
  verbs, which now close on `Error during delete: User ... is not authorized to
  perform: dynamodb:BatchWriteItem` in 26-82 lines.
- Partly audited: `load`, `load_export`, `revert_export`. Their shared write path
  (`shared/export/pipeline/writer.py`) is now correct, but whether every worker entry
  point in those paths records rather than escapes has not been checked — treat the
  verbs as unknown, not as passing.
- There is deliberately **no** `root.py` catch-all yet (it is option 1 in #332), so
  this rule is the only thing standing between a new verb and a 900-line failure.
