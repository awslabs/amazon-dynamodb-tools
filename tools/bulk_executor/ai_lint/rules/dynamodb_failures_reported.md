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
2. **Record on an error accumulator**, with the message built through
   `get_error_message(e)` so AWS's own sentence survives.
3. **Return something the driver can aggregate** (e.g. `0, []`) rather than raising.
4. **The driver must check the accumulator** after `collect()` / `foreachPartition`
   and raise the first error. Recording without checking is worse than not
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

## How to report

For each worker entry point, state one of:

- `conforms — <function>: except Exception -> <accumulator>, driver raises at <line>`
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
  (`delete_partition`). The last three were fixed in the PR that added this rule;
  the first three were the pattern it copies.
- **Does not conform to invariant 2, tracked as #332:** `find`, `count` and `sql`.
  A table the role cannot `Scan` gives 314-324 lines closing on
  `Error Category: UNCLASSIFIED_ERROR; Failed Line Number: 1362; An error occurred
  while calling o304.load. User: ... is not authorized ...`. AWS's sentence is in
  there, but it arrives as an unhandled Py4J exception. Compare the worker-side
  verbs, which now close on `Error during delete: User ... is not authorized to
  perform: dynamodb:BatchWriteItem` in 26-82 lines.
- Not yet audited: `load`, `load_export`, `revert_export`, and
  `shared/export/pipeline`. The export pipeline does use an `error_accumulator` for
  transform and key-resolution failures, but whether every worker entry point in
  those paths is covered has not been checked — treat them as unknown, not as
  passing.
- There is deliberately **no** `root.py` catch-all yet (it is option 1 in #332), so
  this rule is the only thing standing between a new verb and a 900-line failure.
