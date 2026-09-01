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
| records, then surfaces through the seam | **37–52 lines** | `SystemExit: Error in worker 2: User … is not authorized to perform: dynamodb:Scan on resource: …` |
| lets the exception escape | **597–668 lines** | `Exception: Error in parallel execution: … PythonRDD.collectAndServe … org.apache.spark.SparkException` — the denied action is never named |

Both columns are live measurements of the same commands, the second taken on `main`
before this was fixed (`fill` 625, `diff` 668, `update` 650 lines).

The mechanism: a recorded failure lets the Spark task **succeed**, so the driver
decides what the user sees. An escaping exception instead gets retried
`spark.task.maxFailures` (4) times, aborts the job, and reaches the driver wrapped in
Py4J, where the cause is buried behind `collectAndServe`.

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

- **understood** — *the verb expected it and said so*, or the failure is environmental.
  Only `ENVIRONMENT_ERROR_CODES` (denials, expired credentials, throttling that outlived
  the SDK's retries) plus AWS's own "is not authorized to perform" wording are classified
  without a verb's involvement, because those are the operator's to fix whichever verb hit
  them. **Everything else defaults to unexpected**, including `ValidationException`,
  `ResourceNotFoundException` and `ConditionalCheckFailedException`: whether one of those
  is understood is a property of the verb, not the code. A `ValidationException` is a
  phrasable mistake in `fill` (the generator's items don't fit the schema) and a bug in
  code that built its own request. A verb that expected it says so —
  `record_understood_failure`, `understood=True`, or raising `BulkExecutorError` where it
  recognises the condition. This is the check to make when reading a handler: **does the
  verb actually anticipate what it is quietly calling understood?**

  **Look hardest at verbs that take user input into a worker.** `scancount`'s
  `--filter-expression` is only checkable by DynamoDB, in the worker, so the rejection is
  a typo the verb has to expect: it catches `ValidationException` around the scan and
  names the parameter. Narrowing the shared code list without that branch would have
  handed the user a traceback for their own typo — caught before merge, but only by
  asking which verbs pass user input past the client.

  The clearest form is a verb detecting the mistake itself: `fill` checks each
  generated item against the table's key names *before* writing, so the common mistake
  reads `Generated item is missing the table's key attribute(s) ['id']; the item has
  ['payload', 'pknum']` instead of boto3's bare `KeyError: 'id'` out of `batch_writer`'s
  de-duplication. The user can act on it and there is nothing to debug, so the one
  sentence is the whole report. **Pre-empting a predictable mistake this way is better
  than classifying it after the fact** — look for the opportunity when a verb runs user
  data through a library that will fail cryptically.
- **unexpected** — a bug of ours, or a user-supplied generator or transform doing
  something we cannot anticipate. The driver prints the worker's traceback to the
  **console**, once, from the first recorded failure however many workers hit it.
  Nobody should have to open CloudWatch to find it.

The exception type carries the classification, and only understood failures are
`BulkExecutorError`: `root.py` turns that into `sys.exit(str(e))`, so the user gets the
sentence and never learns an exception was involved. An unexpected failure stays an
ordinary exception, because that is what it is — but the driver prints the worker's
traceback *first*, because the frames that name the bug are the worker's and Glue's own
traceback shows only our plumbing. `client/src/runner.py` then suppresses Glue's
exception-analysis blob, keyed on either marker (`BulkExecutorError`, or
`worker_errors.UNEXPECTED_FAILURE_BANNER` — a guard test checks the two trees agree).

Either way the job fails and the failure reason is one line describing the problem. The
traceback goes above that line, never into it.

Do not "fix" a noisy failure by wrapping it in `BulkExecutorError`. That was tried in
the PR that wrote this rule: it removed the traceback from denials (right) and from
genuine bugs (wrong), and it made the type stop meaning anything. Measured on a denied
`diff`: **82 lines with a traceback** re-raising, **52 lines and none** as a
`BulkExecutorError`. The message text was byte-identical, so review and a
message-asserting unit test both miss it — check the path, not just the text.

Code we do not own is the case worth looking for: a user's faker generator or transform
should report with its traceback even when the exception type looks recognisable, which
is what `understood=False` is for at the `Transform function raised an exception` site.

A deterministic guard covers the seam
(`tests/server/test_worker_failures_go_through_worker_errors.py`): no direct adds to an
error accumulator, no raising out of one. Worth knowing that the earlier version of that
guard found a seventh site nobody had noticed (`shared/export/pipeline/writer.py`). Do
not treat the guard as replacing this rule — it checks the plumbing, not whether a
handler exists or is broad enough.

## Traps worth checking explicitly

- **`batch_writer` flushes on `with` exit.** A per-item `try` inside the loop does
  **not** cover writes: `boto3`'s batch writer buffers 25 items and sends them when
  the `with` block exits, so a denied or throttled write raises *outside* the loop.
  A verb with only a per-item handler is still unprotected. This is exactly how
  `find --delete` produced 939 log lines while looking handled.
- **`except ClientError` is too narrow.** A generator returning items that don't
  match the table's key schema raises `KeyError` inside `batch_writer`, which a
  ClientError-only handler misses. That is how `fill` produced 625 lines.
- **`exit()` inside a worker does not do what it looks like.** `SystemExit` is a
  `BaseException`, so the worker's `except Exception` never sees it: the task fails, gets
  four Spark retries, aborts the job, and arrives as a Py4J wrapper. `update` had two of
  these (throttling, `ValidationException`) and they survived the round of fixes that
  caught every other verb, because the code *looks* like it reports politely. **Grep for
  `exit(` in worker code every time you run this rule.** Both now raise
  `BulkExecutorError`, which the worker handler does catch and record.
- **The `except Exception: raise Exception(f"Error in parallel execution: …")` wrapper
  around `collect()`** (in `copy`, `diff`, `fill`, `scancount`, `update`) is not a
  violation. It catches what genuinely escaped — Spark infrastructure failures — and a
  plain exception is the right shape for those. Do not "fix" it into a
  `BulkExecutorError`.

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

The classification from invariant 1 applies here too: a *understood* driver-side failure
(a denial, a validation rejection) belongs in `BulkExecutorError`, an *unexpected* one
keeps its type. The driver already has the frames, so there is nothing to capture and
re-print — but note that an unhandled driver-side exception still drags in Glue's
exception-analysis blob, because the client only suppresses that after seeing
`BulkExecutorError` or the worker-failure banner. Concrete case to check while fixing
#332: a `fill` generator that raises on *every* call dies inside
`check_generator_output_avg_size`'s 10-call size peek, on the driver, unhandled — 73
lines with a Glue blob, closing on `UNCLASSIFIED_ERROR … RuntimeError: faker did
something silly`. The message is fine; the packaging is not.

## Accepted variants

- A **per-item** reporter (`shared/failure_reporter.BoundedFailureReporter`) is fine
  *in addition* to the worker-level handler — it bounds diagnostic logging, it is not
  the safety net. Do not treat its presence as satisfying invariant 1.
- `foreachPartition` has no return value, so an accumulator is the only channel;
  points 1, 2 and 4 still apply.
- **A failure the user asked for is not a failure.** `update` treats
  `ConditionalCheckFailedException` as a normal outcome: it increments
  `failed_accumulator`, logs only the key (bounded per worker), and the driver folds the
  total into the *success* line — `Processed N records: (X updates, Y non-updates, Z
  conditions failed)`, exit 0. Do not flag that as an unreported failure; do check that
  the count reaches the user, since silently dropping it would be the real bug.

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
  unexpected path, break something we do not own — a generator or transform that
  *raises*. Note that a generated item merely missing the table's key attributes is
  now understood and reported as a sentence, so it no longer reaches this path; and a
  generator that raises on *every* call dies on the driver instead, inside `fill`'s
  10-call size peek, which is a different (unhandled) shape. A generator that raises
  after ~20 calls, run with `--numitems 30`, exercises the worker path.
- **Two unit-test traps that made green tests meaningless here.** `pytest.ini` sets
  `testpaths = tests/client tests/server`, so a guard dropped in `tests/` is collected
  by nothing — put source guards under `tests/server/`. And `tests/server/conftest.py`
  mocks `python_modules.shared.errors`, so `get_error_message` returns a `Mock`; a test
  asserting on message text has to patch it on `worker_errors`, not on the verb's
  module.

Judge the result on three things, not one: the **line count** (a conforming denial is
tens of lines, not hundreds), whether `Traceback`, `py4j`, `GlueExceptionAnalysis` or
`at org.apache.spark` appears, and the **closing line**.

Two exceptions to the middle test, or you will chase ghosts:

- **The unexpected path is supposed to print a traceback** — one, from the worker,
  introduced by `worker_errors.UNEXPECTED_FAILURE_BANNER`. Count them: more than one
  traceback, or a Glue exception-analysis blob alongside it, is the finding.
- **Glue emits Java stacks of its own on runs that succeeded.** A successful `copy`
  logged 25 lines of `ScheduledReporter … AWSDILyraMetricsReporter#report` /
  `java.util.ConcurrentModificationException` from `aws-glue-di-package.jar` (issue
  #334). Check the frames' origin before attributing a stack to us.

## How to report

For each worker entry point, state one of:

- `conforms — <function>: except Exception -> record_worker_failure(<accumulator>);
  surfaced by raise_first_worker_error in <the driver function that collects it>`

Name functions, not line numbers — anything you cite here is read months later, against
code that has moved.
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
  (`delete_partition`), and `shared/export/pipeline/writer.py`. All seven record through
  `worker_errors` and surface with `raise_first_worker_error`.

  Verified live 2026-08-31 across six regions, `--XNumberOfWorkers 2`, denials from a
  read-only custom role — every line count below is total console output, and every one
  of these runs had **zero** traceback/Py4J/Glue-blob lines:

  | run | lines | closing line |
  |---|---|---|
  | `scancount` read denied | 37 | `SystemExit: Error in worker 2: … dynamodb:Scan …` |
  | `diff` read denied | 52 | `SystemExit: Error in worker 2: … dynamodb:Scan …` |
  | `fill` write denied | 41 | `SystemExit: Error during writing: … dynamodb:BatchWriteItem …` |
  | `delete` write denied | 46 | `SystemExit: Error during delete: … dynamodb:BatchWriteItem …` |
  | `update` write denied | 39 | `SystemExit: Error in worker 268: … dynamodb:UpdateItem …` |
  | `copy` write denied | 50 | `SystemExit: Error in worker 134: … dynamodb:BatchWriteItem …` |
  | `fill` generated item missing the key | 41 | `SystemExit: Error in worker: Generated item is missing the table's key attribute(s) ['id'] …` |
  | `update` ValidationException | 39 | `SystemExit: Error in worker 268: Validation exception (usually caused by the generator …)` |
  | `fill` generator raises (unexpected) | 48 | 4-frame worker traceback, then `Exception: Error in worker: RuntimeError: faker did something silly` |

  Outputs are kept in `~/Documents/bulk-331-runs/` with `before/` counterparts on `main`
  (597-668 lines each).
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
