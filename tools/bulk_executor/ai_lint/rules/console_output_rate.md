# Rule: be deliberate about how much output a command sends

**Why this can't be a unit test:** the question is "how much could this print on a
realistic run, and was that intended?" It needs a judgment about which paths are normal
and which are failures, and how a user reacts to each. A test would have to construct a
million-item failure to observe it, and could not tell you whether the volume was
deliberate.

## The design intent

**A command should not send more than ~5 MB to the client on a successful run.**

Stdout is paced to 100 KB/s (`shared/throttled_output.py`, PR #322), so output volume
is now spent in **job time** rather than lost data:

| output | time spent printing |
|---|---|
| 2.6 KB (`TOP_N = 10` of ordinary items) | negligible |
| 4 MB (`TOP_N = 10` of 400 KB items) | ~40 s |
| **5 MB (the budget)** | **~50 s** |
| 100 MB | ~17 min |
| 400 MB | exceeds the default 60-minute Glue timeout — the job now **fails** |

Exceeding the budget is not forbidden; it is something to *know about and choose*. A
verb that prints 20 MB on a normal run has added three minutes to every invocation, and
that should be a decision someone made, not a surprise. Full results go to S3 anyway
(`large_output_to_s3.md`), so the console is a preview.

## Sad paths may be unbounded

An error path that prints one line per failed item is **acceptable** when either:

- **It is an unlikely failure.** A wrong key schema or a missing permission is not the
  normal case, and when it happens the user wants to see the problems.
- **The user will interrupt.** If a run is failing wholesale, a flood of errors at
  100 KB/s is a reasonable thing to show — the user reads a few, hits ^C, and fixes the
  cause. Truncating to "first 10 errors" would be less useful, not more.

So do **not** flag "one line per failed item" on its own. Flag it when one of the two
conditions below applies.

### 1. It can fire on a successful run

This is the important test: **can this emitter fire in bulk on a run that succeeds?**
If yes it is happy-path output and belongs inside the 5 MB budget, no matter that the
code calls it an error. A condition-check failure, a skipped record, a "not found" —
these are often *normal outcomes of what the user asked for*, and the count belongs in a
summary line rather than one line per item.

### 2. Nobody can see it

Output only reaches the user if it comes from the **driver**. The client subscribes to
the driver stream and every `<job_run_id>_g-*` executor stream, but
`_pretty_print_log_event` discards executor events outright (`client/src/runner.py`,
"Skip task logs"). So a `print()` inside `foreachPartition` / `mapPartitions` / `rdd.map`:

- is **never shown to the user**, so the "they will see it and interrupt" argument does
  not apply to it;
- is **not paced**, because `root.py` installs the throttle on the driver only;
- still consumes the Live Tail session's capacity, competing with the driver output the
  user *does* see (executor events were 69–74% of delivered events in measured runs).

An unbounded worker-side emitter is therefore pure cost. The fix is not a cap: it is to
count in an accumulator (most of these already have one) and print the total from the
driver, keeping a handful of examples if they help diagnosis.

## Scope — discover the emitters, don't assume a fixed list

Do **not** work from a hard-coded list; enumerate live so new code is covered.

- Every server-side module under `server/src/python_modules/` (verbs **and** `shared/`
  libraries — a shared helper called per item is the most likely offender).
- Find every `print(...)` and `log.info/warning/error/debug(...)` and ask what bounds
  how many times it can fire. A practical sweep: locate call sites lexically nested
  inside a `for` / `while`, then judge each one. Also check emitters not inside a visible
  loop but called from per-item code (a helper invoked inside `foreachPartition`).
- Determine **driver or worker** for each: follow the call back to a `foreachPartition`,
  `mapPartitions` or `rdd.map` lambda, or to straight-line code in `run(...)`.
- Client-side (`client/src/`) is in scope too, though it is rarely per-item.

## Also: never take over flushing

Separate from volume, and the sharpest lesson we have. Do not flush stdout on your own
initiative:

- `print(..., flush=True)` or `sys.stdout.flush()` inside a per-item loop
- `sys.stdout.reconfigure(line_buffering=True)`
- re-wrapping or replacing `sys.stdout` (a second `install()` stacks throttles and
  halves the rate; `sys.__stdout__` or `os.write(1, ...)` skips pacing entirely)

The throttle's first version flushed after every write. Glue's log agent then emitted one
CloudWatch event *per line*: 100,043 events at **710/sec** (822 peak) against Live Tail's
500-events/sec limit, and CloudWatch **discarded 48% of the rows** — while the byte
pacing worked perfectly. Left alone, the same run produced 78 events/sec. Batching is the
log agent's job.

## Background: why volume was ever dangerous

Kept because it explains the budget, and because the numbers are hard to re-derive.
Measured 2026-08-28, us-east-1:

- Unthrottled, a 100,000-row `find` peaked at 5.3 MB/s ingested and ~2 MB/s delivered,
  and **lost 79% of its rows** with `sampled: false` on all 118 updates.
- An isolated single-stream probe lost **28.8% at 3.9 MB/s**.
- Loss was **intermittent** — an identical repeat delivered everything, so "it worked
  when I tried it" proves nothing.
- Post-throttle, four consecutive runs of the same `find` delivered all 100,000 rows.
- Do not rely on `sessionMetadata.sampled` as a safety net: it was `false` throughout the
  run that lost 79%. A detector on it was built, verified live, and rejected (#316,
  #315).

## How to report

For each module, state one of:

- `within intent — <happy-path worst case in bytes, and seconds at 100 KB/s>`, or
- `sad path, accepted — <why: unlikely failure / user will interrupt>, driver-side`, or
- a **finding**, naming the function and line, saying which of the two conditions it
  trips (fires on a successful run / invisible in a worker), the realistic worst case in
  **lines, bytes and seconds**, and the suggested shape (usually: accumulate the count,
  print a total from the driver, keep a few examples).

State what you verified even when clean, so a pass is trustworthy. If a case turns out
to be fine, tighten this file so the next run is sharper.

**Baseline as of this rule's writing** (re-derive rather than trusting this list):

- Within intent: `find` / `sql` / `diff` previews — `TOP_N = 10` plus "…and N more".
  ~2.6 KB for ordinary items; 4 MB and ~40 s for maximum-size items, which is inside the
  budget but worth knowing. Whether to add an explicit byte cap is #321.
- Within intent: `scancount`'s per-segment table (bounded by `--splits`),
  `shared/table_info.py` (a handful of lines), the rate-limiter monitors (periodic, not
  per item), `shared/export/writers/batch_writer.py` (`% 1000`, at `debug`).
- Sad path, accepted: the three `shared/export/validators/*` print one line per failed
  file / row / manifest line, then raise. Driver-side, so the user sees them; a wholesale
  schema or manifest mismatch is exactly the "show the problems and let them ^C" case.
- **Finding (#319): `update/__init__.py` prints one line per item whose condition check
  fails.** This fires on a *successful* run — a conditional update that does not match
  most items is the user getting what they asked for — and it runs in a worker
  (`rdd.map` → `_update_data`), so it is unpaced and the user never sees it. Each line
  interpolates the whole `update_kwargs`. A 2M-item table where the condition mostly
  fails is ~600 MB written to nowhere. `failed_accumulator` already holds the count.
- **Finding (#319): `find.py`'s delete path prints one line per failed item and
  interpolates the whole item.** A sad path, so the volume itself is acceptable in
  principle — but it runs inside `foreachPartition`, so the user never sees any of it
  while it crowds out the driver output they do see. Summarize from the driver instead.
