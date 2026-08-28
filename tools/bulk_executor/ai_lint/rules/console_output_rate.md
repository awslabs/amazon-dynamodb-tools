# Rule: be deliberate about how much output a command produces

**Why this can't be a unit test:** the question is "how much could this print on a
realistic run, who reads it, and was that intended?" It needs a judgment about which
paths are normal and which are failures, and about how a user reacts to each. A test
would have to construct a million-item failure to observe it, and could not tell you
whether the volume was deliberate.

## The policy

Output has two audiences, and they get different budgets.

| | driver output — the user reads it | executor output — only CloudWatch keeps it |
|---|---|---|
| happy path | **under 5 MB** | bounded sample, always |
| sad path | may exceed 5 MB; the user sees the problems and can interrupt | bounded sample, always |
| what carries the totals | a summary line from the driver | an accumulator handed to the driver |

Only the driver reaches the user. The client subscribes to the driver stream *and* every
`<job_run_id>_g-*` executor stream, but `_pretty_print_log_event` discards executor
events outright (`client/src/runner.py`, "Skip task logs"). So an executor `print()` is
never seen by anybody running the command — while still consuming the Live Tail session's
capacity, competing with the driver output the user *does* see (executor events were
69–74% of delivered events in measured runs). It is also unpaced: `root.py` installs the
throttle on the driver only.

### Driver output

Stdout is paced to 100 KB/s (`shared/throttled_output.py`, PR #322), so volume is spent
in **job time** rather than lost data:

| output | time spent printing |
|---|---|
| 2.6 KB (`TOP_N = 10` of ordinary items) | negligible |
| 4 MB (`TOP_N = 10` of 400 KB items) | ~40 s |
| **5 MB (the happy-path budget)** | **~50 s** |
| 100 MB | ~17 min |
| 400 MB | exceeds the default 60-minute Glue timeout — the job **fails** |

Exceeding 5 MB on a successful run is not forbidden; it is something to *know about and
choose*. A verb that prints 20 MB on a normal run has added three minutes to every
invocation, and that should be a decision someone made rather than a surprise. Full
results go to S3 anyway (`large_output_to_s3.md`), so the console is a preview.

**On a sad path the driver may print freely.** If a run is failing wholesale, a flood of
errors is more useful than "first 10 errors" — the user reads a few, hits ^C, and fixes
the cause. So do not flag "one line per failed item" on the driver merely for being
unbounded. Do still flag it for being *wasteful*: interpolating a whole 400 KB item where
the key would do multiplies the flood by 4,000x, and pacing means the user waits for
every byte of it.

The one thing that turns driver output into a real finding is **firing in bulk on a run
that succeeds**. A condition-check failure, a skipped record, a "not found" — these are
often *normal outcomes of what the user asked for*, so they belong inside the 5 MB budget
no matter that the code calls them errors, and the count belongs in a summary line.

### Executor output

Always bounded, because nobody is reading it live and the volume is pure cost:

1. **Cap the count per partition.** A local counter and a first-N sample (10 is a
   reasonable default) is enough to diagnose a systemic failure. Remember the multiplier
   is the partition count, not one: `find` deletes run over 200 partitions and `update`
   over 800 workers, so "first 10" is 2,000 and 8,000 lines respectively.
2. **Cap the line.** Log the key, not the whole item. 10 lines per partition sounds safe
   until each interpolates a 400 KB item: 200 partitions × 10 × 400 KB is **800 MB**. The
   count cap and the size cap are independent, and the size cap usually matters more.
3. **Emit it unconditionally — do not hide it behind `--XDebug`.** Once bounded the cost
   is trivial (200 partitions × 10 × ~100 B ≈ 200 KB), and the point is to diagnose a
   failed run from *its own* logs. Gating it behind a flag means re-running a bulk
   delete or update to find out what went wrong, which is exactly the expensive thing.
4. **Route anything user-facing through the driver.** If a message is phrased for the
   user — a count, a total, "condition expression failed" — the driver must say it.
   Workers report via an accumulator; the driver decides what the user sees.

The codebase already does (4) twice, so a new case has patterns to copy:

- `update/__init__.py` creates `error_accumulator = spark_context.accumulator([],
  ListAccumulator())`, workers `add([...])` to it, and after the `map` the driver prints
  `Processed N records: (X updates, Y non-updates, Z conditions failed)`.
- `shared/export/pipeline/__init__.py` collects worker messages in `debug_accumulator`
  and, in a `finally`, logs them from the driver.

Which exposes a defect a volume check alone would miss: an unbounded executor print is
often **redundant**. `update` already reports its condition-failure count from the driver,
so the per-item worker line spends up to 600 MB restating a number already on screen. Ask
not only "how much?" but "does the driver already say this?"

**Cap what you accumulate, too.** A `ListAccumulator` ships every string back into driver
memory, so accumulate a count plus the first few examples — one entry per failed item
moves the problem instead of fixing it.

## Scope — discover the emitters, don't assume a fixed list

Do **not** work from a hard-coded list; enumerate live so new code is covered.

- Every server-side module under `server/src/python_modules/` (verbs **and** `shared/`
  libraries — a shared helper called per item is the most likely offender).
- Find every `print(...)` and `log.info/warning/error/debug(...)` and ask what bounds how
  many times it can fire. A practical sweep: locate call sites lexically nested inside a
  `for` / `while`, then judge each one. Also check emitters not inside a visible loop but
  called from per-item code (a helper invoked inside `foreachPartition`).
- Decide **driver or worker** for each: follow the call back to a `foreachPartition`,
  `mapPartitions` or `rdd.map` lambda, or to straight-line code in `run(...)`. This
  decides which column of the policy applies.
- Client-side (`client/src/`) is in scope too, though it is rarely per-item.

## Also: never take over flushing

Separate from volume, and the sharpest lesson we have. Do not flush stdout on your own
initiative:

- `print(..., flush=True)` or `sys.stdout.flush()` inside a per-item loop
- `sys.stdout.reconfigure(line_buffering=True)`
- re-wrapping or replacing `sys.stdout` (a second `install()` stacks throttles and halves
  the rate; `sys.__stdout__` or `os.write(1, ...)` skips pacing entirely)

The throttle's first version flushed after every write. Glue's log agent then emitted one
CloudWatch event *per line*: 100,043 events at **710/sec** (822 peak) against Live Tail's
500-events/sec limit, and CloudWatch **discarded 48% of the rows** — while the byte pacing
worked perfectly. Left alone, the same run produced 78 events/sec. Batching is the log
agent's job.

## Background: why volume was ever dangerous

Kept because it explains the budgets, and because the numbers are hard to re-derive.
Measured 2026-08-28, us-east-1:

- Unthrottled, a 100,000-row `find` peaked at 5.3 MB/s ingested and ~2 MB/s delivered, and
  **lost 79% of its rows** with `sampled: false` on all 118 updates.
- An isolated single-stream probe lost **28.8% at 3.9 MB/s**.
- Loss was **intermittent** — an identical repeat delivered everything, so "it worked when
  I tried it" proves nothing.
- Post-throttle, four consecutive runs of the same `find` delivered all 100,000 rows.
- Do not rely on `sessionMetadata.sampled` as a safety net: it was `false` throughout the
  run that lost 79%. A detector on it was built, verified live, and rejected (#316, #315).

## How to report

For each module, state one of:

- `within intent — driver, <happy-path worst case in bytes and seconds at 100 KB/s>`, or
- `sad path, accepted — driver-side, <unlikely failure / user will interrupt>`, or
- `bounded sample — executor, <N per partition × partitions ≈ bytes>`, or
- a **finding**, naming the function and line, saying which rule it trips (fires in bulk
  on a successful run / unbounded executor output / oversized line / user-facing message
  emitted from a worker), the realistic worst case in **lines, bytes and seconds**, and
  the suggested shape.

State what you verified even when clean, so a pass is trustworthy. If a case turns out to
be fine, tighten this file so the next run is sharper.

**Baseline as of this rule's writing** (re-derive rather than trusting this list):

- Within intent: `find` / `sql` / `diff` previews — `TOP_N = 10` plus "…and N more".
  ~2.6 KB for ordinary items; 4 MB and ~40 s for maximum-size items, inside the budget but
  worth knowing. An explicit byte cap is #321.
- Within intent: `scancount`'s per-segment table (bounded by `--splits`),
  `shared/table_info.py` (a handful of lines), the rate-limiter monitors (periodic, not
  per item), `shared/export/writers/batch_writer.py` (`% 1000`, at `debug`).
- Sad path, accepted: the three `shared/export/validators/*` print one line per failed
  file / row / manifest line, then raise. Driver-side, so the user sees them, and a
  wholesale schema or manifest mismatch is the show-and-^C case. Note they are paced, so a
  10M-row invalid manifest would spend ~17 minutes printing before raising.
- **Finding (#319): `update/__init__.py:141`** prints one line per condition-check
  failure. Fires on a *successful* run (a conditional update that matches few items is the
  user getting what they asked for), runs in a worker (`rdd.map` → `_update_data`, 800 of
  them) so nobody sees it, and is **redundant** with the driver's own
  `Z conditions failed`. ~600 MB on a 2M-item table, restating a number already on screen.
- **Finding (#319): `find.py:208`** prints one line per failed delete and interpolates the
  whole item, in a worker over 200 partitions — up to 800 MB that nobody reads, and the
  user is told nothing at all about failed deletes because there is no driver summary.
- Not a verb's problem (#323): 3–8 pairs of rows per 100,000 print on one line, because a
  CloudWatch event boundary can fall between a row and its newline and the client's
  reassembler loses it. Do not flag verbs for it.
