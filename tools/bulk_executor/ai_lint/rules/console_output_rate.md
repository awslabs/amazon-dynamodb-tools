# Rule: console output must be bounded by a constant, never by data size

**Why this can't be a unit test:** the question is "can this code path's output grow
with the number of items, rows, partitions, or failures?" That is a judgment about a
loop's bounds and how often the loop body can fire — not a value to assert on. A test
would have to construct a million-item failure to observe it.

**Why it matters:** the console is delivered by CloudWatch Live Tail, which **drops
data silently** above a surprisingly low volume — no error, no retry, and not always
its own `sampled` flag. The job still reports success, so a truncated answer is
indistinguishable from a complete one.

## What the platform already does for you, and what it does not

`server/src/python_modules/shared/throttled_output.py` (installed once in `root.py`,
PR #322) wraps `sys.stdout` and paces it to **100 KB/s**: every write passes straight
through, then sleeps if that write put the run over budget. Verified on real AWS —
four consecutive runs of a 100,000-row `find` (13.2 MB) delivered **every row**, where
the same command unthrottled gave 9,264 / 20,562 / all / all.

So a verb no longer has to police its own *rate*. Three things the throttle does not
cover, which is what this rule is for:

1. **Total volume.** Throttling converts a data-loss problem into a **wall-clock**
   problem. At 100 KB/s, 100 MB of output takes ~17 minutes; 400 MB exceeds the
   default 60-minute Glue timeout, so an unbounded emitter now *fails the job* instead
   of quietly truncating. Different symptom, still a bug.
2. **Executor-side output.** `root.py` runs on the **driver**, so only driver stdout is
   paced. A `print()` inside a `foreachPartition` / `mapPartitions` body runs in a
   worker process that never installed the throttle, goes to a
   `<job_run_id>_g-*` stream, and is unpaced. Those events are matched by the client's
   prefix subscription (they were 69–74% of delivered events in measured runs) even
   though the client discards them, so they compete with the output the user wants.
3. **Anything that bypasses `sys.stdout`.** See the flushing invariant below.

## The measured thresholds (as of 2026-08-28, us-east-1)

Measured, not documented, except where noted. Keep them here so future changes are
judged against real data rather than intuition.

| Fact | Measurement |
|---|---|
| Live Tail has **two** limits | bytes: silent unflagged loss from ~1 MB/s upward. events: >500 matched in one second → 500 delivered, rest discarded, `sampled = true` (documented) |
| Unpaced driver output | events of **exactly 32 KB** (median = max = 32768 over 495 events), so the 500-events/sec limit works out to ~16 MB/s and is effectively unreachable |
| **Unflagged silent loss**, unthrottled `find` of 100,000 rows | 5.3 MB/s peak ingested, ~2 MB/s delivered, **79% of rows never delivered**, `sampled: false` on **all 118** updates |
| Isolated probe, single log stream | **28.8% loss at 3.9 MB/s** in one trial; 0–3.9% at 1.0–3.9 MB/s in another |
| Intermittency | An identical repeat of the lossy run delivered everything. Loss is **probabilistic**, so "it worked when I tried it" proves nothing |
| **Flushing per line** (a version of the throttle that did this) | one CloudWatch event *per line*: 100,043 events at **710/sec** (822 peak), above the 500/sec threshold for 134 of 141 seconds, **48% of rows discarded** |
| Paced, not flushing (what ships) | 10,119 events (~1.1 KB each), **78 events/sec**, 68 KB/s, all 100,000 rows delivered in 4 of 4 runs |
| Shipped preview scale | `TOP_N = 10` of ordinary items ≈ **~2.6 KB total**, one event |

**Budget to hold to:** total console output per run in the **kilobytes**. The rate is
the throttle's job now; the total is yours. A command whose output can reach megabytes
is a finding even though the throttle will deliver it, because the delivery is bought
with job runtime.

Two traps that are easy to walk back into:

- **Do not rely on `sessionMetadata.sampled` as a safety net.** It was `false` on every
  update of a run that lost 79% of its output. A detector on it was built, verified
  against live CloudWatch, and then rejected as useless (PR #316, issue #315), because
  our loss does not set it.
- **Do not take over flushing.** This is the sharpest lesson available: the throttle's
  first version flushed after every write, Glue's log agent then emitted one event per
  line, and CloudWatch discarded 48% of the output *while the byte pacing worked
  perfectly*. Batching is the log agent's job. Concretely, treat as findings:
  `print(..., flush=True)` or `sys.stdout.flush()` inside a per-item loop,
  `sys.stdout.reconfigure(line_buffering=True)`, and any code that re-wraps or replaces
  `sys.stdout` (a second `install()` would stack throttles and halve the rate; writing
  to `sys.__stdout__` or `os.write(1, ...)` skips pacing entirely).

## Scope — discover the emitters, don't assume a fixed list

Do **not** work from a hard-coded list; enumerate live so new code is covered.

- Every server-side module under `server/src/python_modules/` (verbs **and**
  `shared/` libraries — a shared helper called per item is the most likely offender).
- Find every `print(...)` and `log.info/warning/error/debug(...)` and ask what bounds
  how many times it can fire. A practical sweep: locate call sites lexically nested
  inside a `for` / `while`, then judge each one's bound. Also check emitters not inside
  a visible loop but called from per-item code (a helper invoked inside
  `foreachPartition`).
- Note for each finding whether it runs on the **driver** (paced) or in a **worker**
  (unpaced) — same defect, worse consequences in a worker.
- Client-side (`client/src/`) is in scope for the same reason, though it is rarely
  per-item.

## The invariant

Every console emitter must be bounded by a **constant number of bytes**, not by the
size of the data.

**A count cap is not a byte cap.** `TOP_N = 10` is bounded in *items*, but a DynamoDB
item may be up to 400 KB, so a 10-item preview can be **4 MB**. Since #322 that no
longer risks corruption — the throttle spreads it over ~40 seconds and it arrives
intact — but it is still 4 MB of console output and ~40 seconds of job time. So when
judging a preview, multiply by the **maximum** row size, not the typical one, and say
what the worst case costs in bytes and seconds. Whether the preview should also carry
an explicit byte cap is #321, still open.

Acceptable bounds:

1. **Fixed cap** — a top-N preview, a "first N errors then a count" pattern, or an
   explicit slice. For verbs returning whole DynamoDB items, price it at the 400 KB
   maximum per item unless the code truncates each line.
2. **Sampled by a counter** — e.g. `if local_count % 1000 == 0:`, so output grows far
   slower than the data. (`shared/export/writers/batch_writer.py` is the reference:
   per-operation progress gated on `% 1000`, at `debug`.)
3. **Bounded by configuration, not data** — e.g. one line per Spark segment where
   segments come from `--splits`. Acceptable, but note it if the knob has no upper
   bound.
4. **Once per run** — summaries, totals, table info, an S3 pointer.

A violation is any emitter whose count scales with items / rows / matched records /
failures. **Error and warning paths are in scope and are the most common offenders**:
"one line per failed item" looks harmless because failures are assumed rare, but the
realistic failure shape is systemic — wrong key schema, missing permission, wholesale
throttling — where *every* item fails and the loop prints the whole table.

Per-line size matters as much as line count: a line that interpolates a whole item
(`f"Error ...: {item}"`) multiplies the problem by the item size.

## What to check, per emitter

- **What bounds the iteration?** A constant, a config value, or the data? Name it.
- **Can the body fire on every element?** For an error branch, assume the systemic
  case (all elements fail), not the optimistic one.
- **How big is one line?** Flag interpolation of full items/rows/expressions.
- **Driver or worker?** Worker-side emitters are unpaced.
- **Does anything flush, or write around `sys.stdout`?** See the flushing trap above.
- **Is the full result available elsewhere?** Large output belongs in S3; see
  `large_output_to_s3.md` for that convention (this rule is about *total volume of
  anything printed*, including diagnostics that never go to S3).

## How to report

For each module, state one of:

- `bounded — <the bound>` (naming the constant, the modulo, or the config knob), or
- a **finding**: name the function and line, state what makes it unbounded, give the
  realistic worst case in **lines, bytes, and — since #322 — seconds of job time at
  100 KB/s**, and suggest the bounded form (usually: count them, print the first few,
  print the total).

State what you verified even when clean, so a pass is trustworthy. Do not flag the
accepted patterns above; if a case turns out to be fine, tighten this file so the next
run is sharper.

**Baseline as of this rule's writing** (re-derive rather than trusting this list — it
is a starting point, not the answer):

- Bounded in item count, priced in bytes: `find` / `sql` / `diff` previews
  (`TOP_N = 10` plus an "…and N more" line). ~2.6 KB for ordinary items; up to 4 MB
  and ~40s of pacing for maximum-size items. Delivered intact since #322; an explicit
  byte cap is #321.
- Bounded and fine: `scancount`'s per-segment table (bounded by `--splits`),
  `shared/table_info.py` (per scalable dimension, a handful), the rate-limiter monitors
  (periodic loops with a sleep, not per item),
  `shared/export/writers/batch_writer.py` (`% 1000`, at `debug`).
- Known unbounded, filed as #319: `find.py`'s delete path prints one line per failed
  item **and interpolates the whole item** — and it runs in a worker, so it is unpaced;
  `update/__init__.py` prints one line per item whose condition check fails, which
  happens on a perfectly normal run; the three `shared/export/validators/*` print one
  line per failed file/row/manifest line. Worth re-pricing these now that #322 exists:
  a 2M-item systemic failure at ~200 bytes a line is ~400 MB, which at 100 KB/s would
  blow the default 60-minute Glue timeout rather than truncate.
- Known cosmetic artifact, filed as #323: 3–8 pairs of rows per 100,000 print on one
  line, because a CloudWatch event boundary can fall between a row and its newline and
  the client's reassembler loses it. No data lost, and **not** something a verb can fix
  by changing how it prints — do not flag verbs for it.
