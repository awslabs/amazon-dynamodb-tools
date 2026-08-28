# Rule: console output must be bounded by a constant, never by data size

**Why this can't be a unit test:** the question is "can this code path's output grow
with the number of items, rows, partitions, or failures?" That is a judgment about a
loop's bounds and how often the loop body can fire — not a value to assert on. A test
would have to construct a million-item failure to observe it.

**Why it matters:** the console is delivered by CloudWatch Live Tail, which **drops
data silently** above a surprisingly low volume. It does not error, does not retry,
and does not always set its own `sampled` flag. The job still reports success, so a
truncated answer is indistinguishable from a complete one. This rule exists so no
new command can walk into that.

## The measured thresholds (as of 2026-08-28, us-east-1)

Numbers below are measured, not from documentation, except where noted. Keep them
here so future changes can be judged against real data rather than intuition.

| Fact | Measurement |
|---|---|
| Glue chunks driver output into fixed-size CloudWatch events | **exactly 32 KB** per event (median = max = 32768 over 495 events) |
| Documented sampling threshold | >500 events matched in one second → CloudWatch delivers **500** and discards the rest, reporting `LiveTailSessionMetadata.sampled = true` |
| What that threshold means for us | 500 × 32 KB ≈ **16 MB/s** — effectively unreachable, so the documented, *flagged* limit is not our real constraint |
| **Unflagged silent loss** on a real `find` printing 100,000 rows | 5.3 MB/s peak ingested, ~2 MB/s delivered, **79% of rows never delivered**, `sampled: false` on **all 118** updates |
| Isolated probe, single log stream | **28.8% loss at 3.9 MB/s** in one trial; 0–3.9% loss at 1.0–3.9 MB/s in another |
| Intermittency | An identical repeat of the lossy `find` delivered all 100,000 rows. Loss is **probabilistic**, so "it worked when I tried it" proves nothing |
| Shipped scale, never observed to lose anything | `TOP_N = 10` ≈ **1 event, ~2.6 KB total** |

**Budget to hold to:** total console output per run in the **kilobytes**, and
instantaneous rate below roughly **100 KB/s** — about 10× under the ~1 MB/s where
loss has been observed. Loss has never been seen anywhere near this level. If a
change would put a command into the MB/s range, it is a violation regardless of how
it tests on the day.

Two things that follow and are easy to get wrong:

- **Do not rely on `sessionMetadata.sampled` as a safety net.** It was `false` on
  every update of a run that lost 79% of its output. A detector on it was built,
  verified to work, and then rejected as useless (PR #316, issue #315) precisely
  because our loss does not set it.
- **Executor-side output counts too.** The client subscribes by log-stream *prefix*,
  which matches the driver stream **and** every `<job_run_id>_g-*` executor stream
  (~220 by default). Those events are discarded client-side, but they are matched by
  the session first — 69–74% of delivered events in measured runs were executor
  noise. So a `print()` inside a `foreachPartition` / `mapPartitions` body is not
  "free just because we never show it": it competes with the output the user wants.

## Scope — discover the emitters, don't assume a fixed list

Do **not** work from a hard-coded list; enumerate live so new code is covered.

- Every server-side module under `server/src/python_modules/` (verbs **and**
  `shared/` libraries — a shared helper called per item is the most likely offender).
- Find every `print(...)` and `log.info/warning/error/debug(...)` and ask what bounds
  how many times it can fire. A practical sweep: locate call sites lexically nested
  inside a `for` / `while`, then judge each one's bound. Also check emitters not
  inside a visible loop but called from per-item code (a helper invoked inside
  `foreachPartition`).
- Client-side (`client/src/`) is in scope for the same reason, though it is rarely
  per-item.

## The invariant

Every console emitter must be bounded by a **constant**, not by the size of the data.
Acceptable bounds:

1. **Fixed cap** — a top-N preview (`TOP_N = 10`), a "first N errors then a count"
   pattern, or an explicit slice.
2. **Sampled by a counter** — e.g. `if local_count % 1000 == 0:`, so output grows
   logarithmically-ish rather than linearly. (`shared/export/writers/batch_writer.py`
   is the reference: per-operation progress gated on `% 1000`, at `debug`.)
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
- **Is the full result available elsewhere?** Large output belongs in S3; see
  `large_output_to_s3.md` for that convention (this rule is about *rate and total
  volume of anything printed*, including diagnostics that never go to S3).

## How to report

For each module, state one of:

- `bounded — <the bound>` (naming the constant, the modulo, or the config knob), or
- a **finding**: name the function and line, state what makes it unbounded, give the
  realistic worst case in lines and approximate bytes, and suggest the bounded form
  (usually: count them, print the first few, print the total).

State what you verified even when clean, so a pass is trustworthy. Do not flag the
accepted patterns above; if a case turns out to be fine, tighten this file so the
next run is sharper.

**Baseline as of this rule's writing** (re-derive rather than trusting this list —
it is a starting point, not the answer):

- Bounded and fine: `find` / `sql` / `diff` result previews (`TOP_N = 10` plus an
  "…and N more" line), `scancount`'s per-segment table (bounded by `--splits`),
  `shared/table_info.py` (per scalable dimension, a handful), the rate-limiter
  monitors (periodic loops with a sleep, not per item),
  `shared/export/writers/batch_writer.py` (`% 1000`, at `debug`).
- Known unbounded, filed as #319: `find.py`'s delete path prints one line per failed
  item and interpolates the whole item; `update/__init__.py` prints one line per item
  whose condition check fails; the three `shared/export/validators/*` print one line
  per failed file/row/manifest line.
