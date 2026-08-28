# Rule: commands with potentially large output must always write to S3 and print only a bounded preview

**Why this can't be a unit test:** "does this verb's console output grow without
bound as the result set grows?" is a judgment about how a verb emits results, not
a value you can assert on. It spans a Spark write, a console print loop, and a
pointer message. Easy to see by reading; very hard to test mechanically.

The motivation (issue #86): console delivery goes through CloudWatch Live Tail,
which **silently drops** data above a low volume, so large output to the console is
lossy and unreliable. The fix is a convention: persist everything to S3, show only a
small preview on the console, and point at S3 for the rest.

The specific numbers are measured in `console_output_rate.md` — read them there
rather than restating them here. In short: unflagged silent loss has been measured from
~1 MB/s upward (79% of rows lost in one real run, with `sampled` reporting `false`
throughout). An earlier version of this rule said "~500 records/sec, each record
chopped to ~1KB" — both wrong, and wrong in the reassuring direction.

Since #322, stdout is paced to 100 KB/s (`shared/throttled_output.py`), so a bounded
preview is no longer what stands between the user and a corrupted answer. This
convention still holds for two reasons: the console is a *preview* and S3 is the
deliverable, and pacing buys delivery with job runtime, so a megabyte-scale preview now
costs minutes instead of losing data. Judge previews on volume and time, not on
corruption risk.

## Scope — discover the verbs, don't assume a fixed list

Do **not** work from a hard-coded list. Enumerate the verbs live so a newly added
command is automatically covered:

- Server-side verbs live under `server/src/python_modules/` — each is a
  `<verb>.py` module or a `<verb>/__init__.py` package with a `run(...)` entry
  point. (`shared/` is a library, not a verb.)
- Cross-check the client-side verb list (`client/src/python_modules/`,
  `client/src/runner.py`) so you don't miss one that exists on only one side.

For each verb, first decide whether it produces **unbounded per-item / per-row
output** — output whose size scales with the number of matched, changed, or
returned items. Only those verbs are in scope.

- Out of scope (bounded output): a verb that prints a single count or summary
  line — e.g. `count` / `scancount` (a number), or a mutation verb that prints a
  one-line total like "Deleted N items" / "Total records copied". Say so and name
  why.
- In scope (unbounded output): a verb that prints matched items, result rows, or
  per-item diffs.

## The invariant (for in-scope verbs)

A verb with potentially large output must satisfy **all three**:

1. **Unconditional S3 write.** The full result set is written to S3 on every run,
   **not** gated behind an opt-in flag. Requiring a flag (e.g. `--s3`) to persist
   large output is a violation — a user who doesn't know the flag loses data and
   must re-run (the exact failure #86 describes).
2. **Bounded console preview.** The console prints at most a small fixed top-N
   (the established value is `TOP_N = 10`), and when the result exceeds it, prints
   an explicit "…and N more" style indicator so truncation is never silent.
3. **S3 pointer.** The console prints the S3 location the full output was written
   to.

**Consistency checks** (report as findings, but lower severity than a broken
invariant):

- The S3 location should use the shared prefix the reference verbs use:
  `s3://{s3-bucket-name}/output/{JOB_RUN_ID}`. A verb writing to a different
  layout (e.g. `s3://{bucket}/{job_id}/...` with no `output/` prefix) is
  inconsistent.
- The preview cap should be `10`, matching the reference verbs — not a different
  limit.

**Reference implementations:** `find` and `sql` are the canonical conformers —
both write JSON to `output/{JOB_RUN_ID}` via a Spark `.write`, print the first 10,
print "…and N more", and print the location. Compare in-scope verbs against them.

**Out of scope for this rule:** whether the S3 *location is user-overridable*, and
whether output is emitted in a structured/parseable format (JSON/text/table).
Those are proposed enhancements (issues #180, #184), not the current convention —
do not flag a verb for lacking them.

## What to check, per in-scope verb

- **S3 write present and unconditional?** Point to the write call and confirm no
  `if <flag>:` (or equivalent) guards whether large output is persisted.
- **Console print bounded?** Confirm the print path is limited to a fixed top-N
  with an over-limit indicator — not a loop that prints every item, and not a cap
  so high (e.g. 100) that "preview" becomes "dump".
- **Pointer printed?** Confirm the S3 location is echoed to the console.

## How to report

For each verb, state one of:

- `conforms — unconditional S3 write + top-10 preview + pointer` (name the write
  call and the print block), or
- `out of scope — bounded output (<count | mutation summary>)`, or
- a **finding** naming the specific `run(...)` function and which of the three
  parts is missing or wrong (e.g. "S3 write gated behind `--s3`", "prints up to
  100 instead of a bounded preview", "no S3 pointer printed"), plus any
  consistency deviations (wrong prefix, wrong cap).

State what you verified even when clean, so a pass is trustworthy.

**Conformers as of this rule's writing:** `find`, `sql`, and `diff` all conform —
each writes the full result to `output/{JOB_RUN_ID}` unconditionally, prints a
bounded preview (10), and prints the S3 location. `diff` was aligned to this
convention in the PR for #86/#280 (it previously gated the write behind an opt-in
`--s3` flag and printed up to 100). If a future verb with unbounded output is
added, it must join them; flag it if it doesn't.
