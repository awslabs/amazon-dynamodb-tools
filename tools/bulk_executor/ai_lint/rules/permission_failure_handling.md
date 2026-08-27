# Rule: how a permission failure is handled must match whether the call is load-bearing

**Why this can't be a unit test:** the invariant is a judgment about *consequence*
— "if this call is denied, is what we produce still usable?" You can see the
answer by reading the surrounding code, but nothing mechanical can derive it. A
test can assert that a specific denial exits, or doesn't; it cannot tell you which
of those is correct.

## The invariant

Every AWS call has exactly one correct failure mode, decided by one question:

> **If this call is denied, is the thing we're producing still usable?**

Answer it **per call, not per function** — a single function routinely contains
both kinds, and lumping them together is its own bug. `_create_glue_log_groups` is
the example: creating the group is load-bearing, while managing its retention is a
courtesy, so they get different handling in the same loop.

Read "usable" from the **user's** side, not the job's. A command whose Glue run
succeeds but whose first 40 seconds of output never reached the console has failed
at something the user cares about.

- **No → load-bearing → failing hard is correct.** `exit(1)` or a raise. Creating
  the Glue job, creating the role, creating the S3 bucket: without these there is
  nothing to run, so stopping with a clear message is the kindest outcome.
- **Yes → not load-bearing → warn and proceed.** An optimization, a diagnostic, a
  courtesy default, or a read that exists only to be careful. Log a warning that
  **names the missing permission** and **says what the user loses**, then carry on.

Both failure modes are correct somewhere. The bug is a mismatch — and in practice
the mismatch runs one way: a non-essential call that kills the run.

## Two worked examples, in both directions

**#294 — got it wrong.** `_get_log_group_retention` called `describe_log_groups`
for one reason: to *avoid* clobbering a retention the account owner had chosen.
Unguarded, so a denial propagated to an `exit(1)`. A read whose entire purpose was
caution took down the whole bootstrap — and only on accounts that already had the
log groups, so it was invisible on a fresh account. Fixed in #301: retention
handling now warns and continues.

**Creating the log group in that same function — fatal, correctly.** The first
attempt at #301 downgraded this too, reasoning that Glue creates the groups on its
first run anyway. That reasoning was wrong, and the trap is worth internalising:
"the resource gets created eventually" is not the same as "nothing is lost". The
client blocks on `_wait_for_log_groups_to_exist` before attaching LiveTail,
LiveTail never replays, and the command exits if the groups don't appear inside the
retry budget — so silently skipping creation trades a clear bootstrap error for
lost job output and a command that dies later, further from the cause. Look for
this shape: a call that *looks* like an optimization because something else will
create the resource, where the timing is the whole point.

**The S3 TLS bucket policy — got it right.** `put_bucket_policy` failing is still
fatal, deliberately. The README states the bucket enforces TLS in transit; that is
a documented security property, not a convenience. Proceeding without it would
hand back an environment that silently fails a promise we made. **Do not flag
this** — "warn and proceed" is not automatically the safer choice, and downgrading
a security control to a warning is a regression even though the environment would
technically function.

That contrast is the rule: usability decides, and a documented security guarantee
counts as load-bearing.

## What to check

1. **Enumerate live.** Find every AWS call on the bootstrap and teardown paths
   (`client/src/infrastructure/`) and in the server-side verbs
   (`server/src/python_modules/`). Discover them rather than working from a list;
   a newly added call is the point.

2. **Classify each by consequence, and say which.** For each call, state whether a
   denial leaves the result usable. Useful signals that a call is *not*
   load-bearing: its own docstring or comment describes it as best-effort, a
   diagnostic, a warning, an estimate, an optimization, or "so we don't
   clobber/wait/guess"; the feature it powers is documented as optional; or the
   surrounding code already handles absence (`return None`, a skip path).

3. **Compare against the actual handling.** Flag:
   - A **non-load-bearing call that fails hard** — uncaught, or caught into
     `exit(1)`/raise. This is the #294 shape and the main thing to hunt.
   - A **load-bearing call that is swallowed**, leaving a broken environment that
     looks successful. Rarer, worse when it happens.

4. **Check the warning is actionable, not just present.** A degrading path must
   name the permission and the consequence. Two specific failures to look for:
   - A **hardcoded** permission name in the message. #297: the autoscaling
     degradation always advised granting `DescribeScalableTargets`, which the
     operator already had; the real gap was `DescribeScalingPolicies`. Telling
     someone to grant a permission they hold is worse than saying nothing, because
     it ends their investigation. Derive the name from what actually failed, or
     name every permission the path needs.
   - A message that says something failed but not **what is lost**, so the operator
     can't judge whether to care.

5. **Don't let a warning hide a real breakage.** If the degraded path leaves the
   feature silently unavailable *and* nothing records that fact anywhere, that's
   worth reporting even though the handling is technically correct.

## How to report

Per finding: the call site (`file:line`), whether it is load-bearing, how it is
handled today, how it should be handled, and the consequence of the mismatch.
Then list the calls you classified as correctly handled — including any where you
judged hard failure right *despite* the environment being usable, and why — so a
clean run is distinguishable from a shallow one.
