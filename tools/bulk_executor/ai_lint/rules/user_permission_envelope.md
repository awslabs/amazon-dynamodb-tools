# Rule: running a command must not need more permissions than the User policy documents

**Why this can't be a unit test:** same reason as the Admin envelope — whether a
boto3 call *needs* a documented permission depends on whether its failure is fatal
or swallowed, which branch reaches it, and whether the documented `Resource` scope
covers it. Judgment, not a lookup. This envelope is also the one with no other
safety net: nothing in `make test` or the e2e suite ever runs a command as a
restricted principal, so a read is the only check it gets.

## The envelope

`README.md` §Security defines two user tiers. This rule covers the **second**: the
*"common users that use the Glue context in order to run the bulk tasks"* — the
people who run `./bulk count`, `find`, `load`, and so on after someone else has
bootstrapped.

Its documented policy is the JSON block under **"The execution must be performed
by a role having this policy at minimum"** (§Run the bulk actions), with Sids
`glue`, `logs`, `ddb`.

Do not confuse this with:

- The **Admin envelope** (bootstrap/teardown) — covered by
  [`admin_permission_envelope.md`](admin_permission_envelope.md). Bootstrap-only
  calls do not count against this rule.
- The **Glue job execution role** — the job's own permissions to read/write
  DynamoDB, priced separately and covered by
  [`role_permissions_agree.md`](role_permissions_agree.md). A command's *server-side*
  work runs under that role, not the user's. Only client-side calls count here.

## Accepted extras — do not flag these

Some commands document **additional** policy on top of the base envelope, and a
user lacking them is expected to be handled gracefully rather than pre-authorized:

- **Cross-account `copy` and `diff`** carry their own
  `CrossAccountIdentityBasedPolicy` blocks in the README. Permissions needed only
  for the cross-account form of a command belong to that per-command policy, not
  the base envelope. Treat them as documented-elsewhere, and confirm the failure is
  a clean error rather than a traceback.

The rule's target is the **base** envelope: what an ordinary same-account command
needs on every run.

## What to check

**Direction matters.** Look for *code that needs more than the docs grant*. An
action documented but never called is over-documentation — worth a one-line note,
not a finding.

1. **Enumerate live.** Find every client-side AWS API call reachable from running a
   command. Start at `BulkDynamoDbRunner` (`client/src/runner.py`), the
   version-parity check (`client/src/infrastructure/verifier.py`, which runs on
   every command), the per-command client modules under
   `client/src/python_modules/`, and shared helpers in `client/src/utils/`. Discover
   the command list dynamically — a newly added command is exactly what this rule
   should catch. Include indirect calls: the live-tail event stream, cost-preview
   `describe_table` lookups, and any S3 read used to fetch results.

2. **Map each call to its IAM action.** Mostly mechanical
   (`start_job_run` → `glue:StartJobRun`), with the same caveats as the Admin rule:
   `head_bucket`/`list_objects_v2` → `s3:ListBucket`, `download_file`/`get_object` →
   `s3:GetObject`, and one call may need two actions. Note that reading command
   output from S3 needs an S3 action — the base envelope currently documents **no**
   `s3` Sid at all, so any client-side S3 read on the command path is a finding.

3. **Decide whether a missing action is a real finding**, exactly as in the Admin
   rule: fatal (uncaught, or `exit(1)`/raise) is a finding; gracefully degraded
   (swallowed, feature skips or warns) is an accepted case worth naming. State which.

4. **Check the reachability condition.** A call made only by one command, or only
   when a flag is set, is still a finding if fatal — but say which command and which
   flag, since that sets urgency.

5. **Check `Resource` scope.** The `logs` Sid here carries three ARN forms
   (including the odd `log-group::log-stream:` form that `StartLiveTail` and
   `DescribeLogGroups` need). Verify the scope actually covers the log groups and
   streams the client touches, and that `glue` covers the job name in use.

## How to report

Per finding: the call site (`file:line`), the IAM action needed, which Sid should
carry it (or that a new Sid is required), whether failure is **fatal or degraded**,
which command(s) reach it, and under what condition. Then list the accepted cases
you deliberately did not flag — cross-account extras and gracefully-degrading calls
— so a clean run is distinguishable from a shallow one.

If the code needs nothing beyond the documented policy, say so and list the
capabilities you verified.
