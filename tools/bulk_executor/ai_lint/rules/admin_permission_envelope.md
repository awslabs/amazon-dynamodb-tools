# Rule: the bootstrap path must not need more permissions than the Admin policy documents

**Why this can't be a unit test:** deciding whether a boto3 call *needs* a
documented permission is judgment, not a lookup. The same call can be fine or a
bug depending on whether its failure is fatal or swallowed, whether it sits on an
optional path, and whether the documented statement's `Resource` scope actually
covers the resource it touches. A mechanical method→action diff produces false
positives on every gracefully-degrading call (this codebase has four); a careful
read separates them.

## The envelope

`README.md` §Security defines two user tiers. This rule covers the **first**: the
*"powerful administrative user"* who runs `./bulk bootstrap` and `./bulk teardown`.

Its documented policy is the JSON block under **"The bootstrap must be performed
by a role with this policy at minimum"** (§Bootstrap), with Sids `glueRoleAdmin`,
`passrole`, `s3`, `glue`, `glueConnection`, `logs`. Teardown has **no** separate
documented policy — it runs under this same envelope, so teardown's calls count
against it.

Do not confuse this with:

- The **Glue job execution role** (`AWSGlueServiceRoleBulkDynamoDB-*`) — a service
  principal, covered by [`role_permissions_agree.md`](role_permissions_agree.md).
- The **User envelope** — covered by
  [`user_permission_envelope.md`](user_permission_envelope.md).

## What to check

**Direction matters.** This rule looks for *code that needs more than the docs
grant* — the failure mode that strands an operator who followed the README. An
action documented but never called is over-documentation: note it if you like, but
it is not a finding.

1. **Enumerate live.** Find every AWS API call reachable from the bootstrap and
   teardown entry points. Start at `BootstrapInfrastructure`
   (`client/src/infrastructure/bootstrap.py`) and the teardown module, and follow
   into whatever they call — `client/src/utils/role_validator.py` is reached this
   way and is easy to miss. Grep for `<service>_client.<method>(` but also check
   for paginators, waiters, `boto3.resource`, and `upload_file`-style helpers that
   wrap an API call under a different name. Never work from a hard-coded list; new
   calls are the whole point.

2. **Map each call to its IAM action.** Usually mechanical
   (`create_log_group` → `logs:CreateLogGroup`), but watch the ones that aren't:
   - `head_bucket` → `s3:ListBucket`; `list_objects_v2` → `s3:ListBucket`
   - `upload_file` → `s3:PutObject`
   - `delete_objects` → `s3:DeleteObject`
   - passing a role to Glue → `iam:PassRole` (no method call names it)
   - a single call may require two actions

3. **Decide whether a missing action is a real finding.** For each call whose
   action is absent from the documented policy, read its error handling:
   - **Fatal → finding.** The exception is uncaught, or caught and turned into
     `exit(1)` / a raise. An operator with exactly the documented policy is hard-
     blocked.
   - **Gracefully degraded → not a finding.** Wrapped so an `AccessDenied` is
     swallowed and the feature skips or warns (e.g. `except Exception: return None`
     with a docstring saying the caller then skips the check). Note it as accepted
     so the next run doesn't re-litigate it.
   - Say which it is explicitly. This is the distinction the rule exists for.

4. **Check the reachability condition, and say it out loud.** A call on a rarely-
   taken branch is still a finding if it's fatal when taken, but *when* it fires
   changes urgency. Two branches here are easy to under-rate:
   - the `ResourceAlreadyExistsException` path, which only runs on accounts that
     already have the `/aws-glue/jobs/*` log groups (so it is invisible on a fresh
     account and hits everyone else)
   - the role-refresh path gated on `_needs_role_refresh()`, which fires on the
     next bootstrap after any `__version__` bump

5. **Check `Resource` scope, not just the action name.** An action present in the
   policy but scoped to an ARN pattern that doesn't match what the code touches is
   the same bug wearing a disguise. Compare the documented `Resource` against the
   real resource names/ARNs the call uses.

## How to report

Per finding: the call site (`file:line`), the IAM action it needs, which Sid should
carry it, whether failure is **fatal or degraded**, and the condition under which
the call is reached. Then state the accepted (gracefully-degrading) calls you
deliberately did not flag, so a clean run is distinguishable from a shallow one.

If the code needs nothing beyond the documented policy, say so and list the
capabilities you verified.
