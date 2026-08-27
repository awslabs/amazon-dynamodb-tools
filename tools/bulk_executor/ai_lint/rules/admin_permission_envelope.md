# Rule: the bootstrap path must not need more permissions than the Admin policy documents

**Why this can't be a unit test:** deciding whether a boto3 call *needs* a
documented permission is judgment, not a lookup. The same call can be fine or a
bug depending on whether its failure is fatal or swallowed, whether it sits on an
optional path, whether the permission is documented somewhere other than the
minimum policy block, and whether the documented statement's `Resource` scope
actually covers the resource it touches. A mechanical method→action diff reports
five false positives in `role_validator.py` alone, where the permissions are
documented in prose as optional extras and every call degrades gracefully. Only a
read separates those from an uncaught call that `exit(1)`s.

## The envelope

`README.md` §Security defines two user tiers. This rule covers the **first**: the
*"powerful administrative user"* who runs `./bulk bootstrap` and `./bulk teardown`.

Its documented policy has **two tiers**, and conflating them produces false
positives:

1. **The minimum** — the JSON block under *"The bootstrap must be performed by a
   role with this policy at minimum"* (§Bootstrap), Sids `glueRoleAdmin`,
   `passrole`, `s3`, `glue`, `glueConnection`, `logs`. A fatal call whose action is
   missing here is a finding.

2. **Documented optional extras** — the paragraph *"Optional permissions for the
   caller running `bootstrap` (not the Glue role)"* at the end of §"How the custom
   role is validated at bootstrap". It grants `iam:SimulatePrincipalPolicy`,
   `iam:GetPolicy`, `iam:GetPolicyVersion`, `iam:ListRolePolicies`,
   `iam:GetRolePolicy` for the custom-role validator's best-effort checks, and says
   explicitly that a caller lacking them has the affected check silently skipped.
   These are **deliberately not** in the minimum. An action listed there is
   documented — **not** a finding, and must not be "fixed" into the minimum policy.

Teardown has **no** separate documented policy — it runs under this same envelope,
so teardown's calls count against it.

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

3. **Decide whether a missing action is a real finding.** For each call whose action
   is absent from the **minimum** policy, classify it — and say which class,
   explicitly. This is the distinction the rule exists for:
   - **Documented as an optional extra → not a finding.** Check tier 2 above before
     anything else. Adding one of those actions to the minimum policy would be a
     regression, not a fix.
   - **Fatal → finding.** The exception is uncaught, or caught and turned into
     `exit(1)` / a raise. An operator with exactly the minimum policy is hard-
     blocked.
   - **Gracefully degraded but undocumented → weak finding.** Wrapped so an
     `AccessDenied` is swallowed and the feature skips or warns (e.g.
     `except Exception: return None`). Nobody is blocked, so it is not urgent — but
     the capability is silently unavailable to anyone on the minimum policy, and the
     precedent in tier 2 is that such permissions get *documented as optional*
     rather than left unmentioned. Report it as a documentation gap, not a breakage.

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
the call is reached. Then state the calls you deliberately did **not** flag and why
— separating *documented as optional extras* from *undocumented but gracefully
degrading* — so a clean run is distinguishable from a shallow one, and so nobody
"fixes" a deliberate optional into the minimum policy.

If the code needs nothing beyond the documented policy, say so and list the
capabilities you verified.
