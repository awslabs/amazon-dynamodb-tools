# Rule: the Glue job must not need more permissions than the Glue-role docs state

**Why this can't be a unit test:** most of this envelope's real permissions are
never issued by our code. `dynamodb:Scan` / `BatchWriteItem` / `UpdateItem` come
from inside the Glue DynamoDB connector, so grepping `server/src` for boto3 calls
finds none of them — a mechanical scan would report this envelope clean while
missing its entire data plane. Reasoning about what the *job* needs, versus what
our code happens to call, is the judgment this rule encodes.

## The envelope

This rule covers the **Glue job execution role**
(`AWSGlueServiceRoleBulkDynamoDB-*`, or a custom role passed via `--XRole`) — an
IAM *service role* assumed by `glue.amazonaws.com`, not a human identity. It is the
context with real blast radius: it is what actually touches customer data.

Its requirements are documented under §"Glue job role permissions" and the
"If you provide a custom IAM role" bullets — as managed policies, each paired with
the equivalent granular action where one exists (`pricing:GetProducts`,
`servicequotas:GetServiceQuota`, …). Match on **either** form; a bullet naming only
a managed policy still documents the capability.

Note that `README.md` §Security describes only the two *human* tiers (Admin and
Client). This role is a service principal, not a user tier, so its absence from
that list is expected and is not a documentation finding.

Related but different rules — do not duplicate their work:

- [`role_permissions_agree.md`](role_permissions_agree.md) asks whether the three
  *descriptions* of this role (README, `_add_glue_job_role`, `role_validator.py`)
  agree with each other. **This** rule asks a different question: does the *code*
  need something none of them grant?
- [`permission_envelope_admin.md`](permission_envelope_admin.md) and
  [`permission_envelope_user.md`](permission_envelope_user.md) cover the two human
  contexts. A call made by the CLI on the developer's machine belongs to those; only
  what runs *inside the Glue job* counts here.

## DynamoDB access is deliberately unconstrained — never flag it

**This is the most important thing to get right, and the most tempting false
positive.** The role's DynamoDB grant is intentionally open-ended: read-only on a
single table is a perfectly valid, useful configuration, and so is read-write on
every table. `bootstrap --XRole READ-ONLY` / `READ-WRITE` create broad roles, and a
custom role may be scoped to one table.

Therefore:

- **Never** report a mismatch between what a command *does* to DynamoDB and what
  the role grants. There is no "correct" DynamoDB scope to drift from.
- **Never** report that the README fails to specify DynamoDB actions precisely. Its
  vagueness there is deliberate and correct.
- A user whose role can't read or write the table they targeted is a **runtime
  configuration problem, not a README bug or a code bug.** The right behavior is a
  clear, fatal, polite error — not a pre-flight guarantee.

**Secondary check (report separately, never as envelope drift):** does an
AccessDenied from DynamoDB actually surface as a readable message rather than a raw
Spark/boto traceback? If a command dies on a DynamoDB permission with an opaque
stack trace, that's a UX finding worth a line — but it is explicitly *not* drift,
and it must be reported under its own heading so nobody mistakes it for one.

## What to check

Everything **except** the DynamoDB grant. Those parts are fixed and knowable, which
is exactly why drift there is a real bug (#89 was one: the autoscaling capacity
check needed `application-autoscaling:DescribeScalableTargets`, which nothing
granted).

1. **Enumerate live, from inside the job.** Find every AWS API call made by code
   that runs on a Glue worker or driver: `server/src/root.py`, every module under
   `server/src/python_modules/` (discover the command list dynamically — a new
   command is what this rule should catch), and the shared helpers they import
   (`shared/table_info.py`, `shared/pricing.py`, `shared/rate_limiter.py`, …). Grep
   for `boto3.client(...)` / `.client(` plus the method calls on the result.

2. **Add the calls our code does *not* make.** Reason about the connector and the
   Glue runtime, and state these explicitly rather than silently omitting them:
   - the DynamoDB data plane issued by `spark.read.format("dynamodb")` /
     `df.write.format("dynamodb")` (excluded from drift analysis per the section
     above, but name them so the reader knows they were considered)
   - S3 access for the job script, `--extra-py-files`, temp/shuffle, and command
     output, plus CloudWatch Logs writes — largely covered by the managed
     `AWSGlueServiceRole` policy, so check the README still claims that baseline

3. **Map each call to its IAM action and compare against the docs.** The fixed
   capabilities to verify are: baseline Glue execution (`AWSGlueServiceRole`),
   pricing (`pricing:GetProducts`), service quotas
   (`servicequotas:GetServiceQuota`, `GetAWSDefaultServiceQuota`), autoscaling
   (`application-autoscaling:DescribeScalableTargets`), and any S3 or Logs action
   beyond the managed baseline.

4. **Classify anything missing**, and say which:
   - **Fatal → finding.** An uncaught exception, or one that fails the job.
   - **Gracefully degraded → documentation finding, not a breakage.** The house
     style is to document such a permission *and* its degradation (the autoscaling
     bullet is the model: it states the action, that it can't be resource-scoped,
     and exactly what the job does without it). A degrading call whose permission is
     undocumented is a doc gap; one that is documented is fine.

5. **Check `Resource` scope for the fixed capabilities only.** Note where an action
   genuinely cannot be scoped (`application-autoscaling:DescribeScalableTargets`
   requires `"Resource": "*"`) and confirm the README says so, since an operator
   writing a locked-down policy will otherwise scope it and silently lose the check.

## How to report

Two separate sections:

1. **Envelope drift** — per finding: the call site (`file:line`), the IAM action, the
   capability it belongs to, fatal vs degraded, and which command(s) reach it. Then
   list what you verified clean, including an explicit line confirming you excluded
   the DynamoDB grant on purpose.
2. **DynamoDB error-surfacing (not drift)** — anything from the secondary check.

If the fixed capabilities are all documented, say so and name them.
