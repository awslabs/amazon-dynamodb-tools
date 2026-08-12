# Rule: README, role creation, and custom-role validator must agree

**Why this can't be a unit test:** the same intent ("what IAM permissions the
Glue job needs") is expressed three times in three incompatible shapes — English
prose, policy-building code, and permission-checking code. Nothing mechanical can
confirm the three still mean the same thing after an edit; but a careful read can.

## The three sources that must agree

1. **`README.md`** — the human spec. Section "If you provide a custom IAM role
   for your AWS Glue job" and the nearby "How the custom role is validated at
   bootstrap" subsection: prose + sample policy guidance, one entry per capability.

2. **`client/src/infrastructure/bootstrap.py`** — the *creator*. In
   `_add_glue_job_role` it builds the role: the managed policy ARNs it attaches
   (`AWSGlueServiceRole`, `AmazonDynamoDB*Access`) and the inline policy documents
   it puts (pricing, service quotas, autoscaling), each with a specific `Action`
   list and `Resource` scope.

3. **`client/src/utils/role_validator.py`** — the *checker*. Validates a
   user-supplied custom `--XRole` against the documented minimums via module
   constants (`GLUE_BASELINE_POLICY_ARN`, `PRICING_ACTIONS`, `QUOTA_ACTIONS`,
   `AUTOSCALING_ACTION`, `DYNAMODB_SERVICE`) and per-capability checks.

## What counts as agreement

For **each capability** (baseline Glue execution, pricing, service quotas,
autoscaling, DynamoDB access, role-name prefix, trust policy), verify:

- **Present in all three.** A capability the creator grants but the README omits,
  or the validator never checks — or one the README documents but the creator
  doesn't grant — is drift.
- **Action names match** across creator inline policy, validator constant, and
  README (action or its managed-policy equivalent).
- **Resource scope is consistent with how the validator checks it.** The subtle
  one. If the creator grants an action scoped to a specific resource (e.g. quotas
  on `arn:aws:servicequotas:*:*:dynamodb/*`) but the validator asserts that action
  on `Resource: "*"` (via `SimulatePrincipalPolicy` against the default `*`, or a
  comment claiming "granted on `Resource: "*"` in every documented setup"), then a
  role our own bootstrap creates could FAIL the validator. Flag any case where the
  validator's assumed scope is broader than what the creator actually grants.
- **Severity is defensible.** The validator marks findings FATAL (aborts
  bootstrap) or WARNING (advisory). FATAL is only for conditions that provably
  stop the job with zero false-block risk (wrong role-name prefix; trust policy
  that won't let Glue assume the role). Flag a FATAL that a locked-down-but-valid
  role could satisfy differently, or a genuinely-blocking condition left as a mere
  warning.

## How to report

Per disagreement: name the capability, say which of the three it is
present/absent/different in, and give the concrete mismatch (action name, resource
scope, or severity). If all capabilities agree across all three, report no findings.
