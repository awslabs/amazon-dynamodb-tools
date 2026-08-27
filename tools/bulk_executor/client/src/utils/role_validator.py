"""Validate minimum permissions on a custom IAM role for the Bulk Executor Glue job.

Checks a custom ``--XRole`` against the requirements documented in README.md
under "If you provide a custom IAM role for your AWS Glue job", and returns a
list of :class:`Finding` objects. Each finding carries a ``severity``:

* ``FATAL``   -- the role *cannot* work and bootstrap should stop now with a
  clear message, rather than letting the operator hit an opaque failure later.
  Reserved for conditions that are certain and carry zero false-block risk:
  a role name that the passrole grant can't cover, and a trust policy that
  doesn't let Glue assume the role. The caller (``bootstrap._get_role_name``)
  turns any FATAL finding into an ``exit(1)``.
* ``WARNING`` -- advisory. The job may run degraded (missing pricing/quota/
  autoscaling visibility) or the check couldn't be made authoritative. Never
  blocks bootstrap: a locked-down-but-valid role must not be rejected, which is
  the whole reason this validator exists.

How effective permissions are determined, and why:

* ``pricing:GetProducts`` and the two ``application-autoscaling:Describe*`` reads
  are evaluated with ``iam:SimulatePrincipalPolicy``. That is IAM's own policy
  evaluator, so it correctly accounts for ``Deny`` statements, ``NotAction``,
  condition keys, and permission boundaries -- none of which a hand-rolled "read
  the Allow statements" pass gets right. Neither action supports resource-level
  scoping, so bootstrap grants both on ``Resource: "*"`` and simulating against
  ``*`` is authoritative.
* Service Quotas and DynamoDB access are *presence* checks over the role's Allow
  statements (managed-policy documents + inline policies), NOT simulations. Both
  are resource-scoped in the documented setup -- bootstrap grants the two
  ``servicequotas`` reads on ``arn:aws:servicequotas:*:*:dynamodb/*`` and
  DynamoDB on the target tables -- so simulating either against ``*`` would
  report a denial for a perfectly valid scoped grant, re-introducing the false
  warning this validator is meant to avoid (a role our *own* bootstrap creates
  would fail the quotas simulation). A presence check accepts the scoped grant
  (and the ``ServiceQuotasReadOnlyAccess`` / ``AmazonDynamoDB*Access`` managed
  policies). Its one blind spot -- an Allow later cancelled by a Deny -- is
  documented and accepted, because it can only ever *suppress* a warning, never
  block a bootstrap.

Every IAM read here is best-effort. The reads and ``SimulatePrincipalPolicy``
are deliberately NOT part of the documented minimal bootstrap policy (that
policy is "minimum to bootstrap successfully"; these only improve diagnostics).
If a caller lacks them, the affected check is skipped -- logged, never warned or
blocked. See README.md for the optional permissions that light these up.
"""
from __future__ import annotations

from dataclasses import dataclass

from utils.logger import log

# --- Finding severities -----------------------------------------------------
FATAL = "fatal"
WARNING = "warning"


@dataclass(frozen=True)
class Finding:
    """One validation result. ``severity`` is FATAL or WARNING; ``message`` is
    the human-readable explanation the caller logs (and, for FATAL, aborts on)."""

    severity: str
    message: str


GLUE_SERVICE_PRINCIPAL = "glue.amazonaws.com"
ROLE_NAME_PREFIX = "AWSGlueServiceRole"

# README names this specific managed policy for baseline Glue execution
# (S3, CloudWatch, etc.) with no documented alternative, so we match it by ARN.
GLUE_BASELINE_POLICY_ARN = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"

# ``*``-resource capabilities, evaluated via SimulatePrincipalPolicy (neither
# supports resource scoping, so bootstrap grants both on Resource "*").
PRICING_ACTIONS = ("pricing:GetProducts",)
# Both autoscaling reads are needed for the full diagnostic: ScalableTargets
# gives the min/max the capacity warning keys off, ScalingPolicies gives the
# target-utilization value (issue #297). Bootstrap grants both.
AUTOSCALING_ACTIONS = (
    "application-autoscaling:DescribeScalableTargets",
    "application-autoscaling:DescribeScalingPolicies",
)

# Resource-scoped capabilities -> presence check, not simulation (see docstring).
# Bootstrap scopes the quota reads to arn:aws:servicequotas:*:*:dynamodb/*, so
# simulating them against "*" would false-deny our own created role.
QUOTA_ACTIONS = (
    "servicequotas:getservicequota",
    "servicequotas:getawsdefaultservicequota",
)
DYNAMODB_SERVICE = "dynamodb"

# Simulate decisions that mean "the principal can perform this action".
_ALLOWED_DECISION = "allowed"


def validate_custom_role_permissions(iam_client, role_name: str) -> list[Finding]:
    """Check that a custom role meets the README's documented minimums.

    Returns a list of :class:`Finding` (empty if the role looks good). Gracefully
    returns whatever it could determine on any IAM API failure -- it never
    raises. FATAL findings mean the caller should abort bootstrap; WARNING
    findings are advisory.
    """
    findings: list[Finding] = []

    # --- 1. Role name prefix (deterministic, no API call). FATAL: the bootstrap
    # passrole grant is scoped to AWSGlueServiceRole*, so a non-matching name
    # guarantees Glue can't be handed the role. Zero false-block risk. ---
    findings.extend(_check_name_prefix(role_name))

    try:
        role_resp = iam_client.get_role(RoleName=role_name)
    except Exception:
        log.debug(f"Could not inspect role '{role_name}' — skipping permission validation")
        return findings

    role = role_resp.get("Role", {})
    role_arn = role.get("Arn")

    # --- 2. Trust policy allows the Glue service principal. FATAL: if Glue
    # can't assume the role the job cannot start, and there's no other way to
    # grant that. Zero false-block risk. ---
    findings.extend(_check_trust_policy(role.get("AssumeRolePolicyDocument", {})))

    try:
        attached = iam_client.list_attached_role_policies(RoleName=role_name)
        attached_arns = [p["PolicyArn"] for p in attached.get("AttachedPolicies", [])]
    except Exception:
        log.debug(f"Could not list attached policies for '{role_name}' — skipping the rest")
        return findings

    # --- 3. Baseline Glue execution managed policy (named, ARN-matched). SOFT:
    # README names this policy with no documented alternative, but an equivalent
    # custom grant is conceivable, so we advise rather than block. ---
    findings.extend(_check_glue_baseline(attached_arns))

    # --- 4-5. pricing / autoscaling via SimulatePrincipalPolicy. Both are
    # granted on Resource "*" (neither supports scoping), so simulating on "*"
    # is authoritative. ---
    findings.extend(_check_star_capabilities(iam_client, role_arn))

    # --- 6-7. Service Quotas and DynamoDB, both resource-scoped, so presence
    # checks over the role's Allow statements (see module docstring). Read the
    # managed + inline policy documents once and feed both checks. ---
    allowed = _collect_allowed_actions(iam_client, role_name, attached_arns)
    if allowed is None:
        log.debug(
            f"Could not read policy documents for '{role_name}' — "
            f"skipping the Service Quotas and DynamoDB access checks"
        )
    else:
        findings.extend(_check_quotas(allowed))
        findings.extend(_check_dynamodb(allowed))

    return findings


def _check_name_prefix(role_name: str) -> list[Finding]:
    """README: the custom role name must start with AWSGlueServiceRole."""
    if role_name.startswith(ROLE_NAME_PREFIX):
        return []
    return [Finding(FATAL,
        f"Role name '{role_name}' does not start with '{ROLE_NAME_PREFIX}'. "
        f"The bootstrap passrole permission is scoped to role names beginning "
        f"with '{ROLE_NAME_PREFIX}', so Glue cannot be given this role and the "
        f"job cannot run. Rename the role to start with '{ROLE_NAME_PREFIX}'."
    )]


def _check_trust_policy(trust_doc: dict) -> list[Finding]:
    """Verify glue.amazonaws.com (or a "*" wildcard) is a trusted principal."""
    for statement in trust_doc.get("Statement", []):
        principal = statement.get("Principal", {})
        services = principal.get("Service", [])
        if isinstance(services, str):
            services = [services]
        if GLUE_SERVICE_PRINCIPAL in services or "*" in services:
            return []

    return [Finding(FATAL,
        f"Role trust policy does not allow {GLUE_SERVICE_PRINCIPAL} to assume "
        f"the role. The Glue job cannot assume this role, so it cannot run. Add "
        f"a trust-policy statement allowing the {GLUE_SERVICE_PRINCIPAL} service "
        f"principal to sts:AssumeRole."
    )]


def _check_glue_baseline(attached_arns: list[str]) -> list[Finding]:
    """README: attach the managed policy AWSGlueServiceRole."""
    if GLUE_BASELINE_POLICY_ARN in attached_arns:
        return []
    return [Finding(WARNING,
        "Role is missing the AWSGlueServiceRole managed policy "
        f"({GLUE_BASELINE_POLICY_ARN}). The Glue job requires it for its "
        "baseline execution permissions (S3, CloudWatch, etc.)."
    )]


def _check_star_capabilities(iam_client, role_arn: str | None) -> list[Finding]:
    """pricing / autoscaling, evaluated with SimulatePrincipalPolicy.

    Both are granted on ``Resource: "*"`` (neither action supports resource-level
    scoping), so simulating against ``*`` is authoritative and honors Deny/
    NotAction/conditions. Best-effort: if the role ARN is unknown or the caller
    lacks ``iam:SimulatePrincipalPolicy``, both checks are skipped (not warned).
    Service Quotas is NOT here -- it is resource-scoped, so it is a presence check
    (see :func:`_check_quotas` and the module docstring).
    """
    if not role_arn:
        return []

    actions = list(PRICING_ACTIONS) + list(AUTOSCALING_ACTIONS)
    allowed = _simulate_allowed(iam_client, role_arn, actions)
    if allowed is None:
        log.debug(
            "Could not simulate role permissions (missing "
            "iam:SimulatePrincipalPolicy?) — skipping pricing/autoscaling checks"
        )
        return []

    findings: list[Finding] = []

    if not all(a in allowed for a in PRICING_ACTIONS):
        findings.append(Finding(WARNING,
            "Role cannot call pricing:GetProducts. The job uses it to estimate "
            "DynamoDB operation costs. Attach AWSPriceListServiceFullAccess, or "
            "for maximum lockdown allow the pricing:GetProducts action inline."
        ))

    missing_autoscaling = [a for a in AUTOSCALING_ACTIONS if a not in allowed]
    if missing_autoscaling:
        # Soft: the job still runs without these, it just skips the
        # autoscaling-aware capacity warning / diagnostic (README). Name the
        # actions that are actually missing -- telling an operator to grant a
        # permission they already hold sends them down a dead end (issue #297).
        findings.append(Finding(WARNING,
            f"Role cannot call {', '.join(missing_autoscaling)}. "
            "The job still runs, but it will skip the autoscaling-aware capacity "
            "warning. To enable it, allow those actions on Resource \"*\"."
        ))

    return findings


def _simulate_allowed(iam_client, role_arn: str, actions: list[str]) -> set[str] | None:
    """Return the subset of ``actions`` the role is allowed to perform on ``*``.

    Uses ``iam:SimulatePrincipalPolicy`` -- IAM's own evaluator -- against the
    default resource (``*``). Returns a set of allowed action names, or ``None``
    if the simulation could not be run (caller then skips those checks rather
    than false-warning). Actions are compared case-insensitively via the exact
    names we pass in, since we control that list.
    """
    try:
        allowed: set[str] = set()
        resp = iam_client.simulate_principal_policy(
            PolicySourceArn=role_arn, ActionNames=actions
        )
        for result in resp.get("EvaluationResults", []):
            if result.get("EvalDecision") == _ALLOWED_DECISION:
                allowed.add(result.get("EvalActionName"))
        return allowed
    except Exception:
        return None


def _check_quotas(allowed: set[str]) -> list[Finding]:
    """Service Quotas presence check over the role's Allow statements.

    Bootstrap scopes the two quota reads to
    ``arn:aws:servicequotas:*:*:dynamodb/*``, so a SimulatePrincipalPolicy call
    against ``*`` would false-deny our own created role. Instead we check that the
    role allows both quota actions somewhere (accepting the scoped inline grant
    and the ``ServiceQuotasReadOnlyAccess`` managed policy's ``servicequotas:Get*``
    / ``servicequotas:*`` wildcards). ``allowed`` is the pre-collected set of
    Allow'd actions (lower-cased); the caller skips this check when the policy
    documents couldn't be read.
    """
    if all(_action_allowed(allowed, action) for action in QUOTA_ACTIONS):
        return []

    return [Finding(WARNING,
        "Role cannot read Service Quotas (servicequotas:GetServiceQuota, "
        "servicequotas:GetAWSDefaultServiceQuota). The job uses these to detect "
        "account-level read/write limits. Attach ServiceQuotasReadOnlyAccess, or "
        "for maximum lockdown allow those servicequotas actions (they can be "
        "scoped to arn:aws:servicequotas:*:*:dynamodb/*)."
    )]


def _check_dynamodb(allowed: set[str]) -> list[Finding]:
    """DynamoDB presence check over the role's Allow statements.

    ``allowed`` is the pre-collected set of Allow'd actions (lower-cased); the
    caller skips this check when the policy documents couldn't be read. See the
    module docstring for why this is a presence check and not a
    SimulatePrincipalPolicy call.
    """
    if _has_action_in_service(allowed, DYNAMODB_SERVICE):
        return []

    return [Finding(WARNING,
        "Role has no DynamoDB permissions. Attach AmazonDynamoDBReadOnlyAccess "
        "or AmazonDynamoDBFullAccess, or a more restrictive policy that grants "
        "the DynamoDB actions your operation needs on your target tables."
    )]


def _collect_allowed_actions(
    iam_client, role_name: str, attached_arns: list[str]
) -> set[str] | None:
    """Gather every action Allow'd by the role's managed + inline policies.

    Returns a set of action strings (e.g. {'dynamodb:*', 'servicequotas:get*'})
    or None if the policy documents could not be read (caller then skips the
    checks rather than false-warning). Feeds the Service Quotas and DynamoDB
    presence checks.
    """
    actions: set[str] = set()

    try:
        # Attached managed policies: resolve each to its default version document.
        for arn in attached_arns:
            policy = iam_client.get_policy(PolicyArn=arn)
            version_id = policy["Policy"]["DefaultVersionId"]
            version = iam_client.get_policy_version(
                PolicyArn=arn, VersionId=version_id
            )
            _accumulate_actions(version["PolicyVersion"]["Document"], actions)

        # Inline policies attached directly to the role.
        inline = iam_client.list_role_policies(RoleName=role_name)
        for policy_name in inline.get("PolicyNames", []):
            doc = iam_client.get_role_policy(
                RoleName=role_name, PolicyName=policy_name
            )
            _accumulate_actions(doc["PolicyDocument"], actions)
    except Exception:
        return None

    return actions


def _accumulate_actions(document, actions: set[str]) -> None:
    """Add the actions from every Allow statement in an IAM policy document.

    Actions are lower-cased so matching is case-insensitive (IAM treats action
    names case-insensitively). Only Allow statements contribute; Deny is not
    subtracted -- acceptable here because this feeds only the DynamoDB presence
    check, where the effect is at worst a suppressed advisory warning, never a
    blocked bootstrap (see module docstring).
    """
    statements = document.get("Statement", [])
    if isinstance(statements, dict):
        statements = [statements]
    for stmt in statements:
        if stmt.get("Effect") != "Allow":
            continue
        stmt_actions = stmt.get("Action", [])
        if isinstance(stmt_actions, str):
            stmt_actions = [stmt_actions]
        for action in stmt_actions:
            actions.add(action.lower())


def _has_action_in_service(allowed: set[str], service: str) -> bool:
    """Does the role allow ANY action in `service` (e.g. 'dynamodb')?

    Presence check, not a specific-action check: a role scoped to just
    ``dynamodb:GetItem`` on one table still counts as having DynamoDB access,
    so restrictive per-table policies don't false-warn.
    """
    prefix = f"{service.lower()}:"
    for pattern in allowed:
        if pattern == "*" or pattern.startswith(prefix):
            return True
    return False


def _action_allowed(allowed: set[str], action: str) -> bool:
    """Is a SPECIFIC action (e.g. 'servicequotas:getservicequota') allowed?

    Honors the wildcard forms an Allow statement can take: ``*`` (all actions),
    ``service:*`` (all actions in the service), and a prefix glob like
    ``servicequotas:get*``. Unlike :func:`_has_action_in_service`, this matches
    the named action rather than any action in the service, since the quota check
    needs both specific reads to be permitted. ``action`` and the ``allowed``
    patterns are lower-cased (see :func:`_accumulate_actions`).
    """
    service = action.split(":", 1)[0]
    for pattern in allowed:
        if pattern == "*" or pattern == f"{service}:*" or pattern == action:
            return True
        if pattern.endswith("*") and action.startswith(pattern[:-1]):
            return True
    return False
