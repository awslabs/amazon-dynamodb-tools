"""Validate minimum permissions on a custom IAM role for the Bulk Executor Glue job.

Checks a custom ``--XRole`` against the requirements documented in README.md
under "If you provide a custom IAM role for your AWS Glue job". Returns a list
of human-readable warning strings; an empty list means every documented
requirement is satisfied.

Advisory only: this never raises and never exits. Any IAM read that fails
degrades to "skip that one check" rather than blocking bootstrap.

Why effective-permission checks instead of matching attached-policy ARNs: the
README explicitly allows inline and restrictive alternatives to the managed
policies -- ``pricing:GetProducts`` inline instead of AWSPriceListServiceFullAccess,
the ``servicequotas`` actions inline instead of ServiceQuotasReadOnlyAccess, or a
DynamoDB policy scoped to specific tables instead of the AWS-managed ones.
Matching ARNs would false-warn on every one of those documented lockdown
setups. So we collect the actions the role effectively allows -- from both its
attached managed-policy documents and its inline policies -- and check that the
documented *capabilities* are present, however they were granted.

Known limitation: action collection reads ``Allow`` statements and does not
subtract ``Deny`` statements or evaluate condition keys / ``NotAction``. A role
that grants an action and then denies it via a separate statement would not be
flagged here. This is advisory guidance, not an authorization decision, so the
simplification is acceptable; the live e2e suite validates real roles end to end.
"""
from __future__ import annotations

from utils.logger import log

GLUE_SERVICE_PRINCIPAL = "glue.amazonaws.com"
ROLE_NAME_PREFIX = "AWSGlueServiceRole"

# README names this specific managed policy for baseline Glue execution
# (S3, CloudWatch, etc.) with no documented alternative, so we match it by ARN.
GLUE_BASELINE_POLICY_ARN = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"

# Documented capabilities, expressed as the actions the Glue job actually needs.
# Each may be granted by an AWS-managed policy OR an inline/lockdown policy.
PRICING_ACTIONS = ("pricing:GetProducts",)
QUOTA_ACTIONS = (
    "servicequotas:GetServiceQuota",
    "servicequotas:GetAWSDefaultServiceQuota",
)
AUTOSCALING_ACTION = "application-autoscaling:DescribeScalableTargets"
DYNAMODB_SERVICE = "dynamodb"


def validate_custom_role_permissions(iam_client, role_name: str) -> list[str]:
    """Check that a custom role meets the README's documented minimums.

    Returns a list of warning messages (empty if the role looks good).
    Gracefully returns whatever it could determine on any IAM API failure --
    it never raises.
    """
    warnings: list[str] = []

    # --- 1. Role name prefix (deterministic, no API call) ---
    warnings.extend(_check_name_prefix(role_name))

    try:
        role_resp = iam_client.get_role(RoleName=role_name)
    except Exception:
        log.debug(f"Could not inspect role '{role_name}' — skipping permission validation")
        return warnings

    # --- 2. Trust policy allows the Glue service principal ---
    trust_doc = role_resp.get("Role", {}).get("AssumeRolePolicyDocument", {})
    warnings.extend(_check_trust_policy(trust_doc))

    try:
        attached = iam_client.list_attached_role_policies(RoleName=role_name)
        attached_arns = [p["PolicyArn"] for p in attached.get("AttachedPolicies", [])]
    except Exception:
        log.debug(f"Could not list attached policies for '{role_name}' — skipping the rest")
        return warnings

    # --- 3. Baseline Glue execution managed policy (named, ARN-matched) ---
    warnings.extend(_check_glue_baseline(attached_arns))

    # --- 4-7. Capability checks against the role's effective allowed actions ---
    allowed = _collect_allowed_actions(iam_client, role_name, attached_arns)
    if allowed is None:
        # Couldn't read policy documents (e.g. missing iam:GetPolicy /
        # GetRolePolicy). Skip capability checks rather than false-warn.
        log.debug(
            f"Could not read policy documents for '{role_name}' — "
            f"skipping pricing/quota/autoscaling/DynamoDB checks"
        )
        return warnings

    warnings.extend(_check_capabilities(allowed))
    return warnings


def _check_name_prefix(role_name: str) -> list[str]:
    """README: the custom role name should start with AWSGlueServiceRole."""
    if role_name.startswith(ROLE_NAME_PREFIX):
        return []
    return [
        f"Role name '{role_name}' does not start with '{ROLE_NAME_PREFIX}'. "
        f"The bootstrap passrole permission is scoped to role names beginning "
        f"with '{ROLE_NAME_PREFIX}', so Glue may not be able to assume this role."
    ]


def _check_trust_policy(trust_doc: dict) -> list[str]:
    """Verify glue.amazonaws.com is a trusted principal."""
    for statement in trust_doc.get("Statement", []):
        principal = statement.get("Principal", {})
        services = principal.get("Service", [])
        if isinstance(services, str):
            services = [services]
        if GLUE_SERVICE_PRINCIPAL in services:
            return []

    return [
        f"Role trust policy does not include {GLUE_SERVICE_PRINCIPAL} as a "
        f"trusted principal. The Glue job will not be able to assume this role."
    ]


def _check_glue_baseline(attached_arns: list[str]) -> list[str]:
    """README: attach the managed policy AWSGlueServiceRole."""
    if GLUE_BASELINE_POLICY_ARN in attached_arns:
        return []
    return [
        "Role is missing the AWSGlueServiceRole managed policy "
        f"({GLUE_BASELINE_POLICY_ARN}). The Glue job requires it for its "
        "baseline execution permissions (S3, CloudWatch, etc.)."
    ]


def _check_capabilities(allowed: set[str]) -> list[str]:
    """Check the documented pricing / quota / autoscaling / DynamoDB capabilities."""
    warnings: list[str] = []

    if not all(_action_allowed(allowed, a) for a in PRICING_ACTIONS):
        warnings.append(
            "Role cannot call pricing:GetProducts. The job uses it to estimate "
            "DynamoDB operation costs. Attach AWSPriceListServiceFullAccess, or "
            "for maximum lockdown allow the pricing:GetProducts action inline."
        )

    if not all(_action_allowed(allowed, a) for a in QUOTA_ACTIONS):
        warnings.append(
            "Role cannot read Service Quotas (servicequotas:GetServiceQuota, "
            "servicequotas:GetAWSDefaultServiceQuota). The job uses these to "
            "detect account-level read/write limits. Attach "
            "ServiceQuotasReadOnlyAccess, or for maximum lockdown allow those "
            "servicequotas actions inline."
        )

    if not _action_allowed(allowed, AUTOSCALING_ACTION):
        # Soft: the job still runs without this, it just skips the
        # autoscaling-aware capacity warning (README).
        warnings.append(
            "Role cannot call application-autoscaling:DescribeScalableTargets. "
            "The job still runs, but it will skip the autoscaling-aware capacity "
            "warning. To enable it, allow that action on Resource \"*\"."
        )

    if not _has_action_in_service(allowed, DYNAMODB_SERVICE):
        warnings.append(
            "Role has no DynamoDB permissions. Attach AmazonDynamoDBReadOnlyAccess "
            "or AmazonDynamoDBFullAccess, or a more restrictive policy that grants "
            "the DynamoDB actions your operation needs on your target tables."
        )

    return warnings


def _collect_allowed_actions(
    iam_client, role_name: str, attached_arns: list[str]
) -> set[str] | None:
    """Gather every action Allow'd by the role's managed + inline policies.

    Returns a set of action strings (e.g. {'pricing:GetProducts', 'dynamodb:*'})
    or None if the policy documents could not be read (caller then skips the
    capability checks rather than false-warning).
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
    subtracted (see module docstring).
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


def _action_allowed(allowed: set[str], action: str) -> bool:
    """Is `action` granted by the collected action set, honoring `*` wildcards?

    Matches a specific action against patterns like ``*``, ``pricing:*``, or an
    exact action string. Case-insensitive.
    """
    target = action.lower()
    for pattern in allowed:
        if pattern == "*":
            return True
        if pattern.endswith("*"):
            if target.startswith(pattern[:-1]):
                return True
        elif pattern == target:
            return True
    return False


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
