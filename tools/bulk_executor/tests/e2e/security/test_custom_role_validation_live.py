"""Tier 2 e2e: the custom-role permission validator against REAL IAM roles.

Truth oracle for ``utils.role_validator.validate_custom_role_permissions``. The
unit tests feed a stubbed IAM client canned policy documents; a MagicMock can't
prove the validator's real read path (get_role -> list_attached_role_policies ->
get_policy/get_policy_version + list_role_policies/get_role_policy) actually
returns what the validator expects from live IAM, nor that its effective-action
model matches how IAM really shapes managed-policy documents. This proves it end
to end by building throwaway roles at known shapes and asserting the validator
warns exactly when it should.

Why this matters (the false-green it guards against): the validator's whole
reason to exist is to NOT warn on the documented "maximum lockdown" setups --
inline ``pricing:GetProducts`` instead of AWSPriceListServiceFullAccess, a
DynamoDB policy scoped to specific tables instead of the AWS-managed one. A
unit test can assert that because it hands the validator idealized documents;
only a live role proves the real managed-policy documents (which the validator
resolves via get_policy_version) still satisfy the capability checks.

**Isolation (see AGENTS.md invariants #2 and #3):** every test creates its OWN
throwaway ``AWSGlueServiceRole-bulk-e2e-validate-*`` role and deletes it in a
``finally``. Nothing here touches the shared ``bulk_dynamodb`` Glue job or its
role, so this is parallel-safe (unique suffix per role) and can never disturb a
live job. It is IAM-only -- no Glue job is created or run.

Cost: ~$0 (IAM create/read/delete only). Runtime: ~10-20s each + IAM propagation.
"""
from __future__ import annotations

import json
import sys
import time
from pathlib import Path

import boto3
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "client" / "src"))
from utils.role_validator import validate_custom_role_permissions  # noqa: E402

GLUE_BASELINE_ARN = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
DDB_READONLY_ARN = "arn:aws:iam::aws:policy/AmazonDynamoDBReadOnlyAccess"
PRICING_FULL_ARN = "arn:aws:iam::aws:policy/AWSPriceListServiceFullAccess"
QUOTAS_RO_ARN = "arn:aws:iam::aws:policy/ServiceQuotasReadOnlyAccess"

_GLUE_TRUST = {
    "Version": "2012-10-17",
    "Statement": [{
        "Effect": "Allow",
        "Principal": {"Service": "glue.amazonaws.com"},
        "Action": "sts:AssumeRole",
    }],
}
_EC2_TRUST = {
    "Version": "2012-10-17",
    "Statement": [{
        "Effect": "Allow",
        "Principal": {"Service": "ec2.amazonaws.com"},
        "Action": "sts:AssumeRole",
    }],
}

# The inline policy bootstrap itself attaches for pricing + quotas, plus the
# soft autoscaling action -- the documented "lockdown" grant, inline.
_LOCKDOWN_EXTRAS = {
    "Version": "2012-10-17",
    "Statement": [{
        "Effect": "Allow",
        "Action": [
            "pricing:GetProducts",
            "servicequotas:GetServiceQuota",
            "servicequotas:GetAWSDefaultServiceQuota",
            "application-autoscaling:DescribeScalableTargets",
        ],
        "Resource": "*",
    }],
}


def _unique_role_name() -> str:
    """AWSGlueServiceRole prefix (so the name-prefix check passes) + a
    collision-resistant suffix. No Math.random equivalent needed: account tail
    + a time-derived hex keeps parallel runs from colliding."""
    account_tail = boto3.client("sts").get_caller_identity()["Account"][-4:]
    return f"AWSGlueServiceRole-bulk-e2e-validate-{account_tail}{hex(int(time.time()))[-6:]}"


def _wait_visible(iam, role_name: str) -> None:
    """IAM is eventually consistent; wait for the freshly-created role to read back."""
    for _ in range(10):
        try:
            iam.get_role(RoleName=role_name)
            return
        except iam.exceptions.NoSuchEntityException:
            time.sleep(1)


def _delete_role(iam, role_name: str) -> None:
    """Detach every managed + inline policy, then delete the role. Best-effort."""
    try:
        for p in iam.list_attached_role_policies(RoleName=role_name)["AttachedPolicies"]:
            iam.detach_role_policy(RoleName=role_name, PolicyArn=p["PolicyArn"])
        for name in iam.list_role_policies(RoleName=role_name)["PolicyNames"]:
            iam.delete_role_policy(RoleName=role_name, PolicyName=name)
        iam.delete_role(RoleName=role_name)
    except iam.exceptions.NoSuchEntityException:
        pass


def _wait_attachments_stable(
    iam, role_name, expected_arns, expected_inline,
    *, stable_reads: int = 3, gap_s: float = 1.0, timeout_s: int = 60
) -> None:
    """Wait until list_attached_role_policies + list_role_policies return the
    full expected set on `stable_reads` consecutive reads.

    attach_role_policy/put_role_policy are eventually consistent. If the
    validator ran before a just-attached policy was visible it would emit a
    genuine (but spurious-for-this-test) "missing" warning -- e.g. reading the
    role before AmazonDynamoDBReadOnlyAccess is visible yields a real "no
    DynamoDB permissions" warning. Requiring the complete attached set on a few
    consecutive reads closes that window. This only waits for what WAS attached
    to become visible; it encodes no expectation about pricing/quota/etc., so a
    genuinely missing permission still warns and the assertions still catch it.
    Production roles (attached long ago) satisfy this on the first read.
    """
    want_arns = set(expected_arns)
    want_inline = set(expected_inline or ())
    deadline = time.time() + timeout_s
    consecutive = 0
    have_arns: set[str] = set()
    have_inline: set[str] = set()
    while time.time() < deadline:
        have_arns = {
            p["PolicyArn"]
            for p in iam.list_attached_role_policies(RoleName=role_name)["AttachedPolicies"]
        }
        have_inline = set(iam.list_role_policies(RoleName=role_name)["PolicyNames"])
        if want_arns <= have_arns and want_inline <= have_inline:
            consecutive += 1
            if consecutive >= stable_reads:
                return
        else:
            consecutive = 0
        time.sleep(gap_s)
    raise TimeoutError(
        f"Policies never stabilized on {role_name} after {timeout_s}s "
        f"(IAM propagation stall?): missing managed {want_arns - have_arns}, "
        f"inline {want_inline - have_inline}"
    )


def _make_role(iam, role_name, *, trust, managed_arns=(), inline=None) -> None:
    iam.create_role(
        RoleName=role_name,
        AssumeRolePolicyDocument=json.dumps(trust),
        Tags=[
            {"Key": "purpose", "Value": "bulk_executor e2e role-validation test"},
            {"Key": "ephemeral", "Value": "true"},
        ],
    )
    _wait_visible(iam, role_name)
    for arn in managed_arns:
        iam.attach_role_policy(RoleName=role_name, PolicyArn=arn)
    for name, doc in (inline or {}).items():
        iam.put_role_policy(
            RoleName=role_name, PolicyName=name, PolicyDocument=json.dumps(doc)
        )
    # attach/put are eventually consistent, so wait for the attachments to read
    # back stably before the caller validates -- otherwise the validator could
    # emit a real "missing" warning for a policy that just isn't visible yet.
    _wait_attachments_stable(iam, role_name, managed_arns, (inline or {}).keys())


def test_fully_valid_lockdown_role_produces_no_warnings(e2e_config):
    """A real role with the glue trust, baseline + DynamoDB managed policies,
    and inline lockdown pricing/quota/autoscaling must produce ZERO warnings.

    This is the false-green guard: the validator must not warn on the
    documented maximum-lockdown setup when evaluated against REAL managed-policy
    documents resolved from live IAM."""
    iam = boto3.client("iam")
    role_name = _unique_role_name()
    try:
        _make_role(
            iam, role_name,
            trust=_GLUE_TRUST,
            managed_arns=[GLUE_BASELINE_ARN, DDB_READONLY_ARN],
            inline={"MinimalExtras": _LOCKDOWN_EXTRAS},
        )
        warnings = validate_custom_role_permissions(iam, role_name)
        assert warnings == [], (
            "A fully-valid lockdown role should yield no warnings, but the "
            f"validator returned: {warnings!r}. If this fails, the validator is "
            "false-warning on a documented-valid role — the exact bug this "
            "rework fixes."
        )
    finally:
        _delete_role(iam, role_name)


def test_restrictive_inline_dynamodb_does_not_false_warn(e2e_config):
    """DynamoDB granted only via a restrictive inline policy (specific table)
    must satisfy the DynamoDB check against real IAM -- no managed DDB policy.

    README explicitly blesses "a more restrictive policy targeting specific
    tables"; the old ARN-matching validator false-warned here."""
    iam = boto3.client("iam")
    role_name = _unique_role_name()
    account = e2e_config.aws_account_id
    region = e2e_config.aws_region
    scoped_ddb = {
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Action": ["dynamodb:GetItem", "dynamodb:Query", "dynamodb:Scan"],
            "Resource": f"arn:aws:dynamodb:{region}:{account}:table/only-this-table",
        }],
    }
    try:
        _make_role(
            iam, role_name,
            trust=_GLUE_TRUST,
            managed_arns=[GLUE_BASELINE_ARN],  # NO managed DynamoDB policy
            inline={"MinimalExtras": _LOCKDOWN_EXTRAS, "ScopedDynamo": scoped_ddb},
        )
        warnings = validate_custom_role_permissions(iam, role_name)
        assert not any("no DynamoDB permissions" in w for w in warnings), (
            f"Restrictive inline DynamoDB policy should satisfy the check, but "
            f"got a DynamoDB warning. All warnings: {warnings!r}"
        )
    finally:
        _delete_role(iam, role_name)


def test_missing_pricing_and_quota_warns_against_real_iam(e2e_config):
    """A role missing the pricing + quota grants must warn -- proving the
    capability checks actually fire on a real role (not just on a mock)."""
    iam = boto3.client("iam")
    role_name = _unique_role_name()
    autoscaling_only = {
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Action": ["application-autoscaling:DescribeScalableTargets"],
            "Resource": "*",
        }],
    }
    try:
        _make_role(
            iam, role_name,
            trust=_GLUE_TRUST,
            managed_arns=[GLUE_BASELINE_ARN, DDB_READONLY_ARN],
            inline={"AutoscalingOnly": autoscaling_only},
        )
        warnings = validate_custom_role_permissions(iam, role_name)
        assert any("pricing:GetProducts" in w for w in warnings), (
            f"expected a pricing warning; got {warnings!r}"
        )
        assert any("servicequotas" in w.lower() for w in warnings), (
            f"expected a servicequotas warning; got {warnings!r}"
        )
        # DynamoDB + autoscaling ARE present, so they must NOT warn. Match the
        # DynamoDB warning by its distinctive text ("no DynamoDB permissions"),
        # NOT the bare substring "DynamoDB" -- the pricing warning above also
        # contains the word "DynamoDB" ("...estimate DynamoDB operation
        # costs"), so a substring check would false-match it.
        assert not any("no DynamoDB permissions" in w for w in warnings), (
            f"unexpected DynamoDB-permissions warning; got {warnings!r}"
        )
        assert not any("application-autoscaling" in w for w in warnings)
    finally:
        _delete_role(iam, role_name)


def test_wrong_trust_principal_warns_against_real_iam(e2e_config):
    """A role whose trust policy doesn't allow glue.amazonaws.com must warn,
    even when every permission is otherwise present -- verified on real IAM."""
    iam = boto3.client("iam")
    role_name = _unique_role_name()
    try:
        _make_role(
            iam, role_name,
            trust=_EC2_TRUST,
            managed_arns=[GLUE_BASELINE_ARN, DDB_READONLY_ARN,
                          PRICING_FULL_ARN, QUOTAS_RO_ARN],
            inline={"MinimalExtras": _LOCKDOWN_EXTRAS},
        )
        warnings = validate_custom_role_permissions(iam, role_name)
        assert any("glue.amazonaws.com" in w for w in warnings), (
            f"expected a trust-policy warning; got {warnings!r}"
        )
        # Everything else is satisfied via managed + inline, so ONLY the trust
        # warning should appear.
        assert len(warnings) == 1, (
            f"expected exactly the trust warning, got {warnings!r}"
        )
    finally:
        _delete_role(iam, role_name)
