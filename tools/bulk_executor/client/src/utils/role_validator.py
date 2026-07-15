"""Validate minimum permissions on a custom IAM role for the Bulk Executor Glue job.

Returns a list of human-readable warning strings. Empty list means all checks pass.
Never raises or exits — validation failures are advisory only.
"""
from __future__ import annotations

from utils.logger import log

GLUE_SERVICE_PRINCIPAL = "glue.amazonaws.com"

REQUIRED_MANAGED_POLICY_ARNS = [
    "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole",
]

DYNAMODB_POLICY_ARNS = [
    "arn:aws:iam::aws:policy/AmazonDynamoDBReadOnlyAccess",
    "arn:aws:iam::aws:policy/AmazonDynamoDBFullAccess",
]


def validate_custom_role_permissions(
    iam_client, role_name: str
) -> list[str]:
    """Check that a custom role has the minimum permissions for the Glue job.

    Returns a list of warning messages (empty if role looks good).
    Gracefully returns [] on any IAM API failure.
    """
    warnings: list[str] = []

    try:
        role_resp = iam_client.get_role(RoleName=role_name)
    except Exception:
        log.debug(f"Could not inspect role '{role_name}' — skipping permission validation")
        return []

    try:
        policies_resp = iam_client.list_attached_role_policies(RoleName=role_name)
    except Exception:
        log.debug(f"Could not list policies for role '{role_name}' — skipping permission validation")
        return []

    trust_doc = role_resp["Role"]["AssumeRolePolicyDocument"]
    warnings.extend(_check_trust_policy(trust_doc))

    attached_arns = [
        p["PolicyArn"] for p in policies_resp.get("AttachedPolicies", [])
    ]
    warnings.extend(_check_managed_policies(attached_arns))

    return warnings


def _check_trust_policy(trust_doc: dict) -> list[str]:
    """Verify glue.amazonaws.com is in the trust policy principals."""
    for statement in trust_doc.get("Statement", []):
        principal = statement.get("Principal", {})
        services = principal.get("Service", [])
        if isinstance(services, str):
            services = [services]
        if GLUE_SERVICE_PRINCIPAL in services:
            return []

    return [
        f"Role trust policy does not include {GLUE_SERVICE_PRINCIPAL} as a trusted principal. "
        f"The Glue job will not be able to assume this role."
    ]


def _check_managed_policies(attached_arns: list[str]) -> list[str]:
    """Check that required managed policies are attached."""
    warnings: list[str] = []

    has_glue_service = any(
        arn in attached_arns for arn in REQUIRED_MANAGED_POLICY_ARNS
    )
    if not has_glue_service:
        warnings.append(
            "Role is missing the AWSGlueServiceRole managed policy "
            "(arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole). "
            "The Glue job requires this for S3 and CloudWatch access."
        )

    has_dynamodb = any(arn in attached_arns for arn in DYNAMODB_POLICY_ARNS)
    if not has_dynamodb:
        warnings.append(
            "Role is missing a DynamoDB access policy. Attach either "
            "AmazonDynamoDBReadOnlyAccess or AmazonDynamoDBFullAccess "
            "depending on your use case."
        )

    return warnings
