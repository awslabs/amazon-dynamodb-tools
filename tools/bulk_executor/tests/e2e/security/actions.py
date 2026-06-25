"""Map each documented action to a representative resource ARN.

iam:SimulatePrincipalPolicy needs (action, resource) pairs. The README policy
constrains most actions to wildcarded ARNs; we pick a concrete instance of
each so the simulator can evaluate as it would at bootstrap time.
"""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ActionProbe:
    sid: str
    action: str
    resource: str  # ARN that exemplifies the wildcard pattern in the doc


def probes_for(account_id: str, region: str) -> list[ActionProbe]:
    """Build the (action, resource) probe list parameterized for this account/region."""
    glue_job = f"arn:aws:glue:{region}:{account_id}:job/bulk_dynamodb"
    glue_conn = f"arn:aws:glue:{region}:{account_id}:connection/bulk-dynamodb-connection"
    s3_bucket = f"arn:aws:s3:::aws-glue-bulk-dynamodb-{region}-{account_id}-test"
    log_group = f"arn:aws:logs:{region}:{account_id}:log-group:/aws-glue/jobs/bulk_dynamodb"
    glue_role = f"arn:aws:iam::{account_id}:role/AWSGlueServiceRole-bulk_dynamodb"

    return [
        # glueRoleAdmin: scoped to AWSGlueServiceRole* roles only
        ActionProbe("glueRoleAdmin", "iam:GetRole", glue_role),
        ActionProbe("glueRoleAdmin", "iam:CreateRole", glue_role),
        ActionProbe("glueRoleAdmin", "iam:DeleteRole", glue_role),
        ActionProbe("glueRoleAdmin", "iam:AttachRolePolicy", glue_role),
        ActionProbe("glueRoleAdmin", "iam:DetachRolePolicy", glue_role),
        ActionProbe("glueRoleAdmin", "iam:ListAttachedRolePolicies", glue_role),
        ActionProbe("glueRoleAdmin", "iam:PutRolePolicy", glue_role),
        ActionProbe("glueRoleAdmin", "iam:DeleteRolePolicy", glue_role),
        ActionProbe("glueRoleAdmin", "iam:ListRolePolicies", glue_role),
        # passrole: scoped to AWSGlueServiceRole* with iam:PassedToService=glue
        ActionProbe("passrole", "iam:PassRole", glue_role),
        # s3: bucket and object actions on the bootstrap bucket
        ActionProbe("s3", "s3:CreateBucket", s3_bucket),
        ActionProbe("s3", "s3:DeleteBucket", s3_bucket),
        ActionProbe("s3", "s3:ListBucket", s3_bucket),
        ActionProbe("s3", "s3:PutObject", f"{s3_bucket}/job-script.py"),
        ActionProbe("s3", "s3:DeleteObject", f"{s3_bucket}/job-script.py"),
        ActionProbe("s3", "s3:PutBucketPolicy", s3_bucket),
        # glue: job lifecycle on the named job
        ActionProbe("glue", "glue:CreateJob", glue_job),
        ActionProbe("glue", "glue:UpdateJob", glue_job),
        ActionProbe("glue", "glue:DeleteJob", glue_job),
        ActionProbe("glue", "glue:GetJob", glue_job),
        # glueConnection: connection lifecycle
        ActionProbe("glueConnection", "glue:CreateConnection", glue_conn),
        ActionProbe("glueConnection", "glue:GetConnection", glue_conn),
        ActionProbe("glueConnection", "glue:DeleteConnection", glue_conn),
        # logs: log-group setup
        ActionProbe("logs", "logs:CreateLogGroup", log_group),
        ActionProbe("logs", "logs:PutRetentionPolicy", log_group),
    ]


def passrole_context() -> list[dict]:
    """ContextEntries needed for the passrole condition (iam:PassedToService=glue)."""
    return [
        {
            "ContextKeyName": "iam:PassedToService",
            "ContextKeyValues": ["glue.amazonaws.com"],
            "ContextKeyType": "string",
        }
    ]
