"""Create a short-lived IAM user with a single inline policy, return creds.

Used by the real-bootstrap security test. Always tears down even on failure
(context manager). Names include a uuid so concurrent runs don't collide.
"""
from __future__ import annotations

import contextlib
import json
import time
import uuid
from typing import Iterator, Any

import boto3
from botocore.exceptions import ClientError


def _wait_until_creds_are_usable(creds: dict[str, str], timeout_s: int = 30) -> None:
    """Poll sts:GetCallerIdentity until IAM eventual consistency catches up.

    A freshly-created access key can return InvalidClientTokenId for several
    seconds before the propagation completes. Polling beats a fixed sleep:
    most runs unblock in ~3-5s, and we still cap at timeout_s to fail fast
    if propagation actually stalls.
    """
    sts = boto3.client(
        "sts",
        aws_access_key_id=creds["aws_access_key_id"],
        aws_secret_access_key=creds["aws_secret_access_key"],
    )
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        try:
            sts.get_caller_identity()
            return
        except ClientError as e:
            if e.response["Error"]["Code"] != "InvalidClientTokenId":
                raise
            time.sleep(1)
    raise TimeoutError(
        f"Temp IAM credentials never became usable after {timeout_s}s "
        f"(IAM eventual-consistency stall?)"
    )


@contextlib.contextmanager
def temp_iam_user_with_policy(policy: dict[str, Any]) -> Iterator[dict[str, str]]:
    """Yield {'aws_access_key_id': ..., 'aws_secret_access_key': ...} for a temp user.

    Cleans up the user, access key, and inline policy on exit even if the test fails.
    """
    iam = boto3.client("iam")
    suffix = uuid.uuid4().hex[:8]
    user_name = f"bulk-e2e-security-{suffix}"
    policy_name = f"bulk-bootstrap-{suffix}"
    access_key_id: str | None = None

    try:
        iam.create_user(UserName=user_name, Tags=[
            {"Key": "purpose", "Value": "bulk_executor e2e security test"},
            {"Key": "ephemeral", "Value": "true"},
        ])
        iam.put_user_policy(
            UserName=user_name,
            PolicyName=policy_name,
            PolicyDocument=json.dumps(policy),
        )
        key_resp = iam.create_access_key(UserName=user_name)
        access_key_id = key_resp["AccessKey"]["AccessKeyId"]
        secret = key_resp["AccessKey"]["SecretAccessKey"]
        creds = {
            "aws_access_key_id": access_key_id,
            "aws_secret_access_key": secret,
        }

        _wait_until_creds_are_usable(creds)

        yield creds
    finally:
        # Always clean up, regardless of test outcome.
        if access_key_id:
            with contextlib.suppress(ClientError):
                iam.delete_access_key(UserName=user_name, AccessKeyId=access_key_id)
        with contextlib.suppress(ClientError):
            iam.delete_user_policy(UserName=user_name, PolicyName=policy_name)
        with contextlib.suppress(ClientError):
            iam.delete_user(UserName=user_name)
