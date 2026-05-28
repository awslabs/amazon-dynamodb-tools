"""Refuse to run e2e tests against the wrong AWS account."""
from __future__ import annotations

import sys

import boto3


def assert_account_matches(expected_account_id: str, region: str) -> None:
    """Cross-check ambient AWS credentials against the configured account.

    The first run captures the developer's account ID into .e2e-config.
    Every subsequent run verifies the ambient credentials still resolve to
    the same account — guards against running against the wrong account
    after an aws-vault / role switch.
    """
    sts = boto3.client("sts", region_name=region)
    actual = sts.get_caller_identity()["Account"]
    if actual != expected_account_id:
        sys.exit(
            f"\nAborted: e2e suite is configured for account {expected_account_id}, "
            f"but ambient credentials resolve to {actual}.\n"
            f"Either switch your credentials (aws-vault / ada / etc.) or "
            f"delete tests/e2e/.e2e-config to re-prompt for a different account."
        )
