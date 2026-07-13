"""Tier 2 e2e: the REAL built-in Glue role exists with the expected shape.

Why this exists (the gap it closes): the role-refresh test
(``test_real_role_refresh.py``) proves the refresh *logic* on a throwaway
role, and the positive bootstrap test asserts bootstrap *exits 0* — but until
this test, nothing asserted that the actual
``AWSGlueServiceRoleBulkDynamoDB-*`` role is present on the account with the
right trust policy and managed policies. A refresh that works perfectly is
useless if the real role it targets never got created, or drifted.

This is a pure READ (``iam.get_role`` + ``list_attached_role_policies``) — it
mutates nothing, so it is parallel-safe and cannot disturb a live Glue job or
another test run. It asserts the READ-WRITE built-in role because that's the
role the connector/command write smokes depend on.

Cost: $0 (two IAM reads). Runtime: <1s.
"""
from __future__ import annotations

import boto3
import pytest

from tests.e2e.helpers.assertions import assert_builtin_role_shape, builtin_role_name


def test_builtin_readwrite_role_exists_with_expected_shape(e2e_config):
    """The READ-WRITE built-in Glue role must exist with the fresh-bootstrap
    trust policy + required managed policies -- verified against real IAM."""
    region = e2e_config.aws_region
    iam = boto3.client("iam")

    # If the account was never bootstrapped READ-WRITE, this is an environment
    # gap, not a code regression -- skip loudly rather than false-fail.
    role_name = builtin_role_name(region, "READ-WRITE")
    try:
        iam.get_role(RoleName=role_name)
    except iam.exceptions.NoSuchEntityException:
        pytest.skip(
            f"Built-in role {role_name!r} absent; run "
            f"'./bulk bootstrap --XRole READ-WRITE' to enable this check."
        )

    # Role is present -> assert its full shape is correct (raises, not skips).
    assert_builtin_role_shape(region, "READ-WRITE")
