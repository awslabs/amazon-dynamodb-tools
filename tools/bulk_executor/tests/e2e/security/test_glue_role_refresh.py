"""Tier 2 e2e: version-mismatch role refresh converges against real IAM.

Truth oracle for the #84 fix (PR #233). The unit tests assert that
``update_assume_role_policy`` is *called* on a version-mismatch refresh, but a
MagicMock can't prove real IAM accepts that call or that the refreshed role
ends up matching a fresh bootstrap. This proves it end-to-end against real IAM.

**Isolation (see AGENTS.md invariants #2 and #3):** this test creates its OWN
throwaway role and never touches the shared ``bulk_dynamodb`` Glue job or its
role. That means it is safe under parallel runs (unique uuid per run) and can't
disturb a live Glue job -- unlike anything that re-bootstraps the shared job.

To stay isolated we drive the real ``BootstrapInfrastructure._add_glue_job_role``
refresh path in-process rather than shelling out to ``./bulk bootstrap`` (a full
bootstrap always repoints the SHARED job regardless of ``--XRole``). The only
thing stubbed is ``_get_glue_job_details`` -- the version *source* that triggers
the refresh, not the behavior under test. The behavior under test (real
``update_assume_role_policy`` + policy attach against real IAM) is not mocked.

Flow:
  1. Create a throwaway role with a STALE trust policy (a bogus ``ec2``
     principal a fresh bootstrap would never produce).
  2. Point bootstrap at it as a custom ``--XRole`` and force a version mismatch.
  3. Run the real refresh path (real IAM ``update_assume_role_policy``).
  4. Assert via a real ``iam.get_role`` that the trust policy converged to the
     glue-only shape a fresh bootstrap creates -- the stale principal is gone.
  5. Delete the throwaway role (and detach everything) in ``finally``.

Cost: ~$0 (IAM only, no Glue job runs). Runtime: ~15s + IAM propagation.
"""
from __future__ import annotations

import json
import sys
import time
from pathlib import Path
from unittest.mock import MagicMock

import boto3
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "client" / "src"))
from infrastructure.bootstrap import BootstrapInfrastructure  # noqa: E402
from infrastructure.constants import ROLE_TYPE_READ_ONLY  # noqa: E402

FRESH_TRUST_PRINCIPAL = "glue.amazonaws.com"
STALE_TRUST_PRINCIPAL = "ec2.amazonaws.com"

_STALE_TRUST_POLICY = {
    "Version": "2012-10-17",
    "Statement": [
        {"Effect": "Allow", "Principal": {"Service": FRESH_TRUST_PRINCIPAL}, "Action": "sts:AssumeRole"},
        {"Effect": "Allow", "Principal": {"Service": STALE_TRUST_PRINCIPAL}, "Action": "sts:AssumeRole"},
    ],
}


def _trust_principals(iam, role_name: str) -> list[str]:
    doc = iam.get_role(RoleName=role_name)["Role"]["AssumeRolePolicyDocument"]
    principals: list[str] = []
    for stmt in doc["Statement"]:
        svc = stmt.get("Principal", {}).get("Service")
        principals.extend(svc if isinstance(svc, list) else [svc])
    return principals


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


def test_version_mismatch_refresh_converges_trust_policy_real_iam(e2e_config):
    """A version-mismatch refresh must overwrite a stale trust policy so the
    role matches a fresh bootstrap -- verified against real IAM, on a throwaway
    role that touches no shared state."""
    iam = boto3.client("iam")
    # Prefix satisfies bootstrap's AWSGlueServiceRole naming expectation; uuid
    # (via time-free token) keeps parallel runs from colliding.
    suffix = boto3.client("sts").get_caller_identity()["Account"][-4:] + hex(int(time.time()))[-6:]
    role_name = f"AWSGlueServiceRole-bulk-e2e-refresh-{suffix}"

    try:
        # --- 1. Throwaway role with a STALE trust policy ---
        iam.create_role(
            RoleName=role_name,
            AssumeRolePolicyDocument=json.dumps(_STALE_TRUST_POLICY),
            Tags=[
                {"Key": "purpose", "Value": "bulk_executor e2e role-refresh test"},
                {"Key": "ephemeral", "Value": "true"},
            ],
        )
        # IAM is eventually consistent; give the role a moment to be visible.
        for _ in range(10):
            try:
                iam.get_role(RoleName=role_name)
                break
            except iam.exceptions.NoSuchEntityException:
                time.sleep(1)

        assert STALE_TRUST_PRINCIPAL in _trust_principals(iam, role_name), (
            "test setup failed: stale trust principal was not applied"
        )

        # --- 2/3. Drive the REAL refresh path in-process against the throwaway role ---
        env = MagicMock(aws_region=e2e_config.aws_region, aws_account_id=e2e_config.aws_account_id)
        bootstrap = BootstrapInfrastructure(env)  # real IAM/Glue/S3/logs clients
        # Force the version-mismatch branch WITHOUT touching the shared Glue job:
        # stub only the version source, not the behavior under test.
        bootstrap._get_glue_job_details = MagicMock(
            return_value={"Job": {"DefaultArguments": {"--bulk-dynamodb-version": "0"}}}
        )
        # Custom --XRole routes _get_role_name to our throwaway role by name.
        bootstrap._add_glue_job_role({"XRole": role_name})

        # --- 4. Assert convergence against REAL IAM (not a mock) ---
        principals = _trust_principals(iam, role_name)
        assert principals == [FRESH_TRUST_PRINCIPAL], (
            f"refreshed role trust policy did not converge to a fresh bootstrap: "
            f"expected only [{FRESH_TRUST_PRINCIPAL!r}], got {principals!r}. "
            f"The stale '{STALE_TRUST_PRINCIPAL}' principal should have been overwritten."
        )
    finally:
        # --- 5. Delete the throwaway role no matter what ---
        _delete_role(iam, role_name)
