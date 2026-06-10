"""Security suite fixtures: parsed policy + per-probe parameterization."""
from __future__ import annotations

import pytest

from tests.e2e.security.actions import ActionProbe, probes_for
from tests.e2e.security.job_state_guard import restore_job_role, snapshot_job_role
from tests.e2e.security.policy import parse_bootstrap_policy


@pytest.fixture(scope="session", autouse=True)
def preserve_shared_glue_job(e2e_config):
    """Snapshot + restore the shared bulk_dynamodb Glue job role around this
    suite, so security tests (which bootstrap READ-ONLY and/or tear the job
    down) don't silently flip a developer's READ-WRITE job — which the
    command/connector write smokes depend on. Net-zero and reversible: puts
    the job's role back exactly as it was."""
    original_role = snapshot_job_role(e2e_config.aws_region)
    try:
        yield
    finally:
        restore_job_role(e2e_config.aws_region, original_role)


@pytest.fixture(scope="session")
def bootstrap_policy() -> dict:
    return parse_bootstrap_policy()


@pytest.fixture(scope="session")
def probes(e2e_config) -> list[ActionProbe]:
    return probes_for(e2e_config.aws_account_id, e2e_config.aws_region)
