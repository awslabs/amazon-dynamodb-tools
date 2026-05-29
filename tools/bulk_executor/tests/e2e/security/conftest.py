"""Security suite fixtures: parsed policy + per-probe parameterization."""
from __future__ import annotations

import pytest

from tests.e2e.security.actions import ActionProbe, probes_for
from tests.e2e.security.policy import parse_bootstrap_policy


@pytest.fixture(scope="session")
def bootstrap_policy() -> dict:
    return parse_bootstrap_policy()


@pytest.fixture(scope="session")
def probes(e2e_config) -> list[ActionProbe]:
    return probes_for(e2e_config.aws_account_id, e2e_config.aws_region)
