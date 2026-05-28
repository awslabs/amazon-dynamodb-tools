"""Shared fixtures + pytest config for the e2e suite."""
from __future__ import annotations

import pytest

from tests.e2e.helpers.aws_guard import assert_account_matches
from tests.e2e.helpers.config import E2EConfig, load_or_prompt


def pytest_configure(config):
    config.addinivalue_line(
        "markers",
        "e2e: real-AWS end-to-end test; opt-in via 'make test-e2e-*'",
    )


@pytest.fixture(scope="session")
def e2e_config(request) -> E2EConfig:
    """Resolve config (prompting on first run) and verify ambient AWS account."""
    suite = request.config.getoption("--e2e-suite", default="connector smoke")
    cfg = load_or_prompt(suite)
    assert_account_matches(cfg.aws_account_id, cfg.aws_region)
    return cfg


def pytest_addoption(parser):
    parser.addoption(
        "--e2e-suite",
        action="store",
        default="connector smoke",
        help="Suite name to display in the first-run cost banner.",
    )
