"""Command-suite fixtures: perf collector shared across command tests.

We reuse the connector suite's PerfRow shape so the report rendering
is consistent across both suites — only the title differs.
"""
from __future__ import annotations

import pytest

from tests.e2e.connector.conftest import PerfCollector, PerfRow  # re-export shape


@pytest.fixture(scope="session", autouse=True)
def require_write_capable_job(e2e_config):
    """Preflight: the command smokes all write, so fail fast (once) if the
    deployed Glue job was bootstrapped read-only — instead of letting every
    test crash mid-run with a BatchWriteItem-denied error."""
    from tests.e2e.helpers.assertions import require_write_capable_job as _check
    _check(e2e_config.aws_region)


@pytest.fixture(scope="session")
def cmd_perf_collector() -> PerfCollector:
    return PerfCollector()


@pytest.fixture(scope="session", autouse=True)
def render_command_report_at_end(cmd_perf_collector, request):
    yield
    from tests.e2e.commands.report import render_report
    render_report(cmd_perf_collector.rows)
