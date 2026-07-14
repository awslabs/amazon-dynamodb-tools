"""Whole-system suite fixtures: reuse the connector perf collector + report."""
from __future__ import annotations

import pytest

from tests.e2e.connector.conftest import PerfCollector


@pytest.fixture(scope="session")
def ws_perf_collector() -> PerfCollector:
    return PerfCollector()


@pytest.fixture(scope="session", autouse=True)
def render_ws_report_at_end(ws_perf_collector):
    yield
    from tests.e2e.connector.report import render_report
    render_report(ws_perf_collector.rows)
