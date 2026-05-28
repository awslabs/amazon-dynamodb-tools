"""Connector-suite fixtures: shared perf collector + report rendering at end."""
from __future__ import annotations

from dataclasses import dataclass

import pytest


@dataclass
class PerfRow:
    verb: str
    wall_seconds: float | None
    dpu_seconds: float | None
    items: int | None = None


class PerfCollector:
    """Session-scoped collector. Each test appends one PerfRow."""

    def __init__(self) -> None:
        self.rows: list[PerfRow] = []

    def add(self, row: PerfRow) -> None:
        self.rows.append(row)


@pytest.fixture(scope="session")
def perf_collector() -> PerfCollector:
    return PerfCollector()


@pytest.fixture(scope="session", autouse=True)
def render_report_at_end(perf_collector, request):
    """After all connector tests run, render the smoke report."""
    yield
    from tests.e2e.connector.report import render_report
    render_report(perf_collector.rows)
