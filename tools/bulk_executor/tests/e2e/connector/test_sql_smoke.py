"""Connector smoke: `bulk sql`.

Runs `SELECT * LIMIT 100` and asserts rows came back. Captures
wall-time + DPU-seconds.
"""
import json

import pytest

from tests.e2e.connector.conftest import PerfRow
from tests.e2e.helpers.perf import fetch_perf
from tests.e2e.helpers.verb_runner import run_verb


def _parse_inline_items(stdout: str) -> list[dict]:
    items = []
    for line in stdout.splitlines():
        stripped = line.strip()
        if not stripped.startswith("{") or not stripped.endswith("}"):
            continue
        try:
            items.append(json.loads(stripped))
        except json.JSONDecodeError:
            continue
    return items


@pytest.mark.e2e
class TestSqlSmoke:
    def test_sql_select_limit_returns_rows(self, e2e_config, perf_collector):
        # bulk sql treats --table as the alias inside the query. Hyphens
        # and dots in table names aren't valid SQL identifiers, so the
        # alias is sanitized.
        alias = e2e_config.read_table.replace('-', '_').replace('.', '_')
        query = f"SELECT * FROM {alias} LIMIT 100"

        result = run_verb(
            "sql",
            table=e2e_config.read_table,
            extra_args=["--query", query, "--limit", "100"],
        )
        assert result.succeeded, f"sql failed: {result.stderr[-500:]}"

        items = _parse_inline_items(result.stdout)
        assert len(items) > 0, "sql returned zero inline rows"

        perf = fetch_perf(result.job_run_id, e2e_config.aws_region)
        perf_collector.add(PerfRow(
            verb="sql",
            wall_seconds=result.wall_seconds,
            dpu_seconds=perf.dpu_seconds if perf else None,
            items=len(items),
        ))
