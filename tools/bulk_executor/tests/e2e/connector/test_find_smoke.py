"""Connector smoke: `bulk find --limit 100`.

Runs find with a 100-item limit and asserts items came back.
Captures wall-time + DPU-seconds.
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
class TestFindSmoke:
    def test_find_limit_returns_items(self, e2e_config, perf_collector):
        result = run_verb(
            "find",
            table=e2e_config.read_table,
            extra_args=["--limit", "100"],
        )
        assert result.succeeded, f"find failed: {result.stderr[-500:]}"

        items = _parse_inline_items(result.stdout)
        # bulk find prints up to 10 items inline; the rest go to S3. So we
        # only assert at-least-one inline item came back as a sanity check
        # that the connector materialized real rows.
        assert len(items) > 0, "find returned zero inline items"

        perf = fetch_perf(result.job_run_id, e2e_config.aws_region)
        perf_collector.add(PerfRow(
            verb="find",
            wall_seconds=result.wall_seconds,
            dpu_seconds=perf.dpu_seconds if perf else None,
            items=len(items),
        ))
