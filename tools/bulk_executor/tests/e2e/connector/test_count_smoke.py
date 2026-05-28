"""Connector smoke: `bulk count`.

Runs the read table through the (only) connector path and asserts the
returned count is plausible. Captures wall-time + DPU-seconds.
"""
import re

import pytest

from tests.e2e.connector.conftest import PerfRow
from tests.e2e.helpers.perf import fetch_perf
from tests.e2e.helpers.verb_runner import run_verb

# bulk count prints "Count of matching items: 1,234"
_COUNT_LINE = re.compile(r"Count of matching items:\s*([\d,]+)")


def _parse_count(stdout: str) -> int:
    match = _COUNT_LINE.search(stdout)
    if not match:
        pytest.fail(
            f"Could not parse count from bulk output. Last 1000 chars:\n{stdout[-1000:]}"
        )
    return int(match.group(1).replace(",", ""))


@pytest.mark.e2e
class TestCountSmoke:
    def test_count_returns_a_number(self, e2e_config, perf_collector):
        result = run_verb("count", table=e2e_config.read_table)
        assert result.succeeded, f"count failed: {result.stderr[-500:]}"

        count = _parse_count(result.stdout)
        assert count >= 0, f"got nonsensical count {count}"

        perf = fetch_perf(result.job_run_id, e2e_config.aws_region)
        perf_collector.add(PerfRow(
            verb="count",
            wall_seconds=result.wall_seconds,
            dpu_seconds=perf.dpu_seconds if perf else None,
            items=count,
        ))
