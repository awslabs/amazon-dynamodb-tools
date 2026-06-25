"""Connector smoke: `bulk count`.

Runs the read table through the (only) connector path and asserts the
returned count is plausible. Captures wall-time + DPU-seconds.
"""
import re

import pytest

from tests.e2e.connector.conftest import PerfRow
from tests.e2e.helpers.assertions import assert_glue_succeeded
from tests.e2e.helpers.command_runner import run_command

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
        result = run_command("count", table=e2e_config.read_table)
        perf = assert_glue_succeeded("count", result, e2e_config.aws_region)

        count = _parse_count(result.stdout)
        assert count >= 0, f"got nonsensical count {count}"
        perf_collector.add(PerfRow(
            command="count",
            wall_seconds=result.wall_seconds,
            dpu_seconds=perf.dpu_seconds if perf else None,
            items=count,
        ))
