"""Connector smoke: `bulk scancount` (parallel segmented scan, not the connector).

`scancount` counts a table via a parallel segmented `Select=COUNT` scan across
Spark workers rather than the DynamoDB DataFrame connector. It lives in the
connector suite because it is a read-count path exercised the same way as
`count`. Two smokes here:

- the plain count returns a plausible number (wiring doesn't crash), and
- `--per-segment` emits the skew report (segment table + skew ratio) so the
  feature's user-visible output actually renders end-to-end on real Glue.

Smoke, not true e2e: a small read table proves the report renders and the job
reaches SUCCEEDED, not that any particular skew ratio is correct.
"""
import re

import pytest

from tests.e2e.connector.conftest import PerfRow
from tests.e2e.helpers.assertions import assert_glue_succeeded
from tests.e2e.helpers.command_runner import run_command

# scancount prints "Total records counted: 1,234"
_TOTAL_LINE = re.compile(r"Total records counted:\s*([\d,]+)")
# the per-segment report prints "Skew ratio (max/mean): 1.23x"
_SKEW_LINE = re.compile(r"Skew ratio \(max/mean\):\s*([\d.]+)x")


def _parse_total(stdout: str) -> int:
    match = _TOTAL_LINE.search(stdout)
    if not match:
        pytest.fail(
            f"Could not parse total from scancount output. Last 1000 chars:\n{stdout[-1000:]}"
        )
    return int(match.group(1).replace(",", ""))


@pytest.mark.e2e
class TestScancountSmoke:
    def test_scancount_returns_a_number(self, e2e_config, perf_collector):
        # Keep segments modest so a small read table still counts quickly.
        result = run_command(
            "scancount", table=e2e_config.read_table, extra_args=["--segments", "10"]
        )
        perf = assert_glue_succeeded("scancount", result, e2e_config.aws_region)

        total = _parse_total(result.stdout)
        assert total >= 0, f"got nonsensical total {total}"
        perf_collector.add(PerfRow(
            command="scancount",
            wall_seconds=result.wall_seconds,
            dpu_seconds=perf.dpu_seconds if perf else None,
            items=total,
        ))

    def test_scancount_per_segment_prints_skew_report(self, e2e_config, perf_collector):
        result = run_command(
            "scancount",
            table=e2e_config.read_table,
            extra_args=["--per-segment", "--segments", "10"],
        )
        perf = assert_glue_succeeded("scancount --per-segment", result, e2e_config.aws_region)

        total = _parse_total(result.stdout)
        assert total >= 0, f"got nonsensical total {total}"

        # The per-segment report must actually render: the segment table header
        # and the skew ratio summary line.
        assert "Segment" in result.stdout and "% of Total" in result.stdout, (
            f"per-segment table header missing. Last 1000 chars:\n{result.stdout[-1000:]}"
        )
        skew_match = _SKEW_LINE.search(result.stdout)
        assert skew_match, (
            f"skew ratio line missing from --per-segment output. Last 1000 chars:\n{result.stdout[-1000:]}"
        )
        assert float(skew_match.group(1)) >= 1.0, (
            f"skew ratio (max/mean) must be >= 1.0, got {skew_match.group(1)}"
        )

        perf_collector.add(PerfRow(
            command="scancount --per-segment",
            wall_seconds=result.wall_seconds,
            dpu_seconds=perf.dpu_seconds if perf else None,
            items=total,
        ))
