"""Command smoke: `bulk fill`.

Exercises the wrapper's write_dynamodb_dataframe path against a transient
table. The default generator writes 3 items per call with random keys;
asking for 100 items produces ~33 generator invocations and ~99 items.

We assert the Glue job reached SUCCEEDED (not just that ./bulk exited 0 —
it exits 0 even on a failed job) and that items actually landed in the
table. We don't assert the exact item count because the default generator
is non-deterministic.
"""
from __future__ import annotations

import pytest

from tests.e2e.helpers.assertions import assert_glue_succeeded, assert_table_has_items
from tests.e2e.helpers.transient_table import transient_table
from tests.e2e.helpers.command_runner import run_command
from tests.e2e.connector.conftest import PerfRow


@pytest.mark.e2e
class TestFillSmoke:
    def test_fill_writes_items_to_a_transient_table(
        self, e2e_config, cmd_perf_collector
    ):
        with transient_table(e2e_config.aws_region, label="fill") as table:
            result = run_command(
                "fill",
                table=table,
                extra_args=["--numitems", "100", "--generator", "default"],
            )
            perf = assert_glue_succeeded("fill", result, e2e_config.aws_region)
            items = assert_table_has_items(e2e_config.aws_region, table)

            cmd_perf_collector.add(PerfRow(
                command="fill",
                wall_seconds=result.wall_seconds,
                dpu_seconds=perf.dpu_seconds if perf else None,
                items=items,  # actual count observed in the table
            ))
