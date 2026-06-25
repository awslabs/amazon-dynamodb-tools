"""Command smoke: `bulk update`.

Two-step test: fill a transient table with data, then run update across
it using the touched generator (which adds a `touched` timestamp attribute
to every item). Exercises read+transform+write through the wrapper.

We assert both Glue jobs reached SUCCEEDED (./bulk exits 0 even on a failed
job) and that the seed fill actually landed items, so update has something
to walk over. The signal is "the generator-driven update path doesn't
crash on Glue 5.0".
"""
from __future__ import annotations

import pytest

from tests.e2e.helpers.assertions import assert_glue_succeeded, assert_table_has_items
from tests.e2e.helpers.transient_table import transient_table
from tests.e2e.helpers.command_runner import run_command
from tests.e2e.connector.conftest import PerfRow


@pytest.mark.e2e
class TestUpdateSmoke:
    def test_update_with_touched_generator(self, e2e_config, cmd_perf_collector):
        with transient_table(e2e_config.aws_region, label="update") as table:
            # Seed: write items so update has something to walk over.
            seed = run_command(
                "fill",
                table=table,
                extra_args=["--numitems", "100", "--generator", "default"],
            )
            assert_glue_succeeded("update setup (fill)", seed, e2e_config.aws_region)
            assert_table_has_items(e2e_config.aws_region, table)

            # Exercise: stamp every item with a `touched` timestamp.
            result = run_command(
                "update",
                table=table,
                extra_args=["--generator", "touched"],
            )
            perf = assert_glue_succeeded("update", result, e2e_config.aws_region)

            cmd_perf_collector.add(PerfRow(
                command="update",
                wall_seconds=result.wall_seconds,
                dpu_seconds=perf.dpu_seconds if perf else None,
                items=None,  # touched on whatever fill produced
            ))
