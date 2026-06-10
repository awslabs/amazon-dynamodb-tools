"""Command smoke: `bulk diff`.

Three-step test: create two transient tables, fill both, run diff. Exercises
segmented scans across two tables joined via primary key. We use
--sample-fraction 0.1 to keep the smoke fast.

Tables are filled with the default generator (random keys), so they will
not match — we expect the diff to surface differences. The contract is
"the segmented-scan + join path doesn't crash on Glue 5.0". We assert all
three Glue jobs reached SUCCEEDED (./bulk exits 0 even on a failed job) and
that both seed fills actually landed items — diff over two empty tables
trivially finds no differences, which is the false green this suite catches.
"""
from __future__ import annotations

import pytest

from tests.e2e.helpers.assertions import assert_glue_succeeded, assert_table_has_items
from tests.e2e.helpers.transient_table import transient_table
from tests.e2e.helpers.command_runner import run_command
from tests.e2e.connector.conftest import PerfRow


@pytest.mark.e2e
class TestDiffSmoke:
    def test_diff_two_tables(self, e2e_config, cmd_perf_collector):
        with transient_table(e2e_config.aws_region, label="diff-a") as a, \
             transient_table(e2e_config.aws_region, label="diff-b") as b:
            for table in (a, b):
                seed = run_command(
                    "fill",
                    table=table,
                    extra_args=["--numitems", "100", "--generator", "default"],
                )
                assert_glue_succeeded(f"diff setup (fill {table})", seed, e2e_config.aws_region)
                assert_table_has_items(e2e_config.aws_region, table)

            result = run_command(
                "diff",
                table=a,
                extra_args=["--table2", b, "--sample-fraction", "0.1", "--format", "keys"],
            )
            perf = assert_glue_succeeded("diff", result, e2e_config.aws_region)

            cmd_perf_collector.add(PerfRow(
                command="diff",
                wall_seconds=result.wall_seconds,
                dpu_seconds=perf.dpu_seconds if perf else None,
                items=None,
            ))
