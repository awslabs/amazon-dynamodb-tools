"""Command smoke: `bulk delete`.

Two-step test: fill a transient table with items, then delete them with a
where-predicate that matches all rows. Exercises the read-filter-delete
path on Glue 5.0.

We assert both Glue jobs reached SUCCEEDED (./bulk exits 0 even on a failed
job), that the seed fill landed items, and that the table is empty
afterwards — a match-all delete that leaves rows behind is a real
regression, and verifying it is cheap at smoke sizes.
"""
from __future__ import annotations

import pytest

from tests.e2e.helpers.assertions import (
    assert_glue_succeeded,
    assert_table_has_items,
    table_item_count,
)
from tests.e2e.helpers.transient_table import transient_table
from tests.e2e.helpers.command_runner import run_command
from tests.e2e.connector.conftest import PerfRow


@pytest.mark.e2e
class TestDeleteSmoke:
    def test_delete_with_where_predicate(self, e2e_config, cmd_perf_collector):
        with transient_table(e2e_config.aws_region, label="delete") as table:
            seed = run_command(
                "fill",
                table=table,
                extra_args=["--numitems", "100", "--generator", "default"],
            )
            assert_glue_succeeded("delete setup (fill)", seed, e2e_config.aws_region)
            assert_table_has_items(e2e_config.aws_region, table)

            # Predicate that matches all rows (default generator's pk is a
            # 12-char alphanumeric string; this matches anything).
            result = run_command(
                "delete",
                table=table,
                extra_args=["--where", "pk is not null"],
            )
            perf = assert_glue_succeeded("delete", result, e2e_config.aws_region)

            remaining = table_item_count(e2e_config.aws_region, table)
            assert remaining == 0, (
                f"delete with match-all predicate left {remaining} items behind"
            )

            cmd_perf_collector.add(PerfRow(
                command="delete",
                wall_seconds=result.wall_seconds,
                dpu_seconds=perf.dpu_seconds if perf else None,
                items=None,
            ))
