"""Command smoke: `bulk copy`.

Three-step test: create source + target transient tables, fill the source,
copy source → target. Exercises read-from-A + write-to-B through the
wrapper in a single Spark job.

We assert both Glue jobs reached SUCCEEDED (./bulk exits 0 even on a failed
job) and that the target ends up with the same item count as the source —
"Total records copied: 0" against an empty source is exactly the false
green this suite was written to catch. (Cross-region/cross-account copy is
its own concern; a future test could exercise an ARN-based --target.)
"""
from __future__ import annotations

import pytest

from tests.e2e.helpers.assertions import (
    assert_glue_succeeded,
    assert_table_has_items,
    table_item_count,
)
from tests.e2e.helpers.transient_table import transient_table
from tests.e2e.helpers.command_runner import run_command, run_command_raw
from tests.e2e.connector.conftest import PerfRow


@pytest.mark.e2e
class TestCopySmoke:
    def test_copy_same_region(self, e2e_config, cmd_perf_collector):
        # Two transient tables created in parallel-ish (both must be ACTIVE
        # before the test runs anyway; nesting context managers serializes).
        with transient_table(e2e_config.aws_region, label="copy-src") as src, \
             transient_table(e2e_config.aws_region, label="copy-tgt") as tgt:
            seed = run_command(
                "fill",
                table=src,
                extra_args=["--numitems", "100", "--generator", "default"],
            )
            assert_glue_succeeded("copy setup (fill source)", seed, e2e_config.aws_region)
            src_count = assert_table_has_items(e2e_config.aws_region, src)

            # copy uses --source/--target, not --table.
            result = run_command_raw(
                "copy",
                args=["--source", src, "--target", tgt],
            )
            perf = assert_glue_succeeded("copy", result, e2e_config.aws_region)

            tgt_count = table_item_count(e2e_config.aws_region, tgt)
            assert tgt_count == src_count, (
                f"copy landed {tgt_count} items in target but source had "
                f"{src_count} — copy did not faithfully reproduce the table."
            )

            cmd_perf_collector.add(PerfRow(
                command="copy",
                wall_seconds=result.wall_seconds,
                dpu_seconds=perf.dpu_seconds if perf else None,
                items=tgt_count,
            ))
