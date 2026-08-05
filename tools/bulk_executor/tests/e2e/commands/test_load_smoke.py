"""Command smoke: `bulk load` PITR safety gate.

Verifies that load refuses to run against a table without Point-in-Time
Recovery enabled — the new exit-with-error path added as a safety gate.
"""
from __future__ import annotations

import pytest

from tests.e2e.helpers.transient_table import transient_table
from tests.e2e.helpers.command_runner import run_command


@pytest.mark.e2e
class TestLoadPitrGate:
    def test_load_rejects_table_without_pitr(self, e2e_config):
        """Load must exit non-zero with a PITR error when PITR is disabled."""
        with transient_table(e2e_config.aws_region, pitr=False, label="load-nopitr") as table:
            result = run_command(
                "load",
                table=table,
                extra_args=["--format", "json", "--s3-path", "s3://fake-bucket/fake-path"],
            )
            assert result.exit_code != 0, (
                "load should abort when PITR is disabled"
            )
            assert "point in time recovery" in (result.stdout + result.stderr).lower(), (
                "load should print a clear PITR error message"
            )
