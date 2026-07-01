"""Command e2e: clean error messages for bad parameters.

Verifies that when a user passes bad parameters (missing required args,
non-existent table, invalid source/target), the CLI produces clean
human-readable error messages rather than Python stack traces.
"""
from __future__ import annotations

import pytest

from tests.e2e.helpers.command_runner import run_command_raw


@pytest.mark.e2e
class TestErrorMessages:
    """Bad parameters produce clean error messages, not tracebacks."""

    def test_missing_required_arg_shows_clean_error(self):
        """Omitting a required argument produces an argparse error, no traceback."""
        result = run_command_raw("copy", args=["--source", "some-table"])
        combined = result.stdout + result.stderr
        assert result.exit_code != 0
        assert "Traceback" not in combined
        assert "error:" in combined.lower()

    def test_nonexistent_table_shows_clean_error(self, e2e_config):
        """A table that doesn't exist produces a readable message."""
        result = run_command_raw(
            "fill",
            args=["--table", "bulk-executor-nonexistent-table-xyz-99"],
        )
        combined = result.stdout + result.stderr
        assert result.exit_code != 0
        assert "Traceback" not in combined

    def test_same_source_and_target_shows_clean_error(self, e2e_config):
        """copy --source X --target X produces a parser error, no traceback."""
        result = run_command_raw(
            "copy",
            args=["--source", "same-table", "--target", "same-table"],
        )
        combined = result.stdout + result.stderr
        assert result.exit_code != 0
        assert "Traceback" not in combined
        assert "must be different" in combined.lower() or "error" in combined.lower()
