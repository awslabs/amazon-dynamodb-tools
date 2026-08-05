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


@pytest.mark.e2e
class TestServerSideErrorMessages:
    """Reproduces the three cases hunterhacker demonstrated on #227 where a
    server-side Glue failure previously surfaced as a raw traceback (or, worse,
    as a false success). Each launches a real Glue job, so these live in the
    opt-in command suite. The assertions encode Jason's bar exactly: a clean
    one-line message, no ``GlueExceptionAnalysisListener`` blob, and a non-zero
    exit that reflects the job actually failing."""

    def test_s3_path_access_denied_shows_clean_error(self, e2e_config):
        """Case 1: a bucket the caller doesn't own returns 403.

        ``./bulk load --table <t> --s3-path s3://fakebucket/haha --format csv``
        Previously dumped a HeadObject 403 traceback; now check_s3_file_exists
        raises BulkExecutorError -> root.py sys.exit(clean message)."""
        result = run_command_raw(
            "load",
            args=[
                "--table", e2e_config.write_table,
                "--s3-path", "s3://fakebucket/haha",
                "--format", "csv",
            ],
        )
        combined = result.stdout + result.stderr
        assert result.exit_code != 0, "unowned-bucket load must fail, not succeed"
        assert "Traceback" not in combined
        assert "GlueExceptionAnalysisListener" not in combined
        assert "Access denied" in combined or "doesn't exist" in combined

    def test_negative_limit_shows_clean_error(self, e2e_config):
        """Case 2: ``./bulk find --table <t> --limit -1``.

        Spark rejects a negative limit; find.py now wraps it as
        BulkExecutorError("Invalid 'limit': ...") instead of a bare Exception."""
        result = run_command_raw(
            "find",
            args=["--table", e2e_config.read_table, "--limit", "-1"],
        )
        combined = result.stdout + result.stderr
        assert result.exit_code != 0
        assert "Traceback" not in combined
        assert "GlueExceptionAnalysisListener" not in combined
        assert "Invalid 'limit'" in combined

    def test_owned_bucket_missing_key_reports_failure(self, e2e_config):
        """Case 3 (the worst offender): a bucket we own but a key that doesn't
        exist. Previously printed a simple message but exited 0 / "Job completed
        successfully". load.run now raises BulkExecutorError BEFORE job.commit(),
        so the run is correctly marked failed."""
        missing_path = f"s3://{e2e_config.aws_account_id}-glue-job-bucket/haha"
        result = run_command_raw(
            "load",
            args=[
                "--table", e2e_config.write_table,
                "--s3-path", missing_path,
                "--format", "csv",
            ],
        )
        combined = result.stdout + result.stderr
        assert result.exit_code != 0, (
            "missing key must NOT report 'Job completed successfully' (issue #137 worst offender)"
        )
        assert "Traceback" not in combined
        assert "doesn't exist" in combined or "not accessible" in combined
        assert "completed successfully" not in combined.lower()
