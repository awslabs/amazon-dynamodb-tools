"""Shell out to ./bulk for an e2e test.

Captures stdout, stderr, exit code, the wall-time the wrapper logged to
CloudWatch, and the Glue job-run ID so perf.py can fetch DPU-seconds.
"""
from __future__ import annotations

import re
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
BULK_CLI = REPO_ROOT / "bulk"
# Force the venv interpreter — system python3 lacks boto3.
VENV_PYTHON = REPO_ROOT / ".venv" / "bin" / "python"

# Wrapper emits one of:
#   [connector] read setup for 't' took 0.123s (...)
#   [connector] write of 't' completed in 0.123s
#   [connector] count of 't' completed in 0.123s
_PERF_LINE = re.compile(
    r"\[connector\][^\n]*?(?:took|completed in)\s+(?P<seconds>[\d.]+)s"
)
# Glue prints the job run id to its own stdout; the runner prints it via
# 'Job run id: jr_<hash>' in the LiveTail stream.
_JOB_RUN_LINE = re.compile(r"Job run id:\s*(?P<run_id>jr_[0-9a-f]+)", re.IGNORECASE)


@dataclass
class VerbResult:
    verb: str
    exit_code: int
    stdout: str
    stderr: str
    wall_seconds: float | None  # parsed from [connector] line; None if absent
    job_run_id: str | None      # parsed from CLI stream; None if absent

    @property
    def succeeded(self) -> bool:
        return self.exit_code == 0


def run_verb(
    verb: str,
    *,
    table: str,
    extra_args: list[str] | None = None,
    timeout_s: int = 1800,
) -> VerbResult:
    """Invoke `./bulk <verb> --table <table> ...`.

    Returns when the Glue job finishes (success or failure). Raises
    subprocess.TimeoutExpired only if the entire run exceeds ``timeout_s``
    (default 30 min — covers worst-case Glue cold start + runtime).
    """
    cmd = [
        str(VENV_PYTHON), str(BULK_CLI), verb,
        "--table", table,
    ] + (extra_args or [])

    proc = subprocess.run(
        cmd,
        cwd=str(REPO_ROOT),
        capture_output=True,
        text=True,
        timeout=timeout_s,
    )

    combined = proc.stdout + "\n" + proc.stderr
    wall = _scrape_wall(combined)
    run_id = _scrape_run_id(combined)

    # Mirror the live output to the dev's terminal so they can watch progress.
    sys.stdout.write(proc.stdout)
    sys.stderr.write(proc.stderr)

    return VerbResult(
        verb=verb,
        exit_code=proc.returncode,
        stdout=proc.stdout,
        stderr=proc.stderr,
        wall_seconds=wall,
        job_run_id=run_id,
    )


def _scrape_wall(stream: str) -> float | None:
    """Pick the first [connector] timing line from the run."""
    match = _PERF_LINE.search(stream)
    if match:
        return float(match.group("seconds"))
    return None


def _scrape_run_id(stream: str) -> str | None:
    match = _JOB_RUN_LINE.search(stream)
    return match.group("run_id") if match else None
