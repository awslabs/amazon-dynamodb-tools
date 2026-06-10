"""Snapshot + restore the shared Glue job's role around the security suite.

Why this exists: the real-bootstrap security tests operate on the *shared*
``bulk_dynamodb`` Glue job (the same job the connector and command e2e
suites depend on). The positive test bootstraps it READ-ONLY and then
*tears it down* (deletes the job); the random-negative test bootstraps it
READ-ONLY and leaves it. Either way, a developer who had the job
bootstrapped READ-WRITE (required for the command/connector write smokes)
finds it flipped to read-only or gone after running the security suite —
silent cross-suite interference.

This guard snapshots the job's ``--glue-job-role-name`` before the suite
and restores it afterward. It is net-zero and fully reversible: it puts the
job's role back exactly as it was, makes no new IAM grant, and runs under
the ambient (admin) credentials the suite already uses. If the job was
absent before the suite it is left absent; if it existed, its prior role is
restored (recreating the job via ``bulk bootstrap`` when the positive
test's teardown deleted it).
"""
from __future__ import annotations

import os
import subprocess
from pathlib import Path

import boto3

from tests.e2e.helpers.perf import GLUE_JOB_NAME

REPO_ROOT = Path(__file__).resolve().parents[3]
BULK_CLI = REPO_ROOT / "bulk"
VENV_PYTHON = REPO_ROOT / ".venv" / "bin" / "python"


def snapshot_job_role(region: str) -> str | None:
    """Return the role name the shared Glue job currently uses, or None if
    the job doesn't exist."""
    glue = boto3.client("glue", region_name=region)
    try:
        job = glue.get_job(JobName=GLUE_JOB_NAME)
    except glue.exceptions.EntityNotFoundException:
        return None
    return job["Job"]["DefaultArguments"].get("--glue-job-role-name")


def restore_job_role(region: str, role_name: str | None) -> None:
    """Restore the shared Glue job's role to ``role_name``.

    - role_name is None  → the job didn't exist before; nothing to restore.
    - job still exists    → update its role in place (cheap Glue UpdateJob).
    - job was deleted     → recreate it via ``bulk bootstrap`` with the
                            original role passed as a custom --XRole.
    """
    if role_name is None:
        return

    glue = boto3.client("glue", region_name=region)
    try:
        job = glue.get_job(JobName=GLUE_JOB_NAME)
    except glue.exceptions.EntityNotFoundException:
        job = None

    if job is not None:
        current = job["Job"]["DefaultArguments"].get("--glue-job-role-name")
        if current == role_name:
            return  # already correct, nothing to do
        _rebootstrap_with_role(region, role_name)
        return

    # Job was deleted by the positive test's teardown — recreate it with the
    # original role so the prior state is genuinely restored.
    _rebootstrap_with_role(region, role_name)


def _rebootstrap_with_role(region: str, role_name: str) -> None:
    """Re-run bulk bootstrap pinned to a specific (existing) role name.

    Passing the full role name uses bootstrap's custom-role path, which
    re-points the job without creating or mutating any IAM role.
    """
    account_role = role_name  # bootstrap validates it starts with AWSGlueServiceRole
    env = {
        "PATH": os.environ.get("PATH", ""),
        "HOME": os.environ.get("HOME", ""),
        "AWS_DEFAULT_REGION": region,
    }
    # Pass through ambient creds (admin) so the restore has permission.
    for key in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN", "AWS_PROFILE"):
        if key in os.environ:
            env[key] = os.environ[key]

    subprocess.run(
        [str(VENV_PYTHON), str(BULK_CLI), "bootstrap", "--XRole", account_role],
        cwd=str(REPO_ROOT),
        env=env,
        capture_output=True,
        text=True,
        timeout=600,
        stdin=subprocess.DEVNULL,
    )
