"""Truthful assertions for command smokes.

Why this exists: ``./bulk`` exits 0 even when its underlying Glue job
*fails* — so asserting on the CLI exit code alone produces false-green
tests (a crashed `fill` leaves an empty table, and downstream `copy`/`diff`
then "succeed" trivially against nothing). These helpers assert against the
authoritative signals instead:

  * the Glue job run's terminal ``JobRunState`` (not the CLI exit code), and
  * the actual table contents after a write command.

They also gate the whole write-command suite on the deployed Glue job being
bootstrapped with a write-capable role, so we fail fast with a clear
remediation message instead of false-passing on a read-only account.
"""
from __future__ import annotations

import boto3

from tests.e2e.helpers.command_runner import CommandResult
from tests.e2e.helpers.perf import GLUE_JOB_NAME, JobRunPerf, fetch_perf

# Role-name fragment that marks the read-only bootstrap variant. The Glue
# job's role is fixed at bootstrap time (see client/src/infrastructure/
# bootstrap.py) — a read-only role cannot run fill/update/delete/copy.
_READ_ONLY_ROLE_FRAGMENT = "DdbReadOnly"


def deployed_job_role_name(region: str) -> str:
    """Return the IAM role name baked into the deployed Glue job."""
    glue = boto3.client("glue", region_name=region)
    job = glue.get_job(JobName=GLUE_JOB_NAME)
    return job["Job"]["DefaultArguments"]["--glue-job-role-name"]


# Built-in role naming + the managed policies bootstrap attaches. Kept here
# (not imported from client/src) so the assertion is an INDEPENDENT oracle:
# if bootstrap's own constants drift, this test still checks the contract we
# actually expect on the account, and the mismatch surfaces as a failure.
_BUILTIN_ROLE_PREFIX = "AWSGlueServiceRoleBulkDynamoDB"
_GLUE_SERVICE_MANAGED_POLICY = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
_DDB_MANAGED_POLICY = {
    "READ-WRITE": "arn:aws:iam::aws:policy/AmazonDynamoDBFullAccess",
    "READ-ONLY": "arn:aws:iam::aws:policy/AmazonDynamoDBReadOnlyAccess",
}


def builtin_role_name(region: str, access: str) -> str:
    """The built-in Glue role name a fresh bootstrap creates for ``access``
    ('READ-WRITE' or 'READ-ONLY'). Mirrors bootstrap._get_role_name."""
    role_id = "DdbReadWrite" if access == "READ-WRITE" else "DdbReadOnly"
    return f"{_BUILTIN_ROLE_PREFIX}-{role_id}-{region}"


def assert_builtin_role_shape(region: str, access: str) -> None:
    """Assert the REAL built-in Glue role exists with the fresh-bootstrap shape.

    This is the existence/shape oracle the throwaway role-refresh test can't
    provide: the refresh test proves the *logic* on a disposable role, but says
    nothing about whether the actual ``AWSGlueServiceRoleBulkDynamoDB-*`` role
    is present and correctly formed on the account. Checks:

      1. the role exists (``iam.get_role``),
      2. its trust policy allows exactly ``glue.amazonaws.com``, and
      3. the README-required managed policies are attached (the Glue service
         role + the DynamoDB access policy matching ``access``).

    Read-only: never mutates the role, so it's parallel-safe and has no blast
    radius. Raises AssertionError (not skip) if the role is missing/misshapen —
    a missing built-in role is a real regression, not an environmental gap.
    """
    iam = boto3.client("iam")
    role_name = builtin_role_name(region, access)

    try:
        role = iam.get_role(RoleName=role_name)["Role"]
    except iam.exceptions.NoSuchEntityException:
        raise AssertionError(
            f"Built-in Glue role {role_name!r} does not exist. A "
            f"'{access}' bootstrap should have created it. Run "
            f"'./bulk bootstrap --XRole {access}' and re-check."
        )

    principals = []
    for stmt in role["AssumeRolePolicyDocument"]["Statement"]:
        svc = stmt.get("Principal", {}).get("Service")
        principals.extend(svc if isinstance(svc, list) else [svc])
    assert principals == ["glue.amazonaws.com"], (
        f"Built-in role {role_name!r} trust policy is not the fresh-bootstrap "
        f"shape: expected only ['glue.amazonaws.com'], got {principals!r}."
    )

    attached = {
        p["PolicyArn"]
        for p in iam.list_attached_role_policies(RoleName=role_name)["AttachedPolicies"]
    }
    required = {_GLUE_SERVICE_MANAGED_POLICY, _DDB_MANAGED_POLICY[access]}
    missing = required - attached
    assert not missing, (
        f"Built-in role {role_name!r} is missing required managed policies: "
        f"{sorted(missing)}. Attached: {sorted(attached)}."
    )


def require_write_capable_job(region: str) -> None:
    """Fail the suite fast if the deployed Glue job can't write.

    Write commands (fill/update/delete/copy) cannot succeed when the job is
    bootstrapped with the read-only role. Detecting that up front turns a
    confusing mid-suite ``BatchWriteItem``-denied crash into one actionable
    message.
    """
    role = deployed_job_role_name(region)
    if _READ_ONLY_ROLE_FRAGMENT in role:
        raise AssertionError(
            f"Glue job '{GLUE_JOB_NAME}' is bootstrapped with a read-only role "
            f"({role!r}); write commands cannot succeed. Re-point it with a "
            f"read-write role:\n"
            f"    ./bulk bootstrap --XRole READ-WRITE\n"
            f"(the DdbReadWrite-{region} role already exists on accounts that "
            f"have run a read-write bootstrap before)."
        )


def assert_glue_succeeded(
    command: str, result: CommandResult, region: str
) -> JobRunPerf | None:
    """Assert the command's Glue job reached SUCCEEDED — not just exit 0.

    ``./bulk`` returns 0 even on a failed Glue job, so we check the
    authoritative ``JobRunState`` from ``glue.get_job_run``. The CLI runs
    synchronously, so by the time it returns the job is already terminal —
    no polling loop needed.

    Returns the perf record (so the caller can log DPU-seconds) or ``None``
    if no job-run id was scraped (e.g. a command that errored client-side
    before launching a job — which we surface via the exit-code gate below).
    """
    # Cheap first gate: a non-zero exit is unambiguous failure.
    assert result.succeeded, (
        f"{command}: ./bulk exited {result.exit_code}\n{result.stderr[-1000:]}"
    )

    # A write/read command that launched a job must expose its run id; its
    # absence means the job never started (client-side failure swallowed by
    # the exit code).
    assert result.job_run_id, (
        f"{command}: no Glue job-run id in output — job never launched.\n"
        f"{result.stdout[-1000:]}"
    )

    perf = fetch_perf(result.job_run_id, region)
    assert perf is not None, f"{command}: could not fetch job run {result.job_run_id}"
    assert perf.job_run_state == "SUCCEEDED", (
        f"{command}: Glue job {result.job_run_id} ended {perf.job_run_state!r} "
        f"(not SUCCEEDED), even though ./bulk exited 0.\n{result.stderr[-1000:]}"
    )
    return perf


def table_item_count(region: str, table: str) -> int:
    """Count items in a (small, transient) table via a consistent scan.

    DescribeTable's ItemCount is only refreshed ~every 6h, so it reads 0 for
    a just-filled table. A COUNT scan is authoritative and cheap at smoke
    sizes (~100 items).
    """
    ddb = boto3.client("dynamodb", region_name=region)
    total = 0
    kwargs = {"TableName": table, "Select": "COUNT", "ConsistentRead": True}
    while True:
        resp = ddb.scan(**kwargs)
        total += resp["Count"]
        last_key = resp.get("LastEvaluatedKey")
        if not last_key:
            return total
        kwargs["ExclusiveStartKey"] = last_key


def assert_table_has_items(region: str, table: str) -> int:
    """Assert a table is non-empty (proves a write command actually wrote).

    Returns the observed count so callers can record it in the perf report.
    """
    count = table_item_count(region, table)
    assert count > 0, (
        f"table {table!r} is empty after a write command — the job reported "
        f"success but nothing landed."
    )
    return count
