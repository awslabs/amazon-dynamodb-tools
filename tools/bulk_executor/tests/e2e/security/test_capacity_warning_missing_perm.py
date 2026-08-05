"""Tier 2 e2e for #89: the missing-autoscaling-permission degradation, live.

PR #231's intent is graceful degradation: when the Glue job's role lacks
``application-autoscaling:DescribeScalableTargets``, bulk must NOT crash — it
skips the autoscaling-aware capacity check, logs a visibility warning naming the
missing permission, and the operation proceeds to SUCCEEDED.

There are TWO call sites that read autoscaling settings on a provisioned table,
and #231 has to cover both or the "job proceeds" promise is false:

1. ``_effective_capacity_ceiling`` — the capacity *warning* path. Already
   caught the AccessDenied and logged the visibility warning.
2. ``get_and_print_dynamodb_table_info`` — the Auto Scaling Settings *diagnostic
   print* that runs earlier (``load/__init__.py`` calls it before the write).
   This describe was UNGUARDED (pre-existing, commit 5a933d1), so a provisioned
   ``load`` with the permission denied crashed here at info-print time, before
   any data moved — verified live 2026-07-15 (jr_83361690...). The traceback
   pointed at the ``except`` re-raise (load/__init__.py:117) because ``from
   None`` masked the real origin; the true crash was the diagnostic describe.
   Now guarded: on denial it logs "Could not read autoscaling settings ...
   skipping this diagnostic" and continues.

This test proves the full graceful-degradation path live: a provisioned ``load``
whose role is explicitly denied the permission emits the visibility warning AND
reaches SUCCEEDED (no crash at either call site).

**Why this lives in security/ and not whole_system/:** to make a live job run
without the autoscaling permission we must point the shared ``bulk_dynamodb``
job at a role that lacks it — i.e. mutate the shared job's execution role. That
is exactly what the security suite's autouse ``preserve_shared_glue_job`` guard
(``job_state_guard.py``) snapshots and restores. Running this in a suite
*without* that guard would silently strand the shared job on a crippled role.
Per AGENTS.md invariant #3, role-mutating tests belong here and run isolated.

**Isolation (invariant #6):** we build a THROWAWAY role (unique uuid suffix)
that mirrors a real bootstrap role — trust policy, AWSGlueServiceRole,
AmazonDynamoDBFullAccess, pricing, quotas — plus an explicit *Deny* on
application-autoscaling:DescribeScalableTargets (a plain omission is
insufficient — AmazonDynamoDBFullAccess itself grants the action, so only an
explicit Deny actually removes it). We point the shared job at it for the
duration of the test, then restore the job's original role (Job.Role, directly)
in our own finally before deleting the throwaway role and its policies.

Cost: one live Glue job (~2 min) + IAM. Runtime: ~3-4 min incl. propagation.
"""
from __future__ import annotations

import csv
import io
import json
import sys
import time
import uuid
from pathlib import Path

import boto3
import pytest

from tests.e2e.helpers.assertions import assert_glue_succeeded
from tests.e2e.helpers.command_runner import run_command
from tests.e2e.helpers.glue_bucket import discover_bucket
from tests.e2e.helpers.perf import GLUE_JOB_NAME
from tests.e2e.helpers.transient_table import transient_table

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "client" / "src"))

NUM_ITEMS = 20
PK_PREFIX = "sec-cap-noperm"
# #231 graceful degradation: when the perm is denied, bulk names it and proceeds.
# The capacity-warning path's visibility note:
WARN_CAPACITY_SKIPPED = "the requested-rate capacity check is skipped"
# The diagnostic-print path's skip note (the call site that previously crashed):
WARN_DIAGNOSTIC_SKIPPED = "Could not read autoscaling settings"
# Both notes name the missing permission so the user knows what to grant.
MISSING_PERM = "DescribeScalableTargets"

_TRUST_POLICY = {
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {"Service": "glue.amazonaws.com"},
            "Action": "sts:AssumeRole",
        }
    ],
}
_PRICING_POLICY = {
    "Version": "2012-10-17",
    "Statement": [
        {"Effect": "Allow", "Action": ["pricing:GetProducts"], "Resource": "*"}
    ],
}
_QUOTAS_POLICY = {
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "servicequotas:GetServiceQuota",
                "servicequotas:GetAWSDefaultServiceQuota",
            ],
            "Resource": "arn:aws:servicequotas:*:*:dynamodb/*",
        }
    ],
}
_MANAGED = [
    "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole",
    "arn:aws:iam::aws:policy/AmazonDynamoDBFullAccess",
]
# AmazonDynamoDBFullAccess ITSELF grants application-autoscaling:* — so simply
# omitting the bootstrap's MinimalAutoScalingAccess inline policy does NOT
# remove the permission (the first live run proved this: the job read
# autoscaling settings fine and emitted the provisioned warning instead of the
# visibility warning). An explicit Deny overrides any Allow regardless of which
# policy granted it, which is the faithful way to model "this role cannot read
# autoscaling targets."
_DENY_AUTOSCALING_POLICY = {
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Deny",
            "Action": ["application-autoscaling:DescribeScalableTargets"],
            "Resource": "*",
        }
    ],
}


def _build_csv(run_id: str) -> str:
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(["pk", "sk", "payload"])
    for i in range(NUM_ITEMS):
        writer.writerow([f"{PK_PREFIX}-{run_id}", f"item-{i:04d}", f"data-{i}"])
    return buf.getvalue()


def _upload_fixture(run_id: str, region: str) -> str:
    bucket = discover_bucket(region)
    key = f"e2e/sec-cap-noperm/{run_id}.csv"
    boto3.client("s3", region_name=region).put_object(
        Bucket=bucket, Key=key, Body=_build_csv(run_id).encode("utf-8")
    )
    return f"s3://{bucket}/{key}"


def _create_role_without_autoscaling(iam, role_name: str) -> str:
    """Create a bootstrap-shaped role that CANNOT read autoscaling targets.

    Mirrors BootstrapInfrastructure._add_glue_job_role (trust, managed policies,
    pricing, quotas) but replaces the autoscaling *Allow* with an explicit
    *Deny* on application-autoscaling:DescribeScalableTargets. A plain omission
    is insufficient because AmazonDynamoDBFullAccess grants that action anyway;
    the explicit Deny is what actually makes the job hit AccessDenied on the
    autoscaling lookup. Returns the role ARN.
    """
    resp = iam.create_role(
        RoleName=role_name,
        AssumeRolePolicyDocument=json.dumps(_TRUST_POLICY),
        Tags=[
            # IAM tag values forbid '#' (pattern [\p{L}\p{Z}\p{N}_.:/=+\-@]*),
            # so spell out "issue 89" rather than "#89".
            {"Key": "purpose", "Value": "bulk_executor e2e issue 89 missing-perm test"},
            {"Key": "ephemeral", "Value": "true"},
        ],
    )
    for arn in _MANAGED:
        iam.attach_role_policy(RoleName=role_name, PolicyArn=arn)
    iam.put_role_policy(
        RoleName=role_name, PolicyName="MinimalPricingAccess",
        PolicyDocument=json.dumps(_PRICING_POLICY),
    )
    iam.put_role_policy(
        RoleName=role_name, PolicyName="MinimalQuotasAccess",
        PolicyDocument=json.dumps(_QUOTAS_POLICY),
    )
    # Explicit Deny on the autoscaling lookup — overrides the Allow that
    # AmazonDynamoDBFullAccess carries. This is what makes the job hit
    # AccessDenied and take the #89 visibility-warning degradation path.
    iam.put_role_policy(
        RoleName=role_name, PolicyName="DenyAutoScalingDescribe",
        PolicyDocument=json.dumps(_DENY_AUTOSCALING_POLICY),
    )
    return resp["Role"]["Arn"]


def _delete_role(iam, role_name: str) -> None:
    try:
        for p in iam.list_attached_role_policies(RoleName=role_name)["AttachedPolicies"]:
            iam.detach_role_policy(RoleName=role_name, PolicyArn=p["PolicyArn"])
        for name in iam.list_role_policies(RoleName=role_name)["PolicyNames"]:
            iam.delete_role_policy(RoleName=role_name, PolicyName=name)
        iam.delete_role(RoleName=role_name)
    except iam.exceptions.NoSuchEntityException:
        pass


def _snapshot_job_role_arn(glue) -> str:
    """Return the shared job's current execution-role ARN (Job.Role).

    NOTE: this is deliberately NOT job_state_guard.snapshot_job_role — that one
    reads the ``--glue-job-role-name`` DefaultArgument, which this test does not
    change. We mutate ``Job.Role`` directly, so we must snapshot and restore
    that exact field, or the restore is a silent no-op and the shared job is
    left pointing at the throwaway role after we delete it (a real bug the first
    live run hit). The autouse preserve_shared_glue_job guard is a backstop, but
    it too keys off the argument, so this test owns its own Job.Role restore.
    """
    return glue.get_job(JobName=GLUE_JOB_NAME)["Job"]["Role"]


def _point_shared_job_at_role(glue, role_arn: str) -> None:
    """Repoint the shared bulk_dynamodb Glue job's execution role (Job.Role)."""
    glue.update_job(
        JobName=GLUE_JOB_NAME,
        JobUpdate={"Role": role_arn} | _existing_job_update_fields(glue),
    )


def _existing_job_update_fields(glue) -> dict:
    """Glue UpdateJob requires the full JobUpdate; copy the current command +
    args so we change ONLY the role."""
    job = glue.get_job(JobName=GLUE_JOB_NAME)["Job"]
    fields = {"Command": job["Command"]}
    for key in ("DefaultArguments", "GlueVersion", "WorkerType",
                "NumberOfWorkers", "MaxRetries", "Timeout", "ExecutionProperty",
                "Connections"):
        if key in job:
            fields[key] = job[key]
    return fields


@pytest.mark.e2e
def test_missing_autoscaling_perm_degrades_gracefully_and_job_succeeds(e2e_config):
    """A live load against a PROVISIONED table, run by a Glue role explicitly
    denied application-autoscaling:DescribeScalableTargets, must degrade
    gracefully per #231: bulk names the missing permission at BOTH autoscaling
    call sites (the diagnostic Auto Scaling Settings print AND the capacity
    check) and the job still reaches SUCCEEDED — no crash at either site."""
    region = e2e_config.aws_region
    iam = boto3.client("iam")
    glue = boto3.client("glue", region_name=region)
    run_id = f"{int(time.time())}-{uuid.uuid4().hex[:8]}"
    role_name = f"AWSGlueServiceRole-bulk-e2e-noAS-{uuid.uuid4().hex[:8]}"

    s3_path = _upload_fixture(run_id, region)

    # Snapshot the shared job's ACTUAL execution-role ARN (Job.Role) so this
    # test restores it in its OWN finally — before deleting the throwaway role.
    # We must snapshot Job.Role directly, not the --glue-job-role-name argument
    # (job_state_guard.snapshot_job_role): we mutate Job.Role, so an
    # argument-based restore would be a no-op and leave the shared job pointing
    # at the deleted throwaway role. Restore-then-delete closes that window.
    original_job_role_arn = _snapshot_job_role_arn(glue)

    try:
        role_arn = _create_role_without_autoscaling(iam, role_name)
        # IAM is eventually consistent; let the role settle before Glue assumes it.
        for _ in range(15):
            try:
                iam.get_role(RoleName=role_name)
                break
            except iam.exceptions.NoSuchEntityException:
                time.sleep(1)
        time.sleep(10)  # propagation to STS/Glue assume-role

        _point_shared_job_at_role(glue, role_arn)

        # A provisioned table so the capacity path *tries* to read autoscaling
        # (and gets denied) — on-demand wouldn't consult autoscaling at all.
        with transient_table(
            region, has_sort_key=True, label="cap-noperm", provisioned=(5, 5)
        ) as table:
            result = run_command(
                "load",
                table=table,
                extra_args=[
                    "--format", "csv",
                    "--s3-path", s3_path,
                    "--XMaxWriteRate", "500",
                ],
            )

            # These are log.warning lines, which Glue LiveTail routes to stderr
            # (not stdout), so assert on the combined stream — see
            # CommandResult.output. Asserting on .stdout alone spuriously misses
            # them even though they fired (observed 2026-07-15, jr_3ea58597...).
            out = result.output

            # ISC-8: the DIAGNOSTIC-print path degraded gracefully. This is the
            # call site (get_and_print_dynamodb_table_info) that previously
            # crashed the whole job at info-print time; it must now log the skip
            # note naming the permission instead.
            assert WARN_DIAGNOSTIC_SKIPPED in out, (
                "expected the #89 diagnostic-print skip note "
                f"({WARN_DIAGNOSTIC_SKIPPED!r}) in the live Glue output — the "
                "Auto Scaling Settings print must degrade, not crash. "
                f"output tail:\n{out[-2500:]}"
            )
            # ISC-8b: the CAPACITY-check path also degraded gracefully, naming
            # the same missing permission.
            assert WARN_CAPACITY_SKIPPED in out, (
                "expected the #89 capacity-check skip note "
                f"({WARN_CAPACITY_SKIPPED!r}) in the live Glue output; not found. "
                f"output tail:\n{out[-2500:]}"
            )
            assert MISSING_PERM in out, (
                f"expected the missing permission {MISSING_PERM!r} to be named in "
                f"the live Glue output. output tail:\n{out[-2500:]}"
            )
            # ISC-10 + ISC-9: with both autoscaling call sites guarded, the job
            # proceeds to SUCCEEDED — the #231 graceful-degradation promise held
            # end-to-end. (An unguarded describe previously took it down FAILED.)
            assert_glue_succeeded("load", result, region)
    finally:
        # Restore the shared job's Job.Role to the exact ARN we snapshotted,
        # BEFORE deleting the throwaway, so the job is never left pointing at a
        # deleted role. Direct Job.Role restore — see _snapshot_job_role_arn.
        try:
            _point_shared_job_at_role(glue, original_job_role_arn)
        finally:
            _delete_role(iam, role_name)
