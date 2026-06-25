r"""Create a short-lived DynamoDB table for an e2e command test.

Each command-suite test that needs a clean slate gets its own transient
table so tests are hermetic and developers don't need pre-existing tables
in their account. Always tears down on exit, even if the test fails.

Tables are created PAY_PER_REQUEST with PITR enabled (bulk_executor
refuses to mutate non-PITR tables). Names embed a uuid so concurrent runs
don't collide.

Cost: ~\$0. Empty PAY_PER_REQUEST tables incur near-zero charges.
Latency: ~30s create + propagation, ~5s delete.
"""
from __future__ import annotations

import contextlib
import time
import uuid
from typing import Iterator

import boto3
from botocore.exceptions import ClientError


def _wait_for_active(ddb, table_name: str, timeout_s: int = 120) -> None:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        status = ddb.describe_table(TableName=table_name)["Table"]["TableStatus"]
        if status == "ACTIVE":
            return
        time.sleep(2)
    raise TimeoutError(f"Table {table_name!r} never became ACTIVE in {timeout_s}s")


def _wait_for_pitr(ddb, table_name: str, timeout_s: int = 60) -> None:
    """PITR enable is asynchronous; bulk commands check it at job start."""
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        resp = ddb.describe_continuous_backups(TableName=table_name)
        status = resp["ContinuousBackupsDescription"]["PointInTimeRecoveryDescription"]["PointInTimeRecoveryStatus"]
        if status == "ENABLED":
            return
        time.sleep(2)
    raise TimeoutError(f"PITR on {table_name!r} never enabled in {timeout_s}s")


def _enable_pitr_with_retry(ddb, table_name: str, timeout_s: int = 60) -> None:
    """Call UpdateContinuousBackups with retry on the propagation race.

    Even after DescribeTable says ACTIVE, UpdateContinuousBackups can still
    return ContinuousBackupsUnavailableException ("Backups are being enabled
    ... Please retry later") for several seconds while the continuous-backup
    subsystem propagates the table's existence. Retry until accepted.
    """
    deadline = time.time() + timeout_s
    while True:
        try:
            ddb.update_continuous_backups(
                TableName=table_name,
                PointInTimeRecoverySpecification={"PointInTimeRecoveryEnabled": True},
            )
            return
        except ClientError as e:
            code = e.response["Error"]["Code"]
            if code != "ContinuousBackupsUnavailableException":
                raise
            if time.time() > deadline:
                raise TimeoutError(
                    f"UpdateContinuousBackups for {table_name!r} kept returning "
                    f"ContinuousBackupsUnavailableException for {timeout_s}s"
                )
            time.sleep(2)


@contextlib.contextmanager
def transient_table(
    region: str,
    *,
    has_sort_key: bool = True,
    label: str = "command",
) -> Iterator[str]:
    """Yield the name of a freshly-created PAY_PER_REQUEST DynamoDB table with PITR.

    label: short string included in the table name for debuggability ('fill', 'copy-src', etc).
    has_sort_key: if True, schema is pk(S)+sk(S); else just pk(S).

    Example:
        with transient_table(region, label="fill") as table:
            run_command("fill", table=table, extra_args=["--numitems", "100", "--generator", "default"])
    """
    ddb = boto3.client("dynamodb", region_name=region)
    suffix = uuid.uuid4().hex[:8]
    table_name = f"bulk-e2e-{label}-{suffix}"

    attrs = [{"AttributeName": "pk", "AttributeType": "S"}]
    keys = [{"AttributeName": "pk", "KeyType": "HASH"}]
    if has_sort_key:
        attrs.append({"AttributeName": "sk", "AttributeType": "S"})
        keys.append({"AttributeName": "sk", "KeyType": "RANGE"})

    try:
        ddb.create_table(
            TableName=table_name,
            AttributeDefinitions=attrs,
            KeySchema=keys,
            BillingMode="PAY_PER_REQUEST",
            Tags=[
                {"Key": "purpose", "Value": "bulk_executor e2e command test"},
                {"Key": "ephemeral", "Value": "true"},
            ],
        )
        _wait_for_active(ddb, table_name)
        _enable_pitr_with_retry(ddb, table_name)
        _wait_for_pitr(ddb, table_name)

        yield table_name
    finally:
        with contextlib.suppress(ClientError):
            ddb.delete_table(TableName=table_name)
