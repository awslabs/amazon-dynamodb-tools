"""Connector smoke: `bulk load`.

Loads 10 CSV rows into the writable test table. The fixture's CSV is
generated to match the table's actual key schema (read via
DescribeTable). Items are tagged with a unique
``e2e-load-smoke-<run_id>`` partition-key prefix so the cleanup step
can scope-delete them by prefix.
"""
from __future__ import annotations

import csv
import io
import time
import uuid

import boto3
import pytest

from tests.e2e.connector.conftest import PerfRow
from tests.e2e.helpers.glue_bucket import discover_bucket
from tests.e2e.helpers.perf import fetch_perf
from tests.e2e.helpers.verb_runner import run_verb

NUM_SMOKE_ITEMS = 10
PK_PREFIX = "e2e-load-smoke"


def _describe_key_schema(table_name: str, region: str) -> tuple[str, str | None]:
    """Return (pk_attr_name, sk_attr_name_or_None) for the writable table."""
    ddb = boto3.client("dynamodb", region_name=region)
    desc = ddb.describe_table(TableName=table_name)["Table"]
    pk = next(k for k in desc["KeySchema"] if k["KeyType"] == "HASH")["AttributeName"]
    sk_entry = next((k for k in desc["KeySchema"] if k["KeyType"] == "RANGE"), None)
    sk = sk_entry["AttributeName"] if sk_entry else None
    return pk, sk


def _build_csv(pk_attr: str, sk_attr: str | None, run_id: str) -> str:
    buf = io.StringIO()
    writer = csv.writer(buf)
    if sk_attr:
        writer.writerow([pk_attr, sk_attr, "payload"])
        for i in range(NUM_SMOKE_ITEMS):
            writer.writerow([f"{PK_PREFIX}-{run_id}", f"item-{i:03d}", f"data-{i}"])
    else:
        writer.writerow([pk_attr, "payload"])
        for i in range(NUM_SMOKE_ITEMS):
            writer.writerow([f"{PK_PREFIX}-{run_id}-{i:03d}", f"data-{i}"])
    return buf.getvalue()


def _upload_fixture(bucket: str, key: str, body: str, region: str) -> str:
    s3 = boto3.client("s3", region_name=region)
    s3.put_object(Bucket=bucket, Key=key, Body=body.encode("utf-8"))
    return f"s3://{bucket}/{key}"


def _scope_delete(table_name: str, pk_attr: str, sk_attr: str | None,
                  run_id: str, region: str) -> int:
    """Delete the items this run inserted. Returns count deleted."""
    ddb = boto3.resource("dynamodb", region_name=region)
    table = ddb.Table(table_name)
    deleted = 0
    if sk_attr:
        pk_value = f"{PK_PREFIX}-{run_id}"
        response = table.query(
            KeyConditionExpression="#pk = :pk",
            ExpressionAttributeNames={"#pk": pk_attr},
            ExpressionAttributeValues={":pk": pk_value},
        )
        items = response.get("Items", [])
        with table.batch_writer() as batch:
            for item in items:
                batch.delete_item(Key={pk_attr: item[pk_attr], sk_attr: item[sk_attr]})
                deleted += 1
    else:
        with table.batch_writer() as batch:
            for i in range(NUM_SMOKE_ITEMS):
                batch.delete_item(Key={pk_attr: f"{PK_PREFIX}-{run_id}-{i:03d}"})
                deleted += 1
    return deleted


@pytest.mark.e2e
class TestLoadSmoke:
    def test_load_writes_items(self, e2e_config, perf_collector):
        run_id = f"{int(time.time())}-{uuid.uuid4().hex[:8]}"

        pk_attr, sk_attr = _describe_key_schema(
            e2e_config.write_table, e2e_config.aws_region
        )
        body = _build_csv(pk_attr, sk_attr, run_id)
        bucket = discover_bucket(e2e_config.aws_region)
        s3_key = f"e2e/load-smoke/{run_id}.csv"
        s3_path = _upload_fixture(bucket, s3_key, body, e2e_config.aws_region)

        try:
            result = run_verb(
                "load",
                table=e2e_config.write_table,
                extra_args=["--format", "csv", "--s3-path", s3_path],
            )
            assert result.succeeded, (
                f"load failed (exit {result.exit_code}). "
                f"Last 500 chars of stderr:\n{result.stderr[-500:]}"
            )

            perf = fetch_perf(result.job_run_id, e2e_config.aws_region)
            perf_collector.add(PerfRow(
                verb="load",
                wall_seconds=result.wall_seconds,
                dpu_seconds=perf.dpu_seconds if perf else None,
                items=NUM_SMOKE_ITEMS,
            ))
        finally:
            _scope_delete(
                e2e_config.write_table, pk_attr, sk_attr, run_id,
                e2e_config.aws_region,
            )
