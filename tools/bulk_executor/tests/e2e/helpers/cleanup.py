"""Orphan-cleaner for the load-smoke fixture: `make test-e2e-cleanup`.

If a load-smoke test crashed before its `finally` cleanup ran, this
module sweeps the writable table for any items with the
``e2e-load-smoke-`` partition-key prefix and deletes them.
"""
from __future__ import annotations

import sys

import boto3

from tests.e2e.helpers.aws_guard import assert_account_matches
from tests.e2e.helpers.config import E2EConfig

PK_PREFIX = "e2e-load-smoke"


def _scan_orphans(table_name: str, pk_attr: str, region: str) -> list[dict]:
    ddb = boto3.client("dynamodb", region_name=region)
    paginator = ddb.get_paginator("scan")
    orphans: list[dict] = []
    for page in paginator.paginate(
        TableName=table_name,
        FilterExpression="begins_with(#pk, :prefix)",
        ExpressionAttributeNames={"#pk": pk_attr},
        ExpressionAttributeValues={":prefix": {"S": PK_PREFIX}},
    ):
        orphans.extend(page.get("Items", []))
    return orphans


def main() -> int:
    cfg = E2EConfig.load()
    if cfg is None:
        sys.exit(
            "No tests/e2e/.e2e-config found. Run 'make test-e2e-connector' once "
            "to set up your config, then re-run cleanup."
        )
    assert_account_matches(cfg.aws_account_id, cfg.aws_region)

    ddb_resource = boto3.resource("dynamodb", region_name=cfg.aws_region)
    ddb_client = boto3.client("dynamodb", region_name=cfg.aws_region)

    table_desc = ddb_client.describe_table(TableName=cfg.write_table)["Table"]
    pk_attr = next(
        k for k in table_desc["KeySchema"] if k["KeyType"] == "HASH"
    )["AttributeName"]
    sk_entry = next(
        (k for k in table_desc["KeySchema"] if k["KeyType"] == "RANGE"), None
    )
    sk_attr = sk_entry["AttributeName"] if sk_entry else None

    orphans = _scan_orphans(cfg.write_table, pk_attr, cfg.aws_region)
    if not orphans:
        print("No orphaned e2e-load-smoke-* items found. Nothing to clean.")
        return 0

    print(f"Found {len(orphans)} orphan(s). Deleting...")
    table = ddb_resource.Table(cfg.write_table)
    deserializer = boto3.dynamodb.types.TypeDeserializer()
    with table.batch_writer() as batch:
        for raw_item in orphans:
            item = {k: deserializer.deserialize(v) for k, v in raw_item.items()}
            key = {pk_attr: item[pk_attr]}
            if sk_attr:
                key[sk_attr] = item[sk_attr]
            batch.delete_item(Key=key)
    print(f"Deleted {len(orphans)} orphan(s).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
