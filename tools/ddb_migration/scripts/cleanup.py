"""Post-cutover cleanup of migration metadata on the target table.

Run this after cutover is fully validated (typically 7-14 days post-cutover):

* Sets a TTL attribute (``_ttl``) on every item with ``_tombstone=True`` so
  DynamoDB's TTL feature deletes them. The target table must have TTL enabled
  on the ``_ttl`` attribute. ``deploy.sh`` enables this automatically.
* Removes the ``_migration_ts`` attribute from regular items via paginated
  scan-update. Idempotent: re-running is safe and does nothing on already-clean
  items.

Tombstone TTL defaults to 7 days from now. Override via ``--tombstone-ttl-days``.

Exit codes
----------

* ``0`` — cleanup completed.
* ``1`` — at least one item failed to update.
* ``2`` — usage error.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from typing import Any

import boto3
from botocore.exceptions import ClientError

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("cleanup")


def iter_items(table: Any, projection: list[str], filter_expr: str | None = None) -> Any:
    """Yield items from a paginated Scan using ProjectionExpression."""
    placeholders = {f"#a{i}": name for i, name in enumerate(projection)}
    scan_kwargs: dict[str, Any] = {
        "ProjectionExpression": ", ".join(placeholders.keys()),
        "ExpressionAttributeNames": placeholders,
    }
    if filter_expr:
        scan_kwargs["FilterExpression"] = filter_expr
    while True:
        resp = table.scan(**scan_kwargs)
        for item in resp.get("Items", []):
            yield item
        if "LastEvaluatedKey" not in resp:
            return
        scan_kwargs["ExclusiveStartKey"] = resp["LastEvaluatedKey"]


def expire_tombstones(
    table: Any,
    partition_key: str,
    sort_key: str | None,
    tombstone_ttl_seconds: int,
) -> dict[str, int]:
    """Set _ttl on every tombstone item so DynamoDB TTL deletes it."""
    counts = {"updated": 0, "errors": 0}
    expire_at = int(time.time()) + tombstone_ttl_seconds
    proj = [partition_key]
    if sort_key:
        proj.append(sort_key)
    proj.append("_tombstone")
    for item in iter_items(table, proj, filter_expr="attribute_exists(#a2)"):
        key = {partition_key: item[partition_key]}
        if sort_key and sort_key in item:
            key[sort_key] = item[sort_key]
        try:
            table.update_item(
                Key=key,
                UpdateExpression="SET #ttl = :ttl",
                ExpressionAttributeNames={"#ttl": "_ttl"},
                ExpressionAttributeValues={":ttl": expire_at},
            )
            counts["updated"] += 1
        except ClientError as e:
            log.exception("Failed to set TTL on %s: %s", key, e)
            counts["errors"] += 1
    log.info("Tombstone TTL pass: updated=%d errors=%d", counts["updated"], counts["errors"])
    return counts


def remove_migration_ts(table: Any, partition_key: str, sort_key: str | None) -> dict[str, int]:
    """Strip _migration_ts attribute from items that still carry it."""
    counts = {"updated": 0, "skipped": 0, "errors": 0}
    proj = [partition_key]
    if sort_key:
        proj.append(sort_key)
    proj.append("_migration_ts")
    for item in iter_items(table, proj, filter_expr="attribute_exists(#a2)"):
        key = {partition_key: item[partition_key]}
        if sort_key and sort_key in item:
            key[sort_key] = item[sort_key]
        try:
            table.update_item(
                Key=key,
                UpdateExpression="REMOVE #ts",
                ExpressionAttributeNames={"#ts": "_migration_ts"},
                ConditionExpression="attribute_exists(#ts)",
            )
            counts["updated"] += 1
        except ClientError as e:
            if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
                counts["skipped"] += 1
                continue
            log.exception("Failed to remove _migration_ts from %s: %s", key, e)
            counts["errors"] += 1
    log.info(
        "_migration_ts pass: updated=%d skipped=%d errors=%d",
        counts["updated"], counts["skipped"], counts["errors"],
    )
    return counts


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    p.add_argument("--region", default=os.environ.get("REGION", "us-east-1"))
    p.add_argument("--target-table", default=os.environ.get("TARGET_TABLE"))
    p.add_argument("--partition-key", default=os.environ.get("PARTITION_KEY", "pk"))
    p.add_argument("--sort-key", default=os.environ.get("SORT_KEY"))
    p.add_argument("--tombstone-ttl-days", type=int, default=7)
    p.add_argument("--skip-tombstones", action="store_true")
    p.add_argument("--skip-migration-ts", action="store_true")
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    if not args.target_table:
        log.error("TARGET_TABLE is required")
        return 2
    table = boto3.resource("dynamodb", region_name=args.region).Table(args.target_table)
    total_errors = 0
    if not args.skip_tombstones:
        total_errors += expire_tombstones(
            table, args.partition_key, args.sort_key, args.tombstone_ttl_days * 86400,
        )["errors"]
    if not args.skip_migration_ts:
        total_errors += remove_migration_ts(table, args.partition_key, args.sort_key)["errors"]
    if total_errors > 0:
        log.error("Cleanup completed with %d errors", total_errors)
        return 1
    log.info("CLEANUP COMPLETE")
    return 0


if __name__ == "__main__":
    sys.exit(main())
