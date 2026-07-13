"""DynamoDB Streams replay Lambda — replays source-table changes to the target.

Conflict resolution: every write carries ``_migration_ts`` set to the stream
record's ``ApproximateCreationDateTime`` (epoch seconds). The conditional
expression ``attribute_not_exists(#pk) OR #ts < :ts`` ensures newer timestamps
always win, regardless of arrival order. Backfill writes use ``_migration_ts=0``
so any stream-replayed item beats them.

REMOVE events are written as tombstones (``_tombstone=True``) instead of being
deleted, so the in-flight backfill cannot resurrect them. Tombstones are cleaned
up post-cutover by ``scripts/cleanup.py`` (which sets a TTL attribute and lets
DynamoDB expire them).

Environment variables
---------------------

* ``TARGET_TABLE`` (required) — name of the destination table.
* ``PARTITION_KEY`` (default ``pk``) — partition-key attribute name.
* ``TARGET_REGION`` (defaults to ``AWS_REGION``) — region of the target table.
* ``TRANSFORM_MODULE`` (optional) — dotted Python module path that exposes
  ``transform(item, source_event=None)``. If unset, the bundled ``transform``
  module is used. If neither importable nor present, an identity transform is
  applied.
* ``TARGET_ROLE_ARN`` (optional) — for cross-account, the Lambda assumes this
  role and uses temporary credentials for all target writes.
"""

from __future__ import annotations

import importlib
import json
import logging
import os
import sys
import time
from decimal import Decimal
from typing import Any, Callable

import boto3
from boto3.dynamodb.types import TypeDeserializer
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def _log(level: str, event: str, **fields: Any) -> None:
    payload = {"ts": time.time(), "level": level, "event": event, **fields}
    logger.info(json.dumps(payload, default=str))


def _load_transform() -> Callable[..., Any]:
    """Resolve the user-supplied transform function.

    Order: ``TRANSFORM_MODULE`` env var, then bundled ``transform`` module, then
    identity fallback.
    """
    module_name = os.environ.get("TRANSFORM_MODULE")
    if module_name:
        try:
            mod = importlib.import_module(module_name)
            fn = getattr(mod, "transform")
            _log("info", "transform_loaded", source=module_name)
            return fn
        except (ImportError, AttributeError) as e:
            _log("warning", "transform_load_failed", source=module_name, error=str(e))
    try:
        # Allow co-located transform.py when packaged with the Lambda zip.
        sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        from transform import transform as fn  # type: ignore

        _log("info", "transform_loaded", source="bundled")
        return fn
    except ImportError:
        _log("info", "transform_loaded", source="identity")
        return lambda item, source_event=None: item


# Module-level setup so cold-start cost is paid once.
_TARGET_TABLE = os.environ.get("TARGET_TABLE")
_PARTITION_KEY = os.environ.get("PARTITION_KEY", "pk")
_TARGET_REGION = os.environ.get("TARGET_REGION") or os.environ.get("AWS_REGION") or "us-east-1"
_TARGET_ROLE_ARN = os.environ.get("TARGET_ROLE_ARN")
_DESERIALIZER = TypeDeserializer()
_TRANSFORM = _load_transform()


def _build_target_client() -> Any:
    """Build a DynamoDB resource client, optionally assuming a cross-account role."""
    if _TARGET_ROLE_ARN:
        sts = boto3.client("sts")
        creds = sts.assume_role(
            RoleArn=_TARGET_ROLE_ARN,
            RoleSessionName="ddb-migration-stream-replay",
            DurationSeconds=3600,
        )["Credentials"]
        return boto3.resource(
            "dynamodb",
            region_name=_TARGET_REGION,
            aws_access_key_id=creds["AccessKeyId"],
            aws_secret_access_key=creds["SecretAccessKey"],
            aws_session_token=creds["SessionToken"],
        )
    return boto3.resource("dynamodb", region_name=_TARGET_REGION)


_TABLE = _build_target_client().Table(_TARGET_TABLE) if _TARGET_TABLE else None


def _deserialize_image(image: dict[str, Any]) -> dict[str, Any]:
    return {k: _DESERIALIZER.deserialize(v) for k, v in image.items()}


def _conditional_put(item: dict[str, Any], migration_ts: float) -> str:
    """Put item with newer-wins condition. Returns 'written' or 'skipped'.

    Migration timestamps are stored as Decimal because the boto3 resource API
    rejects native float values.
    """
    ts = Decimal(str(migration_ts))
    item = dict(item)
    item["_migration_ts"] = ts
    try:
        _TABLE.put_item(
            Item=item,
            ConditionExpression="attribute_not_exists(#pk) OR #ts < :ts",
            ExpressionAttributeNames={"#pk": _PARTITION_KEY, "#ts": "_migration_ts"},
            ExpressionAttributeValues={":ts": ts},
        )
        return "written"
    except ClientError as e:
        if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
            return "skipped"
        raise


def handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    """Lambda entry point. Returns ``batchItemFailures`` for partial-batch retry."""
    failures: list[dict[str, str]] = []
    counts = {"written": 0, "skipped": 0, "tombstone": 0, "errors": 0}

    for record in event.get("Records", []):
        event_id = record.get("eventID", "")
        event_name = record.get("eventName", "")
        ddb = record.get("dynamodb", {})
        migration_ts = float(ddb.get("ApproximateCreationDateTime", time.time()))
        try:
            if event_name in ("INSERT", "MODIFY"):
                new_image = ddb.get("NewImage")
                if not new_image:
                    continue
                item = _deserialize_image(new_image)
                transformed = _TRANSFORM(item, record)
                if transformed is None:
                    continue
                outcome = _conditional_put(transformed, migration_ts)
                counts[outcome] += 1
            elif event_name == "REMOVE":
                old_image = ddb.get("OldImage")
                if not old_image:
                    continue
                key_only = _deserialize_image(old_image)
                tombstone = {_PARTITION_KEY: key_only[_PARTITION_KEY], "_tombstone": True}
                # Preserve sort key if present; deserialize_image already did it.
                for k, v in key_only.items():
                    if k == _PARTITION_KEY or k.startswith("_"):
                        continue
                    # Sort key heuristic: any other top-level key from the item's primary key.
                    if "Keys" in ddb and k in _deserialize_image(ddb["Keys"]):
                        tombstone[k] = v
                _conditional_put(tombstone, migration_ts)
                counts["tombstone"] += 1
        except Exception as e:  # noqa: BLE001 — partial-batch retry needs broad catch
            counts["errors"] += 1
            _log("error", "record_failed", event_id=event_id, event_name=event_name, error=str(e))
            failures.append({"itemIdentifier": event_id})

    _log("info", "batch_complete", **counts, failures=len(failures))
    return {"batchItemFailures": failures}
