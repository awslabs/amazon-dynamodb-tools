"""Standalone backfill from a DynamoDB S3 export to the target table.

Use this for tables up to ~100 GiB. For larger tables, fan out across multiple
hosts or use a Glue job (not included in this toolkit).

Reads the export manifest under ``s3://$EXPORT_BUCKET/$EXPORT_PREFIX/``,
deserializes each gzipped JSONL data file, applies ``transform()``, and writes
to the target table with ``_migration_ts=0`` so any stream-replayed item beats
the backfill version.

Includes a circuit breaker that pauses the backfill when the stream-replay
Lambda's ``IteratorAge`` rises above 18h (default), preventing the backfill
from consuming all the target table's write capacity and starving the Lambda.

Configuration via environment variables (see Quick Start in README) or CLI
flags. CLI flags take precedence.
"""

from __future__ import annotations

import argparse
import gzip
import importlib
import io
import json
import logging
import os
import random
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from typing import Any, Callable

import boto3
from boto3.dynamodb.types import TypeDeserializer
from botocore.exceptions import ClientError

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("backfill")


def load_transform() -> Callable[..., Any]:
    """Resolve transform: TRANSFORM_MODULE env var → bundled transform.py → identity."""
    module_name = os.environ.get("TRANSFORM_MODULE")
    if module_name:
        try:
            mod = importlib.import_module(module_name)
            return getattr(mod, "transform")
        except (ImportError, AttributeError) as e:
            log.warning("transform module %s failed to load: %s", module_name, e)
    try:
        sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        from transform import transform  # type: ignore

        return transform
    except ImportError:
        return lambda item, source_event=None: item


def get_data_file_keys(s3_client: Any, bucket: str, prefix: str) -> list[str]:
    """Discover .json.gz data files via the most recent export's manifest.

    DynamoDB exports lay down keys as
    ``{prefix}/AWSDynamoDB/{exportId}/manifest-summary.json``. We list all
    ``manifest-summary.json`` keys under the prefix and pick the lexically
    largest (export IDs include a timestamp prefix so this picks the newest).
    """
    prefix = prefix.rstrip("/") + "/"
    paginator = s3_client.get_paginator("list_objects_v2")
    summaries: list[str] = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []) or []:
            if obj["Key"].endswith("/manifest-summary.json"):
                summaries.append(obj["Key"])
    if not summaries:
        raise RuntimeError(f"No manifest-summary.json found under s3://{bucket}/{prefix}")
    summary_key = sorted(summaries)[-1]
    summary_obj = s3_client.get_object(Bucket=bucket, Key=summary_key)
    summary = json.loads(summary_obj["Body"].read())
    manifest_key = summary["manifestFilesS3Key"]
    manifest_obj = s3_client.get_object(Bucket=bucket, Key=manifest_key)
    keys: list[str] = []
    for line in manifest_obj["Body"].read().decode().splitlines():
        if line.strip():
            keys.append(json.loads(line)["dataFileS3Key"])
    log.info("Discovered %d data files via %s", len(keys), summary_key)
    return keys


def deserialize_item(dynamo_item: dict[str, Any], deserializer: TypeDeserializer) -> dict[str, Any]:
    return {k: deserializer.deserialize(v) for k, v in dynamo_item.items()}


def put_item_with_retry(
    table: Any,
    item: dict[str, Any],
    partition_key: str,
    max_retries: int = 8,
) -> str:
    """Conditional newer-wins put with exponential backoff. Returns 'written' or 'skipped'."""
    item = dict(item)
    item["_migration_ts"] = 0
    for attempt in range(max_retries):
        try:
            table.put_item(
                Item=item,
                ConditionExpression="attribute_not_exists(#pk) OR #ts < :ts",
                ExpressionAttributeNames={"#pk": partition_key, "#ts": "_migration_ts"},
                ExpressionAttributeValues={":ts": 0},
            )
            return "written"
        except ClientError as e:
            code = e.response["Error"]["Code"]
            if code == "ConditionalCheckFailedException":
                return "skipped"
            if code in ("ProvisionedThroughputExceededException", "ThrottlingException"):
                time.sleep(min(2**attempt * 0.1, 30) + random.uniform(0, 0.5))
                continue
            raise
    raise RuntimeError(f"Exhausted {max_retries} retries for item")


def should_pause(
    cw_client: Any,
    lambda_function_name: str,
    pause_threshold_hours: float,
) -> bool:
    """Query CloudWatch for stream-replay IteratorAge; pause if above threshold."""
    end = datetime.now(timezone.utc)
    start = end - timedelta(minutes=5)
    try:
        resp = cw_client.get_metric_statistics(
            Namespace="AWS/Lambda",
            MetricName="IteratorAge",
            Dimensions=[{"Name": "FunctionName", "Value": lambda_function_name}],
            StartTime=start,
            EndTime=end,
            Period=60,
            Statistics=["Maximum"],
        )
    except ClientError as e:
        log.warning("CloudWatch query failed (%s); not pausing", e.response["Error"]["Code"])
        return False
    points = resp.get("Datapoints", [])
    if not points:
        return False
    max_age_ms = max(p["Maximum"] for p in points)
    threshold_ms = pause_threshold_hours * 3600 * 1000
    if max_age_ms > threshold_ms:
        log.warning("IteratorAge %.1fh > threshold %.1fh; pausing", max_age_ms / 3.6e6, pause_threshold_hours)
        return True
    return False


def process_data_file(
    s3_client: Any,
    cw_client: Any,
    table: Any,
    bucket: str,
    s3_key: str,
    config: dict[str, Any],
) -> dict[str, int]:
    """Stream a single .json.gz export file into the target."""
    deserializer = TypeDeserializer()
    transform_fn = config["transform"]
    counts = {"items": 0, "written": 0, "skipped": 0, "errors": 0}
    obj = s3_client.get_object(Bucket=bucket, Key=s3_key)
    raw = obj["Body"].read()
    decompressed = gzip.GzipFile(fileobj=io.BytesIO(raw)).read().decode()
    items: list[dict[str, Any]] = []
    for line in decompressed.splitlines():
        if not line.strip():
            continue
        items.append(json.loads(line)["Item"])
    # Shuffle to spread writes across partitions and avoid hot-shard throttling.
    random.shuffle(items)
    if config["dry_run"]:
        log.info("[dry-run] %s: %d items", s3_key, len(items))
        counts["items"] = len(items)
        return counts
    cadence = config["circuit_breaker_check_interval"]
    for idx, raw_item in enumerate(items, 1):
        if idx % cadence == 0 and should_pause(cw_client, config["lambda_function_name"], config["pause_threshold_hours"]):
            while should_pause(cw_client, config["lambda_function_name"], config["pause_threshold_hours"]):
                time.sleep(60)
        item = deserialize_item(raw_item, deserializer)
        transformed = transform_fn(item)
        if transformed is None:
            counts["items"] += 1
            continue
        try:
            outcome = put_item_with_retry(table, transformed, config["partition_key"])
            counts[outcome] += 1
        except Exception as e:  # noqa: BLE001
            counts["errors"] += 1
            log.exception("Failed to write item from %s: %s", s3_key, e)
        counts["items"] += 1
    log.info(
        "Done %s: %d items, %d written, %d skipped, %d errors",
        s3_key, counts["items"], counts["written"], counts["skipped"], counts["errors"],
    )
    return counts


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    p.add_argument("--export-bucket", default=os.environ.get("EXPORT_BUCKET"))
    p.add_argument("--export-prefix", default=os.environ.get("EXPORT_PREFIX", "exports/"))
    p.add_argument("--target-table", default=os.environ.get("TARGET_TABLE"))
    p.add_argument("--partition-key", default=os.environ.get("PARTITION_KEY", "pk"))
    p.add_argument("--region", default=os.environ.get("REGION", "us-east-1"))
    p.add_argument("--max-workers", type=int, default=int(os.environ.get("MAX_WORKERS", "16")))
    p.add_argument(
        "--lambda-function-name",
        default=os.environ.get("LAMBDA_FUNCTION_NAME", "ddb-migration-stream-replay"),
    )
    p.add_argument(
        "--pause-threshold-hours",
        type=float,
        default=float(os.environ.get("ITERATOR_AGE_PAUSE_HOURS", "18")),
    )
    p.add_argument(
        "--circuit-breaker-check-interval",
        type=int,
        default=int(os.environ.get("CIRCUIT_BREAKER_CHECK_INTERVAL", "5000")),
    )
    p.add_argument("--dry-run", action="store_true", help="Parse files, count items, write nothing")
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    if not args.export_bucket or not args.target_table:
        log.error("EXPORT_BUCKET and TARGET_TABLE are required")
        return 2
    s3_client = boto3.client("s3", region_name=args.region)
    cw_client = boto3.client("cloudwatch", region_name=args.region)
    table = boto3.resource("dynamodb", region_name=args.region).Table(args.target_table)
    config = {
        "transform": load_transform(),
        "partition_key": args.partition_key,
        "lambda_function_name": args.lambda_function_name,
        "pause_threshold_hours": args.pause_threshold_hours,
        "circuit_breaker_check_interval": args.circuit_breaker_check_interval,
        "dry_run": args.dry_run,
    }
    keys = get_data_file_keys(s3_client, args.export_bucket, args.export_prefix)
    if not keys:
        log.warning("No data files found; nothing to backfill")
        return 0
    totals = {"items": 0, "written": 0, "skipped": 0, "errors": 0, "files_failed": 0}
    start = time.time()
    with ThreadPoolExecutor(max_workers=args.max_workers) as pool:
        futures = {
            pool.submit(process_data_file, s3_client, cw_client, table, args.export_bucket, key, config): key
            for key in keys
        }
        for fut in as_completed(futures):
            key = futures[fut]
            try:
                counts = fut.result()
                for k, v in counts.items():
                    totals[k] += v
            except Exception as e:  # noqa: BLE001
                totals["files_failed"] += 1
                log.exception("File failed %s: %s", key, e)
    elapsed = time.time() - start
    rate = totals["items"] / elapsed if elapsed > 0 else 0
    log.info(
        "BACKFILL COMPLETE — items=%d written=%d skipped=%d errors=%d files_failed=%d elapsed=%.1fs rate=%.0f/s",
        totals["items"], totals["written"], totals["skipped"], totals["errors"],
        totals["files_failed"], elapsed, rate,
    )
    return 1 if totals["files_failed"] > 0 or totals["errors"] > 0 else 0


if __name__ == "__main__":
    sys.exit(main())
