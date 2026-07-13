"""Tests for scripts/backfill.py."""

from __future__ import annotations

import gzip
import io
import json
from typing import Any
from unittest.mock import MagicMock

import boto3
import pytest

import backfill


def upload_export(bucket: str, prefix: str, items: list[dict]) -> None:
    """Lay down a fake DynamoDB S3 export: manifest-summary, manifest-files, data file."""
    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket=bucket)
    export_dir = f"{prefix.rstrip('/')}/AWSDynamoDB/01234567890-deadbeef/"
    summary = {"manifestFilesS3Key": f"{export_dir}manifest-files.json"}
    s3.put_object(Bucket=bucket, Key=f"{export_dir}manifest-summary.json", Body=json.dumps(summary))
    data_key = f"{export_dir}data/file1.json.gz"
    manifest_lines = [json.dumps({"dataFileS3Key": data_key})]
    s3.put_object(Bucket=bucket, Key=f"{export_dir}manifest-files.json", Body="\n".join(manifest_lines))
    raw = "\n".join(json.dumps({"Item": item}) for item in items).encode()
    buf = io.BytesIO()
    with gzip.GzipFile(fileobj=buf, mode="w") as gz:
        gz.write(raw)
    s3.put_object(Bucket=bucket, Key=data_key, Body=buf.getvalue())


def ddb_serialize(item: dict) -> dict:
    """Tiny helper: convert a plain Python dict to DDB-JSON for the export."""
    out: dict = {}
    for k, v in item.items():
        if isinstance(v, str):
            out[k] = {"S": v}
        elif isinstance(v, bool):
            out[k] = {"BOOL": v}
        elif isinstance(v, (int, float)):
            out[k] = {"N": str(v)}
    return out


def test_get_data_file_keys_parses_manifest(aws: Any) -> None:
    items = [{"pk": "a", "sk": "1"}, {"pk": "b", "sk": "1"}]
    upload_export("bkt", "exports/", [ddb_serialize(i) for i in items])
    s3 = boto3.client("s3", region_name="us-east-1")
    keys = backfill.get_data_file_keys(s3, "bkt", "exports/")
    assert len(keys) == 1
    assert keys[0].endswith("data/file1.json.gz")


def test_put_item_with_retry_writes_with_zero_migration_ts(target_table: Any) -> None:
    outcome = backfill.put_item_with_retry(target_table, {"pk": "a", "sk": "1", "x": 1}, "pk")
    assert outcome == "written"
    item = target_table.get_item(Key={"pk": "a", "sk": "1"})["Item"]
    assert int(item["_migration_ts"]) == 0


def test_put_item_with_retry_skips_when_newer_exists(target_table: Any) -> None:
    target_table.put_item(Item={"pk": "a", "sk": "1", "_migration_ts": 500})
    outcome = backfill.put_item_with_retry(target_table, {"pk": "a", "sk": "1"}, "pk")
    assert outcome == "skipped"


def test_should_pause_true_when_iterator_age_above_threshold() -> None:
    cw = MagicMock()
    cw.get_metric_statistics.return_value = {"Datapoints": [{"Maximum": 70_000_000}]}
    assert backfill.should_pause(cw, "fn", pause_threshold_hours=18) is True


def test_should_pause_false_when_iterator_age_below_threshold() -> None:
    cw = MagicMock()
    cw.get_metric_statistics.return_value = {"Datapoints": [{"Maximum": 1000}]}
    assert backfill.should_pause(cw, "fn", pause_threshold_hours=18) is False


def test_should_pause_false_when_no_datapoints() -> None:
    cw = MagicMock()
    cw.get_metric_statistics.return_value = {"Datapoints": []}
    assert backfill.should_pause(cw, "fn", pause_threshold_hours=18) is False


def test_dry_run_does_not_write(aws: Any, target_table: Any) -> None:
    items = [{"pk": "a", "sk": "1"}, {"pk": "b", "sk": "1"}]
    upload_export("bkt", "exports/", [ddb_serialize(i) for i in items])
    s3 = boto3.client("s3", region_name="us-east-1")
    cw = MagicMock()
    cw.get_metric_statistics.return_value = {"Datapoints": []}
    config = {
        "transform": lambda item, source_event=None: item,
        "partition_key": "pk",
        "lambda_function_name": "fn",
        "pause_threshold_hours": 18,
        "circuit_breaker_check_interval": 5000,
        "dry_run": True,
    }
    keys = backfill.get_data_file_keys(s3, "bkt", "exports/")
    counts = backfill.process_data_file(s3, cw, target_table, "bkt", keys[0], config)
    assert counts["items"] == 2
    assert counts["written"] == 0
    assert target_table.scan()["Count"] == 0


def test_full_file_pipeline_writes_items(aws: Any, target_table: Any) -> None:
    items = [{"pk": f"a{i}", "sk": "1", "v": i} for i in range(3)]
    upload_export("bkt", "exports/", [ddb_serialize(i) for i in items])
    s3 = boto3.client("s3", region_name="us-east-1")
    cw = MagicMock()
    cw.get_metric_statistics.return_value = {"Datapoints": []}
    config = {
        "transform": lambda item, source_event=None: item,
        "partition_key": "pk",
        "lambda_function_name": "fn",
        "pause_threshold_hours": 18,
        "circuit_breaker_check_interval": 5000,
        "dry_run": False,
    }
    keys = backfill.get_data_file_keys(s3, "bkt", "exports/")
    counts = backfill.process_data_file(s3, cw, target_table, "bkt", keys[0], config)
    assert counts["items"] == 3
    assert counts["written"] == 3
    assert target_table.scan()["Count"] == 3
