"""Tests for lambda/stream_replay.py.

moto's stream-record handling is partial, so we drive the handler with
hand-rolled events and let it write to a moto-backed target table.
"""

from __future__ import annotations

import importlib
import sys
from typing import Any

import boto3
import pytest


@pytest.fixture
def stream_replay_module(monkeypatch: pytest.MonkeyPatch, target_table: Any):
    """Import lambda/stream_replay.py with the right env vars set."""
    monkeypatch.setenv("TARGET_TABLE", "target")
    monkeypatch.setenv("PARTITION_KEY", "pk")
    monkeypatch.setenv("TARGET_REGION", "us-east-1")
    sys.modules.pop("stream_replay", None)
    mod = importlib.import_module("stream_replay")
    return mod


def make_insert_event(pk: str, sk: str, ts: float, **fields: Any) -> dict:
    new_image = {"pk": {"S": pk}, "sk": {"S": sk}, **{k: {"S": str(v)} for k, v in fields.items()}}
    return {
        "Records": [{
            "eventID": f"e-{pk}-{sk}",
            "eventName": "INSERT",
            "dynamodb": {
                "ApproximateCreationDateTime": ts,
                "Keys": {"pk": {"S": pk}, "sk": {"S": sk}},
                "NewImage": new_image,
            },
        }]
    }


def make_remove_event(pk: str, sk: str, ts: float) -> dict:
    return {
        "Records": [{
            "eventID": f"e-{pk}-{sk}",
            "eventName": "REMOVE",
            "dynamodb": {
                "ApproximateCreationDateTime": ts,
                "Keys": {"pk": {"S": pk}, "sk": {"S": sk}},
                "OldImage": {"pk": {"S": pk}, "sk": {"S": sk}, "status": {"S": "PAID"}},
            },
        }]
    }


def test_insert_writes_item_with_migration_ts(stream_replay_module, target_table) -> None:
    result = stream_replay_module.handler(make_insert_event("a", "1", 100.0, status="NEW"), None)
    assert result == {"batchItemFailures": []}
    item = target_table.get_item(Key={"pk": "a", "sk": "1"})["Item"]
    assert item["status"] == "NEW"
    assert float(item["_migration_ts"]) == 100.0


def test_newer_event_overwrites_older(stream_replay_module, target_table) -> None:
    stream_replay_module.handler(make_insert_event("a", "1", 100.0, status="NEW"), None)
    stream_replay_module.handler(make_insert_event("a", "1", 200.0, status="PAID"), None)
    item = target_table.get_item(Key={"pk": "a", "sk": "1"})["Item"]
    assert item["status"] == "PAID"


def test_older_event_does_not_overwrite_newer(stream_replay_module, target_table) -> None:
    stream_replay_module.handler(make_insert_event("a", "1", 200.0, status="PAID"), None)
    stream_replay_module.handler(make_insert_event("a", "1", 100.0, status="NEW"), None)
    item = target_table.get_item(Key={"pk": "a", "sk": "1"})["Item"]
    assert item["status"] == "PAID"


def test_remove_writes_tombstone(stream_replay_module, target_table) -> None:
    stream_replay_module.handler(make_remove_event("a", "1", 300.0), None)
    item = target_table.get_item(Key={"pk": "a", "sk": "1"})["Item"]
    assert item["_tombstone"] is True
    assert "status" not in item  # full image not persisted; only key + flag


def test_unhandled_exception_reported_as_batch_failure(
    monkeypatch: pytest.MonkeyPatch, stream_replay_module
) -> None:
    """Errors other than ConditionalCheckFailed get appended to batchItemFailures."""

    def boom(*a, **kw):
        raise RuntimeError("boom")

    monkeypatch.setattr(stream_replay_module, "_conditional_put", boom)
    event = make_insert_event("a", "1", 100.0)
    result = stream_replay_module.handler(event, None)
    assert result == {"batchItemFailures": [{"itemIdentifier": "e-a-1"}]}


def test_transform_module_env_var_loads_custom_module(
    monkeypatch: pytest.MonkeyPatch, target_table, tmp_path
) -> None:
    """TRANSFORM_MODULE must actually be honored — fixes gap #1 from the review."""
    custom = tmp_path / "my_transform.py"
    custom.write_text(
        "def transform(item, source_event=None):\n"
        "    item['custom'] = True\n"
        "    return item\n"
    )
    monkeypatch.syspath_prepend(str(tmp_path))
    monkeypatch.setenv("TARGET_TABLE", "target")
    monkeypatch.setenv("PARTITION_KEY", "pk")
    monkeypatch.setenv("TARGET_REGION", "us-east-1")
    monkeypatch.setenv("TRANSFORM_MODULE", "my_transform")

    sys.modules.pop("stream_replay", None)
    mod = importlib.import_module("stream_replay")

    mod.handler(make_insert_event("a", "1", 100.0, status="NEW"), None)
    item = target_table.get_item(Key={"pk": "a", "sk": "1"})["Item"]
    assert item["custom"] is True
