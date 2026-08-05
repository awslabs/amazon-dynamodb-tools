"""Tests for scripts/cleanup.py."""

from __future__ import annotations

import time
from typing import Any

import cleanup


def test_expire_tombstones_sets_ttl(target_table: Any) -> None:
    target_table.put_item(Item={"pk": "a", "sk": "1", "_tombstone": True, "_migration_ts": 100})
    target_table.put_item(Item={"pk": "b", "sk": "1", "_tombstone": True, "_migration_ts": 200})
    target_table.put_item(Item={"pk": "c", "sk": "1", "name": "live"})

    counts = cleanup.expire_tombstones(target_table, "pk", "sk", tombstone_ttl_seconds=86400)
    assert counts["updated"] == 2
    assert counts["errors"] == 0

    a = target_table.get_item(Key={"pk": "a", "sk": "1"})["Item"]
    assert "_ttl" in a
    assert a["_ttl"] >= int(time.time())

    c = target_table.get_item(Key={"pk": "c", "sk": "1"})["Item"]
    assert "_ttl" not in c


def test_remove_migration_ts_strips_attribute(target_table: Any) -> None:
    target_table.put_item(Item={"pk": "a", "sk": "1", "_migration_ts": 100, "name": "x"})
    target_table.put_item(Item={"pk": "b", "sk": "1", "name": "y"})

    counts = cleanup.remove_migration_ts(target_table, "pk", "sk")
    assert counts["updated"] == 1

    a = target_table.get_item(Key={"pk": "a", "sk": "1"})["Item"]
    assert "_migration_ts" not in a
    assert a["name"] == "x"

    b = target_table.get_item(Key={"pk": "b", "sk": "1"})["Item"]
    assert "_migration_ts" not in b


def test_idempotent_rerun(target_table: Any) -> None:
    target_table.put_item(Item={"pk": "a", "sk": "1", "_migration_ts": 100})
    cleanup.remove_migration_ts(target_table, "pk", "sk")
    counts = cleanup.remove_migration_ts(target_table, "pk", "sk")
    # Already removed; nothing to update.
    assert counts["updated"] == 0
