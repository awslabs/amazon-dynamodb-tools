"""Tests for scripts/verify_cutover.py."""

from __future__ import annotations

from typing import Any

import verify_cutover


def test_items_match_ignores_migration_metadata() -> None:
    src = {"pk": "a", "sk": "1", "name": "x"}
    tgt = {"pk": "a", "sk": "1", "name": "x", "_migration_ts": 100}
    assert verify_cutover.items_match(src, tgt) is True


def test_items_match_detects_data_divergence() -> None:
    src = {"pk": "a", "sk": "1", "name": "x"}
    tgt = {"pk": "a", "sk": "1", "name": "y"}
    assert verify_cutover.items_match(src, tgt) is False


def test_main_succeeds_when_target_matches(
    monkeypatch: Any, source_table: Any, target_table: Any
) -> None:
    for i in range(20):
        item = {"pk": f"k{i}", "sk": "1", "v": str(i)}
        source_table.put_item(Item=item)
        target_table.put_item(Item={**item, "_migration_ts": 100})
    rc = verify_cutover.main([
        "--source-table", "source",
        "--target-table", "target",
        "--partition-key", "pk",
        "--sort-key", "sk",
        "--sample-size", "20",
    ])
    assert rc == 0


def test_main_fails_when_target_missing_items(
    source_table: Any, target_table: Any
) -> None:
    for i in range(20):
        source_table.put_item(Item={"pk": f"k{i}", "sk": "1", "v": str(i)})
    # Only 5 in target.
    for i in range(5):
        target_table.put_item(Item={"pk": f"k{i}", "sk": "1", "v": str(i), "_migration_ts": 100})
    rc = verify_cutover.main([
        "--source-table", "source",
        "--target-table", "target",
        "--partition-key", "pk",
        "--sort-key", "sk",
        "--sample-size", "20",
    ])
    assert rc == 1


def test_main_fails_when_data_diverges(source_table: Any, target_table: Any) -> None:
    for i in range(20):
        source_table.put_item(Item={"pk": f"k{i}", "sk": "1", "v": str(i)})
        target_table.put_item(Item={"pk": f"k{i}", "sk": "1", "v": "WRONG", "_migration_ts": 100})
    rc = verify_cutover.main([
        "--source-table", "source",
        "--target-table", "target",
        "--partition-key", "pk",
        "--sort-key", "sk",
        "--sample-size", "20",
    ])
    assert rc == 1
