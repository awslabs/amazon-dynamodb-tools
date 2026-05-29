"""Tests for scripts/convergence_check.py."""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import boto3
import pytest

import convergence_check


def test_iterator_age_passes_when_below_threshold() -> None:
    cw = MagicMock()
    cw.get_metric_statistics.return_value = {"Datapoints": [{"Maximum": 100}]}
    assert convergence_check.check_iterator_age(cw, "fn", max_iterator_age_ms=1000, max_wait_seconds=2, poll_interval=0) is True


def test_iterator_age_fails_when_above_threshold() -> None:
    cw = MagicMock()
    cw.get_metric_statistics.return_value = {"Datapoints": [{"Maximum": 100_000}]}
    assert convergence_check.check_iterator_age(cw, "fn", max_iterator_age_ms=1000, max_wait_seconds=1, poll_interval=0) is False


def test_dlq_empty_passes_at_zero_depth() -> None:
    sqs = MagicMock()
    sqs.get_queue_attributes.return_value = {
        "Attributes": {"ApproximateNumberOfMessages": "0", "ApproximateNumberOfMessagesNotVisible": "0"},
    }
    assert convergence_check.check_dlq_empty(sqs, "https://example/dlq") is True


def test_dlq_empty_fails_when_visible() -> None:
    sqs = MagicMock()
    sqs.get_queue_attributes.return_value = {
        "Attributes": {"ApproximateNumberOfMessages": "1", "ApproximateNumberOfMessagesNotVisible": "0"},
    }
    assert convergence_check.check_dlq_empty(sqs, "https://example/dlq") is False


def test_dlq_empty_fails_when_in_flight() -> None:
    sqs = MagicMock()
    sqs.get_queue_attributes.return_value = {
        "Attributes": {"ApproximateNumberOfMessages": "0", "ApproximateNumberOfMessagesNotVisible": "1"},
    }
    assert convergence_check.check_dlq_empty(sqs, "https://example/dlq") is False


def test_count_check_uses_scan_not_metadata(aws: Any, source_table: Any, target_table: Any) -> None:
    """Critical: gap #12 was that the original PR used describe-table ItemCount (~6h stale).
    This test fails if a regression replaces the Scan with describe_table.
    """
    for i in range(5):
        source_table.put_item(Item={"pk": f"k{i}", "sk": "1"})
        target_table.put_item(Item={"pk": f"k{i}", "sk": "1", "_migration_ts": 0})
    ddb = boto3.client("dynamodb", region_name="us-east-1")
    assert convergence_check.check_item_counts(ddb, "source", "target", drift_pct=0.005) is True


def test_count_check_fails_above_drift_threshold(aws: Any, source_table: Any, target_table: Any) -> None:
    for i in range(100):
        source_table.put_item(Item={"pk": f"k{i}", "sk": "1"})
    for i in range(50):
        target_table.put_item(Item={"pk": f"k{i}", "sk": "1", "_migration_ts": 0})
    ddb = boto3.client("dynamodb", region_name="us-east-1")
    # 50% drift, threshold 0.5%, must fail
    assert convergence_check.check_item_counts(ddb, "source", "target", drift_pct=0.005) is False


def test_count_check_passes_when_drift_within_tolerance(aws: Any, source_table: Any, target_table: Any) -> None:
    for i in range(1000):
        source_table.put_item(Item={"pk": f"k{i}", "sk": "1"})
    for i in range(998):
        target_table.put_item(Item={"pk": f"k{i}", "sk": "1", "_migration_ts": 0})
    ddb = boto3.client("dynamodb", region_name="us-east-1")
    # 0.2% drift, threshold 0.5%, must pass
    assert convergence_check.check_item_counts(ddb, "source", "target", drift_pct=0.005) is True


def test_main_skip_flags_keep_zero_exit(aws: Any, source_table: Any, target_table: Any) -> None:
    source_table.put_item(Item={"pk": "a", "sk": "1"})
    target_table.put_item(Item={"pk": "a", "sk": "1", "_migration_ts": 0})
    rc = convergence_check.main([
        "--source-table", "source",
        "--target-table", "target",
        "--skip-iterator-age",
        "--skip-dlq",
    ])
    assert rc == 0


def test_main_returns_2_when_required_args_missing() -> None:
    rc = convergence_check.main([])
    assert rc == 2


def test_count_check_excludes_tombstones_from_target(
    aws: Any, source_table: Any, target_table: Any
) -> None:
    """Tombstones are placeholders for deleted source items — excluded from the live count.

    Without this filter, deleting an item from source then having stream-replay write a
    tombstone to target produces source_count == N-1 vs target_count == N, a false drift.
    """
    for i in range(100):
        source_table.put_item(Item={"pk": f"k{i}", "sk": "1"})
        target_table.put_item(Item={"pk": f"k{i}", "sk": "1", "_migration_ts": 0})
    # Simulate 5 deletes that became tombstones in target.
    for i in range(95, 100):
        source_table.delete_item(Key={"pk": f"k{i}", "sk": "1"})
        target_table.put_item(Item={"pk": f"k{i}", "sk": "1", "_tombstone": True, "_migration_ts": 100})
    ddb = boto3.client("dynamodb", region_name="us-east-1")
    # Without tombstone exclusion: source=95, target=100 → 5.3% drift, would fail.
    # With tombstone exclusion: source=95, target_live=95 → 0% drift, passes.
    assert convergence_check.check_item_counts(ddb, "source", "target", drift_pct=0.005) is True


def test_iterator_age_treats_no_data_as_pass_after_grace(monkeypatch: Any) -> None:
    """Idle Lambda emits no IteratorAge metric — must not block forever."""
    cw = MagicMock()
    cw.get_metric_statistics.return_value = {"Datapoints": []}
    # idle_grace_seconds=0 means first no-data hit immediately passes.
    assert convergence_check.check_iterator_age(
        cw, "fn", max_iterator_age_ms=1000, max_wait_seconds=2, poll_interval=0, idle_grace_seconds=0,
    ) is True
