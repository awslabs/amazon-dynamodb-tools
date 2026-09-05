"""Shared item transformation for DynamoDB zero-downtime migration.

Both ``lambda/stream_replay.py`` and ``scripts/backfill.py`` import ``transform``
from this module. Keeping a single implementation avoids divergence between the
backfill and live-replay paths — divergence breaks the conflict-resolution
invariant (newer ``_migration_ts`` wins) because the same logical item could be
written under two different shapes.

To customize for a real migration, either:

* edit ``transform`` below, or
* set the environment variable ``TRANSFORM_MODULE`` to a Python module path
  (e.g. ``my_transforms.orders``) that exposes a ``transform(item, source_event=None)``
  function. The Lambda and backfill scripts will load it via ``importlib`` at
  startup. The module must be importable from ``PYTHONPATH``.

Contract:

* Input ``item`` is a regular Python ``dict`` (DynamoDB JSON already deserialized).
* ``source_event`` is the full DynamoDB Streams event record for stream replay,
  or ``None`` when called from the backfill path.
* Return the (possibly mutated) ``dict`` to write to the target.
* Return ``None`` to skip the item entirely (filter pattern).

Examples
--------

Rename an attribute::

    def transform(item, source_event=None):
        if "old_name" in item:
            item["new_name"] = item.pop("old_name")
        return item

Compute a new GSI key::

    def transform(item, source_event=None):
        item["status_date_idx"] = f"{item['status']}#{item['created_at']}"
        return item

Filter out items by status::

    def transform(item, source_event=None):
        if item.get("status") == "DELETED":
            return None
        return item

Convert legacy ``Decimal`` floats::

    from decimal import Decimal

    def transform(item, source_event=None):
        if "price" in item and isinstance(item["price"], float):
            item["price"] = Decimal(str(item["price"]))
        return item
"""

from __future__ import annotations

from typing import Any


def transform(item: dict[str, Any], source_event: dict[str, Any] | None = None) -> dict[str, Any] | None:
    """Default identity transform — pass items through unchanged."""
    return item
