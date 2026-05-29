"""Sample-based source ↔ target cutover verifier.

Picks N random items from the source table, looks up the corresponding key in
the target table, applies ``transform()`` to the source item, and asserts
deep-equality (ignoring migration metadata attributes).

This is a confidence check, not a proof of completeness — for that, run
``convergence_check.py`` plus a full Scan COUNT comparison. Use this script
during a smoke test or as a CI gate before flipping app routing.

Exit codes
----------

* ``0`` — sample matches.
* ``1`` — at least one mismatch or missing item.
* ``2`` — usage error.
"""

from __future__ import annotations

import argparse
import importlib
import logging
import os
import random
import sys
from typing import Any, Callable

import boto3

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("verify_cutover")


def load_transform() -> Callable[..., Any]:
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


def sample_source_items(source_table: Any, sample_size: int) -> list[dict[str, Any]]:
    """Reservoir-sample items from the source table via Scan."""
    reservoir: list[dict[str, Any]] = []
    seen = 0
    scan_kwargs: dict[str, Any] = {}
    while True:
        resp = source_table.scan(**scan_kwargs)
        for item in resp.get("Items", []):
            seen += 1
            if len(reservoir) < sample_size:
                reservoir.append(item)
            else:
                idx = random.randint(0, seen - 1)
                if idx < sample_size:
                    reservoir[idx] = item
        if "LastEvaluatedKey" not in resp:
            break
        scan_kwargs["ExclusiveStartKey"] = resp["LastEvaluatedKey"]
    log.info("Sampled %d of %d source items", len(reservoir), seen)
    return reservoir


def items_match(source_item: dict[str, Any], target_item: dict[str, Any]) -> bool:
    """Deep-equality ignoring migration metadata."""
    s = {k: v for k, v in source_item.items() if not k.startswith("_migration") and not k.startswith("_tombstone") and not k.startswith("_ttl")}
    t = {k: v for k, v in target_item.items() if not k.startswith("_migration") and not k.startswith("_tombstone") and not k.startswith("_ttl")}
    return s == t


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    p.add_argument("--region", default=os.environ.get("REGION", "us-east-1"))
    p.add_argument("--source-table", default=os.environ.get("SOURCE_TABLE"))
    p.add_argument("--target-table", default=os.environ.get("TARGET_TABLE"))
    p.add_argument("--partition-key", default=os.environ.get("PARTITION_KEY", "pk"))
    p.add_argument("--sort-key", default=os.environ.get("SORT_KEY"))
    p.add_argument("--sample-size", type=int, default=1000)
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    if not args.source_table or not args.target_table:
        log.error("SOURCE_TABLE and TARGET_TABLE are required")
        return 2
    transform = load_transform()
    ddb = boto3.resource("dynamodb", region_name=args.region)
    source = ddb.Table(args.source_table)
    target = ddb.Table(args.target_table)

    samples = sample_source_items(source, args.sample_size)
    if not samples:
        log.warning("Source table is empty; nothing to verify")
        return 0

    matched = 0
    missing = 0
    diverged = 0
    for src_item in samples:
        key: dict[str, Any] = {args.partition_key: src_item[args.partition_key]}
        if args.sort_key and args.sort_key in src_item:
            key[args.sort_key] = src_item[args.sort_key]
        resp = target.get_item(Key=key)
        target_item = resp.get("Item")
        if not target_item:
            missing += 1
            log.warning("Missing in target: %s", key)
            continue
        expected = transform(dict(src_item))
        if expected is None:
            # Source item filtered by transform; target should also lack it.
            if target_item:
                diverged += 1
                log.warning("Filtered source but target has item: %s", key)
            continue
        if items_match(expected, target_item):
            matched += 1
        else:
            diverged += 1
            log.warning("Diverged: %s", key)

    log.info("=" * 60)
    log.info("  matched=%d  missing=%d  diverged=%d  total=%d", matched, missing, diverged, len(samples))
    log.info("=" * 60)
    if missing == 0 and diverged == 0:
        log.info("VERIFY OK")
        return 0
    log.error("VERIFY FAILED")
    return 1


if __name__ == "__main__":
    sys.exit(main())
