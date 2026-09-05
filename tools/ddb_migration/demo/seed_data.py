"""Seed the demo source table with a configurable number of items."""

from __future__ import annotations

import argparse
import os
import random
import string
import sys
from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal

import boto3


def make_item(idx: int, partition_key: str, sort_key: str | None) -> dict:
    item: dict = {
        partition_key: f"order#{idx:08d}",
        "customer_id": f"cust#{random.randint(1, 1000):04d}",
        "amount": Decimal(str(round(random.uniform(1.0, 999.99), 2))),
        "status": random.choice(["NEW", "PAID", "SHIPPED", "DELIVERED"]),
        "notes": "".join(random.choices(string.ascii_lowercase, k=20)),
    }
    if sort_key:
        item[sort_key] = f"line#{random.randint(1, 5):03d}"
    return item


def write_batch(table, items: list[dict]) -> None:
    with table.batch_writer() as batch:
        for item in items:
            batch.put_item(Item=item)


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--region", default=os.environ.get("REGION", "us-east-1"))
    p.add_argument("--table", default=os.environ.get("SOURCE_TABLE"))
    p.add_argument("--partition-key", default=os.environ.get("PARTITION_KEY", "pk"))
    p.add_argument("--sort-key", default=os.environ.get("SORT_KEY") or None)
    p.add_argument("--count", type=int, default=int(os.environ.get("DEMO_ITEM_COUNT", "10000")))
    p.add_argument("--workers", type=int, default=8)
    args = p.parse_args()
    if not args.table:
        print("SOURCE_TABLE is required", file=sys.stderr)
        return 2
    table = boto3.resource("dynamodb", region_name=args.region).Table(args.table)
    print(f"Seeding {args.count} items into {args.table}...")
    chunk = max(1, args.count // args.workers)
    batches: list[list[dict]] = []
    cur: list[dict] = []
    for i in range(args.count):
        cur.append(make_item(i, args.partition_key, args.sort_key))
        if len(cur) >= 25:
            batches.append(cur)
            cur = []
    if cur:
        batches.append(cur)
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        for _ in pool.map(lambda b: write_batch(table, b), batches):
            pass
    print(f"Seeded {args.count} items")
    return 0


if __name__ == "__main__":
    sys.exit(main())
