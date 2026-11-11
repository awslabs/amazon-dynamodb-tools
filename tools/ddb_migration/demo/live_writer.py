"""Background writer simulating live application traffic during the demo.

Writes a configurable rate of mutations to the source table for a fixed
duration. Mix of inserts (new keys), updates (existing keys), and deletes —
exercises every stream event type. Run from run_demo.sh as a background job.
"""

from __future__ import annotations

import argparse
import os
import random
import string
import sys
import time
from decimal import Decimal

import boto3
from botocore.exceptions import ClientError


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--region", default=os.environ.get("REGION", "us-east-1"))
    p.add_argument("--table", default=os.environ.get("SOURCE_TABLE"))
    p.add_argument("--partition-key", default=os.environ.get("PARTITION_KEY", "pk"))
    p.add_argument("--sort-key", default=os.environ.get("SORT_KEY") or None)
    p.add_argument("--rate", type=int, default=int(os.environ.get("DEMO_LIVE_WRITE_RATE", "5")))
    p.add_argument("--duration", type=int, default=int(os.environ.get("DEMO_LIVE_WRITE_DURATION_SECS", "120")))
    p.add_argument("--seed-count", type=int, default=int(os.environ.get("DEMO_ITEM_COUNT", "10000")))
    args = p.parse_args()
    if not args.table:
        print("SOURCE_TABLE is required", file=sys.stderr)
        return 2

    table = boto3.resource("dynamodb", region_name=args.region).Table(args.table)
    deadline = time.time() + args.duration
    interval = 1.0 / args.rate
    counts = {"insert": 0, "update": 0, "delete": 0, "errors": 0}
    cycle = 0

    while time.time() < deadline:
        op = random.choices(["insert", "update", "delete"], weights=[2, 6, 2])[0]
        try:
            if op == "insert":
                pk = f"order#new#{int(time.time()*1000)}#{cycle:05d}"
                item: dict = {
                    args.partition_key: pk,
                    "customer_id": f"cust#{random.randint(1, 1000):04d}",
                    "amount": Decimal(str(round(random.uniform(1.0, 999.99), 2))),
                    "status": "NEW",
                    "notes": "".join(random.choices(string.ascii_lowercase, k=20)),
                }
                if args.sort_key:
                    item[args.sort_key] = f"line#{random.randint(1, 5):03d}"
                table.put_item(Item=item)
                counts["insert"] += 1
            elif op == "update":
                idx = random.randint(0, args.seed_count - 1)
                key: dict = {args.partition_key: f"order#{idx:08d}"}
                if args.sort_key:
                    key[args.sort_key] = f"line#{random.randint(1, 5):03d}"
                table.update_item(
                    Key=key,
                    UpdateExpression="SET #s = :s",
                    ExpressionAttributeNames={"#s": "status"},
                    ExpressionAttributeValues={":s": random.choice(["PAID", "SHIPPED", "DELIVERED"])},
                )
                counts["update"] += 1
            else:
                idx = random.randint(0, args.seed_count - 1)
                key = {args.partition_key: f"order#{idx:08d}"}
                if args.sort_key:
                    key[args.sort_key] = f"line#{random.randint(1, 5):03d}"
                table.delete_item(Key=key)
                counts["delete"] += 1
        except ClientError as e:
            counts["errors"] += 1
            if e.response["Error"]["Code"] not in ("ConditionalCheckFailedException", "ResourceNotFoundException"):
                print(f"  live_writer error: {e.response['Error']['Code']}", file=sys.stderr)
        cycle += 1
        time.sleep(interval)

    print(f"live_writer done: insert={counts['insert']} update={counts['update']} delete={counts['delete']} errors={counts['errors']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
