"""Pre-cutover convergence gate.

Runs three checks in sequence and exits 0 only when all pass:

1. **Iterator age** — stream-replay Lambda's ``IteratorAge`` is below
   ``--max-iterator-age-ms`` (default 1000 ms). Polls until the timeout.
2. **DLQ depth** — the Lambda's failure DLQ is empty (visible + in-flight).
3. **Item count drift** — Scan COUNT (not the table-metadata ``ItemCount``,
   which is updated only every ~6 hours) on both tables agrees within
   ``--count-drift-pct`` (default 0.5%). ``--ignore-count-drift`` to skip.

Exit codes
----------

* ``0`` — all checks passed; safe to cut over.
* ``1`` — at least one check failed.
* ``2`` — usage error (missing required env var or arg).
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from typing import Any

import boto3
from botocore.exceptions import ClientError

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("convergence_check")


def check_iterator_age(
    cw_client: Any,
    lambda_function_name: str,
    max_iterator_age_ms: int,
    max_wait_seconds: int,
    poll_interval: int = 10,
    idle_grace_seconds: int = 120,
) -> bool:
    """Poll IteratorAge until below threshold or until ``max_wait_seconds`` elapses.

    Lambda only emits IteratorAge on invocation. If the source table is idle,
    the metric has no datapoints — that means caught up, not lagging. After
    ``idle_grace_seconds`` of no datapoints we treat the absence as a pass.
    """
    deadline = time.time() + max_wait_seconds
    no_data_since: float | None = None
    while time.time() < deadline:
        end = datetime.now(timezone.utc)
        start = end - timedelta(minutes=2)
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
            log.error("CloudWatch query failed: %s", e.response["Error"]["Code"])
            return False
        points = resp.get("Datapoints", [])
        if not points:
            if no_data_since is None:
                no_data_since = time.time()
                log.info("No IteratorAge datapoints; starting %ds idle grace window", idle_grace_seconds)
            elif time.time() - no_data_since >= idle_grace_seconds:
                log.info("No IteratorAge datapoints for %ds — treating Lambda as idle/caught-up", idle_grace_seconds)
                return True
            time.sleep(poll_interval)
            continue
        no_data_since = None
        max_age = max(p["Maximum"] for p in points)
        log.info("IteratorAge max=%.0f ms (threshold %d ms)", max_age, max_iterator_age_ms)
        if max_age <= max_iterator_age_ms:
            return True
        time.sleep(poll_interval)
    log.error("IteratorAge did not converge within %ds", max_wait_seconds)
    return False


def check_dlq_empty(sqs_client: Any, dlq_url: str) -> bool:
    try:
        resp = sqs_client.get_queue_attributes(
            QueueUrl=dlq_url,
            AttributeNames=["ApproximateNumberOfMessages", "ApproximateNumberOfMessagesNotVisible"],
        )
    except ClientError as e:
        log.error("SQS query failed: %s", e.response["Error"]["Code"])
        return False
    visible = int(resp["Attributes"]["ApproximateNumberOfMessages"])
    in_flight = int(resp["Attributes"]["ApproximateNumberOfMessagesNotVisible"])
    log.info("DLQ depth: visible=%d in_flight=%d", visible, in_flight)
    return visible == 0 and in_flight == 0


def scan_count(ddb_client: Any, table_name: str, exclude_tombstones: bool = False) -> int:
    """Authoritative item count via Scan with Select=COUNT (paginated).

    ``exclude_tombstones=True`` filters out items where ``_tombstone`` is set,
    which is the right comparison for the target table (tombstones are
    placeholders for deleted source items, not live data).
    """
    total = 0
    kwargs: dict[str, Any] = {"TableName": table_name, "Select": "COUNT"}
    if exclude_tombstones:
        kwargs["FilterExpression"] = "attribute_not_exists(#t)"
        kwargs["ExpressionAttributeNames"] = {"#t": "_tombstone"}
    while True:
        resp = ddb_client.scan(**kwargs)
        total += resp.get("Count", 0)
        if "LastEvaluatedKey" not in resp:
            return total
        kwargs["ExclusiveStartKey"] = resp["LastEvaluatedKey"]


def check_item_counts(
    ddb_client: Any,
    source_table: str,
    target_table: str,
    drift_pct: float,
) -> bool:
    """Compare Scan COUNT on source vs (target − tombstones).

    Tombstones live in the target until ``cleanup.py`` runs post-cutover, so we
    must exclude them when comparing logical item counts.
    """
    log.info("Scanning source table %s (this may take several minutes)...", source_table)
    src_count = scan_count(ddb_client, source_table)
    log.info("Scanning target table %s (excluding tombstones)...", target_table)
    tgt_count = scan_count(ddb_client, target_table, exclude_tombstones=True)
    if src_count == 0:
        log.warning("Source has 0 items; treating count check as vacuously passed")
        return True
    drift = abs(src_count - tgt_count) / src_count
    log.info("Counts: source=%d target_live=%d drift=%.4f (max %.4f)", src_count, tgt_count, drift, drift_pct)
    return drift <= drift_pct


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    p.add_argument("--region", default=os.environ.get("REGION", "us-east-1"))
    p.add_argument("--source-table", default=os.environ.get("SOURCE_TABLE"))
    p.add_argument("--target-table", default=os.environ.get("TARGET_TABLE"))
    p.add_argument("--dlq-url", default=os.environ.get("DLQ_URL"))
    p.add_argument(
        "--lambda-function-name",
        default=os.environ.get("LAMBDA_FUNCTION_NAME", "ddb-migration-stream-replay"),
    )
    p.add_argument("--max-iterator-age-ms", type=int, default=1000)
    p.add_argument("--max-wait-seconds", type=int, default=600)
    p.add_argument("--count-drift-pct", type=float, default=0.005)
    p.add_argument("--ignore-count-drift", action="store_true")
    p.add_argument("--skip-iterator-age", action="store_true", help="(testing only)")
    p.add_argument("--skip-dlq", action="store_true", help="(testing only)")
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    if not args.source_table or not args.target_table:
        log.error("SOURCE_TABLE and TARGET_TABLE are required")
        return 2
    if not args.dlq_url and not args.skip_dlq:
        log.error("DLQ_URL is required (or pass --skip-dlq)")
        return 2

    cw = boto3.client("cloudwatch", region_name=args.region)
    sqs = boto3.client("sqs", region_name=args.region)
    ddb = boto3.client("dynamodb", region_name=args.region)

    results = []
    if args.skip_iterator_age:
        log.warning("Skipping iterator-age check (testing only)")
        results.append(("iterator_age", True))
    else:
        results.append(
            ("iterator_age", check_iterator_age(
                cw, args.lambda_function_name, args.max_iterator_age_ms, args.max_wait_seconds,
            )),
        )
    if args.skip_dlq:
        log.warning("Skipping DLQ check (testing only)")
        results.append(("dlq_empty", True))
    else:
        results.append(("dlq_empty", check_dlq_empty(sqs, args.dlq_url)))
    if args.ignore_count_drift:
        log.warning("Skipping count check (--ignore-count-drift)")
        results.append(("count_match", True))
    else:
        results.append((
            "count_match",
            check_item_counts(ddb, args.source_table, args.target_table, args.count_drift_pct),
        ))

    log.info("=" * 60)
    for name, passed in results:
        log.info("  %s %s", "PASS" if passed else "FAIL", name)
    log.info("=" * 60)

    if all(passed for _, passed in results):
        log.info("CONVERGENCE OK — proceed with cutover")
        return 0
    log.error("CONVERGENCE FAILED — do not cut over")
    return 1


if __name__ == "__main__":
    sys.exit(main())
