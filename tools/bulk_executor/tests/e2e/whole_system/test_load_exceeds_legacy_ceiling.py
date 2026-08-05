"""Whole-system e2e for #182: prove the new connector beats the old 60k ceiling.

The legacy DynamoDB connector could not write faster than ~60k WCU/s to an
on-demand table: it hard-coded a 40k on-demand assumption and expressed the
rate as ``dynamodb.throughput.write.percent`` capped at 1.5x — so 40k x 1.5 =
**60,000 WCU/s was the hard maximum**, no matter what the user asked for.

The Glue 5.x DataFrame connector takes an absolute ``dynamodb.throughput.write``
integer (see the published option reference: *"The write capacity units (WCU)
to use"* — an absolute value, taken directly; it only falls back to
``dynamodb.throughput.write.ratio`` when unset). ``load`` now wires the user's
``--XMaxWriteRate`` straight through as that integer, with no 40k assumption and
no percentage cap. So the connector can be *told* to exceed 60k. This test
proves DynamoDB actually *sustains* it end to end.

What it takes to observe it (and why this test is expensive):

  * **Absolute rate, not percent** — the load runs with ``--XMaxWriteRate``
    above 60k. Under the old connector that request was physically
    unrepresentable; here it flows through verbatim.
  * **Pre-warmed table** — an on-demand table throttles during its cold-start
    ramp, so the table is created with ``WarmThroughput`` at the target rate.
    Without this, the connector's writes bounce off retries and never reach the
    ceiling, making the test a false negative.
  * **Many partition keys** — a single DynamoDB partition caps at ~1000 WCU/s,
    so items are spread across thousands of partition keys; otherwise a hot
    partition, not the connector, would be the bottleneck.
  * **Enough volume to fill a clock-aligned CloudWatch minute above 60k** —
    CloudWatch sums ``ConsumedWriteCapacityUnits`` per 60s. To get one whole
    minute averaging >60k WCU/s the load must sustain the rate for well over a
    minute; the fixture is sized for that. ``_assert_fixture_is_a_real_test``
    fails loudly if it wasn't, so a too-small fixture is never a silent pass.

This test writes millions of items and provisions warm throughput, so it costs
real money (order of a few dollars of DynamoDB write capacity plus Glue DPU per
run) and is **not** part of the default whole-system sweep. It is marked
``max_rate`` and run only via ``make test-e2e-max-rate``. Tune the fixture with
the ``BULK_E2E_MAXRATE_*`` env vars below before a live run.

Runs against a **transient table** (own data, torn down in finally), safe under
parallel runs.
"""
from __future__ import annotations

import csv
import os
import tempfile
import time
import uuid

import boto3
import pytest

from tests.e2e.connector.conftest import PerfRow
from tests.e2e.helpers.assertions import assert_glue_succeeded
from tests.e2e.helpers.capacity import fetch_consumed_write_capacity, utcnow
from tests.e2e.helpers.command_runner import run_command
from tests.e2e.helpers.glue_bucket import discover_bucket
from tests.e2e.helpers.transient_table import transient_table

# The wall the old connector could not climb over: 40k on-demand assumption x
# the 1.5x percentage cap = 60,000 WCU/s. Clearing this is the whole point.
LEGACY_CONNECTOR_CEILING = 60_000

# Target rate we ask the new connector for. Set to 120k — well above the 60k
# legacy ceiling, and now actually *delivered*: a live run lands ~118k WCU/s
# (99% of the request; two full CloudWatch minutes pinned ~116-118k). Getting
# there took two fixes working together: (1) the connector takes
# dynamodb.throughput.write as an absolute integer with no percent cap, and
# (2) load repartitions the write to 100 Spark tasks. At the old repartition(30)
# the same 120k request stalled at ~86k (72%) — parallelism, not the rate knob,
# was the ceiling on a 220-worker cluster. At 100 tasks the knob is the binding
# limit, so requested ≈ observed. Overridable for live tuning.
TARGET_WRITE_RATE = int(os.environ.get("BULK_E2E_MAXRATE_WCU", "120000"))

# Items are intentionally tiny: DynamoDB bills a 1 WCU minimum per write, so a
# ~40-byte item still consumes 1 WCU. That makes item_count ≈ WCU consumed
# while keeping the source fixture small enough to generate and upload. To
# average >60k WCU/s across a full CloudWatch minute, and to survive the load
# straddling minute boundaries, we sustain the rate for ~2.5 min:
#   TARGET_WRITE_RATE * ~150s of headroom, rounded up.
# 120k * ~150s ≈ 18M items. Overridable for live tuning / cost control.
NUM_ITEMS = int(os.environ.get("BULK_E2E_MAXRATE_ITEMS", "18000000"))

# Spread writes so no single partition key is the bottleneck. A single
# DynamoDB partition key is hard-capped at ~1000 WCU/s regardless of the
# table's total capacity, so with too few keys the load throttles per-key long
# before it reaches the table ceiling. At NUM_ITEMS/NUM_PARTITIONS items per
# key and TARGET_WRITE_RATE/NUM_PARTITIONS steady-state WCU/s per key, 150k
# keys keeps every key an order of magnitude under its 1000/s cap even when
# Spark bursts. (A first attempt at 8k keys throttled — 1000+ WriteThrottle
# events/min — and capped observed throughput at ~68k despite the request; the
# 120k live run at 150k keys saw zero throttles.)
NUM_PARTITIONS = int(os.environ.get("BULK_E2E_MAXRATE_PARTITIONS", "150000"))

# Warm throughput headroom above the requested rate. An on-demand table only
# sustains a rate once it has physically split into enough partitions; warm
# throughput is DynamoDB's pre-split lever. Provisioning ABOVE the target (not
# exactly at it) gives the table room to absorb the target without the request
# bumping the provisioned ceiling itself. This is why we don't need a separate
# warm-up write pass — WarmThroughput *is* the pre-split.
WARM_HEADROOM = 1.5  # warm the table to 120k * 1.5 = 180k WCU/s
WARM_WRITE_UNITS = int(TARGET_WRITE_RATE * WARM_HEADROOM)

# Pass criterion: the observed peak must clear the legacy 60k ceiling with
# margin — that is the provable, unfakeable claim (>60k was physically
# impossible under the old 40k x 1.5 percent cap). We request 120k and report
# the observed peak, but we do NOT gate the test on hitting exactly 120k:
# DynamoDB partition-split timing and upstream Spark write parallelism make the
# precise peak environment-dependent, and gating on it would be flaky. Clearing
# 60k is the proof; the reported peak shows how close to the request the run got.
CEILING_CLEARANCE = 1.05  # observed peak must exceed 60k * 1.05 = 63k WCU/s

PK_PREFIX = "ws-maxrate"
PAYLOAD = "x" * 8  # tiny; item stays well under 1KB → 1 WCU per write


def _write_csv_to_s3(bucket: str, key: str, run_id: str, region: str) -> str:
    """Stream a large CSV to a temp file and multipart-upload it to S3.

    NUM_ITEMS can be tens of millions of rows, far too large to build in
    memory, so we generate row-by-row to a temp file and let boto3's
    upload_file do a multipart transfer.
    """
    s3 = boto3.client("s3", region_name=region)
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".csv", delete=False
    ) as fh:
        tmp_path = fh.name
        writer = csv.writer(fh)
        writer.writerow(["pk", "sk", "payload"])
        for i in range(NUM_ITEMS):
            # Spread across partitions; sk keeps each item unique in-partition.
            writer.writerow([
                f"{PK_PREFIX}-{run_id}-{i % NUM_PARTITIONS}",
                f"item-{i:09d}",
                PAYLOAD,
            ])
    try:
        s3.upload_file(tmp_path, bucket, key)
    finally:
        os.unlink(tmp_path)
    return f"s3://{bucket}/{key}"


def _sample_landed(table: str, run_id: str, region: str) -> bool:
    """Prove data actually landed by reading back a few known keys.

    A full COUNT of NUM_ITEMS would be its own multi-million-item scan; the
    60k round-trip test already proves fidelity, so here we spot-check that the
    high-rate write path put real items in the table (not a vacuous SUCCEEDED).
    """
    ddb = boto3.resource("dynamodb", region_name=region)
    tbl = ddb.Table(table)
    probes = [0, NUM_ITEMS // 2, NUM_ITEMS - 1]
    for i in probes:
        resp = tbl.get_item(
            Key={
                "pk": f"{PK_PREFIX}-{run_id}-{i % NUM_PARTITIONS}",
                "sk": f"item-{i:09d}",
            },
            ConsistentRead=True,
        )
        if resp.get("Item", {}).get("payload") != PAYLOAD:
            return False
    return True


def _assert_fixture_is_a_real_test(observed) -> None:
    """Guard against a vacuous pass.

    "peak > 60k" only means something if the load actually sustained a high
    write rate long enough to fill a CloudWatch minute. If we captured no busy
    datapoints, the window was too short or warm throughput never provisioned —
    fail explicitly and tell the next person how to fix it, rather than pass on
    a run that never stressed the connector.
    """
    assert observed.observed_any, (
        "No ConsumedWriteCapacityUnits datapoints captured — the write window "
        "was too short to bucket, or metrics lagged. Grow BULK_E2E_MAXRATE_ITEMS "
        "or widen the capacity window; do not treat this as a pass."
    )
    # At least one minute must have run hot enough that a >60k reading is
    # credible (not a lone edge-of-window blip).
    hot_minutes = [w for w in observed.per_minute_wcu if w > LEGACY_CONNECTOR_CEILING * 0.5]
    assert len(hot_minutes) >= 1, (
        f"Load never sustained a high write rate (per-minute WCU/s: "
        f"{[round(w) for w in observed.per_minute_wcu]}). Either warm "
        f"throughput did not provision or the fixture is too small — grow "
        f"BULK_E2E_MAXRATE_ITEMS so this is a real test."
    )


@pytest.mark.e2e
@pytest.mark.max_rate
class TestLoadExceedsLegacyCeiling:
    def test_load_writes_faster_than_the_old_60k_ceiling(
        self, e2e_config, ws_perf_collector
    ):
        region = e2e_config.aws_region
        run_id = f"{int(time.time())}-{uuid.uuid4().hex[:8]}"
        bucket = discover_bucket(region)
        s3_key = f"e2e/ws-maxrate/{run_id}.csv"
        s3_path = _write_csv_to_s3(bucket, s3_key, run_id, region)

        # Pre-warm the on-demand table ABOVE the target rate so DynamoDB has
        # split into enough partitions to absorb the full target rate without
        # the request bumping the provisioned ceiling; wait for it to provision.
        with transient_table(
            region,
            has_sort_key=True,
            label="ws-maxrate",
            warm_write_units=WARM_WRITE_UNITS,
            wait_for_warm=True,
        ) as table:
            window_start = utcnow()
            result = run_command(
                "load",
                table=table,
                extra_args=[
                    "--format", "csv",
                    "--s3-path", s3_path,
                    "--XMaxWriteRate", str(TARGET_WRITE_RATE),
                ],
            )
            window_end = utcnow()

            # 1. Job truly succeeded (not just ./bulk exit 0).
            perf = assert_glue_succeeded("load", result, region)

            # 2. Data landed — spot-check known keys so SUCCEEDED isn't vacuous.
            assert _sample_landed(table, run_id, region), (
                "high-rate load reported SUCCEEDED but sampled items were "
                "missing or corrupted on read-back."
            )

            # 3. THE PROOF: observed peak sustained write rate cleared the old
            #    60k connector ceiling — something the legacy percent-capped
            #    connector could never do. The CloudWatch observation IS the
            #    behavioral proof and is unfakeable: the old connector's
            #    40k x 1.5 = 60k cap made an observed >60k physically impossible,
            #    so clearing it here proves the clamp is gone. We deliberately do
            #    NOT also assert on the worker's "specified limit" log line: on a
            #    multi-million-item load the client's best-effort live-tail can
            #    miss that early line (it is emitted after a minutes-long S3 read
            #    phase, across live-tail reconnects), so asserting on it would be
            #    flaky — and it proves strictly less than the observed rate does.
            observed = fetch_consumed_write_capacity(
                table, window_start, window_end, region
            )
            _assert_fixture_is_a_real_test(observed)
            floor = LEGACY_CONNECTOR_CEILING * CEILING_CLEARANCE
            pct_of_target = 100.0 * observed.peak_minute_wcu / TARGET_WRITE_RATE
            print(
                f"\n[max-rate] requested {TARGET_WRITE_RATE} WCU/s; observed peak "
                f"{observed.peak_minute_wcu:.0f} WCU/s ({pct_of_target:.0f}% of "
                f"target), vs legacy ceiling {LEGACY_CONNECTOR_CEILING}. "
                f"per-minute WCU/s: {[round(w) for w in observed.per_minute_wcu]}\n"
            )
            assert observed.peak_minute_wcu > floor, (
                f"new connector did NOT beat the legacy 60k ceiling: peak "
                f"sustained {observed.peak_minute_wcu:.0f} WCU/s "
                f"(needed > {floor:.0f}). The old connector maxed at "
                f"{LEGACY_CONNECTOR_CEILING} (40k x 1.5). per-minute WCU/s: "
                f"{[round(w) for w in observed.per_minute_wcu]}"
            )

            ws_perf_collector.add(PerfRow(
                command=(
                    f"load --XMaxWriteRate {TARGET_WRITE_RATE} "
                    f"(peak {observed.peak_minute_wcu:.0f} WCU/s, "
                    f"{pct_of_target:.0f}% of target)"
                ),
                wall_seconds=result.wall_seconds,
                dpu_seconds=perf.dpu_seconds if perf else None,
                items=NUM_ITEMS,
            ))
