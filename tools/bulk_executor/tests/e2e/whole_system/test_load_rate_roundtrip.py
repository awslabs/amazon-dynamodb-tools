"""Whole-system e2e for #182: `load --XMaxWriteRate` end to end.

This is NOT a smoke. A smoke (see ``connector/test_load_smoke.py``) proves the
Glue job accepts ``dynamodb.throughput.write`` and exits SUCCEEDED with a few
items — early detection that the wiring doesn't crash. It cannot prove the two
things #182 is actually about:

  1. **Round-trip fidelity** — a real dataset loaded through the connector
     write path lands *completely and unchanged* (every item, exact values).
  2. **Rate enforcement** — the connector *honors* the requested write rate,
     observed from DynamoDB's own ``ConsumedWriteCapacityUnits`` metric, not
     just that the option was accepted.

To keep (2) from being vacuously true, the fixture is sized so that an
*unthrottled* load would consume far more than the low ceiling we set for
several minutes. ``_assert_fixture_is_a_real_test`` fails loudly if the load
finished too fast for the ceiling to have bitten — otherwise a passing
assertion would prove nothing.

Runs against a **transient table** (own data, torn down in finally), so it is
safe under parallel runs and never touches the shared read/write tables.
"""
from __future__ import annotations

import csv
import io
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

# Sizing: 60k items × ~250 bytes each. At 1 WCU per 1KB write, that is ~60k
# WCUs of work. Unthrottled the connector would drain that in well under a
# minute (>>2000 WCU/s). Capped at 2000 WCU/s it must take ~30s+ of sustained
# writing across at least one full CloudWatch minute — making "peak minute
# stayed <= ceiling" a claim with teeth.
NUM_ITEMS = 60_000
WRITE_RATE_CEILING = 2000          # --XMaxWriteRate (WCU/s)
# CloudWatch SUM/60 is an average; allow headroom for bucket-edge bursts and
# the connector's coarse rate control. A run that ignored the ceiling would
# peak at many multiples of this, so a generous tolerance still discriminates.
RATE_TOLERANCE = 1.5               # peak minute may reach 1.5x the ceiling
PK_PREFIX = "ws-load-rate"


def _build_csv(run_id: str) -> str:
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(["pk", "sk", "payload"])
    # ~200-byte payload so each item is a meaningful (>1KB total is not needed;
    # 60k small items still generate minutes of throttled write work).
    payload = "x" * 200
    for i in range(NUM_ITEMS):
        writer.writerow([f"{PK_PREFIX}-{run_id}", f"item-{i:06d}", payload])
    return buf.getvalue()


def _upload_fixture(bucket: str, key: str, body: str, region: str) -> str:
    boto3.client("s3", region_name=region).put_object(
        Bucket=bucket, Key=key, Body=body.encode("utf-8")
    )
    return f"s3://{bucket}/{key}"


def _count_items(table: str, run_id: str, region: str) -> int:
    """Consistent COUNT of this run's items (single partition → one query)."""
    ddb = boto3.resource("dynamodb", region_name=region)
    total = 0
    kwargs = {
        "Select": "COUNT",
        "ConsistentRead": True,
        "KeyConditionExpression": "#pk = :pk",
        "ExpressionAttributeNames": {"#pk": "pk"},
        "ExpressionAttributeValues": {":pk": f"{PK_PREFIX}-{run_id}"},
    }
    tbl = ddb.Table(table)
    while True:
        resp = tbl.query(**kwargs)
        total += resp["Count"]
        last = resp.get("LastEvaluatedKey")
        if not last:
            return total
        kwargs["ExclusiveStartKey"] = last


def _sample_payload(table: str, run_id: str, region: str) -> str | None:
    """Read one item back to prove values survived, not just item count."""
    ddb = boto3.resource("dynamodb", region_name=region)
    resp = ddb.Table(table).get_item(
        Key={"pk": f"{PK_PREFIX}-{run_id}", "sk": "item-000000"},
        ConsistentRead=True,
    )
    item = resp.get("Item")
    return item["payload"] if item else None


def _assert_fixture_is_a_real_test(observed, wall_seconds: float) -> None:
    """Guard against a vacuous pass.

    The rate assertion only means something if the load actually sustained
    writes long enough for the ceiling to bind. If the whole thing finished
    inside a single CloudWatch minute (or we captured no busy datapoints),
    "peak <= ceiling" proves nothing — so fail explicitly and tell the next
    person to grow the fixture.
    """
    assert observed.observed_any, (
        "No ConsumedWriteCapacityUnits datapoints captured — the write window "
        "was too short to bucket, or metrics lagged. Grow NUM_ITEMS or widen "
        "the capacity window; do not treat this as a pass."
    )
    busy_minutes = [w for w in observed.per_minute_wcu if w > WRITE_RATE_CEILING * 0.1]
    assert len(busy_minutes) >= 1, (
        f"Load did not sustain writes across a full CloudWatch minute "
        f"(per-minute WCU/s: {observed.per_minute_wcu}). The rate ceiling "
        f"could not have bound — grow NUM_ITEMS so this is a real test."
    )


@pytest.mark.e2e
class TestLoadRateRoundTrip:
    def test_load_rate_limited_roundtrips_and_enforces_rate(
        self, e2e_config, ws_perf_collector
    ):
        region = e2e_config.aws_region
        run_id = f"{int(time.time())}-{uuid.uuid4().hex[:8]}"
        bucket = discover_bucket(region)
        s3_key = f"e2e/ws-load-rate/{run_id}.csv"
        s3_path = _upload_fixture(bucket, s3_key, _build_csv(run_id), region)

        with transient_table(region, has_sort_key=True, label="ws-loadrate") as table:
            window_start = utcnow()
            result = run_command(
                "load",
                table=table,
                extra_args=[
                    "--format", "csv",
                    "--s3-path", s3_path,
                    "--XMaxWriteRate", str(WRITE_RATE_CEILING),
                ],
            )
            window_end = utcnow()

            # 1. Job truly succeeded (not just ./bulk exit 0).
            perf = assert_glue_succeeded("load", result, region)

            # 1a. #182 point 1 — load must *state* the write rate, in two
            #     places for two audiences:
            #       - client-side, BEFORE dispatch (stderr, from log.info), so a
            #         user sees the rate they set before paying for the Glue run;
            #       - server-side, DURING the run (stdout, live-tailed from the
            #         Glue worker), confirming the connector resolved the same
            #         rate it was handed.
            #     Enforcement (point 2 below) is necessary but not sufficient;
            #     the original ask was that load *says* what rate it uses.
            client_line = str(WRITE_RATE_CEILING) in result.stderr and (
                "write rate" in result.stderr.lower()
            )
            assert client_line, (
                "load did not state the user-specified write rate client-side "
                f"before dispatch (looked for '{WRITE_RATE_CEILING}' + 'write "
                f"rate' in stderr). stderr tail:\n{result.stderr[-2000:]}"
            )
            assert "Max write rate set to specified limit" in result.stdout, (
                "server-side connector did not log the resolved write rate "
                "during the run (expected 'Max write rate set to specified "
                f"limit' in job logs). stdout tail:\n{result.stdout[-2000:]}"
            )

            # 2. Round-trip fidelity: every item landed, values intact.
            landed = _count_items(table, run_id, region)
            assert landed == NUM_ITEMS, (
                f"round-trip lost data: loaded {NUM_ITEMS}, found {landed}"
            )
            payload = _sample_payload(table, run_id, region)
            assert payload == "x" * 200, (
                f"payload corrupted on round-trip: {payload!r}"
            )

            # 3. Rate enforcement: observed consumed capacity honored the cap.
            observed = fetch_consumed_write_capacity(
                table, window_start, window_end, region
            )
            _assert_fixture_is_a_real_test(observed, result.wall_seconds or 0.0)
            assert observed.peak_minute_wcu <= WRITE_RATE_CEILING * RATE_TOLERANCE, (
                f"connector did NOT enforce --XMaxWriteRate={WRITE_RATE_CEILING}: "
                f"peak sustained {observed.peak_minute_wcu:.0f} WCU/s "
                f"(> {WRITE_RATE_CEILING} x {RATE_TOLERANCE} tolerance). "
                f"per-minute WCU/s: {[round(w) for w in observed.per_minute_wcu]}"
            )

            ws_perf_collector.add(PerfRow(
                command=f"load --XMaxWriteRate {WRITE_RATE_CEILING} (60k roundtrip)",
                wall_seconds=result.wall_seconds,
                dpu_seconds=perf.dpu_seconds if perf else None,
                items=landed,
            ))
