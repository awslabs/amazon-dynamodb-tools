"""Whole-system e2e for #89: `load --XMaxWriteRate` capacity warnings fire live.

PR #231's source + unit tests prove the warning *logic* in isolation
(``tests/server/test_rate_capacity_warnings.py``). This suite proves the
warnings actually **surface on a real Glue job** against a real DynamoDB table
of the right billing topology — the E2E-is-the-proof rule: a connector-path
change is not "done" until it's watched succeed live.

Unlike the #182 rate-*enforcement* test (``test_load_rate_roundtrip.py``, which
needs a 60k fixture so the ceiling has time to bind), the #89 capacity
*warnings* fire at throughput-config setup — *before* any data moves
(``get_dynamodb_throughput_configs``). So a tiny fixture is enough: the warning
is emitted the moment the connector resolves the requested rate against the
table's topology, regardless of how many rows follow. Each scenario therefore
loads only a handful of rows and asserts the specific warning substring in the
live LiveTail stdout, after confirming the Glue job truly SUCCEEDED.

Scenarios 1-4 (checks 2-5) run against **transient tables** (own data + own
throughput shape, torn down in finally), safe under parallel runs. Scenario 5
(check 1, the job-timeout estimate) loads a tiny CSV into the persistent,
already-large ``write_table``: the write estimate keys off the target's existing
ItemCount (which reads 0 for ~6h after a fresh fill, so a transient table would
false-green) while only ~20 rows are actually written, so the warning fires yet
the job finishes fast. The missing-permission degradation (#89 check when the
Glue role lacks ``application-autoscaling:DescribeScalableTargets``) needs to
flip the *shared* job's role, so it lives in
``security/test_capacity_warning_missing_perm.py`` under the security-suite
guard — not here.
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
from tests.e2e.helpers.command_runner import run_command
from tests.e2e.helpers.glue_bucket import discover_bucket
from tests.e2e.helpers.transient_table import autoscaling_target, transient_table

# The warnings fire at config-time, so the fixture only needs enough rows to
# make the load a real (SUCCEEDED) job. Keep it tiny — this is NOT an
# enforcement test.
NUM_ITEMS = 20
PK_PREFIX = "ws-cap-warn"

# Exact substrings emitted by table_info._warn_if_rate_exceeds_capacity /
# _effective_capacity_ceiling. Assert on distinctive fragments (not whole
# sentences) so trivial wording tweaks don't break the test, but the fragment
# is specific enough that only the intended branch produces it.
WARN_PROVISIONED = "exceeds the table's provisioned capacity of"
WARN_AUTOSCALING_MAX = "exceeds the table's autoscaling maximum of"
WARN_SOFT_SCALE_UP = "autoscaling will need to scale up"
WARN_ON_DEMAND_MAX = "on-demand maximum of"
# Emitted by table_info._warn_if_job_may_timeout (issue #89 check 1).
WARN_JOB_TIMEOUT = "the job will likely time out before finishing"

# A deliberately low write rate: against the write_table's millions of existing
# items the config-time estimate (item_count / rate) predicts a multi-hour run
# and warns, while the actual load only writes the ~20-row CSV and finishes in
# the usual ~2 min. That gap is what lets the warning fire on a job that still
# SUCCEEDS — see test_slow_rate_predicted_timeout_warns_load.
SLOW_WRITE_RATE = 100


def _build_csv(run_id: str) -> str:
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(["pk", "sk", "payload"])
    for i in range(NUM_ITEMS):
        writer.writerow([f"{PK_PREFIX}-{run_id}", f"item-{i:04d}", f"data-{i}"])
    return buf.getvalue()


def _upload_fixture(run_id: str, region: str) -> str:
    bucket = discover_bucket(region)
    key = f"e2e/ws-cap-warn/{run_id}.csv"
    boto3.client("s3", region_name=region).put_object(
        Bucket=bucket, Key=key, Body=_build_csv(run_id).encode("utf-8")
    )
    return f"s3://{bucket}/{key}"


def _run_load(table: str, s3_path: str, write_rate: int):
    return run_command(
        "load",
        table=table,
        extra_args=[
            "--format", "csv",
            "--s3-path", s3_path,
            "--XMaxWriteRate", str(write_rate),
        ],
    )


def _assert_warning(result, substring: str, scenario: str) -> None:
    assert substring in result.stdout, (
        f"[{scenario}] expected #89 capacity warning fragment "
        f"{substring!r} in the live Glue job stdout, not found. "
        f"stdout tail:\n{result.stdout[-2500:]}"
    )


@pytest.mark.e2e
class TestCapacityWarningsLive:
    def test_provisioned_no_autoscaling_request_exceeds_warns(
        self, e2e_config, ws_perf_collector
    ):
        """Scenario 1 — provisioned table, no autoscaling: a write rate above
        the provisioned WCU produces the hard 'exceeds provisioned' warning."""
        region = e2e_config.aws_region
        run_id = f"{int(time.time())}-{uuid.uuid4().hex[:8]}"
        s3_path = _upload_fixture(run_id, region)

        # Provisioned at 5 WCU; request 500 → well above the ceiling.
        with transient_table(
            region, has_sort_key=True, label="cap-prov", provisioned=(5, 5)
        ) as table:
            result = _run_load(table, s3_path, write_rate=500)
            perf = assert_glue_succeeded("load", result, region)
            _assert_warning(result, WARN_PROVISIONED, "provisioned-no-AS")

            ws_perf_collector.add(PerfRow(
                command="load #89 provisioned-exceeds warning",
                wall_seconds=result.wall_seconds,
                dpu_seconds=perf.dpu_seconds if perf else None,
                items=NUM_ITEMS,
            ))

    def test_provisioned_autoscaling_above_max_hard_warns(
        self, e2e_config, ws_perf_collector
    ):
        """Scenario 2 — provisioned + autoscaling: a write rate above the
        autoscaling MaxCapacity produces the hard 'exceeds autoscaling
        maximum' warning (not the provisioned one)."""
        region = e2e_config.aws_region
        run_id = f"{int(time.time())}-{uuid.uuid4().hex[:8]}"
        s3_path = _upload_fixture(run_id, region)

        with transient_table(
            region, has_sort_key=True, label="cap-asmax", provisioned=(5, 5)
        ) as table:
            # Autoscaling can climb to 100; request 1000 → above the max.
            with autoscaling_target(
                region, table, min_write=5, max_write=100
            ) as as_max:
                result = _run_load(table, s3_path, write_rate=as_max * 10)
                perf = assert_glue_succeeded("load", result, region)
                _assert_warning(result, WARN_AUTOSCALING_MAX, "provisioned+AS-above-max")

                ws_perf_collector.add(PerfRow(
                    command="load #89 autoscaling-max-exceeds warning",
                    wall_seconds=result.wall_seconds,
                    dpu_seconds=perf.dpu_seconds if perf else None,
                    items=NUM_ITEMS,
                ))

    def test_provisioned_autoscaling_between_floor_and_max_soft_note(
        self, e2e_config, ws_perf_collector
    ):
        """Scenario 3 — provisioned + autoscaling: a write rate between the
        provisioned floor and the autoscaling max produces the SOFT
        'autoscaling will need to scale up' note, distinct from a hard warn."""
        region = e2e_config.aws_region
        run_id = f"{int(time.time())}-{uuid.uuid4().hex[:8]}"
        s3_path = _upload_fixture(run_id, region)

        with transient_table(
            region, has_sort_key=True, label="cap-soft", provisioned=(5, 5)
        ) as table:
            # Floor 5, max 100; request 50 → between floor and max → soft note.
            with autoscaling_target(
                region, table, min_write=5, max_write=100
            ) as as_max:
                request = (5 + as_max) // 2  # 52, comfortably between 5 and 100
                result = _run_load(table, s3_path, write_rate=request)
                perf = assert_glue_succeeded("load", result, region)
                _assert_warning(result, WARN_SOFT_SCALE_UP, "provisioned+AS-soft-note")
                # And it must NOT be the hard 'exceeds' warning.
                assert WARN_AUTOSCALING_MAX not in result.stdout, (
                    "soft-note scenario wrongly emitted the hard "
                    "'exceeds autoscaling maximum' warning"
                )

                ws_perf_collector.add(PerfRow(
                    command="load #89 autoscaling-soft-note",
                    wall_seconds=result.wall_seconds,
                    dpu_seconds=perf.dpu_seconds if perf else None,
                    items=NUM_ITEMS,
                ))

    def test_on_demand_table_max_request_exceeds_warns(
        self, e2e_config, ws_perf_collector
    ):
        """Scenario 4 — on-demand table with MaxWriteRequestUnits set: a write
        rate above that table max produces the 'on-demand maximum' warning."""
        region = e2e_config.aws_region
        run_id = f"{int(time.time())}-{uuid.uuid4().hex[:8]}"
        s3_path = _upload_fixture(run_id, region)

        # On-demand with a table-level write ceiling of 100; request 1000.
        with transient_table(
            region, has_sort_key=True, label="cap-odmax",
            on_demand_max_write_units=100,
        ) as table:
            result = _run_load(table, s3_path, write_rate=1000)
            perf = assert_glue_succeeded("load", result, region)
            _assert_warning(result, WARN_ON_DEMAND_MAX, "on-demand-table-max")

            ws_perf_collector.add(PerfRow(
                command="load #89 on-demand-max-exceeds warning",
                wall_seconds=result.wall_seconds,
                dpu_seconds=perf.dpu_seconds if perf else None,
                items=NUM_ITEMS,
            ))

    def test_slow_rate_predicted_timeout_warns_load(
        self, e2e_config, ws_perf_collector
    ):
        """Scenario 5 (issue #89 check 1) — a write rate too slow to move the
        target table's data before the job timeout emits the 'will likely time
        out' warning, and the job still SUCCEEDS.

        The trick that decouples "warns" from "runs slow": the write estimate
        keys off the *target table's existing size* (DescribeTable ItemCount),
        but the actual load only writes the tiny input CSV. So we load ~20 rows
        into the persistent, already-large write_table: the estimate sees
        millions of items at a slow --XMaxWriteRate and predicts a multi-hour
        run (warns), while only 20 rows are actually written and the job
        finishes in the usual ~2 min. A read-based version can't decouple these
        — throttling the rate to trip the estimate also throttles the real scan.

        write_table (not a transient one) is required: the estimate reads
        DescribeTable's ItemCount, which is 0 for ~6h after a fresh fill, so a
        just-created transient table would false-green. write_table has real,
        long-settled metadata. The 20-row append is a negligible, idempotent-ish
        addition to an already-populated smoke table (same table the load smoke
        writes to).
        """
        region = e2e_config.aws_region
        table = e2e_config.write_table
        run_id = f"{int(time.time())}-{uuid.uuid4().hex[:8]}"
        s3_path = _upload_fixture(run_id, region)

        # Vacuous-pass guard (invariant #1 / whole_system rule): confirm the
        # target's real item count forces the write estimate past the 60-min
        # default timeout at our slow rate BEFORE trusting a pass. The write
        # estimate is item_count * ceil(avg_item_size/1024) units; for the small
        # items here that is ~1 unit/item, so item_count is the floor on units.
        desc = boto3.client("dynamodb", region_name=region).describe_table(
            TableName=table
        )["Table"]
        item_count = int(desc.get("ItemCount", 0) or 0)
        min_estimated_minutes = (item_count / SLOW_WRITE_RATE) / 60
        assert min_estimated_minutes > 60, (
            f"write_table {table!r} is too small to force the #89 timeout warning: "
            f"~{item_count:,} items at {SLOW_WRITE_RATE} u/s is only "
            f"~{min_estimated_minutes:,.1f} min, under the 60-min default timeout. "
            f"This scenario needs a larger, settled write_table (DescribeTable "
            f"ItemCount must be non-zero and sizeable) or it proves nothing — "
            f"refusing to false-green."
        )

        result = _run_load(table, s3_path, write_rate=SLOW_WRITE_RATE)
        perf = assert_glue_succeeded("load", result, region)
        # log.warning → stderr on Glue LiveTail, so assert on combined output.
        assert WARN_JOB_TIMEOUT in result.output, (
            f"[slow-rate-timeout] expected #89 check-1 warning fragment "
            f"{WARN_JOB_TIMEOUT!r} in the live Glue job output, not found. "
            f"output tail:\n{result.output[-2500:]}"
        )

        ws_perf_collector.add(PerfRow(
            command="load #89 slow-rate timeout warning",
            wall_seconds=result.wall_seconds,
            dpu_seconds=perf.dpu_seconds if perf else None,
            items=NUM_ITEMS,
        ))
