"""Observe a table's *actual* consumed write capacity from CloudWatch.

This is what separates a true e2e assertion from a smoke: a smoke proves the
Glue job accepted ``dynamodb.throughput.write`` and didn't crash; this proves
the connector *honored* the requested rate by reading DynamoDB's own
``ConsumedWriteCapacityUnits`` metric back and bounding the sustained rate.

CloudWatch publishes ``ConsumedWriteCapacityUnits`` as a SUM per 60s period.
Dividing each minute's SUM by 60 gives the average WCU/s sustained in that
window. We look at the busy windows (the connector ramps and drains at the
edges) and assert the peak sustained minute stayed at or below the requested
ceiling, within tolerance.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

import boto3


@dataclass
class WriteCapacityObservation:
    per_minute_wcu: list[float]      # average WCU/s for each 60s window
    peak_minute_wcu: float           # highest sustained 60s average
    total_consumed_wcu: float        # SUM across the window (total WCUs billed)

    @property
    def observed_any(self) -> bool:
        return len(self.per_minute_wcu) > 0


def fetch_consumed_write_capacity(
    table: str,
    start: datetime,
    end: datetime,
    region: str,
) -> WriteCapacityObservation:
    """Read ConsumedWriteCapacityUnits for a table over [start, end].

    ``start``/``end`` must be timezone-aware UTC. The window is padded to whole
    minutes because CloudWatch buckets on minute boundaries; a sub-minute
    window can miss the datapoint entirely.
    """
    cw = boto3.client("cloudwatch", region_name=region)
    # Pad to whole-minute boundaries so the busy window's datapoints land.
    start = start.replace(second=0, microsecond=0) - timedelta(minutes=1)
    end = end.replace(second=0, microsecond=0) + timedelta(minutes=2)

    resp = cw.get_metric_statistics(
        Namespace="AWS/DynamoDB",
        MetricName="ConsumedWriteCapacityUnits",
        Dimensions=[{"Name": "TableName", "Value": table}],
        StartTime=start,
        EndTime=end,
        Period=60,
        Statistics=["Sum"],
    )
    points = sorted(resp["Datapoints"], key=lambda d: d["Timestamp"])
    per_minute = [p["Sum"] / 60.0 for p in points]
    total = sum(p["Sum"] for p in points)
    return WriteCapacityObservation(
        per_minute_wcu=per_minute,
        peak_minute_wcu=max(per_minute) if per_minute else 0.0,
        total_consumed_wcu=total,
    )


def utcnow() -> datetime:
    """Timezone-aware now(), so callers stamp the write window consistently."""
    return datetime.now(timezone.utc)
