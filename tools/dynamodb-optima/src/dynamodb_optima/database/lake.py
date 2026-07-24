"""
Metrics Parquet lake — region-partitioned, append-only, DuckDB-native.

Sole owner of metrics lake I/O. Region-correctness is structural: region is in the
partition path and the dedup key, so replicas cannot overwrite each other.

Layout:
    <lake_dir>/account=<id>/region=<r>/table=<t>/ingest_<run_id>.parquet   (landing)
Readers union landing + served (served added in a later change).
"""

import os
from datetime import datetime, timezone

from ..logging import get_logger
from ..paths import get_lake_dir
from .connection import get_database_manager

logger = get_logger("dynamodb_optima.database.lake")

_DEDUP_KEY = (
    "account_id, region, table_name, resource_name, metric_name, timestamp, "
    "statistic, period_seconds"
)


def _partition_dir(account_id: str, region: str, table_name: str):
    return (
        get_lake_dir()
        / f"account={account_id}"
        / f"region={region}"
        / f"table={table_name}"
    )


def write_metrics(rows: list[dict], run_id: str) -> None:
    """Write metric rows to the lake as region-partitioned Parquet (atomic per file)."""
    if not rows:
        return

    ingest_time = datetime.now(timezone.utc)

    groups: dict[tuple, list[dict]] = {}
    for r in rows:
        key = (r["account_id"], r["region"], r["table_name"])
        groups.setdefault(key, []).append(r)

    import pandas as pd

    db = get_database_manager()
    with db.get_connection_context() as conn:
        for (account_id, region, table_name), grp in groups.items():
            part = _partition_dir(account_id, region, table_name)
            os.makedirs(part, exist_ok=True)
            final = part / f"ingest_{run_id}.parquet"
            tmp = part / f"ingest_{run_id}.parquet.tmp"

            df = pd.DataFrame(grp)
            df["ingest_time"] = ingest_time
            if "dimensions" in df.columns:
                import json as _json
                df["dimensions"] = df["dimensions"].apply(
                    lambda d: _json.dumps(d) if isinstance(d, dict) else d
                )

            try:
                conn.register("_lake_write_df", df)
                conn.execute(
                    f"COPY (SELECT * FROM _lake_write_df) TO '{tmp}' "
                    "(FORMAT PARQUET, COMPRESSION ZSTD)"
                )
                os.replace(tmp, final)
            finally:
                conn.unregister("_lake_write_df")
                if os.path.exists(tmp):
                    os.remove(tmp)


def _glob(account_id: str, region: str, table_name: str) -> str:
    return str(_partition_dir(account_id, region, table_name) / "*.parquet")


def read_metrics(account_id: str, region: str, table_name: str, start, end):
    """Return a deduped pandas DataFrame of metrics for a resource + time range."""
    import glob as _glob_mod

    pattern = _glob(account_id, region, table_name)
    if not _glob_mod.glob(pattern):
        import pandas as pd
        return pd.DataFrame()

    db = get_database_manager()
    with db.get_connection_context() as conn:
        return conn.execute(
            f"""
            SELECT * FROM read_parquet('{pattern}', union_by_name=true)
            WHERE timestamp BETWEEN ? AND ?
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY {_DEDUP_KEY} ORDER BY ingest_time DESC
            ) = 1
            """,
            [start, end],
        ).df()


def latest_timestamps(account_id: str, region: str, table_name: str) -> dict:
    """Return {"metric:statistic:period": max_timestamp} for gap-detection.

    Empty dict if no data (caller collects the full window). Timezone-aware UTC.
    """
    import glob as _glob_mod

    pattern = _glob(account_id, region, table_name)
    if not _glob_mod.glob(pattern):
        return {}

    db = get_database_manager()
    with db.get_connection_context() as conn:
        # Use .df() rather than .fetchall(): DuckDB's native TIMESTAMPTZ ->
        # Python datetime conversion in fetchall() requires the optional
        # 'pytz' package, which is not a project dependency. Pandas performs
        # its own tz-aware conversion in .df() without that dependency.
        df = conn.execute(
            f"""
            SELECT metric_name, statistic, period_seconds,
                   MAX(timestamp) AS latest
            FROM read_parquet('{pattern}', union_by_name=true)
            GROUP BY metric_name, statistic, period_seconds
            """
        ).df()

    coverage = {}
    for row in df.itertuples(index=False):
        latest = row.latest
        if latest is not None and hasattr(latest, "to_pydatetime"):
            latest = latest.to_pydatetime()
        if latest is not None and getattr(latest, "tzinfo", None) is None:
            latest = latest.replace(tzinfo=timezone.utc)
        else:
            latest = latest.astimezone(timezone.utc) if latest is not None else None
        coverage[f"{row.metric_name}:{row.statistic}:{row.period_seconds}"] = latest
    return coverage
