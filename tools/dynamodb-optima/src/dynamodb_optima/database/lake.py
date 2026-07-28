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
from pathlib import Path

from ..logging import get_logger
from ..paths import get_lake_dir
from .connection import get_database_manager

logger = get_logger("dynamodb_optima.database.lake")

_DEDUP_KEY = (
    "account_id, region, table_name, resource_name, metric_name, timestamp, "
    "statistic, period_seconds"
)


def _landing_dir(account_id: str, region: str, table_name: str):
    return (
        get_lake_dir()
        / "landing"
        / f"account={account_id}"
        / f"region={region}"
        / f"table={table_name}"
    )


def _served_dir(account_id: str, region: str, table_name: str):
    return (
        get_lake_dir()
        / "served"
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
    import uuid

    db = get_database_manager()
    with db.get_connection_context() as conn:
        for (account_id, region, table_name), grp in groups.items():
            part = _landing_dir(account_id, region, table_name)
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

            # Unique per-write view name: pooled connections may be shared across
            # concurrent writers, and a fixed name would let one writer's DataFrame
            # clobber another's mid-COPY (data loss). uuid makes each registration
            # collision-proof.
            view = f"_lake_write_{uuid.uuid4().hex}"
            try:
                conn.register(view, df)
                conn.execute(
                    f"COPY (SELECT * FROM {view}) TO '{tmp}' "
                    "(FORMAT PARQUET, COMPRESSION ZSTD)"
                )
                os.replace(tmp, final)
            finally:
                conn.unregister(view)
                if os.path.exists(tmp):
                    os.remove(tmp)


def _read_globs(account_id: str, region: str, table_name: str) -> list[str]:
    return [
        str(_landing_dir(account_id, region, table_name) / "*.parquet"),
        str(_served_dir(account_id, region, table_name) / "*.parquet"),
    ]


def _migrate_flat_to_landing() -> None:
    """One-time move of pre-CHANGE-2 flat files
    (<lake>/account=.../region=.../table=.../ingest_*.parquet) into the landing/ zone.
    Idempotent: no-op once nothing flat remains. landing/ and served/ dirs are NOT
    named account=... so they are naturally excluded from the flat glob."""
    import glob as _glob_mod
    import shutil

    lake = get_lake_dir()
    flat_pattern = str(lake / "account=*" / "region=*" / "table=*" / "*.parquet")
    for src in _glob_mod.glob(flat_pattern):
        src_path = Path(src)
        parts = {p.split("=", 1)[0]: p.split("=", 1)[1] for p in src_path.parts if "=" in p}
        dest_dir = _landing_dir(parts["account"], parts["region"], parts["table"])
        os.makedirs(dest_dir, exist_ok=True)
        shutil.move(src, str(dest_dir / src_path.name))


def read_metrics(account_id: str, region: str, table_name: str, start, end):
    """Return a deduped pandas DataFrame of metrics for a resource + time range."""
    import glob as _glob_mod

    globs = _read_globs(account_id, region, table_name)
    existing = [g for g in globs if _glob_mod.glob(g)]
    if not existing:
        import pandas as pd
        return pd.DataFrame()

    files_sql = ", ".join(f"'{g}'" for g in existing)

    db = get_database_manager()
    with db.get_connection_context() as conn:
        # TIMESTAMPTZ renders in the session tz; force UTC so reads are deterministic
        # across machines/timezones (pooled connections may carry a non-UTC tz).
        conn.execute("SET TimeZone='UTC'")
        return conn.execute(
            f"""
            SELECT * FROM read_parquet([{files_sql}], union_by_name=true)
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

    globs = _read_globs(account_id, region, table_name)
    existing = [g for g in globs if _glob_mod.glob(g)]
    if not existing:
        return {}

    files_sql = ", ".join(f"'{g}'" for g in existing)

    db = get_database_manager()
    with db.get_connection_context() as conn:
        # Force UTC so timestamps are deterministic across machines/session tz.
        conn.execute("SET TimeZone='UTC'")
        # Use .df() rather than .fetchall(): DuckDB's native TIMESTAMPTZ ->
        # Python datetime conversion in fetchall() requires the optional
        # 'pytz' package, which is not a project dependency. Pandas performs
        # its own tz-aware conversion in .df() without that dependency.
        df = conn.execute(
            f"""
            SELECT metric_name, statistic, period_seconds,
                   MAX(timestamp) AS latest
            FROM read_parquet([{files_sql}], union_by_name=true)
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
