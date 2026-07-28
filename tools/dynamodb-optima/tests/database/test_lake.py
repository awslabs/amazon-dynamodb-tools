def test_get_lake_dir_under_data(tmp_path, monkeypatch):
    from dynamodb_optima import paths
    monkeypatch.setattr(paths, "_project_root_override", tmp_path)
    lake = paths.get_lake_dir()
    assert lake == tmp_path / "data" / "metrics_lake"


from datetime import datetime, timezone


def _row(account, region, table, metric, ts, value, stat="Sum", period=60):
    return {
        "account_id": account, "region": region, "table_name": table,
        "resource_name": table, "resource_type": "TABLE",
        "metric_name": metric, "operation": None, "operation_type": None,
        "statistic": stat, "period_seconds": period,
        "timestamp": ts, "value": value, "unit": "Count",
        "dimensions": {"TableName": table},
    }


def _lake(tmp_path, monkeypatch):
    from dynamodb_optima import paths
    monkeypatch.setattr(paths, "_project_root_override", tmp_path)
    import importlib
    from dynamodb_optima.database import lake as lake_mod
    importlib.reload(lake_mod)
    return lake_mod


def test_write_metrics_partitions_by_region_no_overwrite(tmp_path, monkeypatch):
    lake = _lake(tmp_path, monkeypatch)
    ts = datetime(2026, 6, 1, tzinfo=timezone.utc)
    rows_e = [_row("111122223333", "us-east-1", "t", "ConsumedWriteCapacityUnits", ts, 100.0)]
    rows_w = [_row("111122223333", "us-west-2", "t", "ConsumedWriteCapacityUnits", ts, 200.0)]
    lake.write_metrics(rows_e, run_id="run1")
    lake.write_metrics(rows_w, run_id="run1")

    from dynamodb_optima import paths
    base = paths.get_lake_dir() / "landing" / "account=111122223333"
    east = list((base / "region=us-east-1" / "table=t").glob("*.parquet"))
    west = list((base / "region=us-west-2" / "table=t").glob("*.parquet"))
    assert len(east) == 1 and len(west) == 1

    e = lake.read_metrics("111122223333", "us-east-1", "t", ts, ts)
    w = lake.read_metrics("111122223333", "us-west-2", "t", ts, ts)
    assert e["value"].tolist() == [100.0]
    assert w["value"].tolist() == [200.0]


def test_read_metrics_dedups_latest_ingest(tmp_path, monkeypatch):
    lake = _lake(tmp_path, monkeypatch)
    ts = datetime(2026, 6, 1, tzinfo=timezone.utc)
    lake.write_metrics([_row("a", "us-east-1", "t", "m", ts, 100.0)], run_id="run1")
    lake.write_metrics([_row("a", "us-east-1", "t", "m", ts, 175.0)], run_id="run2")
    df = lake.read_metrics("a", "us-east-1", "t", ts, ts)
    assert df["value"].tolist() == [175.0]


def test_read_metrics_dedup_key_distinguishes_resource_name(tmp_path, monkeypatch):
    """A TABLE row and its GSI's row can share account/region/table/metric/timestamp/
    statistic/period (only resource_name differs). Dedup must not collapse them."""
    lake = _lake(tmp_path, monkeypatch)
    ts = datetime(2026, 6, 1, tzinfo=timezone.utc)
    table_row = _row("a", "us-east-1", "t", "ConsumedReadCapacityUnits", ts, 60.0)
    gsi_row = dict(table_row, resource_name="t#gsi1", resource_type="GSI", value=999.0)
    lake.write_metrics([table_row], run_id="run_table")
    lake.write_metrics([gsi_row], run_id="run_gsi")

    df = lake.read_metrics("a", "us-east-1", "t", ts, ts)
    assert sorted(df["value"].tolist()) == [60.0, 999.0]


def test_read_metrics_empty_partition_returns_empty(tmp_path, monkeypatch):
    lake = _lake(tmp_path, monkeypatch)
    ts = datetime(2026, 6, 1, tzinfo=timezone.utc)
    df = lake.read_metrics("nope", "us-east-1", "t", ts, ts)
    assert df.empty


def test_write_failure_leaves_no_partial_parquet(tmp_path, monkeypatch):
    """A failed COPY must never leave a readable .parquet (only .tmp, cleaned up)."""
    lake = _lake(tmp_path, monkeypatch)
    ts = datetime(2026, 6, 1, tzinfo=timezone.utc)
    rows = [_row("a", "us-east-1", "t", "m", ts, 1.0)]

    class _BoomConn:
        def register(self, *a, **k): pass
        def unregister(self, *a, **k): pass
        def execute(self, sql, *a, **k):
            raise RuntimeError("boom")
    class _Ctx:
        def __enter__(self): return _BoomConn()
        def __exit__(self, *a): return False
    class _DB:
        def get_connection_context(self): return _Ctx()

    monkeypatch.setattr(lake, "get_database_manager", lambda: _DB())
    import pytest
    with pytest.raises(RuntimeError):
        lake.write_metrics(rows, run_id="rboom")

    from dynamodb_optima import paths
    part = paths.get_lake_dir() / "landing" / "account=a" / "region=us-east-1" / "table=t"
    assert list(part.glob("*.parquet")) == []
    assert list(part.glob("*.tmp")) == []


def test_latest_timestamps_shape(tmp_path, monkeypatch):
    lake = _lake(tmp_path, monkeypatch)
    t1 = datetime(2026, 6, 1, 0, 0, tzinfo=timezone.utc)
    t2 = datetime(2026, 6, 1, 0, 1, tzinfo=timezone.utc)
    lake.write_metrics([
        _row("a", "us-east-1", "t", "ConsumedWriteCapacityUnits", t1, 1.0, stat="Sum", period=60),
        _row("a", "us-east-1", "t", "ConsumedWriteCapacityUnits", t2, 2.0, stat="Sum", period=60),
    ], run_id="r1")
    cov = lake.latest_timestamps("a", "us-east-1", "t")
    assert cov["ConsumedWriteCapacityUnits:Sum:60"] == t2


def test_latest_timestamps_empty(tmp_path, monkeypatch):
    lake = _lake(tmp_path, monkeypatch)
    assert lake.latest_timestamps("nope", "us-east-1", "t") == {}


def test_read_metrics_returns_utc_regardless_of_session_tz(tmp_path, monkeypatch):
    """Timestamps must come back in UTC even if the DuckDB session tz is local."""
    lake = _lake(tmp_path, monkeypatch)
    ts = datetime(2026, 6, 1, 9, 17, tzinfo=timezone.utc)
    lake.write_metrics([_row("a", "us-east-1", "t", "m", ts, 1.0)], run_id="r1")

    # Poison the pooled connection's session tz to a non-UTC zone before reading.
    from dynamodb_optima.database.connection import get_database_manager
    with get_database_manager().get_connection_context() as conn:
        conn.execute("SET TimeZone='America/New_York'")

    df = lake.read_metrics("a", "us-east-1", "t", ts, ts)
    got = df["timestamp"].iloc[0]
    # Underlying instant correct AND rendered in UTC (offset 0 / 'UTC').
    assert got.tzinfo is not None
    assert got.utcoffset().total_seconds() == 0
    assert got.tz_convert("UTC").strftime("%Y-%m-%d %H:%M") == "2026-06-01 09:17"


def test_write_metrics_concurrent_no_data_loss(tmp_path, monkeypatch):
    """Concurrent writers must not clobber each other via a shared registered view name."""
    lake = _lake(tmp_path, monkeypatch)
    from concurrent.futures import ThreadPoolExecutor, as_completed
    from datetime import timedelta
    base = datetime(2026, 6, 1, tzinfo=timezone.utc)

    # Warm the DB once so first-access init isn't the thing under test.
    from dynamodb_optima.database.connection import get_database_manager
    with get_database_manager().get_connection_context() as c:
        c.execute("SELECT 1")

    def work(i):
        region = ["us-east-1", "us-west-2", "eu-central-1", "eu-west-1"][i % 4]
        # DISTINCT timestamp per row, else the dedup key collapses them and this
        # would test dedup, not concurrency.
        rows = [_row("acct", region, f"table{i}", "m", base + timedelta(minutes=j), float(j))
                for j in range(50)]
        lake.write_metrics(rows, run_id=f"run{i}")

    errs = []
    with ThreadPoolExecutor(max_workers=4) as ex:
        futs = {ex.submit(work, i): i for i in range(16)}
        for f in as_completed(futs):
            try:
                f.result()
            except Exception as e:  # pragma: no cover
                errs.append(repr(e))
    assert errs == []

    s = datetime(2026, 1, 1, tzinfo=timezone.utc)
    e = datetime(2027, 1, 1, tzinfo=timezone.utc)
    for i in range(16):
        region = ["us-east-1", "us-west-2", "eu-central-1", "eu-west-1"][i % 4]
        df = lake.read_metrics("acct", region, f"table{i}", s, e)
        assert len(df) == 50, f"table{i} lost data: got {len(df)} rows (view-name collision?)"


def test_write_goes_to_landing_zone(tmp_path, monkeypatch):
    lake = _lake(tmp_path, monkeypatch)
    ts = datetime(2026, 6, 1, tzinfo=timezone.utc)
    lake.write_metrics([_row("a", "us-east-1", "t", "m", ts, 1.0)], run_id="r1")
    from dynamodb_optima import paths
    landing = paths.get_lake_dir() / "landing" / "account=a" / "region=us-east-1" / "table=t"
    assert list(landing.glob("*.parquet"))  # write lands in landing/


def test_migrate_flat_to_landing_moves_files(tmp_path, monkeypatch):
    lake = _lake(tmp_path, monkeypatch)
    from dynamodb_optima import paths
    flat = paths.get_lake_dir() / "account=a" / "region=us-east-1" / "table=t"
    flat.mkdir(parents=True)
    (flat / "ingest_old.parquet").write_bytes(b"stub")
    lake._migrate_flat_to_landing()
    moved = paths.get_lake_dir() / "landing" / "account=a" / "region=us-east-1" / "table=t" / "ingest_old.parquet"
    assert moved.exists()
    assert not (flat / "ingest_old.parquet").exists()
    lake._migrate_flat_to_landing()  # idempotent, no error


def _served_files(account, region, table):
    from dynamodb_optima import paths
    d = paths.get_lake_dir() / "served" / f"account={account}" / f"region={region}" / f"table={table}"
    import glob as g
    return sorted(g.glob(str(d / "*.parquet")))


def test_compact_basic_merges_landing_to_served(tmp_path, monkeypatch):
    lake = _lake(tmp_path, monkeypatch)
    from datetime import timedelta
    base = datetime(2026, 6, 1, tzinfo=timezone.utc)
    for i in range(3):
        lake.write_metrics([_row("a", "us-east-1", "t", "m", base + timedelta(minutes=i), float(i))], run_id=f"r{i}")
    lake.compact_partition("a", "us-east-1", "t")
    assert len(_served_files("a", "us-east-1", "t")) == 1
    from dynamodb_optima import paths
    landing = paths.get_lake_dir() / "landing" / "account=a" / "region=us-east-1" / "table=t"
    import glob as g
    assert g.glob(str(landing / "*.parquet")) == []
    df = lake.read_metrics("a", "us-east-1", "t", base, base + timedelta(hours=1))
    assert len(df) == 3


def test_compact_multi_month_creates_two_shards(tmp_path, monkeypatch):
    lake = _lake(tmp_path, monkeypatch)
    may = datetime(2026, 5, 15, tzinfo=timezone.utc)
    jun = datetime(2026, 6, 15, tzinfo=timezone.utc)
    lake.write_metrics([_row("a", "us-east-1", "t", "m", may, 1.0),
                        _row("a", "us-east-1", "t", "m", jun, 2.0)], run_id="r1")
    lake.compact_partition("a", "us-east-1", "t")
    files = _served_files("a", "us-east-1", "t")
    assert len(files) == 2
    assert any("2026-05" in f for f in files) and any("2026-06" in f for f in files)


def test_compact_dedup_keeps_latest_ingest_vs_existing_served(tmp_path, monkeypatch):
    lake = _lake(tmp_path, monkeypatch)
    ts = datetime(2026, 6, 1, tzinfo=timezone.utc)
    lake.write_metrics([_row("a", "us-east-1", "t", "m", ts, 100.0)], run_id="r1")
    lake.compact_partition("a", "us-east-1", "t")
    lake.write_metrics([_row("a", "us-east-1", "t", "m", ts, 175.0)], run_id="r2")
    lake.compact_partition("a", "us-east-1", "t")
    df = lake.read_metrics("a", "us-east-1", "t", ts, ts)
    assert df["value"].tolist() == [175.0]
    assert len(_served_files("a", "us-east-1", "t")) == 1


def test_compact_ignores_tmp_files(tmp_path, monkeypatch):
    lake = _lake(tmp_path, monkeypatch)
    ts = datetime(2026, 6, 1, tzinfo=timezone.utc)
    lake.write_metrics([_row("a", "us-east-1", "t", "m", ts, 1.0)], run_id="r1")
    from dynamodb_optima import paths
    landing = paths.get_lake_dir() / "landing" / "account=a" / "region=us-east-1" / "table=t"
    (landing / "ingest_inflight.parquet.tmp").write_bytes(b"partial")
    lake.compact_partition("a", "us-east-1", "t")
    assert (landing / "ingest_inflight.parquet.tmp").exists()


def test_compact_idempotent(tmp_path, monkeypatch):
    lake = _lake(tmp_path, monkeypatch)
    ts = datetime(2026, 6, 1, tzinfo=timezone.utc)
    lake.write_metrics([_row("a", "us-east-1", "t", "m", ts, 1.0)], run_id="r1")
    lake.compact_partition("a", "us-east-1", "t")
    lake.compact_partition("a", "us-east-1", "t")
    assert len(_served_files("a", "us-east-1", "t")) == 1


def test_compact_pending_scans_all_partitions(tmp_path, monkeypatch):
    lake = _lake(tmp_path, monkeypatch)
    ts = datetime(2026, 6, 1, tzinfo=timezone.utc)
    lake.write_metrics([_row("a", "us-east-1", "t1", "m", ts, 1.0)], run_id="r1")
    lake.write_metrics([_row("a", "us-west-2", "t2", "m", ts, 2.0)], run_id="r2")
    lake.compact_pending()  # no args -> scan all landing partitions
    assert len(_served_files("a", "us-east-1", "t1")) == 1
    assert len(_served_files("a", "us-west-2", "t2")) == 1


def test_compact_partition_skips_when_locked(tmp_path, monkeypatch):
    lake = _lake(tmp_path, monkeypatch)
    ts = datetime(2026, 6, 1, tzinfo=timezone.utc)
    lake.write_metrics([_row("a", "us-east-1", "t", "m", ts, 1.0)], run_id="r1")
    from dynamodb_optima import paths
    served = paths.get_lake_dir() / "served" / "account=a" / "region=us-east-1" / "table=t"
    served.mkdir(parents=True, exist_ok=True)
    (served / ".compact.lock").write_bytes(b"")   # pre-hold the lock
    lake.compact_partition("a", "us-east-1", "t")  # must skip
    landing = paths.get_lake_dir() / "landing" / "account=a" / "region=us-east-1" / "table=t"
    import glob as g
    assert g.glob(str(landing / "*.parquet"))   # landing untouched


def test_compact_deletes_only_snapshotted_landing(tmp_path, monkeypatch):
    """A landing file appearing AFTER the snapshot must survive (deleted set = snapshot only).
    Hook os.remove (called only during the delete phase, after the snapshot) to drop a new
    landing file once; that new file was never snapshotted so it must NOT be deleted."""
    lake = _lake(tmp_path, monkeypatch)
    from dynamodb_optima import paths
    ts = datetime(2026, 6, 1, tzinfo=timezone.utc)
    lake.write_metrics([_row("a", "us-east-1", "t", "m", ts, 1.0)], run_id="r1")
    landing = paths.get_lake_dir() / "landing" / "account=a" / "region=us-east-1" / "table=t"

    created = {"done": False}
    real_remove = lake.os.remove
    def hooked_remove(path):
        if not created["done"]:
            (landing / "ingest_late.parquet").write_bytes(b"late")
            created["done"] = True
        real_remove(path)
    monkeypatch.setattr(lake.os, "remove", hooked_remove)
    lake.compact_partition("a", "us-east-1", "t")
    monkeypatch.undo()
    assert (landing / "ingest_late.parquet").exists()


def test_compact_pending_continues_past_failing_partition(tmp_path, monkeypatch):
    lake = _lake(tmp_path, monkeypatch)
    ts = datetime(2026, 6, 1, tzinfo=timezone.utc)
    lake.write_metrics([_row("a", "us-east-1", "good", "m", ts, 1.0)], run_id="r1")
    lake.write_metrics([_row("a", "us-east-1", "locked", "m", ts, 1.0)], run_id="r2")
    from dynamodb_optima import paths
    locked_served = paths.get_lake_dir() / "served" / "account=a" / "region=us-east-1" / "table=locked"
    locked_served.mkdir(parents=True, exist_ok=True)
    (locked_served / ".compact.lock").write_bytes(b"")   # locked -> skipped
    lake.compact_pending()
    assert len(_served_files("a", "us-east-1", "good")) == 1  # good one still compacted


def test_get_database_manager_concurrent_init_no_race(tmp_path, monkeypatch):
    """Concurrent first-time get_database_manager() must not race on schema init
    (DuckDB 'Catalog write-write conflict'). Regression for the --workers>1 collect failure."""
    import threading
    from dynamodb_optima import paths
    monkeypatch.setattr(paths, "_project_root_override", tmp_path)
    import dynamodb_optima.database.connection as conn
    # force cold singleton
    monkeypatch.setattr(conn, "_db_manager", None)

    errs = []
    def go():
        try:
            conn.get_database_manager()
        except Exception as e:  # pragma: no cover
            errs.append(repr(e))
    threads = [threading.Thread(target=go) for _ in range(8)]
    for t in threads: t.start()
    for t in threads: t.join()
    assert errs == [], f"concurrent init raced: {errs[:2]}"
    assert conn._db_manager is not None
