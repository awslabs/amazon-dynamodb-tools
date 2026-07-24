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
    base = paths.get_lake_dir() / "account=111122223333"
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
    part = paths.get_lake_dir() / "account=a" / "region=us-east-1" / "table=t"
    assert list(part.glob("*.parquet")) == []
    assert list(part.glob("*.tmp")) == []
