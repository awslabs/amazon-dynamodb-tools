"""
Regression tests for the consumed-capacity readers.

The lake holds the same traffic at 60s and 300s periods, and a per-operation row
next to the table-level row. The analyzers must read one table-level series.
"""

from datetime import datetime, timedelta, timezone

from dynamodb_optima.analysis.capacity_mode import CapacityModeAnalyzer
from dynamodb_optima.analysis.utilization import UtilizationAnalyzer

ACCT, REGION, TABLE = "111122223333", "us-east-1", "t"
READ = "ConsumedReadCapacityUnits"
WRITE = "ConsumedWriteCapacityUnits"


def _row(metric, ts, value, period=60, operation=None):
    return {
        "account_id": ACCT, "region": REGION, "table_name": TABLE,
        "resource_name": TABLE, "resource_type": "TABLE",
        "metric_name": metric, "operation": operation, "operation_type": None,
        "statistic": "Sum", "period_seconds": period,
        "timestamp": ts, "value": value, "unit": "Count",
        "dimensions": {"TableName": TABLE},
    }


def _lake(tmp_path, monkeypatch):
    from dynamodb_optima import paths
    monkeypatch.setattr(paths, "_project_root_override", tmp_path)
    import importlib
    from dynamodb_optima.database import lake as lake_mod
    importlib.reload(lake_mod)
    return lake_mod


def _base():
    now = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    return now - timedelta(hours=1)


def _capacity_reader():
    return CapacityModeAnalyzer.__new__(CapacityModeAnalyzer)._get_metrics


def _utilization_reader():
    a = UtilizationAnalyzer.__new__(UtilizationAnalyzer)
    return lambda *args: a._get_consumed_metrics(*args[:3], TABLE, "TABLE", args[3])


def _units(points):
    return [getattr(p, "units_per_second", p) for p in points]


def _both_periods(lake, metric, run_id):
    """Five 60s rows (60 units each = 1/s) and one 300s row (300 units = 1/s)."""
    t0 = _base()
    rows = [_row(metric, t0 + timedelta(minutes=i), 60.0) for i in range(5)]
    rows.append(_row(metric, t0, 300.0, period=300))
    lake.write_metrics(rows, run_id=run_id)


def test_capacity_mode_reads_one_period(tmp_path, monkeypatch):
    lake = _lake(tmp_path, monkeypatch)
    _both_periods(lake, READ, "r1")
    _both_periods(lake, WRITE, "r2")
    read, write = _capacity_reader()(ACCT, REGION, TABLE, 1)
    assert _units(read) == [1.0] * 5
    assert _units(write) == [1.0] * 5


def test_capacity_mode_skips_operation_rows(tmp_path, monkeypatch):
    lake = _lake(tmp_path, monkeypatch)
    t0 = _base()
    rows = [_row(READ, t0, 60.0), _row(READ, t0, 60.0, operation="Query")]
    lake.write_metrics(rows, run_id="r1")
    read, _ = _capacity_reader()(ACCT, REGION, TABLE, 1)
    assert len(read) == 1


def test_capacity_mode_falls_back_to_300s_only(tmp_path, monkeypatch):
    lake = _lake(tmp_path, monkeypatch)
    t0 = _base()
    rows = [_row(READ, t0 + timedelta(minutes=5 * i), 300.0, period=300) for i in range(3)]
    lake.write_metrics(rows, run_id="r1")
    read, _ = _capacity_reader()(ACCT, REGION, TABLE, 1)
    assert _units(read) == [1.0] * 3


def test_utilization_reads_one_period(tmp_path, monkeypatch):
    lake = _lake(tmp_path, monkeypatch)
    _both_periods(lake, READ, "r1")
    _both_periods(lake, WRITE, "r2")
    read, write = _utilization_reader()(ACCT, REGION, TABLE, 1)
    assert read == [1.0] * 5
    assert write == [1.0] * 5


def test_utilization_skips_operation_rows(tmp_path, monkeypatch):
    lake = _lake(tmp_path, monkeypatch)
    t0 = _base()
    rows = [_row(READ, t0, 60.0), _row(READ, t0, 60.0, operation="Query")]
    lake.write_metrics(rows, run_id="r1")
    read, _ = _utilization_reader()(ACCT, REGION, TABLE, 1)
    assert len(read) == 1
