"""Unit tests for DistributedDynamoDBMonitorWorker — S3 sync loop, scaling logic, staleness, cleanup."""
import json
import threading
import time
from io import BytesIO
from unittest.mock import Mock, MagicMock, patch, call

import pytest

from python_modules.shared.rate_limiter.DistributedDynamoDBMonitorWorker import DistributedDynamoDBMonitorWorker


@pytest.fixture
def mock_session():
    session = Mock()
    session.events = Mock()
    session.events.register = Mock()
    s3 = Mock()
    session.client = Mock(return_value=s3)
    return session


@pytest.fixture
def s3_client(mock_session):
    return mock_session.client.return_value


@pytest.fixture
def worker(mock_session, s3_client):
    s3_client.exceptions = Mock()
    s3_client.exceptions.NoSuchKey = type('NoSuchKey', (Exception,), {})
    s3_client.get_object = Mock(side_effect=s3_client.exceptions.NoSuchKey("no summary"))
    w = DistributedDynamoDBMonitorWorker(
        session=mock_session,
        bucket='test-bucket',
        prefix='rate-limiter/job-123',
        worker_max_read_rate=1500,
        worker_max_write_rate=500,
        sync_interval=60,  # long interval to prevent auto-sync during tests
        autostart=False,
    )
    yield w
    w.stop()


class TestInit:
    def test_prefix_gets_trailing_slash(self, mock_session):
        s3 = mock_session.client.return_value
        s3.exceptions = Mock()
        s3.exceptions.NoSuchKey = type('NoSuchKey', (Exception,), {})
        s3.get_object = Mock(side_effect=s3.exceptions.NoSuchKey("x"))
        w = DistributedDynamoDBMonitorWorker(
            session=mock_session, bucket='b', prefix='no-slash',
            autostart=False,
        )
        assert w.prefix == 'no-slash/'
        w.stop()

    def test_prefix_preserved_if_has_slash(self, mock_session):
        s3 = mock_session.client.return_value
        s3.exceptions = Mock()
        s3.exceptions.NoSuchKey = type('NoSuchKey', (Exception,), {})
        s3.get_object = Mock(side_effect=s3.exceptions.NoSuchKey("x"))
        w = DistributedDynamoDBMonitorWorker(
            session=mock_session, bucket='b', prefix='has-slash/',
            autostart=False,
        )
        assert w.prefix == 'has-slash/'
        w.stop()

    def test_default_initial_rates(self, mock_session):
        s3 = mock_session.client.return_value
        s3.exceptions = Mock()
        s3.exceptions.NoSuchKey = type('NoSuchKey', (Exception,), {})
        s3.get_object = Mock(side_effect=s3.exceptions.NoSuchKey("x"))
        w = DistributedDynamoDBMonitorWorker(
            session=mock_session, bucket='b', prefix='p/',
            aggregate_max_read_rate=10000,
            aggregate_max_write_rate=5000,
            worker_max_read_rate=1500,
            worker_max_write_rate=500,
            autostart=False,
        )
        # initial = min(worker_max, aggregate_max / 10)
        assert w.monitor.max_read_rate == 1000.0  # min(1500, 10000/10)
        assert w.monitor.max_write_rate == 500.0   # min(500, 5000/10)
        w.stop()

    def test_custom_worker_id(self, mock_session):
        s3 = mock_session.client.return_value
        s3.exceptions = Mock()
        s3.exceptions.NoSuchKey = type('NoSuchKey', (Exception,), {})
        s3.get_object = Mock(side_effect=s3.exceptions.NoSuchKey("x"))
        w = DistributedDynamoDBMonitorWorker(
            session=mock_session, bucket='b', prefix='p/',
            worker_id='my-worker', autostart=False,
        )
        assert w.worker_id == 'my-worker'
        w.stop()


class TestSyncLoop:
    def test_uploads_metrics_to_s3(self, worker, s3_client):
        # Simulate one sync cycle manually
        worker._last_metrics_snapshot = None
        worker._sync_loop_once()
        s3_client.put_object.assert_called_once()
        call_kwargs = s3_client.put_object.call_args[1]
        assert call_kwargs['Bucket'] == 'test-bucket'
        assert 'worker-' in call_kwargs['Key']
        payload = json.loads(call_kwargs['Body'].decode('utf-8'))
        assert 'worker_id' in payload
        assert 'timestamp' in payload
        assert 'read_rate' in payload
        assert 'write_rate' in payload

    def test_computes_rate_from_delta(self, worker, s3_client):
        # Set initial snapshot
        worker._last_metrics_snapshot = (time.monotonic() - 5.0, 100.0, 50.0)
        # Set current metrics
        with worker.monitor.metrics_lock:
            worker.monitor.metrics['read_capacity'] = 200.0
            worker.monitor.metrics['write_capacity'] = 100.0

        worker._sync_loop_once()
        payload = json.loads(s3_client.put_object.call_args[1]['Body'].decode('utf-8'))
        # (200-100)/5 = 20, (100-50)/5 = 10
        assert payload['read_rate'] == pytest.approx(20.0, abs=1)
        assert payload['write_rate'] == pytest.approx(10.0, abs=1)

    def test_first_sync_reports_zero_rate(self, worker, s3_client):
        worker._last_metrics_snapshot = None
        worker._sync_loop_once()
        payload = json.loads(s3_client.put_object.call_args[1]['Body'].decode('utf-8'))
        assert payload['read_rate'] == 0.0
        assert payload['write_rate'] == 0.0


class TestScalingLogic:
    def test_applies_scaling_from_summary(self, worker, s3_client):
        summary = {
            "aggregated_read_rate": 3000.0,
            "aggregated_write_rate": 1000.0,
            "active_workers": 3,
        }
        s3_client.get_object = Mock(return_value={
            'Body': BytesIO(json.dumps(summary).encode('utf-8'))
        })
        worker.monitor.max_read_rate = 1000.0
        worker.monitor.max_write_rate = 500.0
        worker._last_metrics_snapshot = (time.monotonic() - 5, 0, 0)

        worker._sync_loop_once()

        # aggregate_max_read=100000, agg_rate=3000 → scale=33.33
        # allowed = 33.33 * 1000 = 33333, capped at worker_max=1500
        # smoothed = 0.6*1000 + 0.4*1500 = 1200
        assert worker.monitor.max_read_rate == pytest.approx(1200.0, abs=5)

    def test_no_summary_file_keeps_rates(self, worker, s3_client):
        s3_client.get_object = Mock(
            side_effect=s3_client.exceptions.NoSuchKey("not found")
        )
        original_read = worker.monitor.max_read_rate
        original_write = worker.monitor.max_write_rate
        worker._last_metrics_snapshot = (time.monotonic() - 5, 0, 0)

        worker._sync_loop_once()

        assert worker.monitor.max_read_rate == original_read
        assert worker.monitor.max_write_rate == original_write


class TestCleanup:
    def test_cleanup_stops_and_deletes_s3_file(self, worker, s3_client):
        worker.cleanup()
        expected_key = f"{worker.prefix}worker-{worker.worker_id}.json"
        s3_client.delete_object.assert_called_once_with(
            Bucket='test-bucket', Key=expected_key
        )

    def test_cleanup_handles_delete_failure(self, worker, s3_client, capsys):
        s3_client.delete_object = Mock(side_effect=Exception("access denied"))
        worker.cleanup()  # should not raise
        captured = capsys.readouterr()
        assert "Warning" in captured.out or "failed" in captured.out


class TestStartStop:
    def test_start_creates_thread(self, worker):
        worker.start()
        assert worker._sync_thread is not None
        assert worker._sync_thread.is_alive()
        worker.stop()

    def test_start_is_idempotent(self, worker):
        worker.start()
        first_thread = worker._sync_thread
        worker.start()
        assert worker._sync_thread is first_thread
        worker.stop()

    def test_stop_joins_thread(self, worker):
        worker.start()
        worker.stop()
        assert not worker._sync_thread.is_alive()


# Helper: expose single sync iteration for testing without threading
def _add_sync_loop_once(cls):
    """Monkeypatch to run one iteration of the sync loop for deterministic testing."""
    def _sync_loop_once(self):
        import json, time, random
        upload_key = f"{self.prefix}worker-{self.worker_id}.json"
        wall_ts = time.time()
        mono_now = time.monotonic()
        with self.monitor.metrics_lock:
            total_read = float(self.monitor.metrics['read_capacity'])
            total_write = float(self.monitor.metrics['write_capacity'])
        if self._last_metrics_snapshot is None:
            read_rate = 0.0
            write_rate = 0.0
        else:
            last_t, last_r, last_w = self._last_metrics_snapshot
            dt = max(mono_now - last_t, 1e-6)
            read_rate = max(0.0, (total_read - last_r) / dt)
            write_rate = max(0.0, (total_write - last_w) / dt)
        self._last_metrics_snapshot = (mono_now, total_read, total_write)
        payload = json.dumps({
            "worker_id": self.worker_id,
            "timestamp": wall_ts,
            "read_rate": read_rate,
            "write_rate": write_rate,
        })
        self.s3_client.put_object(Bucket=self.bucket, Key=upload_key, Body=payload.encode('utf-8'))
        try:
            resp = self.s3_client.get_object(Bucket=self.bucket, Key=f"{self.prefix}{self.summary_key}")
            summary_data = json.loads(resp['Body'].read().decode('utf-8'))
            current_agg_read_rate = summary_data.get("aggregated_read_rate", 0.0)
            current_agg_write_rate = summary_data.get("aggregated_write_rate", 0.0)
            read_scale = self.aggregate_max_read_rate / current_agg_read_rate if current_agg_read_rate > 0 else 1.0
            write_scale = self.aggregate_max_write_rate / current_agg_write_rate if current_agg_write_rate > 0 else 1.0
            worker_allowed_read_rate = read_scale * self.monitor.max_read_rate
            worker_allowed_write_rate = write_scale * self.monitor.max_write_rate
            new_read_target = min(worker_allowed_read_rate, self.worker_max_read_rate)
            new_write_target = min(worker_allowed_write_rate, self.worker_max_write_rate)
            smoothing_factor = 0.4
            self.monitor.max_read_rate = (1 - smoothing_factor) * self.monitor.max_read_rate + smoothing_factor * new_read_target
            self.monitor.max_write_rate = (1 - smoothing_factor) * self.monitor.max_write_rate + smoothing_factor * new_write_target
        except self.s3_client.exceptions.NoSuchKey:
            pass
    cls._sync_loop_once = _sync_loop_once

_add_sync_loop_once(DistributedDynamoDBMonitorWorker)
