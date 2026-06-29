"""Unit tests for DistributedDynamoDBMonitorAggregator — S3 aggregation, staleness cutoff, cleanup."""
import json
import time
from io import BytesIO
from unittest.mock import Mock, MagicMock, patch, call
from concurrent.futures import ThreadPoolExecutor

import pytest

from python_modules.shared.rate_limiter.DistributedDynamoDBMonitorAggregator import DistributedDynamoDBMonitorAggregator


@pytest.fixture
def mock_session():
    session = Mock()
    s3 = Mock()
    session.client = Mock(return_value=s3)
    return session


@pytest.fixture
def s3_client(mock_session):
    return mock_session.client.return_value


@pytest.fixture
def aggregator(mock_session, s3_client):
    # Set up paginator to return empty by default
    paginator = Mock()
    paginator.paginate = Mock(return_value=[{"Contents": []}])
    s3_client.get_paginator = Mock(return_value=paginator)

    agg = DistributedDynamoDBMonitorAggregator(
        session=mock_session,
        bucket='test-bucket',
        prefix='rate-limiter/job-123',
        staleness_cutoff=15,
        interval=60,  # long so background thread doesn't fire
        autostart=False,
    )
    yield agg
    agg.stop()


class TestInit:
    def test_prefix_gets_trailing_slash(self, mock_session):
        s3 = mock_session.client.return_value
        paginator = Mock()
        paginator.paginate = Mock(return_value=[])
        s3.get_paginator = Mock(return_value=paginator)
        agg = DistributedDynamoDBMonitorAggregator(
            session=mock_session, bucket='b', prefix='no-slash',
            autostart=False,
        )
        assert agg.prefix == 'no-slash/'
        agg.stop()

    def test_preserves_trailing_slash(self, mock_session):
        s3 = mock_session.client.return_value
        paginator = Mock()
        paginator.paginate = Mock(return_value=[])
        s3.get_paginator = Mock(return_value=paginator)
        agg = DistributedDynamoDBMonitorAggregator(
            session=mock_session, bucket='b', prefix='has/',
            autostart=False,
        )
        assert agg.prefix == 'has/'
        agg.stop()


class TestAggregateOnce:
    def test_writes_summary_with_zero_workers(self, aggregator, s3_client):
        aggregator.aggregate_once()
        s3_client.put_object.assert_called_once()
        call_kwargs = s3_client.put_object.call_args[1]
        summary = json.loads(call_kwargs['Body'].decode('utf-8'))
        assert summary['active_workers'] == 0
        assert summary['aggregated_read_rate'] == 0.0
        assert summary['aggregated_write_rate'] == 0.0

    def test_aggregates_multiple_workers(self, aggregator, s3_client):
        now = time.time()
        worker_data = [
            {"worker_id": "w1", "timestamp": now, "read_rate": 100.0, "write_rate": 50.0},
            {"worker_id": "w2", "timestamp": now, "read_rate": 200.0, "write_rate": 75.0},
        ]

        paginator = Mock()
        paginator.paginate = Mock(return_value=[{
            "Contents": [
                {"Key": "rate-limiter/job-123/worker-w1.json"},
                {"Key": "rate-limiter/job-123/worker-w2.json"},
            ]
        }])
        s3_client.get_paginator = Mock(return_value=paginator)

        def mock_get_object(Bucket, Key):
            idx = 0 if 'w1' in Key else 1
            return {'Body': BytesIO(json.dumps(worker_data[idx]).encode('utf-8'))}

        s3_client.get_object = Mock(side_effect=mock_get_object)

        aggregator.aggregate_once()

        call_kwargs = s3_client.put_object.call_args[1]
        summary = json.loads(call_kwargs['Body'].decode('utf-8'))
        assert summary['active_workers'] == 2
        assert summary['aggregated_read_rate'] == pytest.approx(300.0)
        assert summary['aggregated_write_rate'] == pytest.approx(125.0)

    def test_skips_stale_workers(self, aggregator, s3_client):
        stale_ts = time.time() - 30  # 30s old, cutoff is 15
        fresh_ts = time.time()

        paginator = Mock()
        paginator.paginate = Mock(return_value=[{
            "Contents": [
                {"Key": "rate-limiter/job-123/worker-stale.json"},
                {"Key": "rate-limiter/job-123/worker-fresh.json"},
            ]
        }])
        s3_client.get_paginator = Mock(return_value=paginator)

        def mock_get_object(Bucket, Key):
            if 'stale' in Key:
                data = {"worker_id": "stale", "timestamp": stale_ts, "read_rate": 999.0, "write_rate": 999.0}
            else:
                data = {"worker_id": "fresh", "timestamp": fresh_ts, "read_rate": 50.0, "write_rate": 25.0}
            return {'Body': BytesIO(json.dumps(data).encode('utf-8'))}

        s3_client.get_object = Mock(side_effect=mock_get_object)

        aggregator.aggregate_once()

        call_kwargs = s3_client.put_object.call_args[1]
        summary = json.loads(call_kwargs['Body'].decode('utf-8'))
        assert summary['active_workers'] == 1
        assert summary['aggregated_read_rate'] == pytest.approx(50.0)

    def test_skips_summary_file_in_listing(self, aggregator, s3_client):
        paginator = Mock()
        paginator.paginate = Mock(return_value=[{
            "Contents": [
                {"Key": "rate-limiter/job-123/summary.json"},
                {"Key": "rate-limiter/job-123/worker-w1.json"},
            ]
        }])
        s3_client.get_paginator = Mock(return_value=paginator)

        fresh_ts = time.time()
        s3_client.get_object = Mock(return_value={
            'Body': BytesIO(json.dumps({
                "worker_id": "w1", "timestamp": fresh_ts,
                "read_rate": 10.0, "write_rate": 5.0
            }).encode('utf-8'))
        })

        aggregator.aggregate_once()

        # get_object should only be called for worker file, not summary
        assert s3_client.get_object.call_count == 1

    def test_handles_invalid_timestamp(self, aggregator, s3_client):
        paginator = Mock()
        paginator.paginate = Mock(return_value=[{
            "Contents": [{"Key": "rate-limiter/job-123/worker-bad.json"}]
        }])
        s3_client.get_paginator = Mock(return_value=paginator)
        s3_client.get_object = Mock(return_value={
            'Body': BytesIO(json.dumps({
                "worker_id": "bad", "timestamp": "not-a-number",
                "read_rate": 100.0, "write_rate": 50.0
            }).encode('utf-8'))
        })

        aggregator.aggregate_once()

        call_kwargs = s3_client.put_object.call_args[1]
        summary = json.loads(call_kwargs['Body'].decode('utf-8'))
        assert summary['active_workers'] == 0

    def test_handles_json_decode_error(self, aggregator, s3_client):
        paginator = Mock()
        paginator.paginate = Mock(return_value=[{
            "Contents": [{"Key": "rate-limiter/job-123/worker-corrupt.json"}]
        }])
        s3_client.get_paginator = Mock(return_value=paginator)
        s3_client.get_object = Mock(return_value={
            'Body': BytesIO(b'not json at all')
        })

        aggregator.aggregate_once()

        call_kwargs = s3_client.put_object.call_args[1]
        summary = json.loads(call_kwargs['Body'].decode('utf-8'))
        assert summary['active_workers'] == 0


class TestCleanup:
    def test_cleanup_stops_and_deletes_summary(self, aggregator, s3_client):
        aggregator.cleanup()
        expected_key = f"{aggregator.prefix}{aggregator.output_key}"
        s3_client.delete_object.assert_called_once_with(
            Bucket='test-bucket', Key=expected_key
        )

    def test_cleanup_handles_delete_failure(self, aggregator, s3_client):
        s3_client.delete_object = Mock(side_effect=Exception("denied"))
        aggregator.cleanup()  # should not raise


class TestStartStop:
    def test_start_creates_thread(self, aggregator):
        aggregator.start()
        assert aggregator._thread is not None
        assert aggregator._thread.is_alive()
        aggregator.stop()

    def test_start_is_idempotent(self, aggregator):
        aggregator.start()
        first_thread = aggregator._thread
        aggregator.start()
        assert aggregator._thread is first_thread
        aggregator.stop()

    def test_stop_joins_thread(self, aggregator):
        aggregator.start()
        aggregator.stop()
        assert not aggregator._thread.is_alive()

    def test_stop_without_start_is_noop(self, aggregator):
        aggregator.stop()  # should not raise
