"""Unit tests for DynamoDBMonitor — event hooks, capacity tracking, bucket deduction."""
import threading
import time
from unittest.mock import Mock, MagicMock, patch, call

import pytest

from python_modules.shared.rate_limiter.DynamoDBMonitor import DynamoDBMonitor


@pytest.fixture
def mock_session():
    session = Mock()
    session.events = Mock()
    session.events.register = Mock()
    return session


@pytest.fixture
def monitor(mock_session):
    m = DynamoDBMonitor(mock_session, max_read_rate=1000, max_write_rate=500, enable_reporting=False)
    yield m
    m.stop()


class TestInit:
    def test_registers_three_event_hooks(self, mock_session):
        DynamoDBMonitor(mock_session, max_read_rate=100, max_write_rate=50, enable_reporting=False)
        calls = mock_session.events.register.call_args_list
        assert len(calls) == 3
        patterns = [c[0][0] for c in calls]
        assert 'provide-client-params.dynamodb.*' in patterns
        assert 'before-call.dynamodb.*' in patterns
        assert 'after-call.dynamodb.*' in patterns

    def test_invalid_read_rate_raises(self, mock_session):
        with pytest.raises(ValueError, match="Invalid rate limits"):
            DynamoDBMonitor(mock_session, max_read_rate=0, max_write_rate=50, enable_reporting=False)

    def test_invalid_write_rate_raises(self, mock_session):
        with pytest.raises(ValueError, match="Invalid rate limits"):
            DynamoDBMonitor(mock_session, max_read_rate=100, max_write_rate=0, enable_reporting=False)

    def test_bucket_rates_match_config(self, mock_session):
        m = DynamoDBMonitor(mock_session, max_read_rate=200, max_write_rate=100, enable_reporting=False)
        assert m._read_bucket.rate == 200.0
        assert m._write_bucket.rate == 100.0
        m.stop()

    def test_bucket_capacity_is_rate_times_multiplier(self, mock_session):
        m = DynamoDBMonitor(mock_session, max_read_rate=200, max_write_rate=100, enable_reporting=False)
        assert m._read_bucket.capacity == 400.0  # 200 * 2
        assert m._write_bucket.capacity == 200.0  # 100 * 2
        m.stop()


class TestAddReturnConsumedCapacity:
    def test_adds_param_when_missing(self, monitor):
        params = {}
        monitor._add_return_consumed_capacity(params)
        assert params['ReturnConsumedCapacity'] == 'TOTAL'

    def test_preserves_existing_param(self, monitor):
        params = {'ReturnConsumedCapacity': 'INDEXES'}
        monitor._add_return_consumed_capacity(params)
        assert params['ReturnConsumedCapacity'] == 'INDEXES'


class TestEnforceRateLimit:
    def test_read_operations_use_read_bucket(self, monitor):
        for op in ('GetItem', 'BatchGetItem', 'Query', 'Scan', 'TransactGetItems'):
            model = Mock()
            model.name = op
            monitor._read_bucket.deduct(monitor._read_bucket.capacity * 3)
            with patch.object(monitor._read_bucket, 'wait_until_positive') as mock_wait:
                monitor._enforce_rate_limit(params={}, model=model)
                mock_wait.assert_called_once()

    def test_write_operations_use_write_bucket(self, monitor):
        for op in ('PutItem', 'UpdateItem', 'DeleteItem', 'BatchWriteItem', 'TransactWriteItems'):
            model = Mock()
            model.name = op
            with patch.object(monitor._write_bucket, 'wait_until_positive') as mock_wait:
                monitor._enforce_rate_limit(params={}, model=model)
                mock_wait.assert_called_once()

    def test_other_operations_do_not_block(self, monitor):
        model = Mock()
        model.name = 'DescribeTable'
        with patch.object(monitor._read_bucket, 'wait_until_positive') as mock_read:
            with patch.object(monitor._write_bucket, 'wait_until_positive') as mock_write:
                monitor._enforce_rate_limit(params={}, model=model)
                mock_read.assert_not_called()
                mock_write.assert_not_called()


class TestTrackConsumedCapacity:
    def test_tracks_dict_consumed_capacity(self, monitor):
        model = Mock()
        model.name = 'PutItem'
        parsed = {
            'ConsumedCapacity': {
                'CapacityUnits': 5.0,
                'WriteCapacityUnits': 5.0,
                'ReadCapacityUnits': 0.0,
            }
        }
        monitor._track_consumed_capacity(http_response=Mock(), parsed=parsed, model=model)
        assert monitor.metrics['write_capacity'] == 5.0
        assert monitor.metrics['read_capacity'] == 0.0

    def test_tracks_list_consumed_capacity(self, monitor):
        model = Mock()
        model.name = 'BatchWriteItem'
        parsed = {
            'ConsumedCapacity': [
                {'CapacityUnits': 3.0, 'WriteCapacityUnits': 3.0, 'ReadCapacityUnits': 0.0},
                {'CapacityUnits': 2.0, 'WriteCapacityUnits': 2.0, 'ReadCapacityUnits': 0.0},
            ]
        }
        monitor._track_consumed_capacity(http_response=Mock(), parsed=parsed, model=model)
        assert monitor.metrics['write_capacity'] == 5.0
        assert monitor.metrics['calls'] == 2

    def test_no_consumed_capacity_is_noop(self, monitor):
        model = Mock()
        model.name = 'PutItem'
        parsed = {}
        monitor._track_consumed_capacity(http_response=Mock(), parsed=parsed, model=model)
        assert monitor.metrics['calls'] == 0

    def test_ambiguous_capacity_for_read_op(self, monitor):
        model = Mock()
        model.name = 'Scan'
        parsed = {
            'ConsumedCapacity': {
                'CapacityUnits': 10.0,
            }
        }
        monitor._track_consumed_capacity(http_response=Mock(), parsed=parsed, model=model)
        assert monitor.metrics['read_capacity'] == 10.0
        assert monitor.metrics['write_capacity'] == 0.0


class TestBucketDeduction:
    def test_read_capacity_deducted_from_read_bucket(self, monitor):
        model = Mock()
        model.name = 'Scan'
        initial_snap = monitor._read_bucket.snapshot()
        parsed = {
            'ConsumedCapacity': {
                'ReadCapacityUnits': 25.0,
                'WriteCapacityUnits': 0.0,
                'CapacityUnits': 25.0,
            }
        }
        monitor._track_consumed_capacity(http_response=Mock(), parsed=parsed, model=model)
        after_snap = monitor._read_bucket.snapshot()
        assert after_snap['tokens'] < initial_snap['tokens']

    def test_write_capacity_deducted_from_write_bucket(self, monitor):
        model = Mock()
        model.name = 'PutItem'
        initial_snap = monitor._write_bucket.snapshot()
        parsed = {
            'ConsumedCapacity': {
                'WriteCapacityUnits': 10.0,
                'ReadCapacityUnits': 0.0,
                'CapacityUnits': 10.0,
            }
        }
        monitor._track_consumed_capacity(http_response=Mock(), parsed=parsed, model=model)
        after_snap = monitor._write_bucket.snapshot()
        assert after_snap['tokens'] < initial_snap['tokens']


class TestRateSetters:
    def test_set_max_read_rate(self, monitor):
        monitor.max_read_rate = 2000
        assert monitor.max_read_rate == 2000.0
        assert monitor._read_bucket.rate == 2000.0

    def test_set_max_write_rate(self, monitor):
        monitor.max_write_rate = 800
        assert monitor.max_write_rate == 800.0
        assert monitor._write_bucket.rate == 800.0

    def test_set_invalid_read_rate_raises(self, monitor):
        with pytest.raises(ValueError, match="read rate must be >= 1"):
            monitor.max_read_rate = 0

    def test_set_invalid_write_rate_raises(self, monitor):
        with pytest.raises(ValueError, match="write rate must be >= 1"):
            monitor.max_write_rate = 0


class TestReporting:
    def test_reporting_thread_starts_when_enabled(self, mock_session):
        m = DynamoDBMonitor(mock_session, max_read_rate=100, max_write_rate=50, enable_reporting=True)
        assert m._report_thread.is_alive()
        m.stop()
        assert not m._report_thread.is_alive()

    def test_stop_is_idempotent(self, monitor):
        monitor.stop()
        monitor.stop()  # should not raise
