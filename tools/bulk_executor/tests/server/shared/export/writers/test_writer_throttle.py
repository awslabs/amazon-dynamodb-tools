"""Unit tests proving silent data loss when throttle retries are exhausted."""

import pytest
from unittest.mock import Mock, patch, MagicMock
import botocore.exceptions

from python_modules.shared.export.writers.batch_writer import BatchWriter
from python_modules.shared.export.utils.enums import Operation

THROTTLE_CODE = 'ProvisionedThroughputExceededException'
VALIDATION_CODE = 'ValidationException'


def _make_throttle_error():
    error_response = {'Error': {'Code': THROTTLE_CODE, 'Message': 'Rate exceeded'}}
    return botocore.exceptions.ClientError(error_response, 'PutItem')


def _real_get_error_code(e):
    return e.response['Error']['Code']


def _real_get_error_message(e):
    return e.response['Error']['Message']


def _mock_accumulator(initial=0):
    acc = Mock()
    acc.value = initial
    values = [initial]
    def add(v):
        values[0] += v
        acc.value = values[0]
    acc.add = add
    return acc


def _mock_list_accumulator():
    acc = Mock()
    acc.value = []
    def add(v):
        acc.value.extend(v)
    acc.add = add
    return acc


class TestBatchWriterThrottleSilentFailure:

    @patch('python_modules.shared.export.writers.batch_writer.get_error_message', side_effect=_real_get_error_message)
    @patch('python_modules.shared.export.writers.batch_writer.get_error_code', side_effect=_real_get_error_code)
    @patch('python_modules.shared.export.writers.batch_writer.RateLimiterWorker')
    def test_throttle_exhaustion_must_report_error(self, mock_rlw_class, mock_gec, mock_gem):
        mock_worker = Mock()
        mock_session = Mock()
        mock_dynamodb = Mock()
        mock_table = Mock()

        mock_rlw_class.return_value = mock_worker
        mock_worker.get_session.return_value = mock_session
        mock_session.resource.return_value = mock_dynamodb
        mock_dynamodb.Table.return_value = mock_table

        mock_batch = MagicMock()
        mock_table.batch_writer.return_value.__enter__ = Mock(return_value=mock_batch)
        mock_table.batch_writer.return_value.__exit__ = Mock(side_effect=_make_throttle_error())

        operations = [
            {"operation": "PUT", "data": {"id": "item-1"}},
        ]

        error_accumulator = _mock_list_accumulator()
        written_accumulator = _mock_accumulator()

        writer = BatchWriter()
        writer.write_partition_to_dynamodb(
            partition_data=iter(operations),
            table_name="test-table",
            rate_limiter_shared_config=Mock(),
            monitor_options={},
            error_accumulator=error_accumulator,
            debug_accumulator=None,
            written_items_accumulator=written_accumulator,
        )

        assert len(error_accumulator.value) > 0, (
            "Throttle exhaustion was silently swallowed. "
            "error_accumulator is empty, so the caller will report success despite data loss."
        )


class TestBatchWriterValidationError:

    @patch('python_modules.shared.export.writers.batch_writer.get_error_message', side_effect=_real_get_error_message)
    @patch('python_modules.shared.export.writers.batch_writer.get_error_code', side_effect=_real_get_error_code)
    @patch('python_modules.shared.export.writers.batch_writer.RateLimiterWorker')
    def test_validation_error_reports_schema_message(self, mock_rlw_class, mock_gec, mock_gem):
        mock_worker = Mock()
        mock_session = Mock()
        mock_dynamodb = Mock()
        mock_table = Mock()

        mock_rlw_class.return_value = mock_worker
        mock_worker.get_session.return_value = mock_session
        mock_session.resource.return_value = mock_dynamodb
        mock_dynamodb.Table.return_value = mock_table

        mock_batch = MagicMock()
        mock_table.batch_writer.return_value.__enter__ = Mock(return_value=mock_batch)
        error_response = {'Error': {'Code': VALIDATION_CODE, 'Message': 'Item size exceeds limit'}}
        mock_table.batch_writer.return_value.__exit__ = Mock(
            side_effect=botocore.exceptions.ClientError(error_response, 'BatchWriteItem')
        )

        operations = [{"operation": Operation.PUT, "data": {"id": "item-1"}}]
        error_accumulator = _mock_list_accumulator()
        written_accumulator = _mock_accumulator()

        writer = BatchWriter()
        writer.write_partition_to_dynamodb(
            partition_data=iter(operations),
            table_name="test-table",
            rate_limiter_shared_config=Mock(),
            monitor_options={},
            error_accumulator=error_accumulator,
            debug_accumulator=None,
            written_items_accumulator=written_accumulator,
        )

        assert len(error_accumulator.value) > 0
        assert "Schema validation error" in error_accumulator.value[0] or "validation" in error_accumulator.value[0].lower()


class TestBatchWriterGenericClientError:

    @patch('python_modules.shared.export.writers.batch_writer.get_error_message', side_effect=_real_get_error_message)
    @patch('python_modules.shared.export.writers.batch_writer.get_error_code', side_effect=_real_get_error_code)
    @patch('python_modules.shared.export.writers.batch_writer.RateLimiterWorker')
    def test_generic_client_error_reports_error(self, mock_rlw_class, mock_gec, mock_gem):
        mock_worker = Mock()
        mock_session = Mock()
        mock_dynamodb = Mock()
        mock_table = Mock()

        mock_rlw_class.return_value = mock_worker
        mock_worker.get_session.return_value = mock_session
        mock_session.resource.return_value = mock_dynamodb
        mock_dynamodb.Table.return_value = mock_table

        mock_batch = MagicMock()
        mock_table.batch_writer.return_value.__enter__ = Mock(return_value=mock_batch)
        error_response = {'Error': {'Code': 'InternalServerError', 'Message': 'Something broke'}}
        mock_table.batch_writer.return_value.__exit__ = Mock(
            side_effect=botocore.exceptions.ClientError(error_response, 'BatchWriteItem')
        )

        operations = [{"operation": Operation.PUT, "data": {"id": "item-1"}}]
        error_accumulator = _mock_list_accumulator()
        written_accumulator = _mock_accumulator()

        writer = BatchWriter()
        writer.write_partition_to_dynamodb(
            partition_data=iter(operations),
            table_name="test-table",
            rate_limiter_shared_config=Mock(),
            monitor_options={},
            error_accumulator=error_accumulator,
            debug_accumulator=None,
            written_items_accumulator=written_accumulator,
        )

        assert len(error_accumulator.value) > 0
        assert "Error during writing" in error_accumulator.value[0]


class TestBatchWriterUnexpectedError:

    @patch('python_modules.shared.export.writers.batch_writer.RateLimiterWorker')
    def test_unexpected_exception_reports_error(self, mock_rlw_class):
        mock_worker = Mock()
        mock_session = Mock()
        mock_dynamodb = Mock()
        mock_table = Mock()

        mock_rlw_class.return_value = mock_worker
        mock_worker.get_session.return_value = mock_session
        mock_session.resource.return_value = mock_dynamodb
        mock_dynamodb.Table.return_value = mock_table

        mock_batch = MagicMock()
        mock_table.batch_writer.return_value.__enter__ = Mock(return_value=mock_batch)
        mock_table.batch_writer.return_value.__exit__ = Mock(
            side_effect=RuntimeError("Unexpected failure")
        )

        operations = [{"operation": Operation.PUT, "data": {"id": "item-1"}}]
        error_accumulator = _mock_list_accumulator()
        written_accumulator = _mock_accumulator()

        writer = BatchWriter()
        writer.write_partition_to_dynamodb(
            partition_data=iter(operations),
            table_name="test-table",
            rate_limiter_shared_config=Mock(),
            monitor_options={},
            error_accumulator=error_accumulator,
            debug_accumulator=None,
            written_items_accumulator=written_accumulator,
        )

        assert len(error_accumulator.value) > 0
        assert "Unexpected error" in error_accumulator.value[0]


class TestBatchWriterSuccessPath:

    @patch('python_modules.shared.export.writers.batch_writer.RateLimiterWorker')
    def test_successful_write_with_put_and_delete(self, mock_rlw_class):
        mock_worker = Mock()
        mock_session = Mock()
        mock_dynamodb = Mock()
        mock_table = Mock()

        mock_rlw_class.return_value = mock_worker
        mock_worker.get_session.return_value = mock_session
        mock_session.resource.return_value = mock_dynamodb
        mock_dynamodb.Table.return_value = mock_table

        mock_batch = MagicMock()
        mock_table.batch_writer.return_value.__enter__ = Mock(return_value=mock_batch)
        mock_table.batch_writer.return_value.__exit__ = Mock(return_value=False)

        operations = [
            {"operation": Operation.PUT, "data": {"id": {"S": "1"}, "name": {"S": "Alice"}}},
            {"operation": Operation.DELETE, "data": {"id": {"S": "2"}}},
        ]
        error_accumulator = _mock_list_accumulator()
        debug_accumulator = _mock_list_accumulator()
        written_accumulator = _mock_accumulator()

        writer = BatchWriter()
        writer.write_partition_to_dynamodb(
            partition_data=iter(operations),
            table_name="test-table",
            rate_limiter_shared_config=Mock(),
            monitor_options={},
            error_accumulator=error_accumulator,
            debug_accumulator=debug_accumulator,
            written_items_accumulator=written_accumulator,
        )

        assert len(error_accumulator.value) == 0
        assert written_accumulator.value == 2
        mock_batch.put_item.assert_called_once()
        mock_batch.delete_item.assert_called_once()
        mock_worker.shutdown.assert_called_once()

    @patch('python_modules.shared.export.writers.batch_writer.RateLimiterWorker')
    def test_progress_logging_at_1000_items(self, mock_rlw_class):
        mock_worker = Mock()
        mock_session = Mock()
        mock_dynamodb = Mock()
        mock_table = Mock()

        mock_rlw_class.return_value = mock_worker
        mock_worker.get_session.return_value = mock_session
        mock_session.resource.return_value = mock_dynamodb
        mock_dynamodb.Table.return_value = mock_table

        mock_batch = MagicMock()
        mock_table.batch_writer.return_value.__enter__ = Mock(return_value=mock_batch)
        mock_table.batch_writer.return_value.__exit__ = Mock(return_value=False)

        operations = [
            {"operation": Operation.PUT, "data": {"id": {"S": str(i)}}}
            for i in range(1001)
        ]
        error_accumulator = _mock_list_accumulator()
        written_accumulator = _mock_accumulator()

        writer = BatchWriter()
        writer.write_partition_to_dynamodb(
            partition_data=iter(operations),
            table_name="test-table",
            rate_limiter_shared_config=Mock(),
            monitor_options={},
            error_accumulator=error_accumulator,
            debug_accumulator=None,
            written_items_accumulator=written_accumulator,
        )

        assert len(error_accumulator.value) == 0
        assert written_accumulator.value == 1001

    @patch('python_modules.shared.export.writers.batch_writer.RateLimiterWorker')
    def test_shutdown_called_even_on_error(self, mock_rlw_class):
        mock_worker = Mock()
        mock_session = Mock()
        mock_dynamodb = Mock()
        mock_table = Mock()

        mock_rlw_class.return_value = mock_worker
        mock_worker.get_session.return_value = mock_session
        mock_session.resource.return_value = mock_dynamodb
        mock_dynamodb.Table.return_value = mock_table

        mock_batch = MagicMock()
        mock_table.batch_writer.return_value.__enter__ = Mock(return_value=mock_batch)
        mock_table.batch_writer.return_value.__exit__ = Mock(
            side_effect=RuntimeError("boom")
        )

        operations = [{"operation": Operation.PUT, "data": {"id": "1"}}]
        error_accumulator = _mock_list_accumulator()
        written_accumulator = _mock_accumulator()

        writer = BatchWriter()
        writer.write_partition_to_dynamodb(
            partition_data=iter(operations),
            table_name="test-table",
            rate_limiter_shared_config=Mock(),
            monitor_options={},
            error_accumulator=error_accumulator,
            debug_accumulator=None,
            written_items_accumulator=written_accumulator,
        )

        mock_worker.shutdown.assert_called_once()
