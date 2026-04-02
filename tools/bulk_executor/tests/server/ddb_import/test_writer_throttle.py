"""Unit tests proving silent data loss when throttle retries are exhausted."""

import pytest
from unittest.mock import Mock, patch, MagicMock
import botocore.exceptions

from python_modules.ddb_import.writers.item_writer import ItemWriter
from python_modules.ddb_import.writers.batch_writer import BatchWriter

THROTTLE_CODE = 'ProvisionedThroughputExceededException'


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


class TestItemWriterThrottleSilentFailure:

    @patch('python_modules.ddb_import.writers.item_writer.get_error_message', side_effect=_real_get_error_message)
    @patch('python_modules.ddb_import.writers.item_writer.get_error_code', side_effect=_real_get_error_code)
    @patch('python_modules.ddb_import.writers.item_writer.RateLimiterWorker')
    def test_throttle_exhaustion_must_report_error(self, mock_rlw_class, mock_gec, mock_gem):
        mock_worker = Mock()
        mock_session = Mock()
        mock_dynamodb = Mock()
        mock_table = Mock()

        mock_rlw_class.return_value = mock_worker
        mock_worker.get_session.return_value = mock_session
        mock_session.resource.return_value = mock_dynamodb
        mock_dynamodb.Table.return_value = mock_table

        mock_table.put_item.side_effect = _make_throttle_error()

        operations = [
            {"operation": "PUT", "data": {"id": "item-1"}, "condition": None, "expr_names": None},
        ]

        error_accumulator = _mock_list_accumulator()
        written_accumulator = _mock_accumulator()

        writer = ItemWriter()
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


class TestBatchWriterThrottleSilentFailure:

    @patch('python_modules.ddb_import.writers.batch_writer.get_error_message', side_effect=_real_get_error_message)
    @patch('python_modules.ddb_import.writers.batch_writer.get_error_code', side_effect=_real_get_error_code)
    @patch('python_modules.ddb_import.writers.batch_writer.RateLimiterWorker')
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
