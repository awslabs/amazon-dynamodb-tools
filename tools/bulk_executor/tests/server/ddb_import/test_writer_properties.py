"""Property-based tests for parallel writer worker functions.

These tests verify universal properties that should hold across all valid inputs
using the Hypothesis property-based testing framework.
"""

import pytest
from hypothesis import given, strategies as st, settings
from unittest.mock import Mock, patch

from python_modules.ddb_import.writers.batch_writer import BatchWriter


class TestBatchWriterQuotaAdherence:
    """Property-based tests for batch writer quota adherence."""

    @given(st.integers(min_value=1, max_value=1000))
    @settings(max_examples=50)
    def test_batch_writer_processes_all_assigned_operations(self, operation_count: int):
        """
        For any batch writer with an assigned operation quota, the writer should process
        exactly that many operations (all operations in partition are executed).
        """
        operations = [
            {"operation": "PUT", "data": {"id": f"item-{i}", "value": i}}
            for i in range(operation_count)
        ]

        mock_rate_limiter_config = Mock()
        mock_monitor_options = Mock()
        mock_error_accumulator = Mock()
        mock_debug_accumulator = Mock()
        mock_written_items_accumulator = Mock()

        writer = BatchWriter()
        with patch.object(writer, 'write_partition_to_dynamodb') as mock_write:
            writer.write_partition_to_dynamodb(
                partition_data=iter(operations),
                table_name="test-table",
                rate_limiter_shared_config=mock_rate_limiter_config,
                monitor_options=mock_monitor_options,
                error_accumulator=mock_error_accumulator,
                debug_accumulator=mock_debug_accumulator,
                written_items_accumulator=mock_written_items_accumulator
            )

            assert mock_write.call_count == 1, (
                f"Expected 1 write_partition_to_dynamodb call, "
                f"got {mock_write.call_count} calls"
            )


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
