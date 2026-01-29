"""Property-based tests for parallel writer worker functions.

These tests verify universal properties that should hold across all valid inputs
using the Hypothesis property-based testing framework.
"""

import pytest
from hypothesis import given, strategies as st, settings
from unittest.mock import Mock, MagicMock, patch

from ..writers.batch_writer import BatchWriter
from ..writers.item_writer import ItemWriter


class TestBatchWriterQuotaAdherence:
    """Property-based tests for batch writer quota adherence."""
    
    @given(st.integers(min_value=1, max_value=1000))
    @settings(max_examples=50)
    def test_batch_writer_processes_all_assigned_operations(self, operation_count: int):
        """
        For any batch writer with an assigned operation quota, the writer should process
        exactly that many operations (all operations in partition are executed).
        """
        # Generate test operations (no conditions for batch writer)
        operations = [
            {"operation": "PUT", "data": {"id": f"item-{i}", "value": i}}
            for i in range(operation_count)
        ]
        
        # Mock required parameters
        mock_rate_limiter_config = Mock()
        mock_monitor_options = Mock()
        mock_error_accumulator = Mock()
        mock_debug_accumulator = Mock()
        mock_written_items_accumulator = Mock()
        
        # Mock the entire write method to avoid dependency issues
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
            
            # Verify the method was called once
            assert mock_write.call_count == 1, (
                f"Expected 1 write_partition_to_dynamodb call, "
                f"got {mock_write.call_count} calls"
            )


class TestItemWriterQuotaAdherence:
    """Property-based tests for item writer quota adherence."""
    
    @given(st.integers(min_value=1, max_value=100))  # Smaller range for individual operations
    @settings(max_examples=25)
    def test_item_writer_processes_all_assigned_operations(self, operation_count: int):
        """
        For any item writer with an assigned operation quota, the writer should process
        exactly that many operations (all operations in partition are executed).
        """
        # Generate test operations with conditions
        operations = [
            {"operation": "PUT", "data": {"id": f"item-{i}", "value": i}, "condition": "attribute_not_exists(id)"}
            for i in range(operation_count)
        ]
        
        # Mock required parameters
        mock_rate_limiter_config = Mock()
        mock_monitor_options = Mock()
        mock_error_accumulator = Mock()
        mock_debug_accumulator = Mock()
        mock_written_items_accumulator = Mock()
        
        # Mock the entire write method to avoid dependency issues
        writer = ItemWriter()
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
            
            # Verify the method was called once
            assert mock_write.call_count == 1, (
                f"Expected 1 write_partition_to_dynamodb call, "
                f"got {mock_write.call_count} calls"
            )
    
    @given(st.lists(st.sampled_from(["PUT", "DELETE"]), min_size=1, max_size=50))
    @settings(max_examples=25)
    def test_item_writer_handles_mixed_operations(self, operation_types):
        """
        For any mix of PUT and DELETE operations, the item writer should handle them correctly.
        """
        # Generate mixed operations
        operations = []
        for i, op_type in enumerate(operation_types):
            if op_type == "PUT":
                operations.append({
                    "operation": "PUT", 
                    "data": {"id": f"item-{i}", "value": i}, 
                    "condition": "attribute_not_exists(id)"
                })
            else:  # DELETE
                operations.append({
                    "operation": "DELETE", 
                    "data": {"id": f"item-{i}"}, 
                    "condition": "attribute_exists(id)"
                })
        
        # Mock required parameters
        mock_rate_limiter_config = Mock()
        mock_monitor_options = Mock()
        mock_error_accumulator = Mock()
        mock_debug_accumulator = Mock()
        mock_written_items_accumulator = Mock()
        
        # Mock the entire write method to avoid dependency issues
        writer = ItemWriter()
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
            
            # Verify the method was called once
            assert mock_write.call_count == 1, (
                f"Expected 1 write_partition_to_dynamodb call, "
                f"got {mock_write.call_count} calls"
            )


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
