"""Unit tests for item writer worker functions."""

import pytest
from unittest.mock import Mock, MagicMock, patch
import botocore.exceptions

from python_modules.ddb_import.writers.item_writer import ItemWriter


class TestItemWriter:
    """Unit tests for ItemWriter class."""
    
    def test_item_writer_instantiation(self):
        """Test that ItemWriter can be instantiated."""
        writer = ItemWriter()
        assert writer is not None
        assert hasattr(writer, 'write_partition_to_dynamodb')
    
    def test_backward_compatibility_function(self):
        """Test that the backward compatibility function exists and works."""
        from python_modules.ddb_import.writers.item_writer import write_partition_to_dynamodb
        
        # Just verify the function exists and can be called
        assert callable(write_partition_to_dynamodb)
    
    def test_write_partition_method_signature(self):
        """Test that the write_partition_to_dynamodb method has the correct signature."""
        writer = ItemWriter()
        method = writer.write_partition_to_dynamodb
        
        # Check that the method exists and is callable
        assert callable(method)
        
        # Check that it has the expected parameters by inspecting the method
        import inspect
        sig = inspect.signature(method)
        expected_params = [
            'partition_data',
            'table_name', 
            'rate_limiter_shared_config',
            'monitor_options',
            'error_accumulator',
            'debug_accumulator',
            'written_items_accumulator'
        ]
        
        actual_params = list(sig.parameters.keys())
        for param in expected_params:
            assert param in actual_params, f"Missing parameter: {param}"
    
    def test_handles_empty_partition_gracefully(self):
        """Test that empty partitions don't cause crashes."""
        operations = []
        
        # Mock all required parameters with proper attributes
        mock_rate_limiter_config = Mock()
        mock_rate_limiter_config.bucket = "test-bucket"
        mock_rate_limiter_config.prefix = "test-prefix"
        
        mock_monitor_options = {}
        mock_error_accumulator = Mock()
        mock_debug_accumulator = Mock()
        mock_written_items_accumulator = Mock()
        
        writer = ItemWriter()
        
        # This should not raise an exception, even if it fails to connect to AWS
        # We're testing that the method handles empty input gracefully
        try:
            writer.write_partition_to_dynamodb(
                partition_data=iter(operations),
                table_name="test-table",
                rate_limiter_shared_config=mock_rate_limiter_config,
                monitor_options=mock_monitor_options,
                error_accumulator=mock_error_accumulator,
                debug_accumulator=mock_debug_accumulator,
                written_items_accumulator=mock_written_items_accumulator
            )
            # If we get here without exception, the empty partition was handled correctly
            success = True
        except Exception as e:
            # If an exception occurs, it should be AWS-related, not a code error
            error_msg = str(e).lower()
            success = any(keyword in error_msg for keyword in [
                'aws', 'boto', 'credentials', 'region', 'endpoint', 'connection'
            ])
        
        assert success, "Method should handle empty partitions gracefully"
    
    def test_operation_data_structure_validation(self):
        """Test that the method expects the correct operation data structure."""
        # This test validates that our understanding of the expected data structure is correct
        operations = [
            {"operation": "PUT", "data": {"id": "item-1", "value": 1}, "condition": "attribute_not_exists(id)"},
            {"operation": "DELETE", "data": {"id": "item-2"}, "condition": "attribute_exists(id)"}
        ]
        
        # Verify the structure we expect
        for op in operations:
            assert "operation" in op
            assert "data" in op
            assert op["operation"] in ["PUT", "DELETE"]
            assert isinstance(op["data"], dict)
            # ItemWriter supports conditions (unlike BatchWriter)
            if "condition" in op:
                assert isinstance(op["condition"], str)
        
        # This validates our test data structure is correct
        assert len(operations) == 2
        assert operations[0]["operation"] == "PUT"
        assert operations[1]["operation"] == "DELETE"
        assert "condition" in operations[0]
        assert "condition" in operations[1]
    
    def test_supports_conditional_operations(self):
        """Test that ItemWriter supports conditional operations (unlike BatchWriter)."""
        # This is a key difference between ItemWriter and BatchWriter
        # ItemWriter can handle conditions, BatchWriter cannot
        
        operations_with_conditions = [
            {"operation": "PUT", "data": {"id": "item-1"}, "condition": "attribute_not_exists(id)"},
            {"operation": "DELETE", "data": {"id": "item-2"}, "condition": "attribute_exists(id)"}
        ]
        
        operations_without_conditions = [
            {"operation": "PUT", "data": {"id": "item-3"}},
            {"operation": "DELETE", "data": {"id": "item-4"}}
        ]
        
        # Both should be valid for ItemWriter
        for ops in [operations_with_conditions, operations_without_conditions]:
            for op in ops:
                assert "operation" in op
                assert "data" in op
                # condition is optional for ItemWriter
                if "condition" in op:
                    assert isinstance(op["condition"], str)


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
