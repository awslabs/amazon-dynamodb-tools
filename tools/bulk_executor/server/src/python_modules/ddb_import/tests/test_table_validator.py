"""Unit tests for TableValidator."""

import pytest
from unittest.mock import Mock, patch
from botocore.exceptions import ClientError
from ..validators.table_validator import TableValidator


class TestTableValidator:
    """Test suite for TableValidator class."""
    
    def test_validate_table_empty_with_empty_table(self):
        """Test validate_table_empty() returns True when table is empty (Count=0)."""
        # Mock DynamoDB client
        mock_dynamodb = Mock()
        mock_dynamodb.scan.return_value = {'Count': 0}
        
        validator = TableValidator(mock_dynamodb)
        
        # Should return True for empty table
        result = validator.validate_table_empty('test-table')
        assert result is True
        
        # Verify scan was called with correct parameters
        mock_dynamodb.scan.assert_called_once_with(
            TableName='test-table',
            Limit=1,
            Select='COUNT'
        )
    
    def test_validate_table_empty_with_non_empty_table(self):
        """Test validate_table_empty() raises ValueError when table has items (Count>0)."""
        # Mock DynamoDB client with non-empty table
        mock_dynamodb = Mock()
        mock_dynamodb.scan.return_value = {'Count': 1}
        
        validator = TableValidator(mock_dynamodb)
        
        # Should raise ValueError for non-empty table
        with pytest.raises(ValueError) as exc_info:
            validator.validate_table_empty('test-table')
        
        # Verify error message includes table name
        error_message = str(exc_info.value)
        assert 'test-table' in error_message
        assert 'not empty' in error_message.lower()
        assert '1' in error_message
    
    def test_validate_table_empty_with_multiple_items(self):
        """Test validate_table_empty() with table containing multiple items."""
        # Mock DynamoDB client with multiple items
        mock_dynamodb = Mock()
        mock_dynamodb.scan.return_value = {'Count': 5}
        
        validator = TableValidator(mock_dynamodb)
        
        # Should raise ValueError
        with pytest.raises(ValueError) as exc_info:
            validator.validate_table_empty('my-table')
        
        # Verify error message includes table name and count
        error_message = str(exc_info.value)
        assert 'my-table' in error_message
        assert 'not empty' in error_message.lower()
    
    def test_validate_table_empty_with_non_existent_table(self):
        """Test validate_table_empty() raises ValueError when table doesn't exist."""
        # Mock DynamoDB client to raise ResourceNotFoundException
        mock_dynamodb = Mock()
        mock_dynamodb.exceptions.ResourceNotFoundException = type('ResourceNotFoundException', (Exception,), {})
        mock_dynamodb.scan.side_effect = mock_dynamodb.exceptions.ResourceNotFoundException(
            "Table not found"
        )
        
        validator = TableValidator(mock_dynamodb)
        
        # Should raise ValueError for non-existent table
        with pytest.raises(ValueError) as exc_info:
            validator.validate_table_empty('non-existent-table')
        
        # Verify error message includes table name
        error_message = str(exc_info.value)
        assert 'non-existent-table' in error_message
        assert 'does not exist' in error_message.lower()
    
    def test_validate_table_empty_error_messages_include_table_name(self):
        """Test that all error messages include the table name for context."""
        mock_dynamodb = Mock()
        
        # Test with non-empty table
        mock_dynamodb.scan.return_value = {'Count': 3}
        validator = TableValidator(mock_dynamodb)
        
        with pytest.raises(ValueError) as exc_info:
            validator.validate_table_empty('specific-table-name')
        
        assert 'specific-table-name' in str(exc_info.value)
        
        # Test with non-existent table
        mock_dynamodb.exceptions.ResourceNotFoundException = type('ResourceNotFoundException', (Exception,), {})
        mock_dynamodb.scan.side_effect = mock_dynamodb.exceptions.ResourceNotFoundException()
        
        validator = TableValidator(mock_dynamodb)
        
        with pytest.raises(ValueError) as exc_info:
            validator.validate_table_empty('another-table-name')
        
        assert 'another-table-name' in str(exc_info.value)
    
    def test_validate_table_empty_with_missing_count_in_response(self):
        """Test validate_table_empty() handles response without Count field."""
        # Mock DynamoDB client with response missing Count
        mock_dynamodb = Mock()
        mock_dynamodb.scan.return_value = {}  # No Count field
        
        validator = TableValidator(mock_dynamodb)
        
        # Should treat missing Count as 0 and return True
        result = validator.validate_table_empty('test-table')
        assert result is True
    
    def test_validate_table_empty_uses_efficient_scan(self):
        """Test that validate_table_empty() uses scan with Limit=1 for efficiency."""
        mock_dynamodb = Mock()
        mock_dynamodb.scan.return_value = {'Count': 0}
        
        validator = TableValidator(mock_dynamodb)
        validator.validate_table_empty('test-table')
        
        # Verify scan was called with Limit=1 and Select='COUNT'
        call_args = mock_dynamodb.scan.call_args
        assert call_args[1]['Limit'] == 1
        assert call_args[1]['Select'] == 'COUNT'
    
    def test_validate_table_empty_with_unexpected_error(self):
        """Test validate_table_empty() handles unexpected errors gracefully."""
        # Mock DynamoDB client to raise unexpected error
        mock_dynamodb = Mock()
        mock_dynamodb.exceptions.ResourceNotFoundException = type('ResourceNotFoundException', (Exception,), {})
        mock_dynamodb.scan.side_effect = Exception("Unexpected error")
        
        validator = TableValidator(mock_dynamodb)
        
        # Should raise ValueError with error details
        with pytest.raises(ValueError) as exc_info:
            validator.validate_table_empty('test-table')
        
        error_message = str(exc_info.value)
        assert 'test-table' in error_message
        assert 'Error validating table' in error_message
    
    def test_validate_table_empty_logs_success(self, caplog):
        """Test that validate_table_empty() logs success message for empty table."""
        
        mock_dynamodb = Mock()
        mock_dynamodb.scan.return_value = {'Count': 0}
        
        validator = TableValidator(mock_dynamodb)
        validator.validate_table_empty('test-table')
        
        # Verify info log was captured
        assert "Table 'test-table' validation successful: table is empty" in caplog.text
    
    def test_validate_table_empty_logs_error_for_non_empty_table(self, caplog):
        """Test that validate_table_empty() logs error for non-empty table."""
        
        mock_dynamodb = Mock()
        mock_dynamodb.scan.return_value = {'Count': 5}
        
        validator = TableValidator(mock_dynamodb)
        
        with pytest.raises(ValueError):
            validator.validate_table_empty('test-table')
        
        # Verify error log was captured
        assert 'test-table' in caplog.text
        assert 'not empty' in caplog.text
    
    def test_validate_table_empty_logs_error_for_non_existent_table(self, caplog):
        """Test that validate_table_empty() logs error for non-existent table."""
        
        mock_dynamodb = Mock()
        mock_dynamodb.exceptions.ResourceNotFoundException = type('ResourceNotFoundException', (Exception,), {})
        mock_dynamodb.scan.side_effect = mock_dynamodb.exceptions.ResourceNotFoundException()
        
        validator = TableValidator(mock_dynamodb)
        
        with pytest.raises(ValueError):
            validator.validate_table_empty('test-table')
        
        # Verify error log was captured
        assert 'test-table' in caplog.text
        assert 'does not exist' in caplog.text
