"""Unit tests for TableValidator."""

import pytest
from unittest.mock import Mock
from ..validators.table_validator import TableValidator


class TestTableValidator:
    """Test suite for TableValidator class."""

    def _make_describe_response(self, key_schema, attr_defs):
        return {'Table': {'KeySchema': key_schema, 'AttributeDefinitions': attr_defs}}

    def test_validate_table_exists_pk_only(self):
        """Test returns pk when table has only a partition key."""
        mock_dynamodb = Mock()
        mock_dynamodb.describe_table.return_value = self._make_describe_response(
            [{'AttributeName': 'id', 'KeyType': 'HASH'}],
            [{'AttributeName': 'id', 'AttributeType': 'S'}],
        )
        result = TableValidator(mock_dynamodb).validate_table_exists('test-table')
        assert result == {'pk': {'name': 'id', 'type': 'S'}}
        assert 'sk' not in result
        mock_dynamodb.describe_table.assert_called_once_with(TableName='test-table')

    def test_validate_table_exists_pk_and_sk(self):
        """Test returns pk and sk when table has both keys."""
        mock_dynamodb = Mock()
        mock_dynamodb.describe_table.return_value = self._make_describe_response(
            [{'AttributeName': 'pk', 'KeyType': 'HASH'}, {'AttributeName': 'sk', 'KeyType': 'RANGE'}],
            [{'AttributeName': 'pk', 'AttributeType': 'S'}, {'AttributeName': 'sk', 'AttributeType': 'N'}],
        )
        result = TableValidator(mock_dynamodb).validate_table_exists('my-table')
        assert result == {'pk': {'name': 'pk', 'type': 'S'}, 'sk': {'name': 'sk', 'type': 'N'}}

    def test_validate_table_exists_non_existent_table(self):
        """Test raises ValueError when table doesn't exist."""
        mock_dynamodb = Mock()
        mock_dynamodb.describe_table.side_effect = type('ResourceNotFoundException', (Exception,), {})()
        with pytest.raises(ValueError, match="does not exist"):
            TableValidator(mock_dynamodb).validate_table_exists('missing-table')

    def test_validate_table_exists_unexpected_error(self):
        """Test raises ValueError on unexpected errors."""
        mock_dynamodb = Mock()
        mock_dynamodb.describe_table.side_effect = Exception("Unexpected")
        with pytest.raises(ValueError, match="Error validating table"):
            TableValidator(mock_dynamodb).validate_table_exists('test-table')

    def test_validate_table_exists_error_includes_table_name(self):
        """Test that error messages include the table name."""
        mock_dynamodb = Mock()
        mock_dynamodb.describe_table.side_effect = type('ResourceNotFoundException', (Exception,), {})()
        with pytest.raises(ValueError) as exc_info:
            TableValidator(mock_dynamodb).validate_table_exists('specific-table')
        assert 'specific-table' in str(exc_info.value)

    def test_validate_table_exists_logs_success(self, caplog):
        """Test that success is logged."""
        mock_dynamodb = Mock()
        mock_dynamodb.describe_table.return_value = self._make_describe_response(
            [{'AttributeName': 'id', 'KeyType': 'HASH'}],
            [{'AttributeName': 'id', 'AttributeType': 'S'}],
        )
        TableValidator(mock_dynamodb).validate_table_exists('test-table')
        assert "validation successful" in caplog.text

    def test_validate_table_exists_logs_error_for_missing_table(self, caplog):
        """Test that error is logged for non-existent table."""
        mock_dynamodb = Mock()
        mock_dynamodb.describe_table.side_effect = type('ResourceNotFoundException', (Exception,), {})()
        with pytest.raises(ValueError):
            TableValidator(mock_dynamodb).validate_table_exists('test-table')
        assert 'does not exist' in caplog.text

    def test_validate_table_exists_binary_key_type(self):
        """Test handles binary (B) key type correctly."""
        mock_dynamodb = Mock()
        mock_dynamodb.describe_table.return_value = self._make_describe_response(
            [{'AttributeName': 'bin_key', 'KeyType': 'HASH'}],
            [{'AttributeName': 'bin_key', 'AttributeType': 'B'}],
        )
        result = TableValidator(mock_dynamodb).validate_table_exists('bin-table')
        assert result == {'pk': {'name': 'bin_key', 'type': 'B'}}
