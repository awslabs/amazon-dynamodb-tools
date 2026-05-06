"""Unit tests for S3Validator."""

import pytest
from unittest.mock import Mock, patch
from python_modules.load_export.validators.s3_validator import S3Validator

class TestS3Validator:
    """Test suite for S3Validator class."""
    
    @pytest.fixture
    def mock_path_resolver(self):
        """Mock ExportPathResolver for testing."""
        def create_mock_resolver(bucket, prefix=""):
            mock_resolver = Mock()
            if prefix:
                mock_resolver.get_data_base_path.return_value = f"s3://{bucket}/{prefix}"
            else:
                mock_resolver.get_data_base_path.return_value = f"s3://{bucket}"

            mock_resolver.get_bucket.return_value = f"{bucket}"
            mock_resolver.get_prefix.return_value = f"{prefix}"
            return mock_resolver
        return create_mock_resolver
    
    def test_validate_path_exists_with_valid_path(self, mock_path_resolver):
        """Test validate_path_exists() returns True when path exists and has objects."""
        # Mock S3 client
        mock_s3 = Mock()
        mock_s3.list_objects_v2.return_value = {
            'Contents': [{'Key': 'some-file.json'}]
        }
        
        validator = S3Validator(mock_s3)
        path_resolver = mock_path_resolver('my-bucket', 'my-prefix')
        
        # Should return True for valid path
        result = validator.validate_path_exists(path_resolver)
        assert result is True
        
        # Verify list_objects_v2 was called with correct parameters
        mock_s3.list_objects_v2.assert_called_once_with(
            Bucket='my-bucket',
            Prefix='my-prefix/',
            MaxKeys=1
        )
    
    def test_validate_path_exists_with_empty_path(self, mock_path_resolver):
        """Test validate_path_exists() raises ValueError when path is empty."""
        # Mock S3 client with empty response
        mock_s3 = Mock()
        mock_s3.list_objects_v2.return_value = {}  # No Contents
        
        validator = S3Validator(mock_s3)
        path_resolver = mock_path_resolver('my-bucket', 'my-prefix')
        
        # Should raise ValueError for empty path
        with pytest.raises(ValueError) as exc_info:
            validator.validate_path_exists(path_resolver)
        
        # Verify error message includes path details
        error_message = str(exc_info.value)
        assert 'my-bucket' in error_message
        assert 'does not exist or is empty' in error_message.lower()
    
    def test_validate_path_exists_with_no_contents(self, mock_path_resolver):
        """Test validate_path_exists() raises ValueError when Contents list is empty."""
        # Mock S3 client with empty Contents list
        mock_s3 = Mock()
        mock_s3.list_objects_v2.return_value = {'Contents': []}
        
        validator = S3Validator(mock_s3)
        path_resolver = mock_path_resolver('test-bucket', 'prefix')
        
        # Should raise ValueError
        with pytest.raises(ValueError) as exc_info:
            validator.validate_path_exists(path_resolver)
        
        error_message = str(exc_info.value)
        assert 'test-bucket' in error_message
        assert 'does not exist or is empty' in error_message.lower()
    
    def test_validate_path_exists_with_non_existent_bucket(self, mock_path_resolver):
        """Test validate_path_exists() raises ValueError when bucket doesn't exist."""
        # Mock S3 client to raise NoSuchBucket
        mock_s3 = Mock()
        mock_s3.exceptions.NoSuchBucket = type('NoSuchBucket', (Exception,), {})
        mock_s3.list_objects_v2.side_effect = mock_s3.exceptions.NoSuchBucket(
            "The specified bucket does not exist"
        )
        
        validator = S3Validator(mock_s3)
        path_resolver = mock_path_resolver('non-existent-bucket', 'my-prefix')
        
        # Should raise ValueError for non-existent bucket
        with pytest.raises(ValueError) as exc_info:
            validator.validate_path_exists(path_resolver)
        
        # Verify error message includes bucket name
        error_message = str(exc_info.value)
        assert 'non-existent-bucket' in error_message
        assert 'does not exist' in error_message.lower()
    
    def test_validate_path_exists_with_access_denied(self, mock_path_resolver):
        """Test validate_path_exists() raises ValueError when access is denied."""
        # Mock S3 client to raise AccessDenied error
        mock_s3 = Mock()
        mock_s3.list_objects_v2.side_effect = Exception("AccessDenied: Access Denied")
        
        validator = S3Validator(mock_s3)
        path_resolver = mock_path_resolver('restricted-bucket', 'my-prefix')
        
        # Should raise ValueError for access denied
        with pytest.raises(ValueError) as exc_info:
            validator.validate_path_exists(path_resolver)
        
        # Verify error message mentions access denied
        error_message = str(exc_info.value)
        assert 'restricted-bucket' in error_message
        assert 'access denied' in error_message.lower()
        assert 's3:ListBucket' in error_message
    
    def test_validate_path_exists_with_403_error(self, mock_path_resolver):
        """Test validate_path_exists() handles 403 Forbidden errors."""
        # Mock S3 client to raise 403 error
        mock_s3 = Mock()
        mock_s3.list_objects_v2.side_effect = Exception("403 Forbidden")
        
        validator = S3Validator(mock_s3)
        path_resolver = mock_path_resolver('forbidden-bucket', 'my-prefix')

        # Should raise ValueError
        with pytest.raises(ValueError) as exc_info:
            validator.validate_path_exists(path_resolver)
        
        error_message = str(exc_info.value)
        assert 'access denied' in error_message.lower()
    
    def test_validate_path_exists_with_invalid_format(self, mock_path_resolver):
        """Test validate_path_exists() raises ValueError for invalid S3 path format."""
        mock_s3 = Mock()
        validator = S3Validator(mock_s3)
        
        # Mock resolver to return invalid path
        mock_resolver = Mock()
        mock_resolver.get_data_base_path.return_value = "invalid-path/bucket"
        
        # Should raise ValueError for path not starting with s3://
        with pytest.raises(ValueError) as exc_info:
            validator.validate_path_exists(mock_resolver)
        
        error_message = str(exc_info.value)
        assert 'invalid s3 path format' in error_message.lower()
        assert "must start with 's3://'" in error_message.lower()
    
    def test_validate_path_exists_with_bucket_only(self, mock_path_resolver):
        """Test validate_path_exists() works with bucket-only path (no prefix)."""
        # Mock S3 client
        mock_s3 = Mock()
        mock_s3.list_objects_v2.return_value = {
            'Contents': [{'Key': 'file.json'}]
        }
        
        validator = S3Validator(mock_s3)
        path_resolver = mock_path_resolver('my-bucket')
        
        # Should work with bucket-only path
        result = validator.validate_path_exists(path_resolver)
        assert result is True
        
        # Verify list_objects_v2 was called with empty prefix
        mock_s3.list_objects_v2.assert_called_once_with(
            Bucket='my-bucket',
            Prefix='',
            MaxKeys=1
        )
    
    def test_validate_path_exists_with_nested_prefix(self, mock_path_resolver):
        """Test validate_path_exists() works with deeply nested prefixes."""
        # Mock S3 client
        mock_s3 = Mock()
        mock_s3.list_objects_v2.return_value = {
            'Contents': [{'Key': 'deep/nested/path/file.json'}]
        }
        
        validator = S3Validator(mock_s3)
        path_resolver = mock_path_resolver('my-bucket', 'deep/nested/path')
        
        # Should work with nested prefix
        result = validator.validate_path_exists(path_resolver)
        assert result is True
        
        # Verify correct prefix was used
        call_args = mock_s3.list_objects_v2.call_args
        assert call_args[1]['Prefix'] == 'deep/nested/path/'
    
    def test_validate_path_exists_uses_efficient_listing(self, mock_path_resolver):
        """Test that validate_path_exists() uses MaxKeys=1 for efficiency."""
        mock_s3 = Mock()
        mock_s3.list_objects_v2.return_value = {
            'Contents': [{'Key': 'file.json'}]
        }
        
        validator = S3Validator(mock_s3)
        path_resolver = mock_path_resolver('test-bucket', 'prefix')

        validator.validate_path_exists(path_resolver)
        
        # Verify MaxKeys=1 was used
        call_args = mock_s3.list_objects_v2.call_args
        assert call_args[1]['MaxKeys'] == 1
    
    def test_validate_path_exists_with_unexpected_error(self, mock_path_resolver):
        """Test validate_path_exists() handles unexpected errors gracefully."""
        # Mock S3 client to raise unexpected error
        mock_s3 = Mock()
        mock_s3.list_objects_v2.side_effect = Exception("Unexpected error occurred")
        
        validator = S3Validator(mock_s3)
        path_resolver = mock_path_resolver('test-bucket', 'prefix')

        # Should raise ValueError with error details
        with pytest.raises(ValueError) as exc_info:
            validator.validate_path_exists(path_resolver)
        
        error_message = str(exc_info.value)
        assert 'test-bucket' in error_message
        assert 'error validating s3 path' in error_message.lower()
        assert 'unexpected error' in error_message.lower()
    
    def test_validate_path_exists_logs_success(self, mock_path_resolver, caplog):
        """Test that validate_path_exists() logs success message for valid path."""
        
        mock_s3 = Mock()
        mock_s3.list_objects_v2.return_value = {
            'Contents': [{'Key': 'file.json'}]
        }
        
        validator = S3Validator(mock_s3)
        path_resolver = mock_path_resolver('test-bucket', 'prefix')

        validator.validate_path_exists(path_resolver)
        
        # Verify info log was captured
        assert 'S3 path validation successful: s3://test-bucket/prefix' in caplog.text
    
    def test_validate_path_exists_logs_error_for_empty_path(self, mock_path_resolver, caplog):
        """Test that validate_path_exists() logs error for empty path."""
        
        mock_s3 = Mock()
        mock_s3.list_objects_v2.return_value = {}
        
        validator = S3Validator(mock_s3)
        path_resolver = mock_path_resolver('test-bucket', 'empty')
        
        with pytest.raises(ValueError):
            validator.validate_path_exists(path_resolver)
        
        # Verify error log was captured
        assert 'test-bucket' in caplog.text
        assert 'does not exist or is empty' in caplog.text
    
    def test_validate_path_exists_logs_error_for_non_existent_bucket(self, mock_path_resolver, caplog):
        """Test that validate_path_exists() logs error for non-existent bucket."""
        
        mock_s3 = Mock()
        mock_s3.exceptions.NoSuchBucket = type('NoSuchBucket', (Exception,), {})
        mock_s3.list_objects_v2.side_effect = mock_s3.exceptions.NoSuchBucket()
        
        validator = S3Validator(mock_s3)
        path_resolver = mock_path_resolver('non-existent', 'empty')
        
        with pytest.raises(ValueError):
            validator.validate_path_exists(path_resolver)
        
        # Verify error log was captured
        assert 'non-existent' in caplog.text
        assert 'does not exist' in caplog.text
    
    def test_validate_path_exists_logs_error_for_access_denied(self, mock_path_resolver, caplog):
        """Test that validate_path_exists() logs error for access denied."""
        
        mock_s3 = Mock()
        mock_s3.list_objects_v2.side_effect = Exception("AccessDenied")
        
        validator = S3Validator(mock_s3)
        path_resolver = mock_path_resolver('restricted', 'empty')

        with pytest.raises(ValueError):
            validator.validate_path_exists(path_resolver)
        
        # Verify error log was captured
        assert 'restricted' in caplog.text
        assert 'Access denied' in caplog.text
