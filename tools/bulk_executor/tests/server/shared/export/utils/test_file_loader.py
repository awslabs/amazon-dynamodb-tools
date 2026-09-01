"""Unit tests for FileLoader."""

import pytest
from unittest.mock import Mock, patch, mock_open
from python_modules.shared.bulk_executor_error import BulkExecutorError
from python_modules.shared.export.utils.file_loader import FileLoader



class TestFileLoaderS3Errors:
    """How _read_from_s3 reports S3 failures the user can act on.

    Observed live while verifying #258: pointing load-export at an export in a bucket the
    Glue job's role cannot read produced a botocore traceback and a Glue exception-analysis
    blob. It is an ordinary mistake -- the managed AWSGlueServiceRole policy only grants
    buckets named aws-glue-* -- so it belongs on the polite path.
    """

    def _loader_raising(self, code):
        from botocore.exceptions import ClientError
        client = Mock()
        client.get_object.side_effect = ClientError(
            {'Error': {'Code': code, 'Message': 'nope'}}, 'GetObject')
        return FileLoader(s3_client=client)

    def test_access_denied_is_understood(self):
        loader = self._loader_raising('AccessDenied')
        with pytest.raises(BulkExecutorError) as raised:
            loader.read_file('s3://someone-elses-bucket/AWSDynamoDB/x/manifest-summary.json')
        message = str(raised.value)
        assert 'Access denied' in message
        assert 's3:GetObject' in message and 's3:ListBucket' in message, "say what to grant"
        assert 'aws-glue-*' in message, "name the trap in the managed policy"

    def test_missing_key_is_understood(self):
        loader = self._loader_raising('NoSuchKey')
        with pytest.raises(BulkExecutorError, match='File not found'):
            loader.read_file('s3://b/AWSDynamoDB/x/manifest-summary.json')

    def test_other_client_errors_are_left_alone(self):
        """An error nobody anticipated keeps its type, so it gets a traceback."""
        from botocore.exceptions import ClientError
        loader = self._loader_raising('InternalError')
        with pytest.raises(ClientError):
            loader.read_file('s3://b/AWSDynamoDB/x/manifest-summary.json')


class TestFileLoader:
    """Test cases for FileLoader class."""
    
    def test_is_s3_path_with_s3_path(self):
        """Test is_s3_path returns True for S3 paths."""
        loader = FileLoader()
        assert loader.is_s3_path('s3://bucket/key') is True
        assert loader.is_s3_path('s3://my-bucket/path/to/file.json') is True
    
    def test_is_s3_path_with_local_path(self):
        """Test is_s3_path returns False for local paths."""
        loader = FileLoader()
        assert loader.is_s3_path('/local/path/file.json') is False
        assert loader.is_s3_path('relative/path/file.json') is False
        assert loader.is_s3_path('01716790307109-5f9d6aaa/manifest.json') is False
    
    def test_parse_s3_path_valid(self):
        """Test parse_s3_path with valid S3 paths."""
        loader = FileLoader()
        
        # Test with key
        bucket, key = loader.parse_s3_path('s3://my-bucket/path/to/file.json')
        assert bucket == 'my-bucket'
        assert key == 'path/to/file.json'
        
        # Test with just bucket
        bucket, key = loader.parse_s3_path('s3://my-bucket')
        assert bucket == 'my-bucket'
        assert key == ''
        
        # Test with bucket and single level key
        bucket, key = loader.parse_s3_path('s3://my-bucket/file.json')
        assert bucket == 'my-bucket'
        assert key == 'file.json'
    
    def test_parse_s3_path_invalid(self):
        """Test parse_s3_path with invalid S3 paths."""
        loader = FileLoader()
        
        # Test with local path
        with pytest.raises(BulkExecutorError, match="Invalid S3 path"):
            loader.parse_s3_path('/local/path/file.json')
        
        # Test with empty path after s3://
        with pytest.raises(BulkExecutorError, match="Invalid S3 path format"):
            loader.parse_s3_path('s3://')
        
        # Test with malformed path
        with pytest.raises(BulkExecutorError, match="Invalid S3 path"):
            loader.parse_s3_path('http://bucket/key')
    
    def test_join_path_with_s3_paths(self):
        """Test join_path with S3 paths."""
        loader = FileLoader()
        
        # Test basic join
        result = loader.join_path('s3://bucket/export', 'manifest-summary.json')
        assert result == 's3://bucket/export/manifest-summary.json'
        
        # Test with trailing slash in base
        result = loader.join_path('s3://bucket/export/', 'manifest-summary.json')
        assert result == 's3://bucket/export/manifest-summary.json'
        
        # Test with multiple parts
        result = loader.join_path('s3://bucket/export', 'data', 'file.json')
        assert result == 's3://bucket/export/data/file.json'
        
        # Test with leading slash in part
        result = loader.join_path('s3://bucket/export', '/manifest-summary.json')
        assert result == 's3://bucket/export/manifest-summary.json'
    
    def test_join_path_with_local_paths(self):
        """Test join_path with local paths."""
        loader = FileLoader()
        
        # Test absolute path
        result = loader.join_path('/local/export', 'manifest-summary.json')
        assert result == '/local/export/manifest-summary.json'
        
        # Test relative path
        result = loader.join_path('01716790307109-5f9d6aaa', 'manifest-summary.json')
        assert result == '01716790307109-5f9d6aaa/manifest-summary.json'
        
        # Test with multiple parts
        result = loader.join_path('/local/export', 'data', 'file.json')
        assert result == '/local/export/data/file.json'
    
    def test_read_file_from_s3(self):
        """Test read_file with mocked S3 client."""
        # Create mock S3 client
        mock_s3_client = Mock()
        mock_response = {
            'Body': Mock()
        }
        mock_response['Body'].read.return_value = b's3 content'
        mock_s3_client.get_object.return_value = mock_response
        
        loader = FileLoader(s3_client=mock_s3_client)
        
        content = loader.read_file('s3://my-bucket/path/to/file.json')
        
        assert content == b's3 content'
        mock_s3_client.get_object.assert_called_once_with(
            Bucket='my-bucket',
            Key='path/to/file.json'
        )
    
    @patch('boto3.client')
    def test_read_file_from_s3_creates_client(self, mock_boto_client):
        """Test read_file creates S3 client if not provided."""
        mock_s3_client = Mock()
        mock_response = {
            'Body': Mock()
        }
        mock_response['Body'].read.return_value = b's3 content'
        mock_s3_client.get_object.return_value = mock_response
        mock_boto_client.return_value = mock_s3_client
        
        loader = FileLoader()  # No S3 client provided
        
        content = loader.read_file('s3://my-bucket/file.json')
        
        assert content == b's3 content'
        mock_boto_client.assert_called_once_with('s3')
        mock_s3_client.get_object.assert_called_once_with(
            Bucket='my-bucket',
            Key='file.json'
        )
    
    def test_read_file_error_handling_s3(self):
        """Test error handling for invalid S3 paths."""
        mock_s3_client = Mock()
        mock_s3_client.get_object.side_effect = Exception("S3 error")
        
        loader = FileLoader(s3_client=mock_s3_client)
        
        with pytest.raises(Exception, match="S3 error"):
            loader.read_file('s3://my-bucket/nonexistent.json')

    def test_read_file_no_such_key_gives_friendly_message(self):
        """Test that NoSuchKey errors produce a friendly message about path depth."""
        from botocore.exceptions import ClientError
        from python_modules.shared.bulk_executor_error import BulkExecutorError
        mock_s3_client = Mock()
        error_response = {'Error': {'Code': 'NoSuchKey', 'Message': 'The specified key does not exist.'}}
        mock_s3_client.get_object.side_effect = ClientError(error_response, 'GetObject')
        mock_s3_client.exceptions.NoSuchKey = type(mock_s3_client.get_object.side_effect)

        loader = FileLoader(s3_client=mock_s3_client)

        with pytest.raises(BulkExecutorError, match="File not found.*ensure the path depth"):
            loader.read_file('s3://my-bucket/wrong/path/manifest-files.json')

    def test_read_file_unanticipated_client_error_propagates(self):
        """A ClientError we do not claim to understand is re-raised unchanged.

        NoSuchKey and AccessDenied are wrapped as BulkExecutorError, because both are
        ordinary mistakes with a sentence worth reading (see TestFileLoaderS3Errors).
        Anything else keeps its type, so it reaches the user as a bug with a traceback.
        """
        from botocore.exceptions import ClientError
        mock_s3_client = Mock()
        error_response = {
            'Error': {'Code': 'InternalError', 'Message': 'We encountered an internal error'}
        }
        original_error = ClientError(error_response, 'GetObject')
        mock_s3_client.get_object.side_effect = original_error

        loader = FileLoader(s3_client=mock_s3_client)

        with pytest.raises(ClientError) as exc_info:
            loader.read_file('s3://my-bucket/forbidden/key.json')
        # The original ClientError is propagated unchanged.
        assert exc_info.value is original_error
        assert exc_info.value.response['Error']['Code'] == 'InternalError'

    def test_read_file_throttling_client_error_propagates(self):
        """A throttling-style ClientError also bypasses the NoSuchKey wrapping."""
        from botocore.exceptions import ClientError
        mock_s3_client = Mock()
        error_response = {
            'Error': {'Code': 'SlowDown', 'Message': 'Please reduce your request rate.'}
        }
        mock_s3_client.get_object.side_effect = ClientError(error_response, 'GetObject')

        loader = FileLoader(s3_client=mock_s3_client)

        with pytest.raises(ClientError, match='SlowDown'):
            loader.read_file('s3://my-bucket/some/key.json')
