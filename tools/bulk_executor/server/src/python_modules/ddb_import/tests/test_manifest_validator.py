"""Unit tests for ManifestValidator."""

import pytest
from unittest.mock import Mock, patch
from ..validators.manifest_validator import ManifestValidator


class TestManifestValidator:
    """Test suite for ManifestValidator class."""
    
    @pytest.fixture
    def mock_path_resolver(self):
        """Mock ExportPathResolver for testing."""
        def create_mock_resolver(bucket, export_id, prefix=""):
            mock_resolver = Mock()
            if prefix:
                mock_resolver.get_manifest_base_path.return_value = f"s3://{bucket}/{prefix}/AWSDynamoDB/{export_id}"
            else:
                mock_resolver.get_manifest_base_path.return_value = f"s3://{bucket}/AWSDynamoDB/{export_id}"
            return mock_resolver
        return create_mock_resolver

    def test_successful_manifest_validation_with_valid_files(self, mock_path_resolver):
        """Test successful manifest validation with valid files."""
        # Mock file loader
        mock_file_loader = Mock()
        
        # Mock manifest-summary.json content
        manifest_summary_content = b'''{
            "version": "2020-06-30",
            "exportArn": "arn:aws:dynamodb:us-west-1:123456789:table/test_table/export/01716790307109-5f9d6aaa",
            "tableArn": "arn:aws:dynamodb:us-west-1:123456789:table/test_table",
            "itemCount": 100,
            "outputFormat": "DYNAMODB_JSON",
            "manifestFilesS3Key": "AWSDynamoDB/01716790307109-5f9d6aaa/manifest-files.json"
        }'''
        
        # Mock manifest-files.json content (newline-delimited JSON)
        manifest_files_content = b'''{"itemCount":50,"md5Checksum":"abc123==","etag":"etag1","dataFileS3Key":"data/file1.json.gz"}
{"itemCount":50,"md5Checksum":"def456==","etag":"etag2","dataFileS3Key":"data/file2.json.gz"}'''
        
        # Mock MD5 files
        manifest_summary_md5 = b"33ETZy9Dzbqz0OjfAREuPQ=="
        manifest_files_md5 = b"9T4Oo/cMh0ZjqQgSFpMNoA=="
        
        # Setup mock file loader to return appropriate content
        def mock_read_file(path):
            if 'manifest-summary.json' in path:
                return manifest_summary_content
            elif 'manifest-files.json' in path:
                return manifest_files_content
            elif 'manifest-summary.md5' in path:
                return manifest_summary_md5
            elif 'manifest-files.md5' in path:
                return manifest_files_md5
            elif 'file1.json.gz' in path or 'file2.json.gz' in path:
                return b"mock data file content"
            raise ValueError(f"Unexpected path: {path}")
        
        mock_file_loader.read_file.side_effect = mock_read_file
        mock_file_loader.join_path.side_effect = lambda base, *parts: f"{base}/{'/'.join(parts)}"
        
        # Mock MD5 validator to always pass
        with patch('server.src.python_modules.ddb_import.validators.manifest_validator.MD5Validator') as mock_md5_class:
            mock_md5_class.validate_file_checksum.return_value = True
            mock_md5_class.validate_file_checksum.return_value = True
            
            validator = ManifestValidator(mock_file_loader)
            path_resolver = mock_path_resolver('test-bucket', '01716790307109-5f9d6aaa', 'prefix')
            result = validator.validate_and_parse_manifests(path_resolver)
            
            # Verify result structure
            assert result['total_item_count'] == 100
            assert result['output_format'] == 'DYNAMODB_JSON'
            assert len(result['data_files']) == 2
            assert result['data_files'][0]['itemCount'] == 50
            assert result['data_files'][1]['itemCount'] == 50
    
    def test_md5_mismatch_for_manifest_summary(self, mock_path_resolver):
        """Test MD5 mismatch for manifest-summary.json."""
        mock_file_loader = Mock()
        
        manifest_summary_content = b'{"itemCount": 100, "outputFormat": "DYNAMODB_JSON"}'
        manifest_summary_md5 = b"wrongchecksum=="
        
        def mock_read_file(path):
            if 'manifest-summary.json' in path:
                return manifest_summary_content
            elif 'manifest-summary.md5' in path:
                return manifest_summary_md5
            raise ValueError(f"Unexpected path: {path}")
        
        mock_file_loader.read_file.side_effect = mock_read_file
        mock_file_loader.join_path.side_effect = lambda base, *parts: f"{base}/{'/'.join(parts)}"
        
        # Mock MD5 validator to raise error for manifest-summary
        with patch('server.src.python_modules.ddb_import.validators.manifest_validator.MD5Validator') as mock_md5:
            mock_md5.validate_file_checksum.side_effect = ValueError("MD5 checksum mismatch")
            
            validator = ManifestValidator(mock_file_loader)
            path_resolver = mock_path_resolver('test-bucket', '01716790307109-5f9d6aaa')
            
            with pytest.raises(ValueError) as exc_info:
                validator.validate_and_parse_manifests(path_resolver)
            
            assert "MD5 checksum mismatch" in str(exc_info.value)
    
    def test_md5_mismatch_for_manifest_files(self, mock_path_resolver):
        """Test MD5 mismatch for manifest-files.json."""
        mock_file_loader = Mock()
        
        manifest_summary_content = b'{"itemCount": 100, "tableArn": "arn:aws:dynamodb:us-west-1:123456789:table/test_table", "outputFormat": "DYNAMODB_JSON", "manifestFilesS3Key": "manifest-files.json"}'
        manifest_summary_md5 = b"validchecksum=="
        manifest_files_content = b'{"itemCount":100,"md5Checksum":"abc==","dataFileS3Key":"file.json.gz"}'
        manifest_files_md5 = b"wrongchecksum=="
        
        def mock_read_file(path):
            if 'manifest-summary.json' in path:
                return manifest_summary_content
            elif 'manifest-summary.md5' in path:
                return manifest_summary_md5
            elif 'manifest-files.json' in path:
                return manifest_files_content
            elif 'manifest-files.md5' in path:
                return manifest_files_md5
            raise ValueError(f"Unexpected path: {path}")
        
        mock_file_loader.read_file.side_effect = mock_read_file
        mock_file_loader.join_path.side_effect = lambda base, *parts: f"{base}/{'/'.join(parts)}"
        
        # Mock MD5 validator to pass for summary, fail for files
        call_count = [0]
        def mock_validate(content, expected):
            call_count[0] += 1
            if call_count[0] == 1:  # First call (manifest-summary)
                return True
            else:  # Second call (manifest-files)
                raise ValueError("MD5 checksum mismatch")
        
        with patch('server.src.python_modules.ddb_import.validators.manifest_validator.MD5Validator') as mock_md5:
            mock_md5.validate_file_checksum.side_effect = mock_validate
            
            validator = ManifestValidator(mock_file_loader)
            path_resolver = mock_path_resolver('test-bucket', '01716790307109-5f9d6aaa')
            
            with pytest.raises(ValueError) as exc_info:
                validator.validate_and_parse_manifests(path_resolver)
            
            assert "MD5 checksum mismatch" in str(exc_info.value)

    def test_invalid_output_format(self, mock_path_resolver):
        """Test invalid outputFormat."""
        mock_file_loader = Mock()
        
        # Manifest with invalid output format
        manifest_summary_content = b'''{
            "itemCount": 100,
            "tableArn": "arn:aws:dynamodb:us-west-1:123456789:table/test_table",
            "outputFormat": "ION",
            "manifestFilesS3Key": "manifest-files.json"
        }'''
        manifest_summary_md5 = b"3EAivRmNbolehaBnIOAVuw=="
        manifest_files_content = b'{"itemCount":100,"md5Checksum":"abc==","dataFileS3Key":"file.json.gz"}'
        manifest_files_md5 = b"aneAu/VLWalqMluGVeBF/w=="
        
        def mock_read_file(path):
            if 'manifest-summary.json' in path:
                return manifest_summary_content
            elif 'manifest-summary.md5' in path:
                return manifest_summary_md5
            elif 'manifest-files.json' in path:
                return manifest_files_content
            elif 'manifest-files.md5' in path:
                return manifest_files_md5
            raise ValueError(f"Unexpected path: {path}")
        
        mock_file_loader.read_file.side_effect = mock_read_file
        mock_file_loader.join_path.side_effect = lambda base, *parts: f"{base}/{'/'.join(parts)}"
        
        with patch('server.src.python_modules.ddb_import.validators.manifest_validator.MD5Validator') as mock_md5:
            mock_md5.validate_file_checksum.return_value = True
            
            validator = ManifestValidator(mock_file_loader)
            path_resolver = mock_path_resolver('test-bucket', '01716790307109-5f9d6aaa')
            
            with pytest.raises(ValueError) as exc_info:
                validator.validate_and_parse_manifests(path_resolver)
            
            error_message = str(exc_info.value)
            assert "outputFormat" in error_message or "format" in error_message.lower()
            assert "ION" in error_message

    def test_item_count_mismatch_between_manifests(self, mock_path_resolver):
        """Test item count mismatch between manifests."""
        mock_file_loader = Mock()
        
        # Manifest summary says 100 items
        manifest_summary_content = b'''{
            "itemCount": 100,
            "tableArn": "arn:aws:dynamodb:us-west-1:123456789:table/test_table",
            "outputFormat": "DYNAMODB_JSON",
            "manifestFilesS3Key": "manifest-files.json"
        }'''
        
        # But manifest files sum to 150 items (50 + 100)
        manifest_files_content = b'''{"itemCount":50,"md5Checksum":"abc==","dataFileS3Key":"file1.json.gz"}
{"itemCount":100,"md5Checksum":"def==","dataFileS3Key":"file2.json.gz"}'''
        
        manifest_summary_md5 = b"BXjh97lJfC/nXLYYJHYvHg=="
        manifest_files_md5 = b"xCaTCyiouoKQzcaV+s6b4w=="
        
        def mock_read_file(path):
            if 'manifest-summary.json' in path:
                return manifest_summary_content
            elif 'manifest-summary.md5' in path:
                return manifest_summary_md5
            elif 'manifest-files.json' in path:
                return manifest_files_content
            elif 'manifest-files.md5' in path:
                return manifest_files_md5
            elif 'file1.json.gz' in path or 'file2.json.gz' in path:
                return b"mock data file content"
            raise ValueError(f"Unexpected path: {path}")
        
        mock_file_loader.read_file.side_effect = mock_read_file
        mock_file_loader.join_path.side_effect = lambda base, *parts: f"{base}/{'/'.join(parts)}"
        
        with patch('server.src.python_modules.ddb_import.validators.manifest_validator.MD5Validator') as mock_md5:
            mock_md5.validate_file_checksum.return_value = True
            
            validator = ManifestValidator(mock_file_loader)
            path_resolver = mock_path_resolver('test-bucket', '01716790307109-5f9d6aaa')
            
            with pytest.raises(ValueError) as exc_info:
                validator.validate_and_parse_manifests(path_resolver)
            
            error_message = str(exc_info.value)
            assert "item count" in error_message.lower() or "count" in error_message.lower()
            assert "100" in error_message
            assert "150" in error_message
    
    def test_malformed_json_in_manifest_summary(self, mock_path_resolver):
        """Test malformed JSON in manifest-summary.json."""
        mock_file_loader = Mock()
        
        # Invalid JSON
        manifest_summary_content = b'{"itemCount": 100, invalid json}'
        manifest_summary_md5 = b"validchecksum=="
        
        def mock_read_file(path):
            if 'manifest-summary.json' in path:
                return manifest_summary_content
            elif 'manifest-summary.md5' in path:
                return manifest_summary_md5
            raise ValueError(f"Unexpected path: {path}")
        
        mock_file_loader.read_file.side_effect = mock_read_file
        mock_file_loader.join_path.side_effect = lambda base, *parts: f"{base}/{'/'.join(parts)}"
        
        with patch('server.src.python_modules.ddb_import.validators.manifest_validator.MD5Validator') as mock_md5:
            mock_md5.validate_file_checksum.return_value = True
            
            validator = ManifestValidator(mock_file_loader)
            path_resolver = mock_path_resolver('test-bucket', '01716790307109-5f9d6aaa')
            
            with pytest.raises(Exception):  # Could be ValueError or JSONDecodeError
                validator.validate_and_parse_manifests(path_resolver)
    
    def test_malformed_json_in_manifest_files(self, mock_path_resolver):
        """Test malformed JSON in manifest-files.json."""
        mock_file_loader = Mock()
        
        manifest_summary_content = b'''{
            "itemCount": 100,
            "outputFormat": "DYNAMODB_JSON",
            "manifestFilesS3Key": "manifest-files.json"
        }'''
        
        # Invalid newline-delimited JSON (second line is malformed)
        manifest_files_content = b'''{"itemCount":50,"md5Checksum":"abc==","dataFileS3Key":"file1.json.gz"}
{invalid json line}'''
        
        manifest_summary_md5 = b"validchecksum=="
        manifest_files_md5 = b"validchecksum2=="
        
        def mock_read_file(path):
            if 'manifest-summary.json' in path:
                return manifest_summary_content
            elif 'manifest-summary.md5' in path:
                return manifest_summary_md5
            elif 'manifest-files.json' in path:
                return manifest_files_content
            elif 'manifest-files.md5' in path:
                return manifest_files_md5
            raise ValueError(f"Unexpected path: {path}")
        
        mock_file_loader.read_file.side_effect = mock_read_file
        mock_file_loader.join_path.side_effect = lambda base, *parts: f"{base}/{'/'.join(parts)}"
        
        with patch('server.src.python_modules.ddb_import.validators.manifest_validator.MD5Validator') as mock_md5:
            mock_md5.validate_file_checksum.return_value = True
            
            validator = ManifestValidator(mock_file_loader)
            path_resolver = mock_path_resolver('test-bucket', '01716790307109-5f9d6aaa')
            
            with pytest.raises(Exception):  # Could be ValueError or JSONDecodeError
                validator.validate_and_parse_manifests(path_resolver)
    
    def test_missing_manifest_summary_json(self, mock_path_resolver):
        """Test missing manifest-summary.json file."""
        mock_file_loader = Mock()
        
        def mock_read_file(path):
            if 'manifest-summary.json' in path:
                raise FileNotFoundError(f"File not found: {path}")
            raise ValueError(f"Unexpected path: {path}")
        
        mock_file_loader.read_file.side_effect = mock_read_file
        mock_file_loader.join_path.side_effect = lambda base, *parts: f"{base}/{'/'.join(parts)}"
        
        validator = ManifestValidator(mock_file_loader)
        path_resolver = mock_path_resolver('test-bucket', '01716790307109-5f9d6aaa')
        
        with pytest.raises(FileNotFoundError) as exc_info:
            validator.validate_and_parse_manifests(path_resolver)
        
        assert "manifest-summary.json" in str(exc_info.value)
    
    def test_missing_manifest_summary_md5(self, mock_path_resolver):
        """Test missing manifest-summary.md5 file."""
        mock_file_loader = Mock()
        
        manifest_summary_content = b'{"itemCount": 100, "outputFormat": "DYNAMODB_JSON"}'
        
        def mock_read_file(path):
            if 'manifest-summary.json' in path:
                return manifest_summary_content
            elif 'manifest-summary.md5' in path:
                raise FileNotFoundError(f"File not found: {path}")
            raise ValueError(f"Unexpected path: {path}")
        
        mock_file_loader.read_file.side_effect = mock_read_file
        mock_file_loader.join_path.side_effect = lambda base, *parts: f"{base}/{'/'.join(parts)}"
        
        validator = ManifestValidator(mock_file_loader)
        path_resolver = mock_path_resolver('test-bucket', '01716790307109-5f9d6aaa')
        
        with pytest.raises(FileNotFoundError) as exc_info:
            validator.validate_and_parse_manifests(path_resolver)
        
        assert "manifest-summary.md5" in str(exc_info.value)
    
    def test_missing_manifest_files_json(self, mock_path_resolver):
        """Test missing manifest-files.json file."""
        mock_file_loader = Mock()
        
        manifest_summary_content = b'''{
            "itemCount": 100,
            "tableArn": "arn:aws:dynamodb:us-west-1:123456789:table/test_table",
            "outputFormat": "DYNAMODB_JSON",
            "manifestFilesS3Key": "manifest-files.json"
        }'''
        manifest_summary_md5 = b"validchecksum=="
        
        def mock_read_file(path):
            if 'manifest-summary.json' in path:
                return manifest_summary_content
            elif 'manifest-summary.md5' in path:
                return manifest_summary_md5
            elif 'manifest-files.json' in path:
                raise FileNotFoundError(f"File not found: {path}")
            raise ValueError(f"Unexpected path: {path}")
        
        mock_file_loader.read_file.side_effect = mock_read_file
        mock_file_loader.join_path.side_effect = lambda base, *parts: f"{base}/{'/'.join(parts)}"
        
        with patch('server.src.python_modules.ddb_import.validators.manifest_validator.MD5Validator') as mock_md5:
            mock_md5.validate_file_checksum.return_value = True
            
            validator = ManifestValidator(mock_file_loader)
            path_resolver = mock_path_resolver('test-bucket', '01716790307109-5f9d6aaa')
            
            with pytest.raises(FileNotFoundError) as exc_info:
                validator.validate_and_parse_manifests(path_resolver)
            
            assert "manifest-files.json" in str(exc_info.value)
    
    def test_missing_manifest_files_md5(self, mock_path_resolver):
        """Test missing manifest-files.md5 file."""
        mock_file_loader = Mock()
        
        manifest_summary_content = b'''{
            "itemCount": 100,
            "tableArn": "arn:aws:dynamodb:us-west-1:123456789:table/test_table",
            "outputFormat": "DYNAMODB_JSON",
            "manifestFilesS3Key": "manifest-files.json"
        }'''
        manifest_summary_md5 = b"validchecksum=="
        manifest_files_content = b'{"itemCount":100,"md5Checksum":"abc==","dataFileS3Key":"file.json.gz"}'
        
        def mock_read_file(path):
            if 'manifest-summary.json' in path:
                return manifest_summary_content
            elif 'manifest-summary.md5' in path:
                return manifest_summary_md5
            elif 'manifest-files.json' in path:
                return manifest_files_content
            elif 'manifest-files.md5' in path:
                raise FileNotFoundError(f"File not found: {path}")
            raise ValueError(f"Unexpected path: {path}")
        
        mock_file_loader.read_file.side_effect = mock_read_file
        mock_file_loader.join_path.side_effect = lambda base, *parts: f"{base}/{'/'.join(parts)}"
        
        with patch('server.src.python_modules.ddb_import.validators.manifest_validator.MD5Validator') as mock_md5:
            mock_md5.validate_file_checksum.return_value = True
            
            validator = ManifestValidator(mock_file_loader)
            path_resolver = mock_path_resolver('test-bucket', '01716790307109-5f9d6aaa')
            
            with pytest.raises(FileNotFoundError) as exc_info:
                validator.validate_and_parse_manifests(path_resolver)
            
            assert "manifest-files.md5" in str(exc_info.value)
