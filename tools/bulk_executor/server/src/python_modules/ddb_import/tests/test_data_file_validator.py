"""Unit tests for DataFileValidator."""
import pytest
from unittest.mock import Mock, patch
from ..validators.data_file_validator import DataFileValidator
from ..validators.md5_validator import MD5Validator


class TestDataFileValidator:

    def test_validate_all_files_success(self):
        mock_file_loader = Mock()
        validator = DataFileValidator(mock_file_loader)
        data_files = [
            {'dataFileS3Key': 'data/file1.gz', 'md5Checksum': 'checksum1'},
            {'dataFileS3Key': 'data/file2.gz', 'md5Checksum': 'checksum2'},
        ]
        mock_file_loader.join_path.side_effect = lambda base, key: f"{base}/{key}"
        mock_file_loader.read_file.side_effect = [b"content1", b"content2"]

        with patch.object(MD5Validator, 'validate_file_checksum', return_value=True):
            result = validator.validate_data_file_checksums(data_files, 's3://bucket', validate_all=True)

        assert result == {'validated_count': 2, 'total_count': 2, 'validation_mode': 'full', 'failed_files': []}

    def test_validate_sample_mode(self):
        mock_file_loader = Mock()
        validator = DataFileValidator(mock_file_loader)
        data_files = [{'dataFileS3Key': f'data/file{i}.gz', 'md5Checksum': f'checksum{i}'} for i in range(10)]
        mock_file_loader.join_path.side_effect = lambda base, key: f"{base}/{key}"
        mock_file_loader.read_file.return_value = b"content"

        with patch.object(MD5Validator, 'validate_file_checksum', return_value=True):
            result = validator.validate_data_file_checksums(data_files, 's3://bucket', validate_all=False, sample_size=3)

        assert result['validated_count'] == 3
        assert result['total_count'] == 10
        assert result['validation_mode'] == 'sample'

    def test_validate_empty_data_files(self):
        validator = DataFileValidator(Mock())
        result = validator.validate_data_file_checksums([], 's3://bucket')
        assert result == {'validated_count': 0, 'total_count': 0, 'validation_mode': 'none', 'failed_files': []}

    def test_validate_checksum_mismatch(self):
        mock_file_loader = Mock()
        validator = DataFileValidator(mock_file_loader)
        data_files = [{'dataFileS3Key': 'data/file1.gz', 'md5Checksum': 'wrong'}]
        mock_file_loader.join_path.side_effect = lambda base, key: f"{base}/{key}"
        mock_file_loader.read_file.return_value = b"content"

        with patch.object(MD5Validator, 'validate_file_checksum', side_effect=ValueError("MD5 mismatch")):
            with pytest.raises(ValueError, match="Data file validation failed"):
                validator.validate_data_file_checksums(data_files, 's3://bucket', validate_all=True)

    def test_validate_missing_data_file_key(self):
        validator = DataFileValidator(Mock())
        data_files = [{'md5Checksum': 'checksum1'}]
        with pytest.raises(ValueError, match="Data file validation failed"):
            validator.validate_data_file_checksums(data_files, 's3://bucket', validate_all=True)

    def test_validate_missing_md5_checksum(self):
        validator = DataFileValidator(Mock())
        data_files = [{'dataFileS3Key': 'data/file1.gz'}]
        with pytest.raises(ValueError, match="Data file validation failed"):
            validator.validate_data_file_checksums(data_files, 's3://bucket', validate_all=True)

    def test_validate_sample_size_larger_than_total(self):
        mock_file_loader = Mock()
        validator = DataFileValidator(mock_file_loader)
        data_files = [
            {'dataFileS3Key': 'data/file1.gz', 'md5Checksum': 'checksum1'},
            {'dataFileS3Key': 'data/file2.gz', 'md5Checksum': 'checksum2'},
        ]
        mock_file_loader.join_path.side_effect = lambda base, key: f"{base}/{key}"
        mock_file_loader.read_file.return_value = b"content"

        with patch.object(MD5Validator, 'validate_file_checksum', return_value=True):
            result = validator.validate_data_file_checksums(data_files, 's3://bucket', validate_all=False, sample_size=10)

        assert result['validated_count'] == 2
        assert result['total_count'] == 2
        assert result['validation_mode'] == 'sample'
