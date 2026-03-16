"""Unit tests for DataFileValidator (checksum validation only)."""
import pytest
from unittest.mock import Mock, patch
from ..validators.data_file_validator import DataFileValidator
from ..validators.md5_validator import MD5Validator


class TestDataFileValidatorChecksums:

    def _make_validator(self, file_loader=None):
        return DataFileValidator(file_loader or Mock())

    def test_validate_all_files_success(self):
        mock_file_loader = Mock()
        validator = self._make_validator(mock_file_loader)
        data_files = [
            {'dataFileS3Key': 'data/file1.gz', 'md5Checksum': 'checksum1'},
            {'dataFileS3Key': 'data/file2.gz', 'md5Checksum': 'checksum2'},
        ]
        mock_file_loader.join_path.side_effect = lambda base, key: f"{base}/{key}"
        mock_file_loader.read_file.side_effect = [b"content1", b"content2"]

        with patch.object(MD5Validator, 'validate_file_checksum', return_value=True):
            result = validator.validate(data_files, 's3://bucket', validate_all=True)

        assert result['validated_count'] == 2
        assert result['total_count'] == 2
        assert result['validation_mode'] == 'full'
        assert len(result['verified_files']) == 2

    def test_validate_sample_mode(self):
        mock_file_loader = Mock()
        validator = self._make_validator(mock_file_loader)
        data_files = [{'dataFileS3Key': f'data/file{i}.gz', 'md5Checksum': f'checksum{i}'} for i in range(10)]
        mock_file_loader.join_path.side_effect = lambda base, key: f"{base}/{key}"
        mock_file_loader.read_file.return_value = b"content"

        with patch.object(MD5Validator, 'validate_file_checksum', return_value=True):
            result = validator.validate(data_files, 's3://bucket', validate_all=False, sample_size=3)

        assert result['validated_count'] == 3
        assert result['total_count'] == 10
        assert result['validation_mode'] == 'sample'
        assert len(result['verified_files']) == 3

    def test_validate_empty_data_files(self):
        result = self._make_validator().validate([], 's3://bucket')
        assert result == {'validated_count': 0, 'total_count': 0, 'validation_mode': 'none', 'failed_files': [], 'verified_files': []}

    def test_validate_checksum_mismatch(self):
        mock_file_loader = Mock()
        validator = self._make_validator(mock_file_loader)
        data_files = [{'dataFileS3Key': 'data/file1.gz', 'md5Checksum': 'wrong'}]
        mock_file_loader.join_path.side_effect = lambda base, key: f"{base}/{key}"
        mock_file_loader.read_file.return_value = b"content"

        with patch.object(MD5Validator, 'validate_file_checksum', side_effect=ValueError("MD5 mismatch")):
            with pytest.raises(ValueError, match="Data file validation failed"):
                validator.validate(data_files, 's3://bucket', validate_all=True)

    def test_validate_missing_data_file_key(self):
        with pytest.raises(ValueError, match="Data file validation failed"):
            self._make_validator().validate([{'md5Checksum': 'checksum1'}], 's3://bucket', validate_all=True)

    def test_validate_missing_md5_checksum(self):
        with pytest.raises(ValueError, match="Data file validation failed"):
            self._make_validator().validate([{'dataFileS3Key': 'data/file1.gz'}], 's3://bucket', validate_all=True)

    def test_validate_sample_size_larger_than_total(self):
        mock_file_loader = Mock()
        validator = self._make_validator(mock_file_loader)
        data_files = [
            {'dataFileS3Key': 'data/file1.gz', 'md5Checksum': 'checksum1'},
            {'dataFileS3Key': 'data/file2.gz', 'md5Checksum': 'checksum2'},
        ]
        mock_file_loader.join_path.side_effect = lambda base, key: f"{base}/{key}"
        mock_file_loader.read_file.return_value = b"content"

        with patch.object(MD5Validator, 'validate_file_checksum', return_value=True):
            result = validator.validate(data_files, 's3://bucket', validate_all=False, sample_size=10)

        assert result['validated_count'] == 2
        assert result['validation_mode'] == 'sample'
        assert len(result['verified_files']) == 2
