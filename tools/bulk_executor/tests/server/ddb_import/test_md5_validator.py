"""Unit tests for MD5Validator."""

import pytest
from unittest.mock import Mock, MagicMock, patch
from python_modules.ddb_import.validators.md5_validator import MD5Validator


class TestMD5Validator:
    """Test suite for MD5Validator class."""
    
    def test_compute_md5_produces_correct_checksum(self):
        """Test that compute_md5() produces correct base64-encoded MD5 checksums."""
        # Test with simple content
        content = b"test content"
        checksum = MD5Validator.compute_md5(content)
        
        # Verify it's a valid base64 string
        assert isinstance(checksum, str)
        assert len(checksum) > 0
        
        # Test with known content and expected checksum
        # "hello world" -> 5eb63bbbe01eeed093cb22bb8f5acdc3 (hex) -> XrY7u+Ae7tCTyyK7j1rNww== (base64)
        known_content = b"hello world"
        expected_checksum = "XrY7u+Ae7tCTyyK7j1rNww=="
        actual_checksum = MD5Validator.compute_md5(known_content)
        assert actual_checksum == expected_checksum
    
    def test_compute_md5_with_empty_content(self):
        """Test compute_md5() with empty content."""
        # Empty content should produce a valid checksum
        # MD5 of empty string: d41d8cd98f00b204e9800998ecf8427e (hex) -> 1B2M2Y8AsgTpgAmY7PhCfg== (base64)
        content = b""
        expected_checksum = "1B2M2Y8AsgTpgAmY7PhCfg=="
        actual_checksum = MD5Validator.compute_md5(content)
        assert actual_checksum == expected_checksum
    
    def test_compute_md5_with_large_content(self):
        """Test compute_md5() with large content."""
        # Test with 1MB of data
        large_content = b"x" * (1024 * 1024)
        checksum = MD5Validator.compute_md5(large_content)
        
        # Verify it produces a valid checksum
        assert isinstance(checksum, str)
        assert len(checksum) > 0
        
        # Verify consistency - same content should produce same checksum
        checksum2 = MD5Validator.compute_md5(large_content)
        assert checksum == checksum2
    
    def test_compute_md5_with_various_content(self):
        """Test compute_md5() with various file contents."""
        test_cases = [
            b"simple text",
            b"123456789",
            b"special chars: !@#$%^&*()",
            b"\n\r\t",
            b"unicode: \xc3\xa9\xc3\xa0\xc3\xbc",  # UTF-8 encoded unicode
        ]
        
        for content in test_cases:
            checksum = MD5Validator.compute_md5(content)
            # Verify each produces a valid base64 string
            assert isinstance(checksum, str)
            assert len(checksum) > 0
            
            # Verify consistency
            checksum2 = MD5Validator.compute_md5(content)
            assert checksum == checksum2
    
    def test_validate_file_checksum_with_matching_checksums(self):
        """Test validate_file_checksum() returns True when checksums match."""
        content = b"test content"
        expected_md5 = MD5Validator.compute_md5(content)
        
        # Should return True and not raise exception
        result = MD5Validator.validate_file_checksum(content, expected_md5)
        assert result is True
    
    def test_validate_file_checksum_raises_error_for_mismatch(self):
        """Test validate_file_checksum() raises ValueError when checksums don't match."""
        content = b"test content"
        wrong_md5 = "wrong_checksum_value=="
        
        # Should raise ValueError with descriptive message
        with pytest.raises(ValueError) as exc_info:
            MD5Validator.validate_file_checksum(content, wrong_md5)
        
        # Verify error message contains both checksums
        error_message = str(exc_info.value)
        assert "MD5 checksum mismatch" in error_message
        assert "Expected:" in error_message
        assert "Actual:" in error_message
        assert wrong_md5 in error_message
    
    def test_validate_file_checksum_with_different_content_same_checksum(self):
        """Test that different content with same expected checksum fails validation."""
        content1 = b"content one"
        content2 = b"content two"
        
        checksum1 = MD5Validator.compute_md5(content1)
        
        # Validating content2 with content1's checksum should fail
        with pytest.raises(ValueError):
            MD5Validator.validate_file_checksum(content2, checksum1)
    
    def test_validate_file_checksum_with_various_sizes(self):
        """Test validate_file_checksum() with various file sizes."""
        test_sizes = [
            0,           # Empty
            1,           # 1 byte
            100,         # Small
            1024,        # 1KB
            10240,       # 10KB
            1024 * 100,  # 100KB
        ]
        
        for size in test_sizes:
            content = b"x" * size
            expected_md5 = MD5Validator.compute_md5(content)
            
            # Should validate successfully
            result = MD5Validator.validate_file_checksum(content, expected_md5)
            assert result is True
