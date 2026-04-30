"""MD5 checksum validation for DynamoDB export files."""

import hashlib
import base64

class MD5Validator:
    """Validates MD5 checksums for manifest and data files."""
    
    @staticmethod
    def compute_md5(file_content: bytes) -> str:
        """
        Compute MD5 checksum of file content.
        
        Args:
            file_content: Raw bytes of the file
            
        Returns:
            Base64-encoded MD5 checksum
        """
        md5_hash = hashlib.md5(file_content)
        return base64.b64encode(md5_hash.digest()).decode('utf-8')
    
    @staticmethod
    def validate_file_checksum(file_content: bytes, expected_md5: str) -> bool:
        """
        Validate file content against expected MD5 checksum.
        
        Args:
            file_content: Raw bytes of the file
            expected_md5: Expected MD5 checksum (base64-encoded)
            
        Returns:
            True if checksums match
            
        Raises:
            ValueError: If checksums don't match
        """
        actual_md5 = MD5Validator.compute_md5(file_content)
        
        if actual_md5 != expected_md5:
            raise ValueError(
                f"MD5 checksum mismatch. Expected: {expected_md5}, Actual: {actual_md5}"
            )
        
        return True
