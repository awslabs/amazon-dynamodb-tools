"""File loader utility for reading files from S3 or local filesystem."""

import os
from typing import Optional, Tuple
import boto3

class FileLoader:
    """Unified file loader for S3 and local filesystem."""
    
    def __init__(self, s3_client=None):
        """
        Initialize file loader.
        
        Args:
            s3_client: Optional boto3 S3 client. If None, will be created when needed.
        """
        self._s3_client = s3_client
    
    def is_s3_path(self, path: str) -> bool:
        """
        Check if path is an S3 path.
        
        Args:
            path: Path to check
            
        Returns:
            True if path starts with 's3://'
        """
        return path.startswith('s3://')
    
    def parse_s3_path(self, s3_path: str) -> Tuple[str, str]:
        """
        Parse S3 path into bucket and key.
        
        Args:
            s3_path: Full S3 path (s3://bucket/key)
            
        Returns:
            Tuple of (bucket, key)
            
        Raises:
            ValueError: If path is not a valid S3 path
        """
        if not self.is_s3_path(s3_path):
            raise ValueError(f"Invalid S3 path: {s3_path}")
        
        # Remove 's3://' prefix
        path_without_prefix = s3_path[5:]
        
        # Split into bucket and key
        parts = path_without_prefix.split('/', 1)
        if len(parts) < 1 or not parts[0]:
            raise ValueError(f"Invalid S3 path format: {s3_path}")
        
        bucket = parts[0]
        key = parts[1] if len(parts) > 1 else ''
        
        return bucket, key
    
    def join_path(self, base_path: str, *parts: str) -> str:
        """
        Join path components, handling both S3.
        
        Args:
            base_path: Base path (S3)
            parts: Path components to join
            
        Returns:
            Joined path with appropriate separator
            
        Example:
            join_path('s3://bucket/export', 'manifest-summary.json')
            # Returns: 's3://bucket/export/manifest-summary.json'
        """
        if self.is_s3_path(base_path):
            # For S3 paths, use forward slash
            # Ensure base_path doesn't end with slash
            base = base_path.rstrip('/')
            # Join with forward slashes
            for part in parts:
                base = base + '/' + part.lstrip('/')
            return base
        else:
            # For local paths, use os.path.join
            return os.path.join(base_path, *parts)
    
    def read_file(self, path: str) -> bytes:
        """
        Read file content from S3.
        
        Args:
            path: File path. Reads from S3.
            
        Returns:
            File content as bytes
            
        Example:
            loader.read_file('s3://bucket/key/file.json')  # Reads from S3
        """
        return self._read_from_s3(path)
    
    def _read_from_s3(self, s3_path: str) -> bytes:
        """Read file from S3."""
        if self._s3_client is None:
            self._s3_client = boto3.client('s3')
        
        bucket, key = self.parse_s3_path(s3_path)
        response = self._s3_client.get_object(Bucket=bucket, Key=key)
        return response['Body'].read()
