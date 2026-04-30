"""S3 path validation for checking bucket and prefix existence."""

from ...shared.logger import log
from ..utils.export_path_resolver import ExportPathResolver

class S3Validator:
    """Validates S3 paths before load operations."""
    
    def __init__(self, s3_client):
        """
        Initialize with a boto3 S3 client.
        
        Args:
            s3_client: boto3 S3 client instance
        """
        self.s3_client = s3_client
    
    def validate_path_exists(self, export_path_resolver: ExportPathResolver) -> bool:
        """
        Check if the S3 path exists and contains objects.
        
        Args:
            export_path_resolver: Export path resolver object
            
        Returns:
            True if path exists and contains objects
            
        Raises:
            ValueError: If path doesn't exist, is empty, or access is denied
        """
        # Parse the S3 path to extract bucket and prefix
        # Format: s3://bucket/prefix/path/
        s3_path = export_path_resolver.get_data_base_path()
        if not s3_path.startswith('s3://'):
            error_msg = f"Invalid S3 path format: {s3_path}. Must start with 's3://'"
            log.error(error_msg)
            raise ValueError(error_msg)

        bucket_name = export_path_resolver.get_bucket()
        prefix_path = export_path_resolver.get_prefix()
        
        # Ensure trailing slash for directory-like behavior
        if prefix_path and not prefix_path.endswith("/"):
            prefix_path = prefix_path + "/"
        
        try:
            # Check if the path exists by listing objects with the prefix
            response = self.s3_client.list_objects_v2(
                Bucket=bucket_name,
                Prefix=prefix_path,
                MaxKeys=1
            )
            
            if 'Contents' not in response or len(response['Contents']) == 0:
                error_msg = (
                    f"S3 path does not exist or is empty: {s3_path}\n"
                    f"Please verify:\n"
                    f"  - Bucket '{bucket_name}' exists and is accessible\n"
                    f"  - Path '{prefix_path}' contains files\n"
                    f"  - AWS credentials have s3:ListBucket permission"
                )
                log.error(error_msg)
                raise ValueError(error_msg)
            
            # Path exists and contains objects
            success_msg = f"S3 path validation successful: {s3_path}"
            log.info(success_msg)
            return True
            
        except ValueError:
            # Re-raise ValueError from our own checks
            raise
        except Exception as e:
            # Check for specific error types
            error_class = e.__class__.__name__
            error_str = str(e)
            
            if error_class == 'NoSuchBucket':
                error_msg = (
                    f"S3 bucket does not exist: {bucket_name}\n"
                    f"Please verify the bucket name is correct and accessible"
                )
                log.error(error_msg)
                raise ValueError(error_msg)
            
            if "AccessDenied" in error_str or "403" in error_str:
                error_msg = (
                    f"Access denied to S3 path: {s3_path}\n"
                    f"Please verify AWS credentials have s3:ListBucket and s3:GetObject permissions"
                )
                log.error(error_msg)
                raise ValueError(error_msg)
            
            # Handle other unexpected errors
            error_msg = f"Error validating S3 path '{s3_path}': {error_str}"
            log.error(error_msg)
            raise ValueError(error_msg)
