"""Path resolver for DynamoDB export S3 locations."""
import re

class ExportPathResolver:
    """Resolves S3 paths for DynamoDB export manifests and data files."""
    
    AWS_DYNAMODB_PREFIX = "AWSDynamoDB"
    
    def __init__(self, s3_path: str):
        """
        Initialize path resolver from S3 path.
        
        Args:
            s3_path: S3 path in format s3://bucket-name/prefix/AWSDynamoDB/export-id
                    or s3://bucket-name/AWSDynamoDB/export-id (prefix optional)
        
        Raises:
            ValueError: If path format is invalid
        """
        # Parse s3://bucket-name/prefix/AWSDynamoDB/export-id
        match = re.match(r'^s3://([^/]+)/(.+)$', s3_path)
        if not match:
            raise ValueError(f"Invalid S3 path format: {s3_path}. Expected: s3://bucket-name/prefix/AWSDynamoDB/export-id")
        
        self.bucket = match.group(1)
        path_parts = match.group(2)
        
        # Find AWSDynamoDB in the path
        if f"/{self.AWS_DYNAMODB_PREFIX}/" not in f"/{path_parts}":
            raise ValueError(f"Path must contain '/{self.AWS_DYNAMODB_PREFIX}/' segment: {s3_path}")
        
        # Split on AWSDynamoDB to get prefix and export_id
        prefix_part, export_part = f"/{path_parts}".split(f"/{self.AWS_DYNAMODB_PREFIX}/", 1)
        
        self.prefix = prefix_part.strip("/") if prefix_part else ""
        self.export_id = export_part.strip("/")
        self.export_id = export_part.strip("/")

    def get_bucket(self) -> str:
        return self.bucket

    def get_prefix(self) -> str:
        return self.prefix

    def get_export_id(self) -> str:
        return self.export_id
    
    def get_manifest_base_path(self) -> str:
        """
        Get the base path where manifest files are located.
        
        Returns:
            S3 path like s3://bucket/prefix/AWSDynamoDB/{export_id}
        """
        return f"{self.get_data_base_path()}/{self.AWS_DYNAMODB_PREFIX}/{self.export_id}"
    
    def get_data_base_path(self) -> str:
        """
        Get the base path for resolving data file keys from manifest.
        
        Returns:
            S3 path like s3://bucket/prefix
        """
        if self.prefix:
            return f"s3://{self.bucket}/{self.prefix}"
        else:
            return f"s3://{self.bucket}"

    def get_base_path(self) -> str:
        """
        Get the bucket path.

        Returns:
            S3 path like s3://bucket
        """
        return f"s3://{self.bucket}"

    def __str__(self):
        return f"ExportPathResolver(bucket='{self.bucket}', prefix='{self.prefix}', export_id='{self.export_id}')"
