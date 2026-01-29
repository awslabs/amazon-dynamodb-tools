"""Path resolver for DynamoDB export S3 locations."""

class ExportPathResolver:
    """Resolves S3 paths for DynamoDB export manifests and data files."""
    
    AWS_DYNAMODB_PREFIX = "AWSDynamoDB"
    
    def __init__(self, bucket: str, export_id: str, prefix: str = ""):
        """
        Initialize path resolver.
        
        Args:
            bucket: S3 bucket name
            export_id: DynamoDB export ID
            prefix: Optional S3 prefix (default: "")
        """
        self.bucket = bucket
        self.export_id = export_id
        self.prefix = prefix.strip("/") if prefix else ""

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
