"""Unit tests for ExportPathResolver."""

import pytest
from python_modules.ddb_import.utils.export_path_resolver import ExportPathResolver


class TestExportPathResolver:
    """Test cases for ExportPathResolver."""

    def test_parse_path_with_prefix(self):
        """Test parsing S3 path with prefix."""
        path = "s3://my-bucket/prod/data/AWSDynamoDB/01716790307109-5f9d6aaa"
        resolver = ExportPathResolver(path)
        
        assert resolver.get_bucket() == "my-bucket"
        assert resolver.get_prefix() == "prod/data"
        assert resolver.get_export_id() == "01716790307109-5f9d6aaa"

    def test_parse_path_without_prefix(self):
        """Test parsing S3 path without prefix - must have trailing slash after bucket."""
        path = "s3://my-bucket//AWSDynamoDB/01716790307109-5f9d6aaa"
        resolver = ExportPathResolver(path)
        
        assert resolver.get_bucket() == "my-bucket"
        assert resolver.get_prefix() == ""
        assert resolver.get_export_id() == "01716790307109-5f9d6aaa"

    def test_parse_path_with_trailing_slash(self):
        """Test parsing S3 path with trailing slash."""
        path = "s3://my-bucket/prod/AWSDynamoDB/01716790307109-5f9d6aaa/"
        resolver = ExportPathResolver(path)
        
        assert resolver.get_bucket() == "my-bucket"
        assert resolver.get_prefix() == "prod"
        assert resolver.get_export_id() == "01716790307109-5f9d6aaa"

    def test_invalid_path_missing_s3_prefix(self):
        """Test error on invalid path missing s3:// prefix."""
        with pytest.raises(ValueError, match="Invalid S3 path format"):
            ExportPathResolver("my-bucket/AWSDynamoDB/export-id")

    def test_invalid_path_missing_awsdynamodb(self):
        """Test error on path missing AWSDynamoDB segment."""
        with pytest.raises(ValueError, match="must contain '/AWSDynamoDB/' segment"):
            ExportPathResolver("s3://my-bucket/prod/export-id")

    def test_get_manifest_base_path(self):
        """Test manifest base path generation."""
        path = "s3://my-bucket/prod/AWSDynamoDB/01716790307109-5f9d6aaa"
        resolver = ExportPathResolver(path)
        
        expected = "s3://my-bucket/prod/AWSDynamoDB/01716790307109-5f9d6aaa"
        assert resolver.get_manifest_base_path() == expected

    def test_get_data_base_path_with_prefix(self):
        """Test data base path with prefix."""
        path = "s3://my-bucket/prod/AWSDynamoDB/export-id"
        resolver = ExportPathResolver(path)
        
        assert resolver.get_data_base_path() == "s3://my-bucket/prod"

    def test_get_data_base_path_without_prefix(self):
        """Test data base path without prefix."""
        path = "s3://my-bucket//AWSDynamoDB/export-id"
        resolver = ExportPathResolver(path)
        
        assert resolver.get_data_base_path() == "s3://my-bucket"
