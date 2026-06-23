"""Unit tests for ExportPathResolver."""

import pytest
from python_modules.shared.export.utils.export_path_resolver import ExportPathResolver


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

    def test_parse_path_without_prefix_single_slash(self):
        """Test parsing S3 path without prefix using natural single-slash form."""
        path = "s3://unicornactiviti-data-export/AWSDynamoDB/01716790307109-5f9d6aaa/"
        resolver = ExportPathResolver(path)

        assert resolver.get_bucket() == "unicornactiviti-data-export"
        assert resolver.get_prefix() == ""
        assert resolver.get_export_id() == "01716790307109-5f9d6aaa"

    def test_parse_path_without_prefix_single_slash_no_trailing(self):
        """Test parsing S3 path without prefix, no trailing slash."""
        path = "s3://my-bucket/AWSDynamoDB/01716790307109-5f9d6aaa"
        resolver = ExportPathResolver(path)

        assert resolver.get_bucket() == "my-bucket"
        assert resolver.get_prefix() == ""
        assert resolver.get_export_id() == "01716790307109-5f9d6aaa"

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

    def test_get_base_path_returns_bucket_only(self):
        """get_base_path returns just s3://<bucket>, ignoring prefix and export id."""
        path = "s3://my-bucket/some/prefix/AWSDynamoDB/01716790307109-5f9d6aaa"
        resolver = ExportPathResolver(path)

        assert resolver.get_base_path() == "s3://my-bucket"

    def test_get_base_path_no_prefix(self):
        """get_base_path returns s3://<bucket> even when no prefix is present."""
        path = "s3://only-bucket/AWSDynamoDB/01716790307109-5f9d6aaa"
        resolver = ExportPathResolver(path)

        assert resolver.get_base_path() == "s3://only-bucket"

    def test_str_repr_includes_bucket_prefix_export_id(self):
        """__str__ should produce a debug-friendly summary of all three fields."""
        path = "s3://my-bucket/prod/data/AWSDynamoDB/01716790307109-5f9d6aaa"
        resolver = ExportPathResolver(path)

        rendered = str(resolver)

        assert "ExportPathResolver(" in rendered
        assert "bucket='my-bucket'" in rendered
        assert "prefix='prod/data'" in rendered
        assert "export_id='01716790307109-5f9d6aaa'" in rendered

    def test_str_repr_no_prefix(self):
        """__str__ shows empty prefix when none was supplied."""
        path = "s3://only-bucket/AWSDynamoDB/01716790307109-5f9d6aaa"
        resolver = ExportPathResolver(path)

        rendered = str(resolver)

        assert "bucket='only-bucket'" in rendered
        assert "prefix=''" in rendered
        assert "export_id='01716790307109-5f9d6aaa'" in rendered
