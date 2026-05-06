"""Unit tests for validate_s3_path and validate_s3_export_path."""

import pytest
from utils import validate_s3_path, validate_s3_export_path


class TestValidateS3Path:

    def test_valid_path(self):
        validate_s3_path("s3://my-bucket/some/path")

    def test_missing_s3_scheme(self):
        with pytest.raises(SystemExit, match="Must start with 's3://'"):
            validate_s3_path("https://my-bucket/path")

    def test_no_path_after_bucket(self):
        with pytest.raises(SystemExit, match="Expected"):
            validate_s3_path("s3://rubbish")

    def test_empty_bucket_name(self):
        with pytest.raises(SystemExit, match="Expected"):
            validate_s3_path("s3:///path")

    def test_plain_string(self):
        with pytest.raises(SystemExit, match="Must start with 's3://'"):
            validate_s3_path("not-an-s3-path")


class TestValidateS3ExportPath:

    def test_valid_export_path_with_prefix(self):
        validate_s3_export_path("s3://my-bucket/prefix/AWSDynamoDB/01716790307109-5f9d6aaa")

    def test_valid_export_path_without_prefix(self):
        validate_s3_export_path("s3://my-bucket/AWSDynamoDB/01716790307109-5f9d6aaa")

    def test_missing_awsdynamodb_segment(self):
        with pytest.raises(SystemExit, match="AWSDynamoDB"):
            validate_s3_export_path("s3://my-bucket/some/other/path")

    def test_inherits_base_validation(self):
        with pytest.raises(SystemExit, match="Must start with 's3://'"):
            validate_s3_export_path("not-an-s3-path")
