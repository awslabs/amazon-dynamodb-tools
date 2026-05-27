"""Unit tests for KeySchemaValidator."""
import gzip
import json
import pytest
from unittest.mock import Mock
from python_modules.load_export.validators.key_schema_validator import KeySchemaValidator


PK_ONLY_SCHEMA = {'pk': {'name': 'name', 'type': 'S'}}
PK_SK_SCHEMA = {'pk': {'name': 'pk', 'type': 'S'}, 'sk': {'name': 'sk', 'type': 'N'}}


def _make_validator(lines, file_key='data/file1.json.gz', compress=True):
    mock_file_loader = Mock()
    mock_file_loader.join_path.side_effect = lambda base, key: f"{base}/{key}"
    content = '\n'.join(lines).encode('utf-8')
    mock_file_loader.read_file.return_value = gzip.compress(content) if compress else content
    validator = KeySchemaValidator(mock_file_loader)
    verified_files = [{'dataFileS3Key': file_key, 'md5Checksum': 'abc'}]
    return validator, verified_files


def _incremental_row(keys, old_image=None, new_image=None):
    row = {"Metadata": {"WriteTimestampMicros": {"N": "123"}}, "Keys": keys}
    if old_image:
        row["OldImage"] = old_image
    if new_image:
        row["NewImage"] = new_image
    return json.dumps(row)


class TestKeySchemaValidatorFullExport:

    def test_pk_only(self):
        lines = [json.dumps({"Item": {"name": {"S": "Alice"}, "age": {"N": "30"}}})]
        validator, files = _make_validator(lines)
        result = validator.validate(files, 's3://bucket', PK_ONLY_SCHEMA, 'FULL_EXPORT')
        assert result['validated_count'] == 1

    def test_pk_sk(self):
        lines = [json.dumps({"Item": {"pk": {"S": "a"}, "sk": {"N": "1"}, "data": {"S": "x"}}})]
        validator, files = _make_validator(lines)
        result = validator.validate(files, 's3://bucket', PK_SK_SCHEMA, 'FULL_EXPORT')
        assert result['validated_count'] == 1

    def test_missing_key_attribute(self):
        lines = [json.dumps({"Item": {"age": {"N": "30"}}})]
        validator, files = _make_validator(lines)
        with pytest.raises(ValueError, match="Key validation failed"):
            validator.validate(files, 's3://bucket', PK_ONLY_SCHEMA, 'FULL_EXPORT')

    def test_wrong_key_type(self):
        lines = [json.dumps({"Item": {"name": {"N": "123"}}})]
        validator, files = _make_validator(lines)
        with pytest.raises(ValueError, match="Key validation failed"):
            validator.validate(files, 's3://bucket', PK_ONLY_SCHEMA, 'FULL_EXPORT')

    def test_missing_item_field(self):
        lines = [json.dumps({"NotItem": {"name": {"S": "Alice"}}})]
        validator, files = _make_validator(lines)
        with pytest.raises(ValueError, match="Key validation failed"):
            validator.validate(files, 's3://bucket', PK_ONLY_SCHEMA, 'FULL_EXPORT')

    def test_uncompressed_file(self):
        lines = [json.dumps({"Item": {"name": {"S": "Alice"}}})]
        validator, files = _make_validator(lines, file_key='data/file1.json', compress=False)
        result = validator.validate(files, 's3://bucket', PK_ONLY_SCHEMA, 'FULL_EXPORT')
        assert result['validated_count'] == 1

    def test_empty_verified_files(self):
        validator = KeySchemaValidator(Mock())
        result = validator.validate([], 's3://bucket', PK_ONLY_SCHEMA, 'FULL_EXPORT')
        assert result == {'validated_count': 0, 'sampled_rows': 0, 'failed_rows': []}


class TestKeySchemaValidatorEmptyFiles:

    def test_first_file_empty_should_skip_to_nonempty_file(self):
        """When the first verified file has 0 items (empty gzip), the validator
        should skip it and sample from a file that actually contains data."""
        valid_line = json.dumps({"Item": {"pk": {"S": "a"}, "sk": {"N": "1"}, "data": {"S": "x"}}})

        mock_file_loader = Mock()
        mock_file_loader.join_path.side_effect = lambda base, key: f"{base}/{key}"

        def read_file(path):
            if 'empty' in path:
                return gzip.compress(b'')
            return gzip.compress(valid_line.encode('utf-8'))

        mock_file_loader.read_file.side_effect = read_file

        verified_files = [
            {'dataFileS3Key': 'data/empty.json.gz', 'md5Checksum': 'abc', 'itemCount': 0},
            {'dataFileS3Key': 'data/has_data.json.gz', 'md5Checksum': 'def', 'itemCount': 1},
        ]

        validator = KeySchemaValidator(mock_file_loader)
        result = validator.validate(verified_files, 's3://bucket', PK_SK_SCHEMA, 'FULL_EXPORT')
        assert result['validated_count'] == 1


class TestKeySchemaValidatorIncrementalExport:

    def test_pk_only(self):
        lines = [_incremental_row(keys={"name": {"S": "Alice"}})]
        validator, files = _make_validator(lines)
        result = validator.validate(files, 's3://bucket', PK_ONLY_SCHEMA, 'INCREMENTAL_EXPORT')
        assert result['validated_count'] == 1

    def test_pk_sk(self):
        lines = [_incremental_row(keys={"pk": {"S": "a"}, "sk": {"N": "1"}})]
        validator, files = _make_validator(lines)
        result = validator.validate(files, 's3://bucket', PK_SK_SCHEMA, 'INCREMENTAL_EXPORT')
        assert result['validated_count'] == 1

    def test_missing_key_attribute(self):
        lines = [_incremental_row(keys={"wrong": {"S": "x"}})]
        validator, files = _make_validator(lines)
        with pytest.raises(ValueError, match="Key validation failed"):
            validator.validate(files, 's3://bucket', PK_ONLY_SCHEMA, 'INCREMENTAL_EXPORT')

    def test_wrong_key_type(self):
        lines = [_incremental_row(keys={"name": {"N": "123"}})]
        validator, files = _make_validator(lines)
        with pytest.raises(ValueError, match="Key validation failed"):
            validator.validate(files, 's3://bucket', PK_ONLY_SCHEMA, 'INCREMENTAL_EXPORT')

    def test_extra_key_attributes(self):
        lines = [_incremental_row(keys={"name": {"S": "Alice"}, "extra": {"S": "bad"}})]
        validator, files = _make_validator(lines)
        with pytest.raises(ValueError, match="Key validation failed"):
            validator.validate(files, 's3://bucket', PK_ONLY_SCHEMA, 'INCREMENTAL_EXPORT')

    def test_missing_keys_field(self):
        lines = [json.dumps({"Metadata": {"WriteTimestampMicros": {"N": "123"}}, "OldImage": {"name": {"S": "x"}}})]
        validator, files = _make_validator(lines)
        with pytest.raises(ValueError, match="Key validation failed"):
            validator.validate(files, 's3://bucket', PK_ONLY_SCHEMA, 'INCREMENTAL_EXPORT')

    def test_multiple_rows_mixed(self):
        lines = [
            _incremental_row(keys={"name": {"S": "Alice"}}),
            _incremental_row(keys={"wrong": {"S": "x"}}),
        ]
        validator, files = _make_validator(lines)
        with pytest.raises(ValueError, match="Key validation failed"):
            validator.validate(files, 's3://bucket', PK_ONLY_SCHEMA, 'INCREMENTAL_EXPORT')


class TestKeySchemaValidatorEdgeCases:

    def test_all_files_zero_item_count_returns_early(self):
        """When all verified_files have itemCount=0, validator returns without reading any file."""
        mock_file_loader = Mock()
        validator = KeySchemaValidator(mock_file_loader)
        verified_files = [
            {'dataFileS3Key': 'data/empty1.json.gz', 'md5Checksum': 'abc', 'itemCount': 0},
            {'dataFileS3Key': 'data/empty2.json.gz', 'md5Checksum': 'def', 'itemCount': 0},
        ]
        result = validator.validate(verified_files, 's3://bucket', PK_ONLY_SCHEMA, 'FULL_EXPORT')
        assert result == {'validated_count': 0, 'sampled_rows': 0, 'failed_rows': []}
        mock_file_loader.read_file.assert_not_called()

    def test_file_read_exception_returns_gracefully(self):
        """When the data file can't be read, validator returns zeros gracefully."""
        mock_file_loader = Mock()
        mock_file_loader.join_path.side_effect = lambda base, key: f"{base}/{key}"
        mock_file_loader.read_file.side_effect = Exception("S3 access denied")
        validator = KeySchemaValidator(mock_file_loader)
        verified_files = [{'dataFileS3Key': 'data/file.json.gz', 'md5Checksum': 'abc', 'itemCount': 5}]
        result = validator.validate(verified_files, 's3://bucket', PK_ONLY_SCHEMA, 'FULL_EXPORT')
        assert result == {'validated_count': 0, 'sampled_rows': 0, 'failed_rows': []}

    def test_malformed_json_row_reported_as_failure(self):
        """Malformed JSON in a data row should cause validation failure."""
        lines = ['{"Item": {"name": {"S": "Alice"}}}', 'not valid json at all']
        validator, files = _make_validator(lines)
        with pytest.raises(ValueError, match="Key validation failed"):
            validator.validate(files, 's3://bucket', PK_ONLY_SCHEMA, 'FULL_EXPORT')

    def test_avg_item_size_computed_correctly(self):
        """avg_item_size should be total byte size / validated row count."""
        line = json.dumps({"Item": {"name": {"S": "Alice"}, "age": {"N": "30"}}})
        lines = [line, line, line]
        validator, files = _make_validator(lines)
        result = validator.validate(files, 's3://bucket', PK_ONLY_SCHEMA, 'FULL_EXPORT')
        expected_size = len(line.encode('utf-8'))
        assert result['avg_item_size'] == expected_size
        assert result['validated_count'] == 3

    def test_error_diagnostic_handles_unparseable_first_row(self):
        """When the first row is malformed JSON, error diagnostics don't crash."""
        lines = ['not-json-at-all', json.dumps({"Item": {"name": {"S": "Alice"}}})]
        validator, files = _make_validator(lines)
        with pytest.raises(ValueError, match="Key validation failed"):
            validator.validate(files, 's3://bucket', PK_ONLY_SCHEMA, 'FULL_EXPORT')
