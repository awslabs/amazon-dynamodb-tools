"""Unit tests for KeySchemaValidator."""
import gzip
import json
import pytest
from unittest.mock import Mock
from ..validators.key_schema_validator import KeySchemaValidator


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
