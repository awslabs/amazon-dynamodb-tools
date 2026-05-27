import hashlib
import pytest
from python_modules.load_export.parsers.records import FullExportRecord, IncrementalExportRecord
from python_modules.load_export.transform.pkmd5_add_attribute import (
    _get_pk_name,
    transform_full_record,
    transform_incremental_record,
)


class TestGetPkName:

    def test_extracts_pk_name(self):
        schema = {"pk": {"name": "Id", "type": "N"}}
        assert _get_pk_name(schema) == "Id"

    def test_extracts_pk_with_sk_present(self):
        schema = {"pk": {"name": "UserId", "type": "S"}, "sk": {"name": "ts", "type": "N"}}
        assert _get_pk_name(schema) == "UserId"


class TestTransformFullRecord:

    def test_adds_md5_of_numeric_pk(self):
        record = FullExportRecord(
            item={"Id": 42, "status": "active"},
            table_key_schema={"pk": {"name": "Id", "type": "N"}},
        )
        result = transform_full_record(record)
        expected_md5 = hashlib.md5(b"42").hexdigest()
        assert len(result) == 1
        assert result[0] is record
        assert record.item["pk_md5"] == expected_md5

    def test_adds_md5_of_string_pk(self):
        record = FullExportRecord(
            item={"UserId": "user-abc", "data": "x"},
            table_key_schema={"pk": {"name": "UserId", "type": "S"}},
        )
        transform_full_record(record)
        expected_md5 = hashlib.md5(b"user-abc").hexdigest()
        assert record.item["pk_md5"] == expected_md5

    def test_missing_pk_uses_empty_string(self):
        record = FullExportRecord(
            item={"other": "value"},
            table_key_schema={"pk": {"name": "Id", "type": "N"}},
        )
        transform_full_record(record)
        expected_md5 = hashlib.md5(b"").hexdigest()
        assert record.item["pk_md5"] == expected_md5

    def test_preserves_existing_attributes(self):
        record = FullExportRecord(
            item={"Id": 1, "name": "Alice", "age": 30},
            table_key_schema={"pk": {"name": "Id", "type": "N"}},
        )
        transform_full_record(record)
        assert record.item["name"] == "Alice"
        assert record.item["age"] == 30
        assert "pk_md5" in record.item

    def test_composite_key_uses_pk_only(self):
        record = FullExportRecord(
            item={"pk": "user123", "sk": "profile", "data": "x"},
            table_key_schema={"pk": {"name": "pk", "type": "S"}, "sk": {"name": "sk", "type": "S"}},
        )
        transform_full_record(record)
        expected_md5 = hashlib.md5(b"user123").hexdigest()
        assert record.item["pk_md5"] == expected_md5


class TestTransformIncrementalRecord:

    def test_adds_md5_to_new_image(self):
        record = IncrementalExportRecord(
            keys={"Id": 42},
            new_image={"Id": 42, "status": "active"},
            old_image=None,
            table_key_schema={"pk": {"name": "Id", "type": "N"}},
            write_timestamp_micros=1000,
        )
        result = transform_incremental_record(record)
        expected_md5 = hashlib.md5(b"42").hexdigest()
        assert len(result) == 1
        assert result[0].new_image["pk_md5"] == expected_md5

    def test_delete_record_unchanged(self):
        record = IncrementalExportRecord(
            keys={"Id": 42},
            new_image=None,
            old_image={"Id": 42, "status": "active"},
            table_key_schema={"pk": {"name": "Id", "type": "N"}},
            write_timestamp_micros=1000,
        )
        result = transform_incremental_record(record)
        assert len(result) == 1
        assert result[0].new_image is None

    def test_uses_keys_dict_for_pk_value(self):
        record = IncrementalExportRecord(
            keys={"UserId": "abc123"},
            new_image={"UserId": "abc123", "data": "x"},
            old_image=None,
            table_key_schema={"pk": {"name": "UserId", "type": "S"}},
            write_timestamp_micros=1000,
        )
        transform_incremental_record(record)
        expected_md5 = hashlib.md5(b"abc123").hexdigest()
        assert record.new_image["pk_md5"] == expected_md5

    def test_missing_pk_in_keys_uses_empty_string(self):
        record = IncrementalExportRecord(
            keys={"other": "val"},
            new_image={"other": "val", "data": "x"},
            old_image=None,
            table_key_schema={"pk": {"name": "Id", "type": "N"}},
            write_timestamp_micros=1000,
        )
        transform_incremental_record(record)
        expected_md5 = hashlib.md5(b"").hexdigest()
        assert record.new_image["pk_md5"] == expected_md5
