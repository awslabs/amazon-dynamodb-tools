import pytest
from python_modules.load_export.parsers.records import FullExportRecord, IncrementalExportRecord
from python_modules.load_export.transform.pii_remove_attribute import (
    _get_key_attribute_names,
    _remove_pii,
    transform_full_record,
    transform_incremental_record,
)


class TestGetKeyAttributeNames:

    def test_pk_only(self):
        schema = {"pk": {"name": "UserId", "type": "S"}}
        assert _get_key_attribute_names(schema) == {"UserId"}

    def test_pk_and_sk(self):
        schema = {"pk": {"name": "UserId", "type": "S"}, "sk": {"name": "ts", "type": "N"}}
        assert _get_key_attribute_names(schema) == {"UserId", "ts"}


class TestRemovePii:

    def test_removes_pii_attribute(self):
        item = {"Id": 1, "Name": "Alice", "email": "a@b.com"}
        _remove_pii(item, {"Id"})
        assert "Name" not in item
        assert item == {"Id": 1, "email": "a@b.com"}

    def test_skips_key_attribute(self):
        item = {"Name": "Alice", "data": "x"}
        _remove_pii(item, {"Name"})
        assert item["Name"] == "Alice"

    def test_missing_pii_attribute_no_error(self):
        item = {"Id": 1, "email": "a@b.com"}
        _remove_pii(item, {"Id"})
        assert item == {"Id": 1, "email": "a@b.com"}

    def test_removes_non_string_pii_value(self):
        item = {"Id": 1, "Name": 999}
        _remove_pii(item, {"Id"})
        assert "Name" not in item


class TestTransformFullRecord:

    def test_removes_pii_from_item(self):
        record = FullExportRecord(
            item={"Id": 42, "Name": "Alice", "status": "active"},
            table_key_schema={"pk": {"name": "Id", "type": "N"}},
        )
        result = transform_full_record(record)
        assert len(result) == 1
        assert result[0] is record
        assert "Name" not in record.item
        assert record.item == {"Id": 42, "status": "active"}

    def test_does_not_remove_pk(self):
        record = FullExportRecord(
            item={"Name": "Alice", "other": "data"},
            table_key_schema={"pk": {"name": "Name", "type": "S"}},
        )
        transform_full_record(record)
        assert record.item["Name"] == "Alice"

    def test_does_not_remove_sk(self):
        record = FullExportRecord(
            item={"Id": 1, "Name": "Alice"},
            table_key_schema={"pk": {"name": "Id", "type": "N"}, "sk": {"name": "Name", "type": "S"}},
        )
        transform_full_record(record)
        assert "Name" in record.item

    def test_item_without_pii_attribute_unchanged(self):
        record = FullExportRecord(
            item={"Id": 1, "email": "a@b.com"},
            table_key_schema={"pk": {"name": "Id", "type": "N"}},
        )
        transform_full_record(record)
        assert record.item == {"Id": 1, "email": "a@b.com"}


class TestTransformIncrementalRecord:

    def test_removes_pii_from_new_image(self):
        record = IncrementalExportRecord(
            keys={"Id": 42},
            new_image={"Id": 42, "Name": "Alice", "status": "active"},
            old_image=None,
            table_key_schema={"pk": {"name": "Id", "type": "N"}},
            write_timestamp_micros=1000,
        )
        result = transform_incremental_record(record)
        assert len(result) == 1
        assert "Name" not in result[0].new_image

    def test_delete_record_unchanged(self):
        record = IncrementalExportRecord(
            keys={"Id": 42},
            new_image=None,
            old_image={"Id": 42, "Name": "Alice"},
            table_key_schema={"pk": {"name": "Id", "type": "N"}},
            write_timestamp_micros=1000,
        )
        result = transform_incremental_record(record)
        assert len(result) == 1
        assert result[0].new_image is None

    def test_does_not_remove_key_attribute_from_new_image(self):
        record = IncrementalExportRecord(
            keys={"Name": "Alice"},
            new_image={"Name": "Alice", "data": "x"},
            old_image=None,
            table_key_schema={"pk": {"name": "Name", "type": "S"}},
            write_timestamp_micros=1000,
        )
        transform_incremental_record(record)
        assert record.new_image["Name"] == "Alice"
