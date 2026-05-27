import pytest
from python_modules.load_export.parsers.records import FullExportRecord, IncrementalExportRecord
from python_modules.load_export.transform.pii_mask_attribute import (
    _get_key_attribute_names,
    _mask_word,
    _mask_value,
    _mask_pii,
    transform_full_record,
    transform_incremental_record,
)


class TestMaskWord:

    def test_masks_middle_characters(self):
        assert _mask_word("Alice") == "A***e"

    def test_two_char_word(self):
        assert _mask_word("Jo") == "Jo"

    def test_single_char_unchanged(self):
        assert _mask_word("A") == "A"

    def test_empty_string_unchanged(self):
        assert _mask_word("") == ""

    def test_three_char_word(self):
        assert _mask_word("Bob") == "B*b"


class TestMaskValue:

    def test_masks_each_word(self):
        assert _mask_value("Alice Smith") == "A***e S***h"

    def test_single_word(self):
        assert _mask_value("Jo") == "Jo"

    def test_non_string_unchanged(self):
        assert _mask_value(42) == 42

    def test_none_unchanged(self):
        assert _mask_value(None) is None

    def test_list_unchanged(self):
        val = ["a", "b"]
        assert _mask_value(val) is val

    def test_empty_string(self):
        assert _mask_value("") == ""

    def test_multiple_spaces(self):
        assert _mask_value("A B C") == "A B C"


class TestGetKeyAttributeNames:

    def test_pk_only(self):
        schema = {"pk": {"name": "Id", "type": "N"}}
        assert _get_key_attribute_names(schema) == {"Id"}

    def test_pk_and_sk(self):
        schema = {"pk": {"name": "Id", "type": "N"}, "sk": {"name": "sort_key", "type": "S"}}
        assert _get_key_attribute_names(schema) == {"Id", "sort_key"}


class TestMaskPii:

    def test_masks_pii_attribute(self):
        item = {"Id": 1, "Name": "Alice Smith", "email": "a@b.com"}
        _mask_pii(item, {"Id"})
        assert item["Name"] == "A***e S***h"
        assert item["email"] == "a@b.com"

    def test_skips_key_attribute(self):
        item = {"Name": "Alice"}
        _mask_pii(item, {"Name"})
        assert item["Name"] == "Alice"

    def test_missing_pii_attribute_no_error(self):
        item = {"Id": 1, "other": "value"}
        _mask_pii(item, {"Id"})
        assert item == {"Id": 1, "other": "value"}

    def test_non_string_pii_value_unchanged(self):
        item = {"Id": 1, "Name": 12345}
        _mask_pii(item, {"Id"})
        assert item["Name"] == 12345


class TestTransformFullRecord:

    def test_masks_pii_in_item(self):
        record = FullExportRecord(
            item={"Id": 42, "Name": "Alice Smith", "status": "active"},
            table_key_schema={"pk": {"name": "Id", "type": "N"}},
        )
        result = transform_full_record(record)
        assert len(result) == 1
        assert result[0] is record
        assert record.item["Name"] == "A***e S***h"
        assert record.item["Id"] == 42

    def test_does_not_mask_pk_named_attribute(self):
        record = FullExportRecord(
            item={"Name": "Alice", "other": "data"},
            table_key_schema={"pk": {"name": "Name", "type": "S"}},
        )
        transform_full_record(record)
        assert record.item["Name"] == "Alice"

    def test_does_not_mask_sk_named_attribute(self):
        record = FullExportRecord(
            item={"Id": 1, "Name": "Alice"},
            table_key_schema={"pk": {"name": "Id", "type": "N"}, "sk": {"name": "Name", "type": "S"}},
        )
        transform_full_record(record)
        assert record.item["Name"] == "Alice"

    def test_item_without_pii_attribute(self):
        record = FullExportRecord(
            item={"Id": 1, "email": "a@b.com"},
            table_key_schema={"pk": {"name": "Id", "type": "N"}},
        )
        transform_full_record(record)
        assert record.item == {"Id": 1, "email": "a@b.com"}


class TestTransformIncrementalRecord:

    def test_masks_pii_in_new_image(self):
        record = IncrementalExportRecord(
            keys={"Id": 42},
            new_image={"Id": 42, "Name": "Alice Smith"},
            old_image=None,
            table_key_schema={"pk": {"name": "Id", "type": "N"}},
            write_timestamp_micros=1000,
        )
        result = transform_incremental_record(record)
        assert len(result) == 1
        assert result[0].new_image["Name"] == "A***e S***h"

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

    def test_does_not_mask_key_attribute_in_new_image(self):
        record = IncrementalExportRecord(
            keys={"Name": "Alice"},
            new_image={"Name": "Alice", "data": "x"},
            old_image=None,
            table_key_schema={"pk": {"name": "Name", "type": "S"}},
            write_timestamp_micros=1000,
        )
        transform_incremental_record(record)
        assert record.new_image["Name"] == "Alice"
