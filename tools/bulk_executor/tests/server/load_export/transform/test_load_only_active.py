import pytest
from python_modules.shared.export.parsers.records import FullExportRecord, IncrementalExportRecord
from python_modules.load_export.transform.load_only_active import (
    transform_full_record,
    transform_incremental_record,
)


class TestTransformFullRecord:

    def test_active_record_kept(self):
        record = FullExportRecord(
            item={"pk": "user1", "status": "active", "data": "x"},
            table_key_schema={"pk": {"name": "pk", "type": "S"}},
        )
        result = transform_full_record(record)
        assert len(result) == 1
        assert result[0] is record

    def test_inactive_record_filtered(self):
        record = FullExportRecord(
            item={"pk": "user1", "status": "inactive", "data": "x"},
            table_key_schema={"pk": {"name": "pk", "type": "S"}},
        )
        result = transform_full_record(record)
        assert result == []

    def test_missing_status_filtered(self):
        record = FullExportRecord(
            item={"pk": "user1", "data": "x"},
            table_key_schema={"pk": {"name": "pk", "type": "S"}},
        )
        result = transform_full_record(record)
        assert result == []

    def test_status_none_filtered(self):
        record = FullExportRecord(
            item={"pk": "user1", "status": None},
            table_key_schema={"pk": {"name": "pk", "type": "S"}},
        )
        result = transform_full_record(record)
        assert result == []

    def test_status_case_sensitive(self):
        record = FullExportRecord(
            item={"pk": "user1", "status": "Active"},
            table_key_schema={"pk": {"name": "pk", "type": "S"}},
        )
        result = transform_full_record(record)
        assert result == []


class TestTransformIncrementalRecord:

    def test_active_new_image_kept(self):
        record = IncrementalExportRecord(
            keys={"pk": "user1"},
            new_image={"pk": "user1", "status": "active"},
            old_image=None,
            table_key_schema={"pk": {"name": "pk", "type": "S"}},
            write_timestamp_micros=1000,
        )
        result = transform_incremental_record(record)
        assert len(result) == 1
        assert result[0] is record

    def test_inactive_new_image_filtered(self):
        record = IncrementalExportRecord(
            keys={"pk": "user1"},
            new_image={"pk": "user1", "status": "inactive"},
            old_image=None,
            table_key_schema={"pk": {"name": "pk", "type": "S"}},
            write_timestamp_micros=1000,
        )
        result = transform_incremental_record(record)
        assert result == []

    def test_delete_record_kept(self):
        record = IncrementalExportRecord(
            keys={"pk": "user1"},
            new_image=None,
            old_image={"pk": "user1", "status": "active"},
            table_key_schema={"pk": {"name": "pk", "type": "S"}},
            write_timestamp_micros=1000,
        )
        result = transform_incremental_record(record)
        assert len(result) == 1
        assert result[0] is record

    def test_missing_status_in_new_image_filtered(self):
        record = IncrementalExportRecord(
            keys={"pk": "user1"},
            new_image={"pk": "user1", "data": "x"},
            old_image=None,
            table_key_schema={"pk": {"name": "pk", "type": "S"}},
            write_timestamp_micros=1000,
        )
        result = transform_incremental_record(record)
        assert result == []

    def test_status_not_string_filtered(self):
        record = IncrementalExportRecord(
            keys={"pk": "user1"},
            new_image={"pk": "user1", "status": 1},
            old_image=None,
            table_key_schema={"pk": {"name": "pk", "type": "S"}},
            write_timestamp_micros=1000,
        )
        result = transform_incremental_record(record)
        assert result == []
