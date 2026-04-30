"""Unit tests for FullExportParser."""

import pytest
import json
import base64
from decimal import Decimal
from python_modules.load_export.parsers.full_export_parser import FullExportParser

SAMPLE_KEY_SCHEMA = {'pk': {'name': 'name', 'type': 'S'}}

class TestFullExportParser:
    """Test cases for FullExportParser."""

    def setup_method(self):
        self.parser = FullExportParser(SAMPLE_KEY_SCHEMA)

    def test_parse_to_record_simple(self):
        line = '{"Item":{"name":{"S":"John"},"age":{"N":"30"}}}'
        record = self.parser.parse_to_record(line)
        assert record.item == {"name": "John", "age": Decimal("30")}
        assert record.table_key_schema == SAMPLE_KEY_SCHEMA

    def test_resolve_simple(self):
        line = '{"Item":{"name":{"S":"John"},"age":{"N":"30"}}}'
        record = self.parser.parse_to_record(line)
        result = self.parser.resolve(record)
        assert result["operation"] == "PUT"
        assert result["data"] == {"name": "John", "age": Decimal("30")}

    def test_parse_complex_item_with_list(self):
        line = '{"Item":{"name":{"S":"Argyros1003"},"activities":{"L":[{"M":{"activity":{"S":"Play violin"},"timestamp":{"S":"Sat, 04 May 2024 04:49:44 GMT"}}}]}}}'
        record = self.parser.parse_to_record(line)
        result = self.parser.resolve(record)
        assert result["operation"] == "PUT"
        assert result["data"]["name"] == "Argyros1003"
        assert len(result["data"]["activities"]) == 1

    def test_parse_item_with_all_types(self):
        line = json.dumps({"Item": {
            "pk": {"S": "user123"}, "age": {"N": "25"}, "active": {"BOOL": True},
            "metadata": {"NULL": True}, "tags": {"SS": ["admin", "user"]},
            "config": {"M": {"theme": {"S": "dark"}}},
            "history": {"L": [{"S": "login"}]}
        }})
        record = self.parser.parse_to_record(line)
        result = self.parser.resolve(record)
        assert result["data"]["pk"] == "user123"
        assert result["data"]["active"] is True
        assert result["data"]["metadata"] is None
        assert result["data"]["tags"] == {"admin", "user"}

    def test_malformed_json_raises_error(self):
        with pytest.raises(ValueError, match="Malformed JSON"):
            self.parser.parse_to_record('{"Item":{"name":{"S":"John"}')

    def test_non_object_json_raises_error(self):
        with pytest.raises(ValueError, match="must be a JSON object"):
            self.parser.parse_to_record('"just a string"')

    def test_missing_item_field_raises_error(self):
        with pytest.raises(ValueError, match="missing 'Item' field"):
            self.parser.parse_to_record('{"NotItem":{"name":{"S":"John"}}}')

    def test_item_with_binary_data(self):
        line = '{"Item":{"id":{"S":"test"},"data":{"B":"SGVsbG8gV29ybGQ="}}}'
        record = self.parser.parse_to_record(line)
        result = self.parser.resolve(record)
        assert result["data"]["data"] == base64.b64decode("SGVsbG8gV29ybGQ=")

    def test_number_precision_preserved(self):
        line = '{"Item":{"decimal":{"N":"123.456789"},"integer":{"N":"42"}}}'
        record = self.parser.parse_to_record(line)
        result = self.parser.resolve(record)
        assert result["data"]["decimal"] == Decimal("123.456789")
        assert isinstance(result["data"]["integer"], Decimal)
