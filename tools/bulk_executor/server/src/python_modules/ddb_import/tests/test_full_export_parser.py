"""Unit tests for FullExportParser."""

import pytest
import json
import base64
from decimal import Decimal
from ..parsers.full_export_parser import FullExportParser


class TestFullExportParser:
    """Test cases for FullExportParser."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.parser = FullExportParser()
    
    def test_parse_simple_item(self):
        """Test parsing a simple item with string and number attributes."""
        line = '{"Item":{"name":{"S":"John"},"age":{"N":"30"}}}'
        
        operation, item_data, condition = self.parser.parse_export_line(line)
        
        assert operation == "PUT"
        assert condition is None
        assert item_data == {"name": "John", "age": Decimal("30")}
    
    def test_parse_complex_item_with_list(self):
        """Test parsing item with complex list structure like the sample data."""
        line = '{"Item":{"name":{"S":"Argyros1003"},"activities":{"L":[{"M":{"activity":{"S":"Play violin"},"timestamp":{"S":"Sat, 04 May 2024 04:49:44 GMT"}}},{"M":{"activity":{"S":"Listen to music"},"timestamp":{"S":"Sat, 04 May 2024 04:50:21 GMT"}}}]}}}'
        
        operation, item_data, condition = self.parser.parse_export_line(line)
        
        assert operation == "PUT"
        assert condition is None
        assert item_data == {
            "name": "Argyros1003",
            "activities": [
                {"activity": "Play violin", "timestamp": "Sat, 04 May 2024 04:49:44 GMT"},
                {"activity": "Listen to music", "timestamp": "Sat, 04 May 2024 04:50:21 GMT"}
            ]
        }
    
    def test_parse_item_with_all_types(self):
        """Test parsing item with various DynamoDB attribute types."""
        line = '''{
            "Item": {
                "pk": {"S": "user123"},
                "sk": {"S": "profile"},
                "age": {"N": "25"},
                "score": {"N": "99.5"},
                "active": {"BOOL": true},
                "deleted": {"BOOL": false},
                "metadata": {"NULL": true},
                "tags": {"SS": ["admin", "user"]},
                "scores": {"NS": ["100", "95", "88"]},
                "config": {"M": {"theme": {"S": "dark"}, "notifications": {"BOOL": true}}},
                "history": {"L": [{"S": "login"}, {"S": "logout"}]}
            }
        }'''
        
        operation, item_data, condition = self.parser.parse_export_line(line)
        
        assert operation == "PUT"
        assert condition is None
        assert item_data == {
            "pk": "user123",
            "sk": "profile", 
            "age": Decimal("25"),
            "score": Decimal("99.5"),
            "active": True,
            "deleted": False,
            "metadata": None,
            "tags": {"admin", "user"},
            "scores": {Decimal("100"), Decimal("95"), Decimal("88")},
            "config": {"theme": "dark", "notifications": True},
            "history": ["login", "logout"]
        }
    
    def test_parse_nested_maps_and_lists(self):
        """Test parsing deeply nested maps and lists."""
        line = '''{
            "Item": {
                "id": {"S": "test"},
                "nested": {"M": {
                    "level1": {"M": {
                        "level2": {"L": [
                            {"M": {"key": {"S": "value1"}}},
                            {"M": {"key": {"S": "value2"}}}
                        ]}
                    }}
                }}
            }
        }'''
        
        operation, item_data, condition = self.parser.parse_export_line(line)
        
        assert operation == "PUT"
        assert condition is None
        assert item_data == {
            "id": "test",
            "nested": {
                "level1": {
                    "level2": [
                        {"key": "value1"},
                        {"key": "value2"}
                    ]
                }
            }
        }
    
    def test_parse_empty_item(self):
        """Test parsing item with minimal attributes."""
        line = '{"Item":{"id":{"S":"empty"}}}'
        
        operation, item_data, condition = self.parser.parse_export_line(line)
        
        assert operation == "PUT"
        assert condition is None
        assert item_data == {"id": "empty"}
    
    def test_malformed_json_raises_error(self):
        """Test that malformed JSON raises ValueError."""
        line = '{"Item":{"name":{"S":"John"}'  # Missing closing braces
        
        with pytest.raises(ValueError) as exc_info:
            self.parser.parse_export_line(line)
        assert "Malformed JSON" in str(exc_info.value)
    
    def test_non_object_json_raises_error(self):
        """Test that non-object JSON raises ValueError."""
        line = '"just a string"'
        
        with pytest.raises(ValueError) as exc_info:
            self.parser.parse_export_line(line)
        assert "Export line must be a JSON object" in str(exc_info.value)
    
    def test_missing_item_field_raises_error(self):
        """Test that missing Item field raises ValueError."""
        line = '{"NotItem":{"name":{"S":"John"}}}'
        
        with pytest.raises(ValueError) as exc_info:
            self.parser.parse_export_line(line)
        assert "Export line missing 'Item' field" in str(exc_info.value)
    
    def test_empty_item_field(self):
        """Test parsing with empty Item field."""
        line = '{"Item":{}}'
        
        operation, item_data, condition = self.parser.parse_export_line(line)
        
        assert operation == "PUT"
        assert condition is None
        assert item_data == {}
    
    def test_item_with_binary_data(self):
        """Test parsing item with binary attribute."""
        line = '{"Item":{"id":{"S":"test"},"data":{"B":"SGVsbG8gV29ybGQ="}}}'
        
        operation, item_data, condition = self.parser.parse_export_line(line)
        
        assert operation == "PUT"
        assert condition is None
        assert item_data["id"] == "test"
        # Binary data is returned as base64 decoded bytes
        assert item_data["data"] == base64.b64decode("SGVsbG8gV29ybGQ=")
    
    def test_item_with_binary_set(self):
        """Test parsing item with binary set attribute."""
        line = '{"Item":{"id":{"S":"test"},"binaries":{"BS":["SGVsbG8=","V29ybGQ="]}}}'
        
        operation, item_data, condition = self.parser.parse_export_line(line)
        
        assert operation == "PUT"
        assert condition is None
        assert item_data["id"] == "test"
        # Binary set is returned as a set of base64 decoded bytes
        expected_binaries = {base64.b64decode("SGVsbG8="), base64.b64decode("V29ybGQ=")}
        assert item_data["binaries"] == expected_binaries
    
    def test_real_export_format(self):
        """Test parsing actual export format from sample data file."""
        # This matches the format from the sample data file
        line = '{"Item":{"name":{"S":"Sparkles1902"},"activities":{"L":[{"M":{"activity":{"S":"Brush teeth"},"timestamp":{"S":"Sun, 14 Apr 2024 02:19:28 GMT"}}},{"M":{"activity":{"S":"Went to school"},"timestamp":{"S":"Sun, 14 Apr 2024 02:22:54 GMT"}}},{"M":{"activity":{"S":"Eat hotdogs"},"timestamp":{"S":"Sun, 21 Apr 2024 15:21:43 GMT"}}}]}}}'
        
        operation, item_data, condition = self.parser.parse_export_line(line)
        
        assert operation == "PUT"
        assert condition is None
        assert item_data["name"] == "Sparkles1902"
        assert len(item_data["activities"]) == 3
        assert item_data["activities"][0]["activity"] == "Brush teeth"
        assert item_data["activities"][1]["activity"] == "Went to school"
        assert item_data["activities"][2]["activity"] == "Eat hotdogs"
    
    def test_converts_ddb_json_to_plain_python(self):
        """Test that the parser converts DDB-JSON format to plain Python format."""
        original_ddb_item = {
            "pk": {"S": "test"},
            "num": {"N": "42"},
            "flag": {"BOOL": True},
            "null_val": {"NULL": True},
            "map_val": {"M": {"nested": {"S": "value"}}},
            "list_val": {"L": [{"S": "item1"}, {"N": "123"}]}
        }
        line = json.dumps({"Item": original_ddb_item})
        
        operation, item_data, condition = self.parser.parse_export_line(line)
        
        assert operation == "PUT"
        assert condition is None
        # Verify conversion to plain Python format
        assert item_data == {
            "pk": "test",
            "num": Decimal("42"),
            "flag": True,
            "null_val": None,
            "map_val": {"nested": "value"},
            "list_val": ["item1", Decimal("123")]
        }
    
    def test_number_precision_preserved(self):
        """Test that number precision is preserved during conversion."""
        from decimal import Decimal
        line = '{"Item":{"decimal":{"N":"123.456789"},"integer":{"N":"42"}}}'
        
        operation, item_data, condition = self.parser.parse_export_line(line)
        
        assert operation == "PUT"
        assert condition is None
        # All numbers are preserved as Decimal objects for precision
        assert item_data["decimal"] == Decimal("123.456789")
        assert item_data["integer"] == Decimal("42")
        assert isinstance(item_data["integer"], Decimal)
    
    def test_large_numbers_handled_correctly(self):
        """Test that large numbers are handled correctly."""
        from decimal import Decimal
        line = '{"Item":{"big_int":{"N":"9223372036854775807"},"big_decimal":{"N":"999999999999999.999999999"}}}'
        
        operation, item_data, condition = self.parser.parse_export_line(line)
        
        assert operation == "PUT"
        assert condition is None
        assert item_data["big_int"] == Decimal("9223372036854775807")
        # Large decimals are preserved as Decimal objects for precision
        assert item_data["big_decimal"] == Decimal("999999999999999.999999999")
