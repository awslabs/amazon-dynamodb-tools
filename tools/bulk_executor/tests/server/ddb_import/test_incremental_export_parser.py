"""Unit tests for IncrementalExportParser."""

import unittest
import json
import sys
import os

# Add the parent directory to the path so we can import the modules
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from python_modules.ddb_import.parsers.incremental_export_parser import IncrementalExportParser


class TestIncrementalExportParser(unittest.TestCase):
    """Test cases for IncrementalExportParser."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.parser = IncrementalExportParser()
    
    def test_parse_insert_operation(self):
        """Test parsing an INSERT operation (NewImage only)."""
        record = {
            "Keys": {"id": {"N": "123"}},
            "NewImage": {"id": {"N": "123"}, "name": {"S": "John Doe"}},
            "Metadata": {"eventName": "INSERT"}
        }
        
        line = json.dumps(record)
        result = self.parser.parse_export_line(line)
        
        operation, data, condition = result
        self.assertEqual(operation, "PUT")
        self.assertEqual(data, {"id": 123, "name": "John Doe"})
        self.assertEqual(condition, "attribute_not_exists(id)")
    
    def test_parse_modify_operation(self):
        """Test parsing a MODIFY operation (both OldImage and NewImage)."""
        record = {
            "Keys": {"id": {"N": "123"}},
            "OldImage": {"id": {"N": "123"}, "name": {"S": "John"}},
            "NewImage": {"id": {"N": "123"}, "name": {"S": "John Doe"}},
            "Metadata": {"eventName": "MODIFY"}
        }
        
        line = json.dumps(record)
        result = self.parser.parse_export_line(line)
        
        operation, data, condition = result
        self.assertEqual(operation, "PUT")
        self.assertEqual(data, {"id": 123, "name": "John Doe"})
        self.assertEqual(condition, "attribute_exists(id)")
    
    def test_parse_remove_operation(self):
        """Test parsing a REMOVE operation (OldImage only)."""
        record = {
            "Keys": {"id": {"N": "123"}},
            "OldImage": {"id": {"N": "123"}, "name": {"S": "John Doe"}},
            "Metadata": {"eventName": "REMOVE"}
        }
        
        line = json.dumps(record)
        result = self.parser.parse_export_line(line)
        
        operation, data, condition = result
        self.assertEqual(operation, "DELETE")
        self.assertEqual(data, {"id": 123})
        self.assertEqual(condition, "attribute_exists(id)")
    
    def test_parse_composite_key(self):
        """Test parsing with composite primary key."""
        record = {
            "Keys": {"pk": {"S": "USER"}, "sk": {"S": "123"}},
            "NewImage": {"pk": {"S": "USER"}, "sk": {"S": "123"}, "name": {"S": "John"}},
            "Metadata": {"eventName": "INSERT"}
        }
        
        line = json.dumps(record)
        result = self.parser.parse_export_line(line)
        
        operation, data, condition = result
        self.assertEqual(operation, "PUT")
        self.assertEqual(data, {"pk": "USER", "sk": "123", "name": "John"})
        self.assertEqual(condition, "attribute_not_exists(pk)")  # Uses first key
    
    def test_parse_malformed_json(self):
        """Test parsing malformed JSON."""
        with self.assertRaises(ValueError) as context:
            self.parser.parse_export_line("invalid json")
        
        self.assertIn("Malformed JSON", str(context.exception))
    
    def test_parse_missing_keys(self):
        """Test parsing record missing Keys field."""
        record = {"NewImage": {"id": {"N": "123"}}}
        line = json.dumps(record)
        
        with self.assertRaises(ValueError) as context:
            self.parser.parse_export_line(line)
        
        self.assertIn("missing 'Keys' field", str(context.exception))
    
    def test_parse_missing_metadata(self):
        """Test parsing record missing Metadata field."""
        record = {"Keys": {"id": {"N": "123"}}, "NewImage": {"id": {"N": "123"}}}
        line = json.dumps(record)
        
        with self.assertRaises(ValueError) as context:
            self.parser.parse_export_line(line)
        
        self.assertIn("missing 'Metadata' field", str(context.exception))
    
    def test_parse_invalid_record(self):
        """Test parsing record with neither OldImage nor NewImage."""
        record = {
            "Keys": {"id": {"N": "123"}},
            "Metadata": {"eventName": "UNKNOWN"}
        }
        line = json.dumps(record)
        
        with self.assertRaises(ValueError) as context:
            self.parser.parse_export_line(line)
        
        self.assertIn("no OldImage or NewImage", str(context.exception))
    
    def test_parse_non_dict_json(self):
        """Test parsing JSON that's not a dictionary."""
        with self.assertRaises(ValueError) as context:
            self.parser.parse_export_line('["not", "a", "dict"]')
        
        self.assertIn("must be a JSON object", str(context.exception))


if __name__ == '__main__':
    unittest.main()
