"""Unit tests for BaseExportParser."""

import unittest
from unittest.mock import patch, MagicMock
import sys
import os

# Add the parent directory to the path so we can import the modules
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from python_modules.ddb_import.parsers.base_parser import BaseExportParser


class ConcreteParser(BaseExportParser):
    """Concrete implementation for testing the abstract base class."""
    
    def parse_export_line(self, line):
        return ("PUT", {"test": "data"}, None)


class TestBaseExportParser(unittest.TestCase):
    """Test cases for BaseExportParser."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.parser = ConcreteParser()
    
    def test_cannot_instantiate_abstract_class(self):
        """Test that BaseExportParser cannot be instantiated directly."""
        with self.assertRaises(TypeError):
            BaseExportParser()
    
    def test_deserialize_item_simple(self):
        """Test deserializing a simple DDB-JSON item."""
        ddb_item = {
            'name': {'S': 'John Doe'},
            'age': {'N': '30'},
            'active': {'BOOL': True}
        }
        
        result = self.parser.deserialize_item(ddb_item)
        
        expected = {
            'name': 'John Doe',
            'age': 30,
            'active': True
        }
        self.assertEqual(result, expected)
    
    def test_deserialize_item_with_sets(self):
        """Test deserializing DDB-JSON with sets."""
        ddb_item = {
            'tags': {'SS': ['tag1', 'tag2', 'tag3']},
            'numbers': {'NS': ['1', '2', '3']}
        }
        
        result = self.parser.deserialize_item(ddb_item)
        
        expected = {
            'tags': {'tag1', 'tag2', 'tag3'},
            'numbers': {1, 2, 3}
        }
        self.assertEqual(result, expected)
    
    def test_deserialize_item_with_list_and_map(self):
        """Test deserializing DDB-JSON with lists and maps."""
        ddb_item = {
            'items': {'L': [{'S': 'item1'}, {'N': '42'}]},
            'metadata': {'M': {'created': {'S': '2024-01-01'}, 'version': {'N': '1'}}}
        }
        
        result = self.parser.deserialize_item(ddb_item)
        
        expected = {
            'items': ['item1', 42],
            'metadata': {'created': '2024-01-01', 'version': 1}
        }
        self.assertEqual(result, expected)
    
    def test_deserialize_item_empty(self):
        """Test deserializing empty or None items."""
        self.assertIsNone(self.parser.deserialize_item(None))
        self.assertEqual(self.parser.deserialize_item({}), {})
    
    def test_deserialize_item_with_null(self):
        """Test deserializing DDB-JSON with null values."""
        ddb_item = {
            'name': {'S': 'John'},
            'middle_name': {'NULL': True}
        }
        
        result = self.parser.deserialize_item(ddb_item)
        
        expected = {
            'name': 'John',
            'middle_name': None
        }
        self.assertEqual(result, expected)


if __name__ == '__main__':
    unittest.main()
