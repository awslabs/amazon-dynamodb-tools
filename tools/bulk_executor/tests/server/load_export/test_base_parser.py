"""Unit tests for BaseExportParser."""

import unittest
from unittest.mock import patch, MagicMock
import sys
import os

# Add the parent directory to the path so we can import the modules
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from python_modules.load_export.parsers.base_parser import BaseExportParser


class ConcreteParser(BaseExportParser):
    """Concrete implementation for testing the abstract base class."""
    
    def parse_to_record(self, line):
        return None

    def resolve(self, record):
        return {"operation": "PUT", "data": {"test": "data"}}


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

    def test_deserialize_item_with_nested_binary(self):
        """Test deserializing DDB-JSON with binary values nested inside lists and maps."""
        ddb_item = {
            'bin': {'B': 'fecabeba'},
            'bins': {'BS': ['beba', 'feca']},
            'mylist': {'L': [
                {'S': 'xx'},
                {'B': 'feca'},
                {'L': [{'B': 'deadbeef'}]},
            ]},
            'mymap': {'M': {
                'inner_bin': {'B': 'cafe'},
                'nested': {'M': {'deep_bin': {'B': 'abcd'}}},
            }},
        }

        result = self.parser.deserialize_item(ddb_item)

        import base64
        self.assertEqual(result['bin'], base64.b64decode('fecabeba'))
        self.assertEqual(result['bins'], {base64.b64decode('beba'), base64.b64decode('feca')})
        self.assertEqual(result['mylist'][0], 'xx')
        self.assertEqual(result['mylist'][1], base64.b64decode('feca'))
        self.assertEqual(result['mylist'][2], [base64.b64decode('deadbeef')])
        self.assertEqual(result['mymap']['inner_bin'], base64.b64decode('cafe'))
        self.assertEqual(result['mymap']['nested']['deep_bin'], base64.b64decode('abcd'))


if __name__ == '__main__':
    unittest.main()
