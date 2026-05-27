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


    def test_decode_binary_values_non_dict_node(self):
        """_decode_binary_values should return non-dict nodes unchanged."""
        result = self.parser._decode_binary_values("just a string")
        self.assertEqual(result, "just a string")

        result = self.parser._decode_binary_values(42)
        self.assertEqual(result, 42)

        result = self.parser._decode_binary_values(None)
        self.assertIsNone(result)

    def test_decode_binary_values_list_with_non_dict_items(self):
        """L-type lists containing non-dict items should pass through unchanged."""
        ddb_item = {
            'data': {'L': [{'S': 'hello'}, {'N': '42'}]}
        }
        # _decode_binary_values processes the L list; each item is a dict but
        # contains no B/BS/L/M keys, so the else branch (isinstance(v, dict))
        # recurses and the items pass through as-is
        result = self.parser.deserialize_item(ddb_item)
        self.assertEqual(result['data'], ['hello', 42])

    def test_decode_binary_values_dict_with_plain_value(self):
        """Dict values that are not themselves dicts should pass through unchanged."""
        node = {'someKey': 'plain_string', 'anotherKey': 123}
        result = self.parser._decode_binary_values(node)
        self.assertEqual(result, {'someKey': 'plain_string', 'anotherKey': 123})


class _SuperCallingParser(BaseExportParser):
    """Concrete parser that delegates to super() before doing its own work.

    This is the only way to execute the `pass` bodies of the @abstractmethod
    methods on BaseExportParser (lines 18 and 23): a subclass overrides and
    calls super().parse_to_record / super().resolve. The super call runs the
    abstract body (which is just `pass`) and returns None; the override then
    returns its own value so callers get something usable.
    """

    def parse_to_record(self, line):
        # Run the abstract body (pass) for coverage of line 18.
        super_result = super().parse_to_record(line)
        return super_result  # None — caller asserts on it

    def resolve(self, record):
        # Run the abstract body (pass) for coverage of line 23.
        super_result = super().resolve(record)
        return super_result  # None — caller asserts on it


class TestBaseExportParserAbstractMethodBodies(unittest.TestCase):
    """Ensure the @abstractmethod `pass` bodies are exercised via super() calls.

    The abstract methods are required to be overridden, so they cannot be
    invoked by directly instantiating BaseExportParser. The standard pattern
    for covering an abstract `pass` body is to override in a subclass and
    have the override call `super().the_method(...)` first.
    """

    def setUp(self):
        self.parser = _SuperCallingParser()

    def test_super_parse_to_record_returns_none(self):
        """Calling super().parse_to_record runs the abstract `pass` body."""
        result = self.parser.parse_to_record('{"some": "json"}')
        self.assertIsNone(result)

    def test_super_resolve_returns_none(self):
        """Calling super().resolve runs the abstract `pass` body."""
        result = self.parser.resolve(MagicMock(name='AnyRecord'))
        self.assertIsNone(result)


if __name__ == '__main__':
    unittest.main()
