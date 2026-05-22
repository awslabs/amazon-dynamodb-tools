"""
Bug condition exploration tests for DynamoDB binary attribute serialization.

**Validates: Requirements 1.1, 1.2**

These tests verify that the diff module can handle DynamoDB items containing
binary attributes. On unfixed code, these tests MUST FAIL with TypeError,
confirming the bug exists. After the fix is applied, these tests will PASS.

The bug occurs when json.dumps() attempts to serialize bytes objects in
DynamoDB items, which are not JSON serializable by default.
"""

import json
import unittest

from python_modules.diff import item_matches, log_diff


class MockStream:
    """Mock SegmentStream for testing log_diff function."""
    def __init__(self, item, pk='id', sk=None):
        self.item = item
        self.pk = pk
        self.sk = sk
    
    def head(self):
        return self.item
    
    def head_key(self):
        key = {self.pk: self.item[self.pk]}
        if self.sk and self.sk in self.item:
            key[self.sk] = self.item[self.sk]
        return key


class TestBinaryAttributeSerialization(unittest.TestCase):
    """
    Test cases for binary attribute serialization in diff operations.
    
    These tests encode the expected behavior: items with binary attributes
    should be successfully serialized without crashing with TypeError.
    """

    def test_simple_binary_attribute(self):
        """
        Test case 1: Simple binary attribute
        
        Input: {'id': {'S': 'item1'}, 'data': {'B': b'\x00\x01\x02\x03'}}
        Expected: Successfully serializes without TypeError
        Bug condition: containsBinaryAttribute returns true
        """
        item = {'id': {'S': 'item1'}, 'data': {'B': b'\x00\x01\x02\x03'}}
        
        # This should not raise TypeError
        result = item_matches(item, item)
        self.assertTrue(result, "Identical items with binary attributes should match")

    def test_binary_in_nested_map(self):
        """
        Test case 2: Binary in nested map
        
        Input: {'id': {'S': 'item1'}, 'metadata': {'M': {'binary_field': {'B': b'\xff\xfe'}}}}
        Expected: Successfully serializes without TypeError
        Bug condition: containsBinaryAttribute returns true (nested)
        """
        item = {
            'id': {'S': 'item1'},
            'metadata': {'M': {'binary_field': {'B': b'\xff\xfe'}}}
        }
        
        # This should not raise TypeError
        result = item_matches(item, item)
        self.assertTrue(result, "Identical items with nested binary attributes should match")

    def test_binary_in_list(self):
        """
        Test case 3: Binary in list
        
        Input: {'id': {'S': 'item1'}, 'items': {'L': [{'B': b'\x00\x01'}, {'S': 'text'}]}}
        Expected: Successfully serializes without TypeError
        Bug condition: containsBinaryAttribute returns true (in list)
        """
        item = {
            'id': {'S': 'item1'},
            'items': {'L': [{'B': b'\x00\x01'}, {'S': 'text'}]}
        }
        
        # This should not raise TypeError
        result = item_matches(item, item)
        self.assertTrue(result, "Identical items with binary in list should match")

    def test_multiple_binary_attributes(self):
        """
        Test case 4: Multiple binary attributes
        
        Input: {'id': {'S': 'item1'}, 'data1': {'B': b'\x00'}, 'data2': {'B': b'\x01'}}
        Expected: Successfully serializes without TypeError
        Bug condition: containsBinaryAttribute returns true (multiple)
        """
        item = {
            'id': {'S': 'item1'},
            'data1': {'B': b'\x00'},
            'data2': {'B': b'\x01'}
        }
        
        # This should not raise TypeError
        result = item_matches(item, item)
        self.assertTrue(result, "Identical items with multiple binary attributes should match")

    def test_log_diff_with_binary_attribute(self):
        """
        Test log_diff function with binary attributes.
        
        Expected: Successfully logs item without TypeError
        Bug condition: containsBinaryAttribute returns true
        """
        item = {'id': {'S': 'item1'}, 'data': {'B': b'\x00\x01\x02\x03'}}
        stream = MockStream(item, pk='id')
        
        # This should not raise TypeError
        result = log_diff('-', stream, concise_format=True)
        self.assertIsNotNone(result)
        self.assertIn('item1', result)

    def test_log_diff_full_format_with_binary(self):
        """
        Test log_diff function in full format with binary attributes.
        
        Expected: Successfully logs full item without TypeError
        Bug condition: containsBinaryAttribute returns true
        """
        item = {'id': {'S': 'item1'}, 'data': {'B': b'\xff\xfe'}}
        stream = MockStream(item, pk='id')
        
        # This should not raise TypeError
        result = log_diff('-', stream, concise_format=False)
        self.assertIsNotNone(result)
        self.assertIn('item1', result)

    def test_different_binary_items_not_matching(self):
        """
        Test that different items with binary attributes are correctly identified as different.
        
        Expected: item_matches returns False for different binary items
        """
        item1 = {'id': {'S': 'item1'}, 'data': {'B': b'\x00\x01'}}
        item2 = {'id': {'S': 'item1'}, 'data': {'B': b'\x02\x03'}}
        
        # This should not raise TypeError and should return False
        result = item_matches(item1, item2)
        self.assertFalse(result, "Different items with different binary data should not match")

    def test_binary_with_nested_structures(self):
        """
        Test complex nested structures with binary attributes at multiple levels.
        
        Expected: Successfully serializes without TypeError
        """
        item = {
            'id': {'S': 'item1'},
            'metadata': {'M': {
                'binary_field': {'B': b'\xff\xfe'},
                'nested_list': {'L': [
                    {'B': b'\x00\x01'},
                    {'S': 'text'},
                    {'M': {'inner_binary': {'B': b'\xaa\xbb'}}}
                ]}
            }}
        }
        
        # This should not raise TypeError
        result = item_matches(item, item)
        self.assertTrue(result, "Complex nested structures with binary should match")


if __name__ == '__main__':
    unittest.main()
