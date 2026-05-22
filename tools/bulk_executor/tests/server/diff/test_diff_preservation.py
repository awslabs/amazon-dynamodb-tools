"""
Preservation property tests for DynamoDB non-binary attribute serialization.

**Validates: Requirements 3.1, 3.2, 3.3, 3.4**

These tests verify that the diff module correctly handles DynamoDB items
WITHOUT binary attributes. These tests establish the baseline behavior that
must be preserved after the fix is applied.

The tests use property-based testing to generate many test cases automatically,
ensuring that non-binary items continue to serialize and compare correctly
after the binary attribute fix is implemented.

Preservation Requirements:
- 3.1: Items with only standard attributes (strings, numbers, booleans, lists, maps)
       must continue to serialize and compare correctly
- 3.2: Nested structures with standard attributes must continue to serialize correctly
- 3.3: Identical non-binary items must continue to be identified as matching
- 3.4: Different non-binary items must continue to be identified as different
"""

import json
import unittest

from hypothesis import given, strategies as st

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


# Strategy for generating DynamoDB string attributes
def dynamodb_string():
    """Generate a DynamoDB string attribute: {'S': 'value'}"""
    return st.builds(lambda s: {'S': s}, st.text())


# Strategy for generating DynamoDB number attributes
def dynamodb_number():
    """Generate a DynamoDB number attribute: {'N': '123'}"""
    return st.builds(lambda n: {'N': str(n)}, st.integers())


# Strategy for generating DynamoDB boolean attributes
def dynamodb_boolean():
    """Generate a DynamoDB boolean attribute: {'BOOL': True/False}"""
    return st.builds(lambda b: {'BOOL': b}, st.booleans())


# Strategy for generating DynamoDB null attributes
def dynamodb_null():
    """Generate a DynamoDB null attribute: {'NULL': True}"""
    return st.just({'NULL': True})


# Forward declaration for recursive strategies
dynamodb_value = None


# Strategy for generating DynamoDB map attributes (nested)
def dynamodb_map():
    """Generate a DynamoDB map attribute: {'M': {...}}"""
    # Use a limited depth to avoid infinite recursion
    return st.builds(
        lambda m: {'M': m},
        st.dictionaries(
            st.text(min_size=1),
            st.deferred(lambda: dynamodb_value),
            max_size=3
        )
    )


# Strategy for generating DynamoDB list attributes (nested)
def dynamodb_list():
    """Generate a DynamoDB list attribute: {'L': [...]}"""
    # Use a limited depth to avoid infinite recursion
    return st.builds(
        lambda lst: {'L': lst},
        st.lists(
            st.deferred(lambda: dynamodb_value),
            max_size=3
        )
    )


# Define the recursive strategy for non-binary DynamoDB values
dynamodb_value = st.one_of(
    dynamodb_string(),
    dynamodb_number(),
    dynamodb_boolean(),
    dynamodb_null(),
    dynamodb_map(),
    dynamodb_list()
)


# Strategy for generating complete non-binary DynamoDB items
def non_binary_dynamodb_item():
    """Generate a complete non-binary DynamoDB item with required pk."""
    return st.builds(
        lambda pk_val, attrs: {
            'id': {'S': pk_val},
            **attrs
        },
        st.text(min_size=1),
        st.dictionaries(
            st.text(min_size=1).filter(lambda x: x != 'id'),
            dynamodb_value,
            max_size=5
        )
    )


class TestPreservationNonBinaryItems(unittest.TestCase):
    """
    Property-based tests for preservation of non-binary item behavior.
    
    These tests verify that items without binary attributes continue to
    serialize and compare correctly after the fix is applied.
    """

    def test_non_binary_item_with_strings_numbers_booleans(self):
        """
        Test case 1: Non-binary items with strings, numbers, booleans
        
        Requirement 3.1: Items with only standard attributes must continue
        to serialize and compare correctly.
        
        Input: {'id': {'S': 'item1'}, 'count': {'N': '42'}, 'active': {'BOOL': True}}
        Expected: Successfully serializes and compares
        """
        item = {
            'id': {'S': 'item1'},
            'count': {'N': '42'},
            'active': {'BOOL': True}
        }
        
        # Should serialize without error
        result = item_matches(item, item)
        self.assertTrue(result, "Identical non-binary items should match")
        
        # Should produce valid JSON
        json_str = json.dumps(item, sort_keys=True)
        self.assertIsInstance(json_str, str)
        self.assertIn('item1', json_str)
        self.assertIn('42', json_str)

    def test_nested_maps_without_binary(self):
        """
        Test case 2: Nested maps without binary
        
        Requirement 3.2: Nested structures with standard attributes must
        continue to serialize correctly.
        
        Input: {'id': {'S': 'item1'}, 'metadata': {'M': {'name': {'S': 'test'}, 'value': {'N': '10'}}}}
        Expected: Successfully serializes and compares
        """
        item = {
            'id': {'S': 'item1'},
            'metadata': {'M': {
                'name': {'S': 'test'},
                'value': {'N': '10'}
            }}
        }
        
        # Should serialize without error
        result = item_matches(item, item)
        self.assertTrue(result, "Identical nested items should match")
        
        # Should produce valid JSON
        json_str = json.dumps(item, sort_keys=True)
        self.assertIsInstance(json_str, str)
        self.assertIn('test', json_str)

    def test_lists_without_binary(self):
        """
        Test case 3: Lists without binary
        
        Requirement 3.1: Items with lists of standard attributes must
        continue to serialize correctly.
        
        Input: {'id': {'S': 'item1'}, 'items': {'L': [{'S': 'a'}, {'N': '1'}, {'BOOL': True}]}}
        Expected: Successfully serializes and compares
        """
        item = {
            'id': {'S': 'item1'},
            'items': {'L': [
                {'S': 'a'},
                {'N': '1'},
                {'BOOL': True}
            ]}
        }
        
        # Should serialize without error
        result = item_matches(item, item)
        self.assertTrue(result, "Identical items with lists should match")
        
        # Should produce valid JSON
        json_str = json.dumps(item, sort_keys=True)
        self.assertIsInstance(json_str, str)

    def test_identical_non_binary_items_match(self):
        """
        Test case 5: Identical non-binary items should match
        
        Requirement 3.3: Identical non-binary items must continue to be
        identified as matching.
        
        Expected: item_matches returns True for identical items
        """
        item1 = {
            'id': {'S': 'item1'},
            'count': {'N': '42'},
            'active': {'BOOL': True}
        }
        item2 = {
            'id': {'S': 'item1'},
            'count': {'N': '42'},
            'active': {'BOOL': True}
        }
        
        result = item_matches(item1, item2)
        self.assertTrue(result, "Identical non-binary items should match")

    def test_different_non_binary_items_not_match(self):
        """
        Test case 6: Different non-binary items should not match
        
        Requirement 3.4: Different non-binary items must continue to be
        identified as different.
        
        Expected: item_matches returns False for different items
        """
        item1 = {
            'id': {'S': 'item1'},
            'count': {'N': '42'}
        }
        item2 = {
            'id': {'S': 'item1'},
            'count': {'N': '43'}
        }
        
        result = item_matches(item1, item2)
        self.assertFalse(result, "Different non-binary items should not match")

    def test_log_diff_with_non_binary_items(self):
        """
        Test log_diff function with non-binary items.
        
        Requirement 3.1: Non-binary items must continue to serialize correctly
        in log_diff function.
        
        Expected: Successfully logs item without error
        """
        item = {
            'id': {'S': 'item1'},
            'count': {'N': '42'}
        }
        stream = MockStream(item, pk='id')
        
        # Should serialize without error
        result = log_diff('-', stream, concise_format=True)
        self.assertIsNotNone(result)
        self.assertIn('item1', result)

    def test_log_diff_full_format_with_non_binary(self):
        """
        Test log_diff function in full format with non-binary items.
        
        Requirement 3.2: Nested non-binary items must continue to serialize
        correctly in full format.
        
        Expected: Successfully logs full item without error
        """
        item = {
            'id': {'S': 'item1'},
            'metadata': {'M': {'name': {'S': 'test'}}}
        }
        stream = MockStream(item, pk='id')
        
        # Should serialize without error
        result = log_diff('-', stream, concise_format=False)
        self.assertIsNotNone(result)
        self.assertIn('item1', result)
        self.assertIn('test', result)

    @given(non_binary_dynamodb_item())
    def test_property_non_binary_item_serializes(self, item):
        """
        Property: Any non-binary DynamoDB item should serialize successfully.
        
        Requirement 3.1: Items with only standard attributes must continue
        to serialize and compare correctly.
        
        This property-based test generates many random non-binary items and
        verifies they all serialize without error.
        """
        # Should not raise any exception
        try:
            json_str = json.dumps(item, sort_keys=True)
            self.assertIsInstance(json_str, str)
        except TypeError as e:
            self.fail(f"Non-binary item should serialize without TypeError: {e}")

    @given(non_binary_dynamodb_item())
    def test_property_identical_non_binary_items_match(self, item):
        """
        Property: Identical non-binary items should always match.
        
        Requirement 3.3: Identical non-binary items must continue to be
        identified as matching.
        
        This property-based test verifies that comparing an item with itself
        always returns True.
        """
        result = item_matches(item, item)
        self.assertTrue(result, "Identical non-binary items should always match")

    @given(non_binary_dynamodb_item(), non_binary_dynamodb_item())
    def test_property_non_binary_item_comparison_consistency(self, item1, item2):
        """
        Property: Non-binary item comparison should be consistent.
        
        Requirement 3.3, 3.4: Item comparison must be consistent and
        deterministic.
        
        This property-based test verifies that comparing two items multiple
        times produces the same result.
        """
        result1 = item_matches(item1, item2)
        result2 = item_matches(item1, item2)
        self.assertEqual(result1, result2, "Item comparison should be consistent")

    @given(non_binary_dynamodb_item())
    def test_property_non_binary_item_log_diff_concise(self, item):
        """
        Property: Non-binary items should log successfully in concise format.
        
        Requirement 3.1: Non-binary items must continue to serialize correctly
        in log_diff function.
        
        This property-based test verifies that any non-binary item can be
        logged in concise format without error.
        """
        stream = MockStream(item, pk='id')
        try:
            result = log_diff('-', stream, concise_format=True)
            self.assertIsInstance(result, str)
        except TypeError as e:
            self.fail(f"Non-binary item should log without TypeError: {e}")

    @given(non_binary_dynamodb_item())
    def test_property_non_binary_item_log_diff_full(self, item):
        """
        Property: Non-binary items should log successfully in full format.
        
        Requirement 3.2: Nested non-binary items must continue to serialize
        correctly in full format.
        
        This property-based test verifies that any non-binary item can be
        logged in full format without error.
        """
        stream = MockStream(item, pk='id')
        try:
            result = log_diff('-', stream, concise_format=False)
            self.assertIsInstance(result, str)
        except TypeError as e:
            self.fail(f"Non-binary item should log without TypeError: {e}")


class TestPreservationComplexStructures(unittest.TestCase):
    """
    Additional preservation tests for complex nested structures.
    """

    def test_deeply_nested_non_binary_structure(self):
        """
        Test case 4: Mixed nested structures with multiple levels
        
        Requirement 3.2: Complex nested structures with multiple levels
        must continue to serialize correctly.
        
        Expected: Successfully serializes and compares
        """
        item = {
            'id': {'S': 'item1'},
            'level1': {'M': {
                'level2': {'M': {
                    'level3': {'L': [
                        {'S': 'text'},
                        {'N': '123'},
                        {'BOOL': False}
                    ]}
                }}
            }}
        }
        
        # Should serialize without error
        result = item_matches(item, item)
        self.assertTrue(result, "Deeply nested non-binary items should match")
        
        # Should produce valid JSON
        json_str = json.dumps(item, sort_keys=True)
        self.assertIsInstance(json_str, str)

    def test_multiple_attributes_non_binary(self):
        """
        Test that items with many attributes serialize correctly.
        
        Requirement 3.1: Items with multiple standard attributes must
        continue to serialize correctly.
        """
        item = {
            'id': {'S': 'item1'},
            'attr1': {'S': 'value1'},
            'attr2': {'N': '100'},
            'attr3': {'BOOL': True},
            'attr4': {'NULL': True},
            'attr5': {'L': [{'S': 'a'}, {'N': '1'}]},
            'attr6': {'M': {'nested': {'S': 'value'}}}
        }
        
        result = item_matches(item, item)
        self.assertTrue(result, "Items with many attributes should match")

    def test_empty_nested_structures(self):
        """
        Test that empty maps and lists serialize correctly.
        
        Requirement 3.1: Empty nested structures must serialize correctly.
        """
        item = {
            'id': {'S': 'item1'},
            'empty_map': {'M': {}},
            'empty_list': {'L': []}
        }
        
        result = item_matches(item, item)
        self.assertTrue(result, "Items with empty nested structures should match")


if __name__ == '__main__':
    unittest.main()
