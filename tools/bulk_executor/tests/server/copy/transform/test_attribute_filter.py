"""Unit tests for copy/transform/attribute_filter.py.

Covers transform_item: the list-only return contract ([item] to keep, []
to skip), attribute matching, and case sensitivity.
"""
from python_modules.copy.transform.attribute_filter import transform_item


class TestTransformItem:

    def test_matching_item_kept(self):
        item = {"pk": "user1", "status": "active", "data": "x"}
        result = transform_item(item)
        assert result == [item]

    def test_non_matching_item_filtered(self):
        item = {"pk": "user1", "status": "inactive"}
        result = transform_item(item)
        assert result == []

    def test_missing_attribute_filtered(self):
        item = {"pk": "user1", "data": "x"}
        result = transform_item(item)
        assert result == []

    def test_case_sensitive_value_matching(self):
        item = {"pk": "user1", "status": "Active"}
        result = transform_item(item)
        assert result == []
