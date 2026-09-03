"""Unit tests for copy/transform/pii_redact.py.

Covers transform_item: redacting the named PII attributes in place, leaving
other attributes alone, case-sensitive matching, and the list-only return.
"""
from python_modules.copy.transform.pii_redact import transform_item, REDACTED_VALUE


class TestTransformItem:

    def test_redacts_all_pii_attributes_present(self):
        item = {"Id": 42, "Name": "Alice", "Email": "alice@example.com", "status": "active"}
        result = transform_item(item)
        assert result == [item]
        assert item["Name"] == REDACTED_VALUE
        assert item["Email"] == REDACTED_VALUE
        assert item["status"] == "active", "non-PII attributes are left alone"
        assert item["Id"] == 42

    def test_missing_pii_attributes_are_skipped(self):
        item = {"Id": 42, "status": "active"}
        result = transform_item(item)
        assert result == [{"Id": 42, "status": "active"}]

    def test_partial_pii_attributes_present(self):
        item = {"Id": 42, "Name": "Bob"}
        result = transform_item(item)
        assert result == [{"Id": 42, "Name": REDACTED_VALUE}]

    def test_modifies_in_place_and_returns_same_object(self):
        item = {"Id": 42, "Name": "Alice"}
        result = transform_item(item)
        assert result[0] is item, "item is modified and returned in place, not copied"

    def test_case_sensitive_attribute_matching(self):
        item = {"Id": 42, "name": "Alice"}  # lowercase, not in PII_ATTRIBUTES
        result = transform_item(item)
        assert result[0]["name"] == "Alice", "matching is case-sensitive"
