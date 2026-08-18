from python_modules.copy_transform.pii_redact import transform_item, REDACTED_VALUE


class TestTransformItem:

    def test_redacts_all_pii_attributes_present(self):
        item = {"Id": 42, "Name": "Alice", "Email": "alice@example.com", "status": "active"}
        result = transform_item(item)
        assert result["Name"] == REDACTED_VALUE
        assert result["Email"] == REDACTED_VALUE
        assert result["status"] == "active", "non-PII attributes are left alone"
        assert result["Id"] == 42

    def test_missing_pii_attributes_are_skipped(self):
        item = {"Id": 42, "status": "active"}
        result = transform_item(item)
        assert result == {"Id": 42, "status": "active"}

    def test_partial_pii_attributes_present(self):
        item = {"Id": 42, "Name": "Bob"}
        result = transform_item(item)
        assert result == {"Id": 42, "Name": REDACTED_VALUE}

    def test_returns_the_same_item_object(self):
        item = {"Id": 42, "Name": "Alice"}
        result = transform_item(item)
        assert result is item, "item is modified and returned in place, not copied"

    def test_case_sensitive_attribute_matching(self):
        item = {"Id": 42, "name": "Alice"}  # lowercase, not in PII_ATTRIBUTES
        result = transform_item(item)
        assert result["name"] == "Alice", "matching is case-sensitive"
