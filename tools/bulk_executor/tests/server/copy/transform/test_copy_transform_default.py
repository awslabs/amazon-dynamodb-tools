"""Unit tests for copy/transform/default.py.

Covers transform_item: the no-op passthrough returns the item unchanged and
in place, mirroring load_export/transform/default.py.
"""
from python_modules.copy.transform.default import transform_item


class TestTransformItem:

    def test_returns_item_unchanged(self):
        item = {"pk": "user1", "status": "active", "n": 3}
        result = transform_item(item)
        assert result == {"pk": "user1", "status": "active", "n": 3}

    def test_returns_the_same_object(self):
        item = {"pk": "user1"}
        assert transform_item(item) is item
