"""Unit tests for the default passthrough transform module.

Covers `python_modules/load_export/transform/default.py`:
- transform_full_record: passthrough that wraps a single full-export record in a list
- transform_incremental_record: passthrough that wraps a single incremental
  record in a list

These passthroughs are the no-op default used when the user does not configure
a custom transform module. They are simple but exercised by the
`load_transform_module('default')` integration test indirectly; this module
calls them directly so the function bodies are executed and counted.
"""

from unittest.mock import MagicMock

from python_modules.load_export.transform import default as default_transform


class TestTransformFullRecord:
    """Default passthrough for full-export records."""

    def test_returns_single_element_list_with_record(self):
        """The record is returned wrapped in a one-element list."""
        record = MagicMock(name='FullExportRecord')
        result = default_transform.transform_full_record(record)
        assert result == [record]

    def test_returns_list_type(self):
        """Result is a list (not a tuple/generator), per the type hint."""
        record = MagicMock(name='FullExportRecord')
        result = default_transform.transform_full_record(record)
        assert isinstance(result, list)
        assert len(result) == 1

    def test_does_not_mutate_record_identity(self):
        """The record object inside the list is the same instance passed in."""
        record = MagicMock(name='FullExportRecord')
        result = default_transform.transform_full_record(record)
        assert result[0] is record


class TestTransformIncrementalRecord:
    """Default passthrough for incremental-export records."""

    def test_returns_single_element_list_with_record(self):
        """The record is returned wrapped in a one-element list."""
        record = MagicMock(name='IncrementalExportRecord')
        result = default_transform.transform_incremental_record(record)
        assert result == [record]

    def test_returns_list_type(self):
        """Result is a list (not a tuple/generator), per the type hint."""
        record = MagicMock(name='IncrementalExportRecord')
        result = default_transform.transform_incremental_record(record)
        assert isinstance(result, list)
        assert len(result) == 1

    def test_does_not_mutate_record_identity(self):
        """The record object inside the list is the same instance passed in."""
        record = MagicMock(name='IncrementalExportRecord')
        result = default_transform.transform_incremental_record(record)
        assert result[0] is record
