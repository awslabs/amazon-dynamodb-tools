"""Unit tests for filter_loader.load_filter_function."""
import pytest
from python_modules.ddb_import.filter.filter_loader import load_filter_function


class TestFilterLoader:

    def test_load_example_filter_item(self):
        """Loading example.filter_item should return a callable."""
        fn = load_filter_function('example', 'filter_item')
        assert callable(fn)

    def test_load_example_filter_by_pk_prefix(self):
        """Loading example.filter_by_pk_prefix should return a callable."""
        fn = load_filter_function('example', 'filter_by_pk_prefix')
        assert callable(fn)

    def test_load_default_filter_item(self):
        """Loading default.filter_item should return a callable."""
        fn = load_filter_function('default', 'filter_item')
        assert callable(fn)

    def test_load_nonexistent_module(self):
        with pytest.raises(ImportError):
            load_filter_function('nonexistent_module', 'filter_item')

    def test_load_nonexistent_function(self):
        with pytest.raises(AttributeError):
            load_filter_function('example', 'nonexistent_function')
