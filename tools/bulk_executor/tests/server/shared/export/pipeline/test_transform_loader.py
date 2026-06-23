"""Unit tests for transform_loader.load_transform_module."""
import pytest
from python_modules.shared.export.pipeline.transform_loader import load_transform_module


class TestTransformLoader:

    def test_load_default_module(self):
        module = load_transform_module('default', 'python_modules.load_export.transform')
        assert hasattr(module, 'transform_full_record')
        assert hasattr(module, 'transform_incremental_record')

    def test_load_example_module(self):
        module = load_transform_module('load_only_active', 'python_modules.load_export.transform')
        assert hasattr(module, 'transform_full_record')
        assert hasattr(module, 'transform_incremental_record')

    def test_default_full_is_callable(self):
        module = load_transform_module('default', 'python_modules.load_export.transform')
        assert callable(module.transform_full_record)

    def test_default_incremental_is_callable(self):
        module = load_transform_module('default', 'python_modules.load_export.transform')
        assert callable(module.transform_incremental_record)

    def test_load_nonexistent_module(self):
        with pytest.raises(ImportError):
            load_transform_module('nonexistent_module', 'python_modules.load_export.transform')
