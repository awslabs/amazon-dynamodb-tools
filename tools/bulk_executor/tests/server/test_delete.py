"""Unit tests for the `delete` server-side verb.

Covers `python_modules/delete.py`:
- Module importability and sys.path manipulation (lines 1-5)
- run() delegation to find.run() with identical arguments (lines 8-9)

The delete verb is a thin wrapper: it imports find and delegates run()
entirely. These tests verify the delegation contract.
"""

import sys
from unittest.mock import MagicMock, Mock, patch

import pytest

# find.py imports from awsglue.transforms, pyspark.sql, pyspark.sql.functions
# which conftest doesn't cover. Mock them before importing delete.
sys.modules.setdefault('awsglue.transforms', Mock())
sys.modules.setdefault('pyspark.sql', Mock())
sys.modules.setdefault('pyspark.sql.functions', Mock())

from python_modules import delete as delete_module


class TestModuleImport:
    """Lines 1-5: module imports and sys.path setup."""

    def test_module_is_importable(self):
        """Line 1-5: module loads without error under test mocking."""
        assert delete_module is not None

    def test_module_exposes_run_function(self):
        """Line 8: run() is defined and callable."""
        assert callable(delete_module.run)

    def test_module_imports_find(self):
        """Line 5: `from python_modules import find` makes find accessible."""
        assert hasattr(delete_module, 'find')


class TestRunDelegation:
    """Lines 8-9: run() passes all four arguments to find.run()."""

    def test_run_delegates_to_find_run(self, monkeypatch):
        """Line 9: find.run is called with (job, spark_context, glue_context, parsed_args)."""
        mock_find_run = MagicMock()
        monkeypatch.setattr(delete_module.find, 'run', mock_find_run)

        job = MagicMock()
        sc = MagicMock()
        gc = MagicMock()
        args = {'table': 'my-table', 'XAction': 'delete'}

        delete_module.run(job, sc, gc, args)

        mock_find_run.assert_called_once_with(job, sc, gc, args)

    def test_run_returns_none(self, monkeypatch):
        """Line 9: run() has no return statement — returns None implicitly."""
        monkeypatch.setattr(delete_module.find, 'run', MagicMock())
        result = delete_module.run(MagicMock(), MagicMock(), MagicMock(), {})
        assert result is None

    def test_run_propagates_exception_from_find(self, monkeypatch):
        """Line 9: exceptions from find.run() bubble up unmodified."""
        monkeypatch.setattr(
            delete_module.find, 'run',
            MagicMock(side_effect=RuntimeError('find exploded'))
        )
        with pytest.raises(RuntimeError, match='find exploded'):
            delete_module.run(MagicMock(), MagicMock(), MagicMock(), {})
