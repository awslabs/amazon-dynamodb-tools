"""Unit tests for the `count` server-side verb.

Covers `python_modules/count.py`:
- Module importability (lines 1-5: sys.path manipulation and find import)
- run(): delegates to find.run with identical arguments (lines 8-9)

count.py is a thin wrapper — it imports find and calls find.run().
"""

import sys
from unittest.mock import MagicMock, Mock

import pytest

# find.py (imported by count.py) needs these additional mocks beyond conftest
sys.modules.setdefault('awsglue.transforms', Mock())
sys.modules.setdefault('pyspark.sql', Mock())
sys.modules.setdefault('pyspark.sql.functions', Mock())

from python_modules import count as count_module


class TestModuleImport:
    """count.py modifies sys.path and imports find at module level."""

    def test_module_is_importable(self):
        """Lines 1-5: module loads without error under test mocks."""
        assert count_module is not None

    def test_module_exposes_run_function(self):
        """Line 8: run() is the public entry point."""
        assert callable(count_module.run)


class TestRun:
    """count.run() delegates entirely to find.run() with the same args."""

    def test_delegates_to_find_run(self, monkeypatch):
        """Lines 8-9: run() calls find.run with all four positional args."""
        mock_find_run = MagicMock()
        monkeypatch.setattr(count_module.find, 'run', mock_find_run)

        job = MagicMock()
        sc = MagicMock()
        gc = MagicMock()
        args = {'table': 'my-table'}

        count_module.run(job, sc, gc, args)

        mock_find_run.assert_called_once_with(job, sc, gc, args)

    def test_returns_none_implicitly(self, monkeypatch):
        """Line 9: no return statement — run() returns None."""
        monkeypatch.setattr(count_module.find, 'run', MagicMock())
        result = count_module.run(MagicMock(), MagicMock(), MagicMock(), {})
        assert result is None

    def test_propagates_exception_from_find(self, monkeypatch):
        """Line 9: exceptions from find.run bubble up unhandled."""
        monkeypatch.setattr(
            count_module.find, 'run',
            MagicMock(side_effect=RuntimeError('find exploded'))
        )
        with pytest.raises(RuntimeError, match='find exploded'):
            count_module.run(MagicMock(), MagicMock(), MagicMock(), {})
