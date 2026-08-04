"""Unit tests for the client sql command module (client/src/python_modules/sql.py).

Named test_sql_client.py (not test_sql.py) to avoid a basename collision with the
server-side tests/server/test_sql.py under pytest's rootdir import mode.

Covers:
- positive_int: argparse type for --limit (valid, non-integer, zero, negative)
- run(): a non-positive --limit is rejected client-side (argparse SystemExit)
  before any Glue job is launched, matching the server's defense-in-depth check.
"""

import importlib.util
import os
import sys
from unittest.mock import patch, MagicMock

import argparse
import pytest

CLIENT_SRC = os.path.join(os.path.dirname(__file__), '..', '..', 'client', 'src')


def _import_client_sql():
    """Import client/src/python_modules/sql.py without colliding with server's package."""
    path = os.path.join(CLIENT_SRC, 'python_modules', 'sql.py')
    spec = importlib.util.spec_from_file_location('client_sql', os.path.abspath(path))
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class TestPositiveInt:
    """positive_int rejects non-integers and non-positive values for --limit."""

    def test_valid_positive_returns_int(self):
        sql = _import_client_sql()
        assert sql.positive_int('100') == 100

    def test_non_integer_raises(self):
        sql = _import_client_sql()
        with pytest.raises(argparse.ArgumentTypeError, match="must be an integer"):
            sql.positive_int('abc')

    def test_zero_raises(self):
        sql = _import_client_sql()
        with pytest.raises(argparse.ArgumentTypeError, match="must be a positive integer"):
            sql.positive_int('0')

    def test_negative_raises(self):
        sql = _import_client_sql()
        with pytest.raises(argparse.ArgumentTypeError, match="must be a positive integer"):
            sql.positive_int('-5')


class TestRunLimitValidation:
    """A bad --limit is caught client-side (argparse SystemExit) before launch."""

    @patch('utils.validate_tables')
    def test_negative_limit_rejected_before_launch(self, mock_validate_tables):
        sql = _import_client_sql()
        env_configs = MagicMock()
        with patch(
            'sys.argv',
            ['bulk', 'sql', '--table', 'my-table',
             '--query', 'SELECT * FROM my-table', '--limit', '-1'],
        ):
            # argparse exits (code 2) on an invalid --limit type before run()
            # reaches table validation.
            with pytest.raises(SystemExit):
                sql.run(env_configs)
        # We never got as far as validating the table / launching.
        mock_validate_tables.assert_not_called()

    @patch('utils.validate_tables')
    def test_valid_limit_passes_argparse(self, mock_validate_tables):
        sql = _import_client_sql()
        mock_validate_tables.return_value = None
        env_configs = MagicMock()
        with patch(
            'sys.argv',
            ['bulk', 'sql', '--table', 'my-table',
             '--query', 'SELECT * FROM my-table', '--limit', '10'],
        ):
            ok, result = sql.run(env_configs)
        assert ok is True
        assert result['limit'] == 10
        mock_validate_tables.assert_called_once()
