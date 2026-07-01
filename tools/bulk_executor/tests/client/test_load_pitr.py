"""Unit tests for the load command module (client/src/python_modules/load.py).

Covers:
- validate_tables is called with pitr_enabled=True (safety gate)
"""

import importlib
import importlib.util
import os
import sys
from unittest.mock import patch, MagicMock

import pytest

CLIENT_SRC = os.path.join(os.path.dirname(__file__), '..', '..', 'client', 'src')


def _import_client_load():
    """Import client/src/python_modules/load.py without colliding with server's package."""
    path = os.path.join(CLIENT_SRC, 'python_modules', 'load.py')
    spec = importlib.util.spec_from_file_location('client_load', os.path.abspath(path))
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class TestLoadPitrSafetyGate:
    """Load MUST require PITR enabled on the target table."""

    @patch('utils.validate_tables')
    def test_load_passes_pitr_enabled_true(self, mock_validate_tables):
        """validate_tables must be called with pitr_enabled=True for load."""
        mock_validate_tables.return_value = None
        load = _import_client_load()

        env_configs = MagicMock()
        with patch(
            'sys.argv',
            ['bulk', 'load', '--table', 'my-table', '--format', 'json', '--s3-path', 's3://bucket/path'],
        ):
            load.run(env_configs)

        mock_validate_tables.assert_called_once()
        call_kwargs = mock_validate_tables.call_args
        assert call_kwargs.kwargs.get('pitr_enabled') is True, \
            "load must call validate_tables with pitr_enabled=True"

    @patch('utils.validate_tables')
    def test_load_aborts_when_pitr_disabled(self, mock_validate_tables):
        """Load aborts with clear error when PITR is off (via validate_tables SystemExit)."""
        mock_validate_tables.side_effect = SystemExit(
            "For safety, point in time recovery (PITR) must be enabled for table 'my-table'"
            " before performing bulk mutations against it"
        )
        load = _import_client_load()

        env_configs = MagicMock()
        with patch(
            'sys.argv',
            ['bulk', 'load', '--table', 'my-table', '--format', 'json', '--s3-path', 's3://bucket/path'],
        ):
            with pytest.raises(SystemExit, match="point in time recovery"):
                load.run(env_configs)
