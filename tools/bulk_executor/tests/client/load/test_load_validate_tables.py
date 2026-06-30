"""Unit tests for load.run() — validate_tables invocation.

PR #218 added pitr_enabled=True to the validate_tables call in load.py
(commit 187adee). The reviewer asked for a unit test proving that load
always requires PITR when validating the target table.
"""

import argparse
import importlib.util
import os
import sys
from unittest.mock import MagicMock, patch

import pytest

_CLIENT_SRC = os.path.abspath(
    os.path.join(os.path.dirname(__file__), '..', '..', '..', 'client', 'src')
)
_LOAD_PY = os.path.join(_CLIENT_SRC, 'python_modules', 'load.py')


@pytest.fixture
def load_module():
    """Import load.py from client/src without server-side collision."""
    spec = importlib.util.spec_from_file_location('client_load', _LOAD_PY)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture
def csv_args():
    """Minimal Namespace for CSV format."""
    return argparse.Namespace(
        verb='load',
        table='my-table',
        format='csv',
        s3_path='s3://bucket/path',
    )


@pytest.fixture
def json_args():
    """Minimal Namespace for JSON format."""
    return argparse.Namespace(
        verb='load',
        table='json-table',
        format='json',
        s3_path='s3://bucket/data.json',
    )


@pytest.fixture
def parquet_args():
    """Minimal Namespace for Parquet format."""
    return argparse.Namespace(
        verb='load',
        table='parquet-table',
        format='parquet',
        s3_path='s3://bucket/data.parquet',
    )


class TestLoadValidateTablesCallsPitrEnabled:
    """load.run() must call validate_tables with pitr_enabled=True."""

    def _run_load_with_args(self, load_module, fake_args):
        """Helper: run load.run() with mocked parser and utils."""
        mock_utils = MagicMock()
        mock_utils.glue_job_arguments.return_value = argparse.ArgumentParser(add_help=False)
        mock_utils.environment_arguments.return_value = argparse.ArgumentParser(add_help=False)
        mock_utils.validate_tables.return_value = None

        mock_parser = MagicMock()
        mock_parser.parse_args.return_value = fake_args

        with patch.object(load_module, 'utils', mock_utils):
            with patch.object(load_module, 'BulkArgumentParser', return_value=mock_parser):
                load_module.run({'region': 'us-east-1'})

        return mock_utils

    def test_csv_load_calls_validate_tables_with_pitr_enabled(self, load_module, csv_args):
        """CSV format load must require pitr_enabled=True."""
        mock_utils = self._run_load_with_args(load_module, csv_args)

        mock_utils.validate_tables.assert_called_once()
        _, kwargs = mock_utils.validate_tables.call_args
        assert kwargs.get('pitr_enabled') is True, (
            "load.run() must pass pitr_enabled=True to validate_tables "
            "so that bulk loads cannot target tables without PITR safety"
        )

    def test_json_load_calls_validate_tables_with_pitr_enabled(self, load_module, json_args):
        """JSON format load must require pitr_enabled=True."""
        mock_utils = self._run_load_with_args(load_module, json_args)

        mock_utils.validate_tables.assert_called_once()
        _, kwargs = mock_utils.validate_tables.call_args
        assert kwargs.get('pitr_enabled') is True, (
            "JSON format loads must also require pitr_enabled=True"
        )

    def test_parquet_load_calls_validate_tables_with_pitr_enabled(self, load_module, parquet_args):
        """Parquet format load must require pitr_enabled=True."""
        mock_utils = self._run_load_with_args(load_module, parquet_args)

        mock_utils.validate_tables.assert_called_once()
        _, kwargs = mock_utils.validate_tables.call_args
        assert kwargs.get('pitr_enabled') is True, (
            "Parquet format loads must also require pitr_enabled=True"
        )

    def test_validate_tables_receives_correct_table_name(self, load_module, csv_args):
        """validate_tables must receive the table name from parsed args."""
        mock_utils = self._run_load_with_args(load_module, csv_args)

        args, _ = mock_utils.validate_tables.call_args
        assert 'my-table' in args, (
            "validate_tables must receive the table name from the parsed arguments"
        )
