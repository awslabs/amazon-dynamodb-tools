"""Tests for the top-level error handling in the `bulk` driver script.

Verifies that known AWS errors (ClientError, BotoCoreError) produce a clean
one-line message + exit(1) instead of a full Python stack trace.

The `bulk` script wraps its main execution in a try/except for ClientError
and BotoCoreError. These tests exercise that code path by running the script
as a subprocess from the correct working directory.
"""

import subprocess
import sys
import os
import tempfile

import pytest

BULK_DIR = os.path.abspath(
    os.path.join(os.path.dirname(__file__), '..', '..')
)


def _run_bulk_subprocess(inject_code):
    """Run the bulk script with injected setup code.

    The wrapper adds necessary paths and patches, then exec's the bulk script.
    """
    preamble = (
        "import sys, os\n"
        f"os.chdir('{BULK_DIR}')\n"
        f"sys.path.insert(0, '{BULK_DIR}')\n"
        f"sys.path.insert(0, '{BULK_DIR}/client/src')\n"
    )
    script = preamble + inject_code + "\nexec(compile(open('bulk').read(), 'bulk', 'exec'))\n"

    with tempfile.NamedTemporaryFile(mode='w', suffix='.py', delete=False) as f:
        f.write(script)
        f.flush()
        try:
            result = subprocess.run(
                [sys.executable, f.name],
                capture_output=True,
                text=True,
                cwd=BULK_DIR,
                timeout=10,
            )
        finally:
            os.unlink(f.name)
    return result


INJECT_CLIENT_ERROR_ACCESS_DENIED = """\
from unittest.mock import patch, MagicMock
import botocore.exceptions

sys.argv = ['bulk', 'copy', '--source', 'src-tbl', '--target', 'dst-tbl']

error_response = {
    'Error': {
        'Code': 'AccessDeniedException',
        'Message': 'Not authorized to perform dynamodb:DescribeTable on table/src-tbl'
    }
}

mock_env = MagicMock(aws_region='us-east-1', aws_account_id='123456789012')

def fake_action(env_configs):
    raise botocore.exceptions.ClientError(error_response, 'DescribeTable')

patch('env_configs.EnvConfigs', return_value=mock_env).start()
patch('utils.logger.init').start()
_m = patch('importlib.import_module').start()
_mod = MagicMock()
_mod.run = fake_action
_m.return_value = _mod
"""

INJECT_CLIENT_ERROR_RESOURCE_NOT_FOUND = """\
from unittest.mock import patch, MagicMock
import botocore.exceptions

sys.argv = ['bulk', 'copy', '--source', 'src-tbl', '--target', 'dst-tbl']

error_response = {
    'Error': {
        'Code': 'ResourceNotFoundException',
        'Message': 'Requested resource not found: Table: nonexistent-table not found'
    }
}

mock_env = MagicMock(aws_region='us-east-1', aws_account_id='123456789012')

def fake_action(env_configs):
    raise botocore.exceptions.ClientError(error_response, 'DescribeTable')

patch('env_configs.EnvConfigs', return_value=mock_env).start()
patch('utils.logger.init').start()
_m = patch('importlib.import_module').start()
_mod = MagicMock()
_mod.run = fake_action
_m.return_value = _mod
"""

INJECT_BOTOCORE_ENDPOINT_ERROR = """\
from unittest.mock import patch, MagicMock
import botocore.exceptions

sys.argv = ['bulk', 'copy', '--source', 'src-tbl', '--target', 'dst-tbl']

mock_env = MagicMock(aws_region='us-east-1', aws_account_id='123456789012')

def fake_action(env_configs):
    raise botocore.exceptions.EndpointConnectionError(
        endpoint_url='https://dynamodb.bad-region.amazonaws.com')

patch('env_configs.EnvConfigs', return_value=mock_env).start()
patch('utils.logger.init').start()
_m = patch('importlib.import_module').start()
_mod = MagicMock()
_mod.run = fake_action
_m.return_value = _mod
"""

INJECT_TYPE_ERROR = """\
from unittest.mock import patch, MagicMock

sys.argv = ['bulk', 'copy', '--source', 'src-tbl', '--target', 'dst-tbl']

mock_env = MagicMock(aws_region='us-east-1', aws_account_id='123456789012')

def fake_action(env_configs):
    raise TypeError("something internal broke")

patch('env_configs.EnvConfigs', return_value=mock_env).start()
patch('utils.logger.init').start()
_m = patch('importlib.import_module').start()
_mod = MagicMock()
_mod.run = fake_action
_m.return_value = _mod
"""


class TestClientErrorHandling:
    """Verify ClientError triggers clean message + exit(1), no stack trace."""

    def test_access_denied_shows_code_and_message(self):
        result = _run_bulk_subprocess(INJECT_CLIENT_ERROR_ACCESS_DENIED)
        assert result.returncode == 1
        assert 'Traceback' not in result.stderr
        assert 'AccessDeniedException' in result.stderr
        assert 'Not authorized' in result.stderr
        assert 'Job failed.' in result.stderr

    def test_resource_not_found_shows_clean_error(self):
        result = _run_bulk_subprocess(INJECT_CLIENT_ERROR_RESOURCE_NOT_FOUND)
        assert result.returncode == 1
        assert 'Traceback' not in result.stderr
        assert 'ResourceNotFoundException' in result.stderr
        assert 'Job failed.' in result.stderr


class TestBotoCoreErrorHandling:
    """Verify BotoCoreError (non-ClientError) triggers clean message."""

    def test_endpoint_connection_error_clean_output(self):
        result = _run_bulk_subprocess(INJECT_BOTOCORE_ENDPOINT_ERROR)
        assert result.returncode == 1
        assert 'Traceback' not in result.stderr
        assert 'Job failed.' in result.stderr

    def test_non_aws_error_still_shows_traceback(self):
        """Non-AWS errors (e.g. TypeError) should still produce a traceback
        so bugs are visible during development."""
        result = _run_bulk_subprocess(INJECT_TYPE_ERROR)
        assert result.returncode == 1
        assert 'Traceback' in result.stderr
        assert 'TypeError' in result.stderr
