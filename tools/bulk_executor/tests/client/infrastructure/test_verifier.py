"""Unit tests for client/src/infrastructure/verifier.py.

Covers the three module-level functions:

- _get_glue_job_details: returns the get_job response, returns None on the
  EntityNotFoundException ClientError code, and exits on any other error
  shape.
- assert_version_parity: pulls --bulk-dynamodb-version from the persisted
  DefaultArguments and compares against __version__.__version__. Raises
  ValueError with a higher/lower-tinged message when versions disagree,
  raises ValueError when the remote version is missing entirely, and is
  a no-op when local and remote match.
- is_existing_glue_job: wraps glue_client.get_job; True on success, False
  on the EntityNotFoundException ClientError, exit on any other error.

All AWS calls go through a MagicMock glue_client. The boto3 ClientError
shape is constructed by hand because botocore's ClientError takes a
specific {'Error': {'Code': ..., 'Message': ...}} dict and an op name.
"""

from unittest.mock import MagicMock, patch

import pytest
from botocore.exceptions import ClientError

from infrastructure import verifier as verifier_module
from infrastructure.verifier import (
    _get_glue_job_details,
    assert_version_parity,
    is_existing_glue_job,
)


def _entity_not_found_error():
    """Build the canonical EntityNotFoundException ClientError instance."""
    return ClientError(
        {'Error': {'Code': 'EntityNotFoundException', 'Message': 'missing'}},
        'GetJob',
    )


def _other_client_error():
    """A ClientError with a different code, for the unexpected-error branch."""
    return ClientError(
        {'Error': {'Code': 'AccessDeniedException', 'Message': 'nope'}},
        'GetJob',
    )


class TestGetGlueJobDetails:
    """Behavior of the private _get_glue_job_details helper."""

    def test_returns_response_on_success(self):
        glue = MagicMock()
        glue.get_job.return_value = {'Job': {'Name': 'bulk_dynamodb'}}
        assert _get_glue_job_details(glue) == {'Job': {'Name': 'bulk_dynamodb'}}
        glue.get_job.assert_called_once_with(JobName='bulk_dynamodb')

    def test_returns_none_when_job_missing(self):
        glue = MagicMock()
        glue.get_job.side_effect = _entity_not_found_error()
        assert _get_glue_job_details(glue) is None

    def test_exits_on_other_client_error(self):
        glue = MagicMock()
        glue.get_job.side_effect = _other_client_error()
        with pytest.raises(SystemExit) as excinfo:
            _get_glue_job_details(glue)
        assert excinfo.value.code == 1


class TestAssertVersionParity:
    """Behavior of assert_version_parity around the persisted version arg."""

    def test_no_op_when_versions_match(self):
        from __version__ import __version__ as VERSION

        glue = MagicMock()
        glue.get_job.return_value = {
            'Job': {'DefaultArguments': {'--bulk-dynamodb-version': VERSION}}
        }
        # Matching local and remote versions should return cleanly without
        # raising. Bumping __version__ shouldn't break this test.
        assert assert_version_parity(glue, MagicMock()) is None

    def test_raises_when_remote_higher(self):
        glue = MagicMock()
        glue.get_job.return_value = {
            'Job': {'DefaultArguments': {'--bulk-dynamodb-version': '99'}}
        }
        with pytest.raises(ValueError) as excinfo:
            assert_version_parity(glue, MagicMock())
        msg = str(excinfo.value)
        assert 'must match exactly' in msg
        assert 'upgrade the local client' in msg

    def test_raises_when_remote_lower(self, monkeypatch):
        # Local version is '0', so to make remote "lower" we patch the
        # local VERSION to a higher value.
        monkeypatch.setattr(verifier_module, 'VERSION', '5')
        glue = MagicMock()
        glue.get_job.return_value = {
            'Job': {'DefaultArguments': {'--bulk-dynamodb-version': '1'}}
        }
        with pytest.raises(ValueError) as excinfo:
            assert_version_parity(glue, MagicMock())
        msg = str(excinfo.value)
        assert 'must match exactly' in msg
        assert 'new bootstrap' in msg

    def test_raises_when_remote_version_missing(self):
        # DefaultArguments present but no --bulk-dynamodb-version key.
        glue = MagicMock()
        glue.get_job.return_value = {'Job': {'DefaultArguments': {}}}
        with pytest.raises(ValueError) as excinfo:
            assert_version_parity(glue, MagicMock())
        assert 'Remote version not available' in str(excinfo.value)

    def test_raises_when_job_does_not_exist(self):
        # _get_glue_job_details returns None -> falls through to the
        # bottom-of-function ValueError.
        glue = MagicMock()
        glue.get_job.side_effect = _entity_not_found_error()
        with pytest.raises(ValueError) as excinfo:
            assert_version_parity(glue, MagicMock())
        assert 'Remote version not available' in str(excinfo.value)


class TestIsExistingGlueJob:
    """Behavior of is_existing_glue_job — True/False/exit triad."""

    def test_returns_true_when_job_exists(self):
        glue = MagicMock()
        glue.get_job.return_value = {'Job': {'Name': 'bulk_dynamodb'}}
        assert is_existing_glue_job(glue) is True
        glue.get_job.assert_called_once_with(JobName='bulk_dynamodb')

    def test_returns_false_on_entity_not_found(self):
        glue = MagicMock()
        glue.get_job.side_effect = _entity_not_found_error()
        assert is_existing_glue_job(glue) is False

    def test_exits_on_other_client_error(self):
        glue = MagicMock()
        glue.get_job.side_effect = _other_client_error()
        with pytest.raises(SystemExit) as excinfo:
            is_existing_glue_job(glue)
        assert excinfo.value.code == 1

    def test_exits_on_non_client_error_without_response(self):
        # A plain Exception has no `.response` attribute, so the
        # `hasattr(e, 'response')` guard is False and we go to the
        # else branch -> exit(1).
        glue = MagicMock()
        glue.get_job.side_effect = RuntimeError('boom')
        with pytest.raises(SystemExit) as excinfo:
            is_existing_glue_job(glue)
        assert excinfo.value.code == 1
