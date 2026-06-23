"""Unit tests for python_modules/shared/poison_pill.py.

Covers:
- PoisonPillConfig: key derivation from bucket + job_run_id
- PoisonPillDriver: cleanup deletes the S3 marker, swallows errors
- PoisonPillWorker:
  - signal() writes the poison marker to S3
  - check() returns False initially, True after signal
  - check() rate-limits S3 HEAD calls
  - is_systemic_error() classifies known fatal errors correctly
"""

import sys
import time
from unittest.mock import MagicMock, patch

import botocore.exceptions
import pytest

import python_modules.shared.poison_pill as poison_pill_module
from python_modules.shared.poison_pill import (
    PoisonPillConfig,
    PoisonPillDriver,
    PoisonPillWorker,
    _CHECK_INTERVAL_SECONDS,
)


class TestPoisonPillConfig:
    def test_key_derivation(self):
        config = PoisonPillConfig(bucket="my-bucket", job_run_id="jr_12345")
        assert config.bucket == "my-bucket"
        assert config.key == "server/poison-pill/jr_12345/poison-pill"

    def test_different_job_run_ids_produce_different_keys(self):
        c1 = PoisonPillConfig(bucket="b", job_run_id="run-a")
        c2 = PoisonPillConfig(bucket="b", job_run_id="run-b")
        assert c1.key != c2.key


class TestPoisonPillDriver:
    @patch.object(poison_pill_module, "Session")
    def test_cleanup_deletes_object(self, mock_session_cls):
        mock_s3 = MagicMock()
        mock_session_cls.return_value.client.return_value = mock_s3

        config = PoisonPillConfig(bucket="bkt", job_run_id="jr1")
        driver = PoisonPillDriver(config)
        driver.cleanup()

        mock_s3.delete_object.assert_called_once_with(Bucket="bkt", Key=config.key)

    @patch.object(poison_pill_module, "Session")
    def test_cleanup_swallows_errors(self, mock_session_cls):
        mock_s3 = MagicMock()
        mock_s3.delete_object.side_effect = Exception("network error")
        mock_session_cls.return_value.client.return_value = mock_s3

        config = PoisonPillConfig(bucket="bkt", job_run_id="jr1")
        driver = PoisonPillDriver(config)
        driver.cleanup()  # Should not raise


class TestPoisonPillWorkerSignal:
    @patch.object(poison_pill_module, "Session")
    def test_signal_writes_marker(self, mock_session_cls):
        mock_s3 = MagicMock()
        mock_session_cls.return_value.client.return_value = mock_s3

        config = PoisonPillConfig(bucket="bkt", job_run_id="jr1")
        worker = PoisonPillWorker(config)
        worker.signal("AccessDeniedException in worker 42")

        mock_s3.put_object.assert_called_once_with(
            Bucket="bkt",
            Key=config.key,
            Body=b"AccessDeniedException in worker 42",
        )

    @patch.object(poison_pill_module, "Session")
    def test_signal_sets_poisoned_flag(self, mock_session_cls):
        mock_s3 = MagicMock()
        mock_session_cls.return_value.client.return_value = mock_s3

        config = PoisonPillConfig(bucket="bkt", job_run_id="jr1")
        worker = PoisonPillWorker(config)
        worker.signal("fatal")

        assert worker.check() is True

    @patch.object(poison_pill_module, "Session")
    def test_signal_swallows_s3_errors(self, mock_session_cls):
        mock_s3 = MagicMock()
        mock_s3.put_object.side_effect = Exception("timeout")
        mock_session_cls.return_value.client.return_value = mock_s3

        config = PoisonPillConfig(bucket="bkt", job_run_id="jr1")
        worker = PoisonPillWorker(config)
        worker.signal("fatal")  # Should not raise


class TestPoisonPillWorkerCheck:
    @patch.object(poison_pill_module, "Session")
    def test_check_returns_false_when_no_marker(self, mock_session_cls):
        mock_s3 = MagicMock()
        no_such_key = type("NoSuchKey", (Exception,), {})
        mock_s3.exceptions.NoSuchKey = no_such_key
        mock_s3.head_object.side_effect = no_such_key()
        mock_session_cls.return_value.client.return_value = mock_s3

        config = PoisonPillConfig(bucket="bkt", job_run_id="jr1")
        worker = PoisonPillWorker(config)
        worker._last_check = 0.0  # Force check

        assert worker.check() is False

    @patch.object(poison_pill_module, "Session")
    def test_check_returns_true_when_marker_exists(self, mock_session_cls):
        mock_s3 = MagicMock()
        mock_s3.exceptions.NoSuchKey = type("NoSuchKey", (Exception,), {})
        mock_s3.head_object.return_value = {}  # Object exists
        mock_session_cls.return_value.client.return_value = mock_s3

        config = PoisonPillConfig(bucket="bkt", job_run_id="jr1")
        worker = PoisonPillWorker(config)
        worker._last_check = 0.0  # Force check

        assert worker.check() is True

    @patch.object(poison_pill_module, "Session")
    def test_check_rate_limits_calls(self, mock_session_cls):
        mock_s3 = MagicMock()
        no_such_key = type("NoSuchKey", (Exception,), {})
        mock_s3.exceptions.NoSuchKey = no_such_key
        mock_s3.head_object.side_effect = no_such_key()
        mock_session_cls.return_value.client.return_value = mock_s3

        config = PoisonPillConfig(bucket="bkt", job_run_id="jr1")
        worker = PoisonPillWorker(config)
        worker._last_check = time.monotonic()  # Just checked

        # Should return False without calling S3 (rate limited)
        assert worker.check() is False
        mock_s3.head_object.assert_not_called()

    @patch.object(poison_pill_module, "Session")
    def test_check_swallows_unexpected_errors(self, mock_session_cls):
        mock_s3 = MagicMock()
        mock_s3.exceptions.NoSuchKey = type("NoSuchKey", (Exception,), {})
        mock_s3.head_object.side_effect = RuntimeError("unexpected")
        mock_session_cls.return_value.client.return_value = mock_s3

        config = PoisonPillConfig(bucket="bkt", job_run_id="jr1")
        worker = PoisonPillWorker(config)
        worker._last_check = 0.0

        assert worker.check() is False  # Should not raise


class TestIsSystemicError:
    def test_access_denied_by_error_code(self):
        e = botocore.exceptions.ClientError(
            {"Error": {"Code": "AccessDeniedException", "Message": "denied"}},
            "Scan"
        )
        assert PoisonPillWorker.is_systemic_error(e) is True

    def test_validation_exception_by_error_code(self):
        e = botocore.exceptions.ClientError(
            {"Error": {"Code": "ValidationException", "Message": "bad"}},
            "Scan"
        )
        assert PoisonPillWorker.is_systemic_error(e) is True

    def test_throttle_is_not_systemic(self):
        e = botocore.exceptions.ClientError(
            {"Error": {"Code": "ProvisionedThroughputExceededException", "Message": "slow down"}},
            "Scan"
        )
        # Throttling with retries IS systemic (it means retries are exhausted)
        assert PoisonPillWorker.is_systemic_error(e) is False

    def test_module_not_found_by_class_name(self):
        e = ModuleNotFoundError("No module named 'foo'")
        assert PoisonPillWorker.is_systemic_error(e) is True

    def test_expired_token_in_message(self):
        e = Exception("Something ExpiredTokenException something")
        assert PoisonPillWorker.is_systemic_error(e) is True

    def test_conditional_check_failed_is_not_systemic(self):
        e = botocore.exceptions.ClientError(
            {"Error": {"Code": "ConditionalCheckFailedException", "Message": "condition"}},
            "UpdateItem"
        )
        assert PoisonPillWorker.is_systemic_error(e) is False

    def test_generic_exception_is_not_systemic(self):
        e = ValueError("some value error")
        assert PoisonPillWorker.is_systemic_error(e) is False
