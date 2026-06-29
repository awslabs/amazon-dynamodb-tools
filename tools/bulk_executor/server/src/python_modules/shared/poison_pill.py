import time

from boto3 import Session

from .logger import log

_POISON_KEY_SUFFIX = "poison-pill"
_CHECK_INTERVAL_SECONDS = 5


class _NoOpPoisonPill:
    """Placeholder when no poison-pill config is provided. All operations are no-ops."""

    def signal(self, reason):
        pass

    def check(self):
        return False


_NOOP = _NoOpPoisonPill()


class PoisonPillConfig:
    """Shared configuration for poison-pill coordination between driver and workers."""

    def __init__(self, bucket, job_run_id):
        self.bucket = bucket
        self.key = f"server/poison-pill/{job_run_id}/{_POISON_KEY_SUFFIX}"


class PoisonPillDriver:
    """Driver-side poison-pill lifecycle: cleanup on shutdown."""

    def __init__(self, config):
        self._config = config
        self._s3 = Session().client("s3")

    def cleanup(self):
        try:
            self._s3.delete_object(Bucket=self._config.bucket, Key=self._config.key)
        except Exception:
            pass


class PoisonPillWorker:
    """
    Worker-side poison-pill: signal fatal errors and check for abort.

    Call `check()` between scan pages. It rate-limits S3 reads to at most
    once per _CHECK_INTERVAL_SECONDS. Call `signal(reason)` when this worker
    hits a non-recoverable systemic error.
    """

    SYSTEMIC_ERRORS = frozenset([
        "AccessDeniedException",
        "ModuleNotFoundError",
        "OutOfMemoryError",
        "ValidationException",
        "ResourceNotFoundException",
        "ExpiredTokenException",
    ])

    def __init__(self, config):
        self._config = config
        self._s3 = Session().client("s3")
        self._last_check = 0.0
        self._poisoned = False

    def signal(self, reason):
        """Write the poison marker so all other workers abort."""
        try:
            self._s3.put_object(
                Bucket=self._config.bucket,
                Key=self._config.key,
                Body=reason.encode("utf-8"),
            )
        except Exception:
            pass
        self._poisoned = True

    def check(self):
        """
        Return True if the job has been poisoned (another worker signaled abort).

        Rate-limits the S3 HEAD call to once per _CHECK_INTERVAL_SECONDS.
        """
        if self._poisoned:
            return True

        now = time.monotonic()
        if now - self._last_check < _CHECK_INTERVAL_SECONDS:
            return False

        self._last_check = now
        try:
            self._s3.head_object(Bucket=self._config.bucket, Key=self._config.key)
            self._poisoned = True
            return True
        except self._s3.exceptions.NoSuchKey:
            return False
        except Exception:
            return False

    @classmethod
    def is_systemic_error(cls, error):
        """Determine if an exception represents a non-recoverable systemic error."""
        error_str = str(type(error).__name__)
        if error_str in cls.SYSTEMIC_ERRORS:
            return True

        if hasattr(error, "response") and error.response:
            error_code = error.response.get("Error", {}).get("Code", "")
            if error_code in cls.SYSTEMIC_ERRORS:
                return True

        msg = str(error)
        return any(key in msg for key in cls.SYSTEMIC_ERRORS)
