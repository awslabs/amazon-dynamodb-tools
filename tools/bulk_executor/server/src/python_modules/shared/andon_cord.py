import time

from boto3 import Session

from .logger import log

_ANDON_KEY_SUFFIX = "andon-cord"
_CHECK_INTERVAL_SECONDS = 5


class _NoOpAndonCord:
    """Placeholder when no andon cord config is provided. All operations are no-ops."""

    def signal(self, reason):
        pass

    def check(self):
        return False


_NOOP = _NoOpAndonCord()


class AndonCordConfig:
    """Shared configuration for andon cord coordination between driver and workers."""

    def __init__(self, bucket, job_run_id):
        self.bucket = bucket
        self.key = f"server/andon-cord/{job_run_id}/{_ANDON_KEY_SUFFIX}"


class AndonCordDriver:
    """Driver-side andon cord lifecycle: cleanup on shutdown."""

    def __init__(self, config):
        self._config = config
        self._s3 = Session().client("s3")

    def cleanup(self):
        try:
            self._s3.delete_object(Bucket=self._config.bucket, Key=self._config.key)
        except Exception:
            pass


class AndonCordWorker:
    """
    Worker-side andon cord: signal fatal errors and check for abort.

    Like pulling the andon cord on a production line — when one worker
    hits a non-recoverable systemic error, the whole job stops rather
    than wasting compute on guaranteed failure.

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
        self._triggered = False

    def signal(self, reason):
        """Pull the andon cord — all other workers will abort."""
        try:
            self._s3.put_object(
                Bucket=self._config.bucket,
                Key=self._config.key,
                Body=reason.encode("utf-8"),
            )
        except Exception:
            pass
        self._triggered = True

    def check(self):
        """
        Return True if the andon cord has been pulled (another worker signaled abort).

        Rate-limits the S3 HEAD call to once per _CHECK_INTERVAL_SECONDS.
        """
        if self._triggered:
            return True

        now = time.monotonic()
        if now - self._last_check < _CHECK_INTERVAL_SECONDS:
            return False

        self._last_check = now
        try:
            self._s3.head_object(Bucket=self._config.bucket, Key=self._config.key)
            self._triggered = True
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
