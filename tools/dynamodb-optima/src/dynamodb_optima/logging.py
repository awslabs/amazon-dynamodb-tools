"""
Structured logging configuration for DynamoDB Optima platform.

Uses structlog for structured logging with JSON output for files
and human-readable output for console.
"""

import logging
import sys
from pathlib import Path
from typing import Any

import structlog
from structlog.types import EventDict

from .config import get_settings


def add_app_context(logger: Any, method_name: str, event_dict: EventDict) -> EventDict:
    """Add application context to log events."""
    event_dict["app"] = "metrics_collector"
    event_dict["version"] = "2.0.0"
    return event_dict


class DualOutputLogger:
    """Logger that outputs JSON to file and human-readable to console."""

    def __init__(self, console_logger, file_logger):
        self.console_logger = console_logger
        self.file_logger = file_logger

    def debug(self, event, **kwargs):
        self.console_logger.debug(event, **kwargs)
        self.file_logger.debug(event, **kwargs)

    def info(self, event, **kwargs):
        self.console_logger.info(event, **kwargs)
        self.file_logger.info(event, **kwargs)

    def warning(self, event, **kwargs):
        self.console_logger.warning(event, **kwargs)
        self.file_logger.warning(event, **kwargs)

    def error(self, event, **kwargs):
        self.console_logger.error(event, **kwargs)
        self.file_logger.error(event, **kwargs)

    def critical(self, event, **kwargs):
        self.console_logger.critical(event, **kwargs)
        self.file_logger.critical(event, **kwargs)


def configure_logging() -> None:
    """Configure file-only structured logging with clean CLI output."""
    settings = get_settings()

    # Ensure logs directory exists
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)

    # Suppress verbose AWS SDK logging completely
    logging.getLogger("boto3").setLevel(logging.CRITICAL)
    logging.getLogger("botocore").setLevel(logging.CRITICAL)
    logging.getLogger("aioboto3").setLevel(logging.CRITICAL)
    logging.getLogger("aiobotocore").setLevel(logging.CRITICAL)
    logging.getLogger("urllib3").setLevel(logging.CRITICAL)
    logging.getLogger("s3transfer").setLevel(logging.CRITICAL)
    logging.getLogger("botocore.credentials").setLevel(logging.CRITICAL)
    logging.getLogger("botocore.utils").setLevel(logging.CRITICAL)
    logging.getLogger("botocore.hooks").setLevel(logging.CRITICAL)
    logging.getLogger("botocore.loaders").setLevel(logging.CRITICAL)

    # Also suppress any handlers that might write to stderr
    for logger_name in ["boto3", "botocore", "aioboto3", "aiobotocore", "urllib3"]:
        logger = logging.getLogger(logger_name)
        logger.handlers = []
        logger.propagate = False

    # Create file-only logger for structured logging
    file_logger = logging.getLogger("metrics_collector.file")
    file_logger.setLevel(logging.DEBUG)
    file_logger.handlers = []
    file_handler = logging.FileHandler(log_dir / "metrics_collector.log")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(logging.Formatter("%(message)s"))
    file_logger.addHandler(file_handler)
    file_logger.propagate = False

    # Base processors for structured logging
    base_processors = [
        structlog.contextvars.merge_contextvars,
        add_app_context,
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="ISO"),
    ]

    # Configure structlog to write only to file with JSON format
    def file_only_processor(logger, method_name, event_dict):
        """Process log entry and send only to file logger in JSON format."""
        try:
            file_entry = structlog.processors.JSONRenderer()(
                logger, method_name, event_dict.copy()
            )
            getattr(file_logger, method_name)(file_entry)
        except Exception:
            # Silently ignore any logging errors to prevent console output
            pass
        # Return empty string to suppress console output
        return ""

    # Configure structlog
    structlog.configure(
        processors=base_processors + [file_only_processor],
        wrapper_class=structlog.make_filtering_bound_logger(logging.DEBUG),
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )


def get_logger(name: str) -> structlog.BoundLogger:
    """Get a structured logger instance."""
    return structlog.get_logger(name)
