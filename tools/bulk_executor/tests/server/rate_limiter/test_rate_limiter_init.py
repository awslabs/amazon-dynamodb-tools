"""Unit tests for rate_limiter __init__ wrapper classes — log levels."""
import logging
import importlib.util
import sys
import types
from pathlib import Path
from unittest.mock import patch, Mock

import pytest

_shared_parent = Path(__file__).resolve().parents[3] / "server" / "src" / "python_modules" / "shared"
_rl_path = _shared_parent / "rate_limiter"

# Ensure the rate_limiter package is registered so relative imports resolve.
# The conftest.py already set up logger and submodules; we just need the
# __init__.py loaded as a proper package.
_pkg_name = "python_modules.shared.rate_limiter._real_init"

# Load __init__.py as a module with the package set so relative imports work.
_init_spec = importlib.util.spec_from_file_location(
    "_rl_init_test",
    str(_rl_path / "__init__.py"),
    submodule_search_locations=[str(_rl_path)],
)
_init_spec.submodule_search_locations = [str(_rl_path)]
_init_mod = importlib.util.module_from_spec(_init_spec)
_init_mod.__package__ = "python_modules.shared.rate_limiter"
_init_spec.loader.exec_module(_init_mod)

RateLimiterAggregator = _init_mod.RateLimiterAggregator
RateLimiterWorker = _init_mod.RateLimiterWorker
RateLimiterSharedConfig = _init_mod.RateLimiterSharedConfig


@pytest.fixture
def shared_config():
    return RateLimiterSharedConfig(bucket="test-bucket", job_run_id="job-123")


class TestRateLimiterAggregatorInit:
    def test_initializing_message_logged_at_debug(self, shared_config, caplog):
        with patch.object(
            _init_mod, "DistributedDynamoDBMonitorAggregator"
        ), patch.object(_init_mod, "Session"):
            with caplog.at_level(logging.DEBUG, logger="rate_limiter_tests"):
                RateLimiterAggregator(shared_config)

        initializing_records = [
            r for r in caplog.records if "Initializing" in r.message
        ]
        assert len(initializing_records) == 1
        assert initializing_records[0].levelno == logging.DEBUG

    def test_shutdown_message_logged_at_debug(self, shared_config, caplog):
        with patch.object(
            _init_mod, "DistributedDynamoDBMonitorAggregator"
        ), patch.object(_init_mod, "Session"):
            agg = RateLimiterAggregator(shared_config)
            with caplog.at_level(logging.DEBUG, logger="rate_limiter_tests"):
                agg.shutdown()

        shutdown_records = [
            r for r in caplog.records if "Shutting down" in r.message
        ]
        assert len(shutdown_records) == 1
        assert shutdown_records[0].levelno == logging.DEBUG


class TestRateLimiterWorkerShutdown:
    def test_shutdown_message_logged_at_debug(self, shared_config, caplog):
        with patch.object(
            _init_mod, "DistributedDynamoDBMonitorWorker"
        ), patch.object(_init_mod, "Session"):
            worker = RateLimiterWorker(shared_config)
            with caplog.at_level(logging.DEBUG, logger="rate_limiter_tests"):
                worker.shutdown()

        shutdown_records = [
            r for r in caplog.records if "Shutting down" in r.message
        ]
        assert len(shutdown_records) == 1
        assert shutdown_records[0].levelno == logging.DEBUG
