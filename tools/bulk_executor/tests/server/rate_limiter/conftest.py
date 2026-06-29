"""Conftest for rate_limiter tests — loads real submodules without replacing
the parent rate_limiter entry that tests/server/conftest.py installed."""
import sys
import types
import logging
import importlib.util
from pathlib import Path
from unittest.mock import Mock

_shared_parent = Path(__file__).resolve().parents[3] / "server" / "src" / "python_modules" / "shared"
_rl_path = _shared_parent / "rate_limiter"

_real_logger = logging.getLogger('rate_limiter_tests')
_real_logger.setLevel(logging.DEBUG)

# Ensure the logger module exists for rate_limiter's `from ..logger import log`
if 'python_modules.shared.logger' not in sys.modules:
    _logger_module = types.ModuleType('python_modules.shared.logger')
    _logger_module.log = _real_logger
    _logger_module.init = Mock()
    sys.modules['python_modules.shared.logger'] = _logger_module
else:
    _logger_module = sys.modules['python_modules.shared.logger']
    if not hasattr(_logger_module, 'log') or _logger_module.log is None:
        _logger_module.log = _real_logger

# Load each rate_limiter submodule into sys.modules so test imports resolve.
# We do NOT replace sys.modules['python_modules.shared.rate_limiter'] — that
# stays as the Mock from server/conftest.py so other server tests still see
# MockRateLimiterWorker etc.
for _mod_name in ('TokenBucket', 'DynamoDBMonitor', 'DistributedDynamoDBMonitorWorker', 'DistributedDynamoDBMonitorAggregator'):
    _fqn = f'python_modules.shared.rate_limiter.{_mod_name}'
    _spec = importlib.util.spec_from_file_location(_fqn, str(_rl_path / f'{_mod_name}.py'))
    _mod = importlib.util.module_from_spec(_spec)
    sys.modules[_fqn] = _mod
    _spec.loader.exec_module(_mod)
