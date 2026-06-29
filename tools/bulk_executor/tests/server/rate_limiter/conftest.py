"""Conftest for rate_limiter tests — imports real modules instead of mocks."""
import sys
import types
import logging
import importlib.util
from pathlib import Path
from unittest.mock import Mock

_shared_parent = Path(__file__).resolve().parents[3] / "server" / "src" / "python_modules" / "shared"
_rl_path = _shared_parent / "rate_limiter"

# Set up logger mock (rate_limiter modules import from ..logger)
_real_logger = logging.getLogger('rate_limiter_tests')
_real_logger.setLevel(logging.DEBUG)

_logger_module = types.ModuleType('python_modules.shared.logger')
_logger_module.log = _real_logger
_logger_module.init = Mock()

# Overwrite the mocked entries with real namespace packages
_pm = types.ModuleType('python_modules')
_pm.__path__ = [str(_shared_parent.parent)]
sys.modules['python_modules'] = _pm

_pms = types.ModuleType('python_modules.shared')
_pms.__path__ = [str(_shared_parent)]
_pms.logger = _logger_module
sys.modules['python_modules.shared'] = _pms
sys.modules['python_modules.shared.logger'] = _logger_module

# Register the real rate_limiter package
_rl = types.ModuleType('python_modules.shared.rate_limiter')
_rl.__path__ = [str(_rl_path)]
_rl.__file__ = str(_rl_path / "__init__.py")
sys.modules['python_modules.shared.rate_limiter'] = _rl

# Load each rate_limiter submodule
for _mod_name in ('TokenBucket', 'DynamoDBMonitor', 'DistributedDynamoDBMonitorWorker', 'DistributedDynamoDBMonitorAggregator'):
    _fqn = f'python_modules.shared.rate_limiter.{_mod_name}'
    _spec = importlib.util.spec_from_file_location(_fqn, str(_rl_path / f'{_mod_name}.py'))
    _mod = importlib.util.module_from_spec(_spec)
    sys.modules[_fqn] = _mod
    _spec.loader.exec_module(_mod)
    setattr(_rl, _mod_name, _mod)
