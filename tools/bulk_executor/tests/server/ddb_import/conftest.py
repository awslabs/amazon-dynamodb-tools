"""Conftest that mocks external dependencies before any test collection."""
import sys
from unittest.mock import Mock

# Mock AWS Glue and PySpark modules before any imports
sys.modules['awsglue'] = Mock()
sys.modules['awsglue.context'] = Mock()
sys.modules['awsglue.job'] = Mock()
sys.modules['pyspark'] = Mock()
sys.modules['pyspark.context'] = Mock()
sys.modules['pyspark.accumulators'] = Mock()

# Mock shared modules at all possible resolution paths
# Use a real logger for shared.logger.log so caplog works
import logging
import types

_real_logger = logging.getLogger('ddb_import')
_real_logger.setLevel(logging.DEBUG)

_logger_module = types.ModuleType('shared.logger')
_logger_module.log = _real_logger
_logger_module.init = Mock()

for prefix in ['shared', 'python_modules.shared']:
    sys.modules[prefix] = Mock()
    sys.modules[f'{prefix}.rate_limiter'] = Mock()
    sys.modules[f'{prefix}.logger'] = _logger_module
    sys.modules[f'{prefix}.errors'] = Mock()
    sys.modules[f'{prefix}.pricing'] = Mock()
    sys.modules[f'{prefix}.table_info'] = Mock()


class MockRateLimiterWorker:
    def __init__(self, *args, **kwargs):
        self.session = Mock()
        self.session.resource = lambda *args, **kwargs: __import__('boto3').resource(*args, **kwargs)

    def get_session(self):
        return self.session

    def shutdown(self):
        pass


sys.modules['python_modules.shared.rate_limiter'].RateLimiterWorker = MockRateLimiterWorker
sys.modules['shared.rate_limiter'].RateLimiterWorker = MockRateLimiterWorker
