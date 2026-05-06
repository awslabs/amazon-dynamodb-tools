#!/usr/bin/env python3
"""Test runner that sets up mocks before importing anything else."""

import sys
from unittest.mock import Mock
import boto3

# Mock AWS Glue and PySpark modules before any imports
sys.modules['awsglue'] = Mock()
sys.modules['awsglue.context'] = Mock()
sys.modules['awsglue.job'] = Mock()
sys.modules['pyspark'] = Mock()
sys.modules['pyspark.context'] = Mock()
sys.modules['pyspark.accumulators'] = Mock()

# Mock shared modules
sys.modules['shared'] = Mock()
sys.modules['shared.rate_limiter'] = Mock()
sys.modules['shared.logger'] = Mock()
sys.modules['shared.errors'] = Mock()
sys.modules['shared.pricing'] = Mock()
sys.modules['shared.table_info'] = Mock()

# Create a mock RateLimiterWorker that uses the same boto3.resource that tests mock
class MockRateLimiterWorker:
    def __init__(self, *args, **kwargs):
        self.session = Mock()
        # Use a lambda that will pick up the mocked boto3.resource at runtime
        self.session.resource = lambda *args, **kwargs: __import__('boto3').resource(*args, **kwargs)
    
    def get_session(self):
        return self.session
    
    def shutdown(self):
        pass

# Replace the RateLimiterWorker in the shared module
sys.modules['shared.rate_limiter'].RateLimiterWorker = MockRateLimiterWorker

# Now run pytest
import pytest

if __name__ == "__main__":
    sys.exit(pytest.main(sys.argv[1:]))
