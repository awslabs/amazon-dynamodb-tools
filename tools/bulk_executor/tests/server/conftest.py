"""Conftest that mocks external dependencies before any test collection."""
import sys
from unittest.mock import Mock

# Mock AWS Glue and PySpark modules before any imports.
# AccumulatorParam is used as a base class in some verbs (e.g., copy.py),
# so it must be a real class — Mock() doesn't support subclass override
# semantics. Substitute `object` as the base.
class _AccumulatorParamStub:
    """Minimal stand-in for pyspark's AccumulatorParam base class.

    Real AccumulatorParam requires zero() and addInPlace() — subclasses
    override both. We use a plain class so subclass.zero() / addInPlace()
    actually run.
    """
    pass


# Preserve real pyspark.sql.types before mocking — the types module is pure
# Python data classes with no Spark runtime dependency.
import pyspark.sql.types as _real_sql_types

_pyspark = Mock()
_pyspark.AccumulatorParam = _AccumulatorParamStub
sys.modules['awsglue'] = Mock()
sys.modules['awsglue.context'] = Mock()
sys.modules['awsglue.job'] = Mock()
sys.modules['pyspark'] = _pyspark
sys.modules['pyspark.context'] = Mock()
sys.modules['pyspark.accumulators'] = Mock()
sys.modules['pyspark.sql'] = Mock()
sys.modules['pyspark.sql.types'] = _real_sql_types

# Mock shared modules at all possible resolution paths
# Use a real logger for shared.logger.log so caplog works
import logging
import types

_real_logger = logging.getLogger('load_export')
_real_logger.setLevel(logging.DEBUG)

_logger_module = types.ModuleType('shared.logger')
_logger_module.log = _real_logger
_logger_module.init = Mock()

import pathlib as _pathlib

_server_src = _pathlib.Path(__file__).resolve().parents[2] / "server" / "src"

# Register python_modules.shared as a real package with filesystem path
# so that shared.export and its submodules resolve naturally
import types as _types_mod

for prefix, fs_path in [
    ('shared', str(_server_src / "python_modules" / "shared")),
    ('python_modules.shared', str(_server_src / "python_modules" / "shared")),
]:
    _shared_pkg = _types_mod.ModuleType(prefix)
    _shared_pkg.__path__ = [fs_path]
    sys.modules[prefix] = _shared_pkg

    # Mock the non-export shared modules
    sys.modules[f'{prefix}.rate_limiter'] = Mock()
    sys.modules[f'{prefix}.logger'] = _logger_module
    sys.modules[f'{prefix}.errors'] = Mock()
    sys.modules[f'{prefix}.pricing'] = Mock()
    sys.modules[f'{prefix}.table_info'] = Mock()
    sys.modules[f'{prefix}.glue_connector'] = Mock()


# Make the wrapper module's read/write/count return DataFrame-shaped mocks
# rather than bare Mock() objects. This lets verb-side tests treat the
# wrapper as a black-box source/sink without each test having to wire its
# own fixture for `count() -> int`, `cache() -> self`, etc.
#
# The stub keeps a per-thread reference to the last returned DataFrame so
# tests can do `find_module.read_dynamodb_dataframe.last_df` to assert on
# transformations the verb applied (orderBy, limit, etc.).
def _make_dataframe_mock():
    df = Mock()
    df.count.return_value = 0
    # Chainable transformations all return the same df mock so call chains
    # like records.filter(...).orderBy(...).limit(...) work.
    for method in ('cache', 'filter', 'orderBy', 'limit', 'select', 'repartition', 'toDF'):
        getattr(df, method).return_value = df
    df.toJSON.return_value = Mock()
    return df


class _ReadDataFrameStub:
    """Callable stub that records its last-returned DataFrame mock."""
    def __init__(self):
        self.last_df = None
        self.calls = []

    def __call__(self, *args, **kwargs):
        df = _make_dataframe_mock()
        self.last_df = df
        self.calls.append((args, kwargs))
        return df


def _count_dynamodb_stub(*args, **kwargs):
    return 0


def _write_dynamodb_stub(*args, **kwargs):
    return None


for prefix in ['shared', 'python_modules.shared']:
    sys.modules[f'{prefix}.glue_connector'].read_dynamodb_dataframe = _ReadDataFrameStub()
    sys.modules[f'{prefix}.glue_connector'].count_dynamodb_table = _count_dynamodb_stub
    sys.modules[f'{prefix}.glue_connector'].write_dynamodb_dataframe = _write_dynamodb_stub

# Import the real module — no pyspark dependency, so no mocking needed
import importlib.util
_spec = importlib.util.spec_from_file_location(
    "python_modules.shared.bulk_executor_error",
    str(__import__('pathlib').Path(__file__).resolve().parents[2] / "server/src/python_modules/shared/bulk_executor_error.py")
)
_be_module = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_be_module)
for prefix in ['shared', 'python_modules.shared']:
    sys.modules[f'{prefix}.bulk_executor_error'] = _be_module


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
