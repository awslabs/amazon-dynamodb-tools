"""Unit tests for the `fill` server-side verb.

Covers `python_modules/fill/__init__.py`:
- print_dynamodb_table_info: cost arithmetic, billing mode branches (PROVISIONED
  vs PAY_PER_REQUEST vs implicit else), helper call wiring
- check_generator_output_avg_size: item serialization and averaging across
  10 generator invocations
- run(): argument wiring (table, numitems defaults, generator selection,
  importlib dynamic load), parallelization math (items_per_worker distribution),
  spark accumulator setup, rate-limiter config, error propagation,
  rate-limiter shutdown in finally, collect-based dispatch
- _fill_data: boto3 Config (timeouts, retries), batch_writer with
  overwrite_by_pkeys, generator yielding dicts vs lists, num_items cap,
  inner throttle exception (log + re-raise), outer ClientError branches
  (throttle vs validation vs other), rate-limiter shutdown in finally,
  total_inserted_accumulator.add, return value

The existing tests/server/conftest.py mocks awsglue, pyspark, and
shared modules at all resolution paths. These tests build on that.
"""

import json
import math
from unittest.mock import MagicMock, patch, call

import botocore.exceptions
import pytest

from python_modules import fill as fill_module


# The star import `from python_modules.shared.errors import *` at fill.py line 12
# doesn't inject real names in the test environment because conftest replaces
# the errors module with Mock(). Inject the real implementations so fill.py's
# runtime references resolve.

class _ListAccumulator:
    def zero(self, initialValue):
        return []

    def addInPlace(self, v1, v2):
        v1.extend(v2)
        return v1


def _get_error_code(e):
    if hasattr(e, 'response') and e.response:
        error_response = e.response.get('Error')
        if error_response:
            return error_response.get('Code')
    return None


def _get_error_message(e):
    if hasattr(e, 'response') and e.response:
        error_response = e.response.get('Error')
        if error_response:
            msg = error_response.get('Message')
            if msg:
                return msg
    return str(e)


fill_module.ListAccumulator = _ListAccumulator
fill_module.get_error_code = _get_error_code
fill_module.get_error_message = _get_error_message


# --- Fixtures ---------------------------------------------------------------

@pytest.fixture
def table_info_mocks(monkeypatch):
    """Replace shared.table_info helpers imported into fill module namespace."""
    get_info = MagicMock(return_value={
        'billing_mode': 'PROVISIONED',
        'write_pricing_category': 'write_cap',
        'key_schema': {'hash': {'name': 'pk'}, 'range': {'name': 'sk'}},
    })
    get_throughput = MagicMock(return_value={'monitor': 'opts'})
    monkeypatch.setattr(fill_module, 'get_and_print_dynamodb_table_info', get_info)
    monkeypatch.setattr(fill_module, 'get_dynamodb_throughput_configs', get_throughput)
    return MagicMock(get_info=get_info, get_throughput=get_throughput)


@pytest.fixture
def pricing_mock(monkeypatch):
    """Replace PricingUtility so print_dynamodb_table_info doesn't hit network."""
    pricing_instance = MagicMock()
    pricing_instance.get_on_demand_capacity_pricing.return_value = {
        'write_cap': '0.00000125'
    }
    pricing_cls = MagicMock(return_value=pricing_instance)
    monkeypatch.setattr(fill_module, 'PricingUtility', pricing_cls)
    return pricing_instance


@pytest.fixture
def rate_limiter_mocks(monkeypatch):
    """Replace RateLimiterSharedConfig and RateLimiterAggregator."""
    config_cls = MagicMock(side_effect=lambda **kw: MagicMock(**kw))
    aggregator_cls = MagicMock()
    monkeypatch.setattr(fill_module, 'RateLimiterSharedConfig', config_cls)
    monkeypatch.setattr(fill_module, 'RateLimiterAggregator', aggregator_cls)
    return MagicMock(config=config_cls, aggregator=aggregator_cls)


@pytest.fixture
def spark_context():
    """Mock SparkContext that records accumulator() and parallelize() calls."""
    sc = MagicMock()
    sc.accumulator = MagicMock(side_effect=lambda init, *args: MagicMock(value=init))
    rdd = MagicMock()
    sc.parallelize = MagicMock(return_value=rdd)
    return sc


@pytest.fixture
def base_args():
    return {
        'table': 'my-table',
        'numitems': '500',
        'generator': 'custom_gen',
        'generatorfunctionname': 'make_items',
        's3-bucket-name': 'rate-bucket',
        'JOB_RUN_ID': 'jr-001',
    }


# --- print_dynamodb_table_info -----------------------------------------------

class TestPrintDynamodbTableInfo:
    """Tests for the cost calculation and billing mode branching (lines 30-54)."""

    def test_provisioned_billing_mode_logs_provisioned_cost(self, monkeypatch, pricing_mock, caplog):
        """Line 49-50: PROVISIONED billing mode prints provisioned cost."""
        monkeypatch.setattr(fill_module, 'get_and_print_dynamodb_table_info', MagicMock(return_value={
            'billing_mode': 'PROVISIONED',
            'write_pricing_category': 'write_cap',
        }))
        session = MagicMock(region_name='us-east-1')

        import logging
        with caplog.at_level(logging.INFO, logger='load_export'):
            result = fill_module.print_dynamodb_table_info(session, 'tbl', 100, 2048)

        assert any('Provisioned' in m or 'provisioned' in m for m in caplog.messages), \
            "PROVISIONED billing prints provisioned cost message"
        assert result['billing_mode'] == 'PROVISIONED'

    def test_pay_per_request_billing_mode_logs_ondemand_cost(self, monkeypatch, pricing_mock, caplog):
        """Line 51-52: PAY_PER_REQUEST billing mode prints on-demand cost."""
        monkeypatch.setattr(fill_module, 'get_and_print_dynamodb_table_info', MagicMock(return_value={
            'billing_mode': 'PAY_PER_REQUEST',
            'write_pricing_category': 'write_cap',
        }))
        session = MagicMock(region_name='us-west-2')

        import logging
        with caplog.at_level(logging.INFO, logger='load_export'):
            fill_module.print_dynamodb_table_info(session, 'tbl', 200, 1024)

        assert any('On-demand' in m or 'on-demand' in m.lower() for m in caplog.messages), \
            "PAY_PER_REQUEST billing prints on-demand cost message"

    def test_unknown_billing_mode_skips_both_cost_lines(self, monkeypatch, pricing_mock, caplog):
        """Lines 49-52: neither PROVISIONED nor PAY_PER_REQUEST → no cost log."""
        monkeypatch.setattr(fill_module, 'get_and_print_dynamodb_table_info', MagicMock(return_value={
            'billing_mode': 'UNKNOWN',
            'write_pricing_category': 'write_cap',
        }))
        session = MagicMock(region_name='us-east-1')

        import logging
        with caplog.at_level(logging.INFO, logger='load_export'):
            fill_module.print_dynamodb_table_info(session, 'tbl', 10, 512)

        assert not any('Provisioned' in m or 'On-demand' in m for m in caplog.messages), \
            "unknown billing mode should not print either cost line"

    def test_avg_write_units_calculation(self, monkeypatch, pricing_mock, caplog):
        """Line 35: avg_write_units_per_item = ceil(avg_size / 1024)."""
        monkeypatch.setattr(fill_module, 'get_and_print_dynamodb_table_info', MagicMock(return_value={
            'billing_mode': 'PROVISIONED',
            'write_pricing_category': 'write_cap',
        }))
        session = MagicMock(region_name='us-east-1')

        import logging
        with caplog.at_level(logging.INFO, logger='load_export'):
            fill_module.print_dynamodb_table_info(session, 'tbl', 10, 1500)

        # ceil(1500/1024) = 2 write units per item
        assert any('2 write units' in m for m in caplog.messages), \
            "ceil(1500/1024) should be 2 write units per item"

    def test_returns_table_info(self, monkeypatch, pricing_mock):
        """Line 54: function returns the table_info dict."""
        expected = {'billing_mode': 'PROVISIONED', 'write_pricing_category': 'write_cap'}
        monkeypatch.setattr(fill_module, 'get_and_print_dynamodb_table_info',
                            MagicMock(return_value=expected))
        session = MagicMock(region_name='us-east-1')
        result = fill_module.print_dynamodb_table_info(session, 'tbl', 1, 100)
        assert result is expected, "must return table_info from get_and_print_dynamodb_table_info"

    def test_write_units_total_in_log(self, monkeypatch, pricing_mock, caplog):
        """Line 47: logs total write units = numitems * avg_write_units_per_item."""
        monkeypatch.setattr(fill_module, 'get_and_print_dynamodb_table_info', MagicMock(return_value={
            'billing_mode': 'PROVISIONED',
            'write_pricing_category': 'write_cap',
        }))
        session = MagicMock(region_name='us-east-1')

        import logging
        with caplog.at_level(logging.INFO, logger='load_export'):
            fill_module.print_dynamodb_table_info(session, 'tbl', 50, 2048)

        # ceil(2048/1024)=2, 50*2=100 write units
        assert any('100' in m for m in caplog.messages), \
            "should log 100 total write units (50 items * 2 WU each)"


# --- check_generator_output_avg_size ----------------------------------------

class TestCheckGeneratorOutputAvgSize:
    """Tests for average item size calculation (lines 56-69)."""

    def test_computes_average_across_10_invocations(self, monkeypatch):
        """Lines 64-68: calls generate() 10 times and averages total_size/total_count."""
        items = [{'pk': 'a', 'data': 'x'}]
        generate = MagicMock(return_value=items)

        # Mock TypeSerializer to return predictable JSON
        mock_serializer = MagicMock()
        mock_serializer.serialize = MagicMock(side_effect=lambda item: item)

        with patch('python_modules.fill.json.dumps', return_value='x' * 100):
            with patch('boto3.dynamodb.types.TypeSerializer', return_value=mock_serializer):
                result = fill_module.check_generator_output_avg_size(generate)

        assert generate.call_count == 10, "generate called 10 times (line 64 range(10))"
        assert result == 100.0, "each of 10 calls yields 1 item of 100 bytes → avg=100"

    def test_multiple_items_per_generate_call(self, monkeypatch):
        """Line 65: iterates all items returned by each generate() call."""
        items = [{'a': 1}, {'b': 2}]
        generate = MagicMock(return_value=items)

        mock_serializer = MagicMock()
        mock_serializer.serialize = MagicMock(side_effect=lambda item: item)

        with patch('python_modules.fill.json.dumps', return_value='y' * 50):
            with patch('boto3.dynamodb.types.TypeSerializer', return_value=mock_serializer):
                result = fill_module.check_generator_output_avg_size(generate)

        # 10 calls * 2 items = 20 items, each 50 bytes → avg = 50
        assert result == 50.0


# --- run() ------------------------------------------------------------------

class TestRunArgumentWiring:
    """Tests for how run() extracts and routes parsed_args (lines 72-85)."""

    def test_uses_default_numitems_when_not_provided(self, monkeypatch, table_info_mocks,
                                                      rate_limiter_mocks, spark_context, pricing_mock):
        """Line 74: defaults to 1000 if numitems not in parsed_args."""
        args = {'table': 'tbl', 's3-bucket-name': 'b', 'JOB_RUN_ID': 'j'}

        mock_module = MagicMock()
        mock_module.generate = MagicMock(return_value=[{'pk': 'x'}])
        monkeypatch.setattr(fill_module.importlib, 'import_module', MagicMock(return_value=mock_module))
        monkeypatch.setattr(fill_module, 'check_generator_output_avg_size', MagicMock(return_value=100))
        monkeypatch.setattr(fill_module, 'print_dynamodb_table_info', MagicMock(return_value={
            'key_schema': {'hash': {'name': 'pk'}},
        }))

        rdd = spark_context.parallelize.return_value
        rdd.map.return_value.collect.return_value = []

        fill_module.run(MagicMock(), spark_context, MagicMock(), args)

        # parallelize_count = ceil(1000/10000) = 1
        par_call = spark_context.parallelize.call_args
        data = par_call.args[0]
        assert sum(x[1] for x in data) == 1000, "default numitems is 1000"

    def test_uses_default_generator_name(self, monkeypatch, table_info_mocks,
                                          rate_limiter_mocks, spark_context, pricing_mock):
        """Line 76: defaults generator to 'default' if not in args."""
        args = {'table': 'tbl', 'numitems': '10', 's3-bucket-name': 'b', 'JOB_RUN_ID': 'j'}

        mock_module = MagicMock()
        mock_module.generate = MagicMock(return_value=[{'pk': 'x'}])
        import_mock = MagicMock(return_value=mock_module)
        monkeypatch.setattr(fill_module.importlib, 'import_module', import_mock)
        monkeypatch.setattr(fill_module, 'check_generator_output_avg_size', MagicMock(return_value=100))
        monkeypatch.setattr(fill_module, 'print_dynamodb_table_info', MagicMock(return_value={
            'key_schema': {'hash': {'name': 'pk'}},
        }))

        rdd = spark_context.parallelize.return_value
        rdd.map.return_value.collect.return_value = []

        fill_module.run(MagicMock(), spark_context, MagicMock(), args)

        import_mock.assert_called_once_with('python_modules.fill.default')

    def test_uses_default_generator_function_name(self, monkeypatch, table_info_mocks,
                                                    rate_limiter_mocks, spark_context, pricing_mock):
        """Line 77: defaults generatorfunctionname to 'generate'."""
        args = {'table': 'tbl', 'numitems': '10', 's3-bucket-name': 'b', 'JOB_RUN_ID': 'j'}

        mock_module = MagicMock()
        mock_module.generate = MagicMock(return_value=[{'pk': 'x'}])
        monkeypatch.setattr(fill_module.importlib, 'import_module', MagicMock(return_value=mock_module))
        monkeypatch.setattr(fill_module, 'check_generator_output_avg_size', MagicMock(return_value=100))
        monkeypatch.setattr(fill_module, 'print_dynamodb_table_info', MagicMock(return_value={
            'key_schema': {'hash': {'name': 'pk'}},
        }))

        rdd = spark_context.parallelize.return_value
        rdd.map.return_value.collect.return_value = []

        fill_module.run(MagicMock(), spark_context, MagicMock(), args)

        # getattr(module, 'generate') should be used as the generate function
        # Check that check_generator_output_avg_size was called with module.generate
        fill_module.check_generator_output_avg_size.assert_called_once_with(mock_module.generate)

    def test_custom_generator_and_function_name(self, monkeypatch, table_info_mocks,
                                                  rate_limiter_mocks, spark_context, pricing_mock, base_args):
        """Lines 76-77, 83-84: custom generator module and function name."""
        mock_module = MagicMock()
        mock_module.make_items = MagicMock(return_value=[{'pk': 'x'}])
        import_mock = MagicMock(return_value=mock_module)
        monkeypatch.setattr(fill_module.importlib, 'import_module', import_mock)
        monkeypatch.setattr(fill_module, 'check_generator_output_avg_size', MagicMock(return_value=100))
        monkeypatch.setattr(fill_module, 'print_dynamodb_table_info', MagicMock(return_value={
            'key_schema': {'hash': {'name': 'pk'}},
        }))

        rdd = spark_context.parallelize.return_value
        rdd.map.return_value.collect.return_value = []

        fill_module.run(MagicMock(), spark_context, MagicMock(), base_args)

        import_mock.assert_called_once_with('python_modules.fill.custom_gen')
        fill_module.check_generator_output_avg_size.assert_called_once_with(mock_module.make_items)


class TestRunParallelization:
    """Tests for work distribution math (lines 94-99)."""

    def test_parallelize_count_ceil_division(self, monkeypatch, table_info_mocks,
                                              rate_limiter_mocks, spark_context, pricing_mock):
        """Line 94: parallelize_count = ceil(record_count / 10000)."""
        args = {'table': 'tbl', 'numitems': '25000', 's3-bucket-name': 'b', 'JOB_RUN_ID': 'j'}

        mock_module = MagicMock()
        mock_module.generate = MagicMock(return_value=[{'pk': 'x'}])
        monkeypatch.setattr(fill_module.importlib, 'import_module', MagicMock(return_value=mock_module))
        monkeypatch.setattr(fill_module, 'check_generator_output_avg_size', MagicMock(return_value=100))
        monkeypatch.setattr(fill_module, 'print_dynamodb_table_info', MagicMock(return_value={
            'key_schema': {'hash': {'name': 'pk'}},
        }))

        rdd = spark_context.parallelize.return_value
        rdd.map.return_value.collect.return_value = []

        fill_module.run(MagicMock(), spark_context, MagicMock(), args)

        par_call = spark_context.parallelize.call_args
        # ceil(25000/10000) = 3
        assert par_call.args[1] == 3, "parallelize_count = ceil(25000/10000) = 3"

    def test_items_per_worker_distributes_remainder(self, monkeypatch, table_info_mocks,
                                                     rate_limiter_mocks, spark_context, pricing_mock):
        """Lines 97-99: remainder is distributed among first workers."""
        args = {'table': 'tbl', 'numitems': '10003', 's3-bucket-name': 'b', 'JOB_RUN_ID': 'j'}

        mock_module = MagicMock()
        mock_module.generate = MagicMock(return_value=[{'pk': 'x'}])
        monkeypatch.setattr(fill_module.importlib, 'import_module', MagicMock(return_value=mock_module))
        monkeypatch.setattr(fill_module, 'check_generator_output_avg_size', MagicMock(return_value=100))
        monkeypatch.setattr(fill_module, 'print_dynamodb_table_info', MagicMock(return_value={
            'key_schema': {'hash': {'name': 'pk'}},
        }))

        rdd = spark_context.parallelize.return_value
        rdd.map.return_value.collect.return_value = []

        fill_module.run(MagicMock(), spark_context, MagicMock(), args)

        par_call = spark_context.parallelize.call_args
        data = par_call.args[0]
        items_list = [x[1] for x in data]
        # ceil(10003/10000) = 2 workers; 10003//2 = 5001 base; remainder 1
        # First worker gets 5002, second gets 5001
        assert items_list == [5002, 5001], "remainder distributed to first workers"
        assert sum(items_list) == 10003

    def test_small_count_single_worker(self, monkeypatch, table_info_mocks,
                                        rate_limiter_mocks, spark_context, pricing_mock):
        """Line 94: <= 10000 items means single partition."""
        args = {'table': 'tbl', 'numitems': '5000', 's3-bucket-name': 'b', 'JOB_RUN_ID': 'j'}

        mock_module = MagicMock()
        mock_module.generate = MagicMock(return_value=[{'pk': 'x'}])
        monkeypatch.setattr(fill_module.importlib, 'import_module', MagicMock(return_value=mock_module))
        monkeypatch.setattr(fill_module, 'check_generator_output_avg_size', MagicMock(return_value=100))
        monkeypatch.setattr(fill_module, 'print_dynamodb_table_info', MagicMock(return_value={
            'key_schema': {'hash': {'name': 'pk'}},
        }))

        rdd = spark_context.parallelize.return_value
        rdd.map.return_value.collect.return_value = []

        fill_module.run(MagicMock(), spark_context, MagicMock(), args)

        par_call = spark_context.parallelize.call_args
        assert par_call.args[1] == 1, "ceil(5000/10000) = 1 partition"
        data = par_call.args[0]
        assert [x[1] for x in data] == [5000]


class TestRunAccumulators:
    """Tests for accumulator setup (lines 101-104)."""

    def test_total_inserted_accumulator_starts_at_zero(self, monkeypatch, table_info_mocks,
                                                        rate_limiter_mocks, spark_context, pricing_mock):
        """Line 101: total_inserted_accumulator seeded with 0."""
        args = {'table': 'tbl', 'numitems': '10', 's3-bucket-name': 'b', 'JOB_RUN_ID': 'j'}
        mock_module = MagicMock()
        mock_module.generate = MagicMock(return_value=[{'pk': 'x'}])
        monkeypatch.setattr(fill_module.importlib, 'import_module', MagicMock(return_value=mock_module))
        monkeypatch.setattr(fill_module, 'check_generator_output_avg_size', MagicMock(return_value=100))
        monkeypatch.setattr(fill_module, 'print_dynamodb_table_info', MagicMock(return_value={
            'key_schema': {'hash': {'name': 'pk'}},
        }))
        rdd = spark_context.parallelize.return_value
        rdd.map.return_value.collect.return_value = []

        fill_module.run(MagicMock(), spark_context, MagicMock(), args)

        first_acc = spark_context.accumulator.call_args_list[0]
        assert first_acc.args[0] == 0, "total_inserted starts at 0"

    def test_error_accumulator_seeded_with_empty_list_and_ListAccumulator(self, monkeypatch, table_info_mocks,
                                                                           rate_limiter_mocks, spark_context, pricing_mock):
        """Line 104: error_accumulator seeded with [] and ListAccumulator instance."""
        args = {'table': 'tbl', 'numitems': '10', 's3-bucket-name': 'b', 'JOB_RUN_ID': 'j'}
        mock_module = MagicMock()
        mock_module.generate = MagicMock(return_value=[{'pk': 'x'}])
        monkeypatch.setattr(fill_module.importlib, 'import_module', MagicMock(return_value=mock_module))
        monkeypatch.setattr(fill_module, 'check_generator_output_avg_size', MagicMock(return_value=100))
        monkeypatch.setattr(fill_module, 'print_dynamodb_table_info', MagicMock(return_value={
            'key_schema': {'hash': {'name': 'pk'}},
        }))
        rdd = spark_context.parallelize.return_value
        rdd.map.return_value.collect.return_value = []

        fill_module.run(MagicMock(), spark_context, MagicMock(), args)

        err_acc = spark_context.accumulator.call_args_list[1]
        assert err_acc.args[0] == [], "error accumulator seeded with []"
        assert isinstance(err_acc.args[1], fill_module.ListAccumulator), \
            "second param is a ListAccumulator instance"


class TestRunRateLimiter:
    """Tests for rate-limiter wiring in run() (lines 106-113)."""

    def test_shared_config_receives_bucket_and_job_run_id(self, monkeypatch, table_info_mocks,
                                                           rate_limiter_mocks, spark_context, pricing_mock, base_args):
        """Lines 106-109: RateLimiterSharedConfig gets bucket and job_run_id."""
        mock_module = MagicMock()
        mock_module.make_items = MagicMock(return_value=[{'pk': 'x'}])
        monkeypatch.setattr(fill_module.importlib, 'import_module', MagicMock(return_value=mock_module))
        monkeypatch.setattr(fill_module, 'check_generator_output_avg_size', MagicMock(return_value=100))
        monkeypatch.setattr(fill_module, 'print_dynamodb_table_info', MagicMock(return_value={
            'key_schema': {'hash': {'name': 'pk'}},
        }))
        rdd = spark_context.parallelize.return_value
        rdd.map.return_value.collect.return_value = []

        fill_module.run(MagicMock(), spark_context, MagicMock(), base_args)

        config_call = rate_limiter_mocks.config.call_args
        assert config_call.kwargs == {'bucket': 'rate-bucket', 'job_run_id': 'jr-001'}

    def test_throughput_configs_called_with_write_mode(self, monkeypatch, table_info_mocks,
                                                        rate_limiter_mocks, spark_context, pricing_mock, base_args):
        """Line 113: get_dynamodb_throughput_configs called with modes=['write']."""
        mock_module = MagicMock()
        mock_module.make_items = MagicMock(return_value=[{'pk': 'x'}])
        monkeypatch.setattr(fill_module.importlib, 'import_module', MagicMock(return_value=mock_module))
        monkeypatch.setattr(fill_module, 'check_generator_output_avg_size', MagicMock(return_value=100))
        monkeypatch.setattr(fill_module, 'print_dynamodb_table_info', MagicMock(return_value={
            'key_schema': {'hash': {'name': 'pk'}},
        }))
        rdd = spark_context.parallelize.return_value
        rdd.map.return_value.collect.return_value = []

        fill_module.run(MagicMock(), spark_context, MagicMock(), base_args)

        table_info_mocks.get_throughput.assert_called_once()
        call_kwargs = table_info_mocks.get_throughput.call_args.kwargs
        assert call_kwargs['modes'] == ['write']
        assert call_kwargs['format'] == 'monitor'


class TestRunErrorHandling:
    """Tests for error propagation from workers (lines 117-126)."""

    def test_parallel_execution_exception_is_wrapped(self, monkeypatch, table_info_mocks,
                                                      rate_limiter_mocks, spark_context, pricing_mock, base_args):
        """Lines 120-121: Exception during collect() is wrapped with message."""
        mock_module = MagicMock()
        mock_module.make_items = MagicMock(return_value=[{'pk': 'x'}])
        monkeypatch.setattr(fill_module.importlib, 'import_module', MagicMock(return_value=mock_module))
        monkeypatch.setattr(fill_module, 'check_generator_output_avg_size', MagicMock(return_value=100))
        monkeypatch.setattr(fill_module, 'print_dynamodb_table_info', MagicMock(return_value={
            'key_schema': {'hash': {'name': 'pk'}},
        }))
        monkeypatch.setattr(fill_module, 'get_error_message', lambda e: f"msg:{e}")

        rdd = spark_context.parallelize.return_value
        rdd.map.return_value.collect.side_effect = RuntimeError('boom')

        with pytest.raises(Exception, match='Error in parallel execution'):
            fill_module.run(MagicMock(), spark_context, MagicMock(), base_args)

    def test_aggregator_shutdown_even_on_collect_failure(self, monkeypatch, table_info_mocks,
                                                          spark_context, pricing_mock, base_args):
        """Lines 122-123: rate_limiter_aggregator.shutdown() in finally."""
        mock_module = MagicMock()
        mock_module.make_items = MagicMock(return_value=[{'pk': 'x'}])
        monkeypatch.setattr(fill_module.importlib, 'import_module', MagicMock(return_value=mock_module))
        monkeypatch.setattr(fill_module, 'check_generator_output_avg_size', MagicMock(return_value=100))
        monkeypatch.setattr(fill_module, 'print_dynamodb_table_info', MagicMock(return_value={
            'key_schema': {'hash': {'name': 'pk'}},
        }))
        monkeypatch.setattr(fill_module, 'get_error_message', lambda e: str(e))

        agg_instance = MagicMock()
        monkeypatch.setattr(fill_module, 'RateLimiterSharedConfig', MagicMock())
        monkeypatch.setattr(fill_module, 'RateLimiterAggregator', MagicMock(return_value=agg_instance))

        rdd = spark_context.parallelize.return_value
        rdd.map.return_value.collect.side_effect = RuntimeError('explode')

        with pytest.raises(Exception):
            fill_module.run(MagicMock(), spark_context, MagicMock(), base_args)

        agg_instance.shutdown.assert_called_once(), \
            "aggregator.shutdown() must fire in finally even on failure"

    def test_error_accumulator_first_error_raised(self, monkeypatch, table_info_mocks,
                                                    rate_limiter_mocks, spark_context, pricing_mock, base_args):
        """Lines 124-126: if error_accumulator has values, raise the first."""
        mock_module = MagicMock()
        mock_module.make_items = MagicMock(return_value=[{'pk': 'x'}])
        monkeypatch.setattr(fill_module.importlib, 'import_module', MagicMock(return_value=mock_module))
        monkeypatch.setattr(fill_module, 'check_generator_output_avg_size', MagicMock(return_value=100))
        monkeypatch.setattr(fill_module, 'print_dynamodb_table_info', MagicMock(return_value={
            'key_schema': {'hash': {'name': 'pk'}},
        }))

        # Wire accumulators: first for total, second for errors with a value
        accs = [MagicMock(value=10), MagicMock(value=['first err', 'second err'])]
        spark_context.accumulator = MagicMock(side_effect=accs)
        rdd = spark_context.parallelize.return_value
        rdd.map.return_value.collect.return_value = []

        with pytest.raises(Exception, match='first err'):
            fill_module.run(MagicMock(), spark_context, MagicMock(), base_args)

    def test_no_errors_logs_total(self, monkeypatch, table_info_mocks,
                                   rate_limiter_mocks, spark_context, pricing_mock, base_args, caplog):
        """Line 129: when no errors, logs total records filled."""
        mock_module = MagicMock()
        mock_module.make_items = MagicMock(return_value=[{'pk': 'x'}])
        monkeypatch.setattr(fill_module.importlib, 'import_module', MagicMock(return_value=mock_module))
        monkeypatch.setattr(fill_module, 'check_generator_output_avg_size', MagicMock(return_value=100))
        monkeypatch.setattr(fill_module, 'print_dynamodb_table_info', MagicMock(return_value={
            'key_schema': {'hash': {'name': 'pk'}},
        }))

        accs = [MagicMock(value=42), MagicMock(value=[])]
        spark_context.accumulator = MagicMock(side_effect=accs)
        rdd = spark_context.parallelize.return_value
        rdd.map.return_value.collect.return_value = []

        import logging
        with caplog.at_level(logging.INFO, logger='load_export'):
            fill_module.run(MagicMock(), spark_context, MagicMock(), base_args)

        assert any('42' in m for m in caplog.messages), \
            "total_inserted_accumulator.value (42) should appear in log"


class TestRunMapDispatch:
    """Tests for the lambda passed to parallelize().map().collect() (line 118-119)."""

    def test_map_lambda_calls_fill_data_with_correct_args(self, monkeypatch, table_info_mocks,
                                                           rate_limiter_mocks, spark_context, pricing_mock):
        """Line 118-119: lambda receives (idx, items_count) tuple, calls _fill_data."""
        args = {'table': 'my-tbl', 'numitems': '10', 's3-bucket-name': 'b', 'JOB_RUN_ID': 'j'}
        mock_module = MagicMock()
        mock_module.generate = MagicMock(return_value=[{'pk': 'x'}])
        monkeypatch.setattr(fill_module.importlib, 'import_module', MagicMock(return_value=mock_module))
        monkeypatch.setattr(fill_module, 'check_generator_output_avg_size', MagicMock(return_value=100))
        monkeypatch.setattr(fill_module, 'print_dynamodb_table_info', MagicMock(return_value={
            'key_schema': {'hash': {'name': 'pk'}},
        }))

        captured = []

        def fake_fill_data(*a, **kw):
            captured.append(a)
            return 0

        monkeypatch.setattr(fill_module, '_fill_data', fake_fill_data)

        # Make map actually invoke the lambda
        def fake_map(fn):
            result_mock = MagicMock()
            results = [fn(x) for x in [(0, 10)]]
            result_mock.collect.return_value = results
            return result_mock

        spark_context.parallelize.return_value.map = fake_map

        fill_module.run(MagicMock(), spark_context, MagicMock(), args)

        assert len(captured) == 1
        # _fill_data(monitor_options, table_name, num_items, generate, total_acc, error_acc, shared_config, key_names)
        assert captured[0][1] == 'my-tbl', "table_name passed as 2nd arg"
        assert captured[0][2] == 10, "num_items (from tuple[1]) passed as 3rd arg"


# --- _fill_data --------------------------------------------------------------

def _make_fill_data_deps(monkeypatch, num_items=5, generate_items=None):
    """Helper to set up _fill_data dependencies. Returns (table, bw, generate, total_acc, error_acc)."""
    rl_instance = MagicMock()
    session = MagicMock()
    rl_instance.get_session.return_value = session

    table = MagicMock()
    bw = MagicMock()
    bw.__enter__ = MagicMock(return_value=bw)
    bw.__exit__ = MagicMock(return_value=False)
    table.batch_writer.return_value = bw
    session.resource.return_value.Table.return_value = table

    if generate_items is None:
        generate_items = [{'pk': 'val'}]
    generate = MagicMock(return_value=generate_items)
    total_acc = MagicMock()
    error_acc = MagicMock()

    monkeypatch.setattr(fill_module, 'RateLimiterWorker', MagicMock(return_value=rl_instance))

    return table, bw, generate, total_acc, error_acc, rl_instance


class TestFillDataConfig:
    """Tests for boto3 Config inside _fill_data (lines 138-145)."""

    def test_boto3_config_has_4s_timeouts_and_50_retries(self, monkeypatch):
        """Lines 139-144: Config with connect_timeout=4, read_timeout=4, 50 retries."""
        seen_configs = []
        rl_instance = MagicMock()
        session = MagicMock()
        rl_instance.get_session.return_value = session

        def capture_resource(name, **kw):
            seen_configs.append(kw.get('config'))
            r = MagicMock()
            table = MagicMock()
            bw = MagicMock()
            bw.__enter__ = MagicMock(return_value=bw)
            bw.__exit__ = MagicMock(return_value=False)
            table.batch_writer.return_value = bw
            r.Table.return_value = table
            return r

        session.resource = capture_resource
        monkeypatch.setattr(fill_module, 'RateLimiterWorker', MagicMock(return_value=rl_instance))

        generate = MagicMock(return_value=[{'pk': 'x'}])
        fill_module._fill_data({}, 'tbl', 1, generate, MagicMock(), MagicMock(), MagicMock(), ['pk'])

        assert len(seen_configs) == 1, "one resource() call for target"
        cfg = seen_configs[0]
        assert cfg.connect_timeout == 4.0
        assert cfg.read_timeout == 4.0
        assert cfg.retries['mode'] == 'standard'
        assert cfg.retries['total_max_attempts'] == 50


class TestFillDataBatchWriter:
    """Tests for batch_writer usage and item insertion (lines 148-162)."""

    def test_batch_writer_uses_overwrite_by_pkeys(self, monkeypatch):
        """Line 152: batch_writer(overwrite_by_pkeys=key_names)."""
        table, bw, generate, total_acc, error_acc, rl = _make_fill_data_deps(monkeypatch, num_items=1)

        fill_module._fill_data({}, 'tbl', 1, generate, total_acc, error_acc, MagicMock(), ['pk', 'sk'])

        table.batch_writer.assert_called_once_with(overwrite_by_pkeys=['pk', 'sk'])

    def test_items_put_via_batch_writer(self, monkeypatch):
        """Lines 160-161: each item from generator goes through batch.put_item."""
        table, bw, generate, total_acc, error_acc, rl = _make_fill_data_deps(
            monkeypatch, num_items=3, generate_items=[{'pk': 'a'}, {'pk': 'b'}, {'pk': 'c'}])

        fill_module._fill_data({}, 'tbl', 3, generate, total_acc, error_acc, MagicMock(), ['pk'])

        assert bw.put_item.call_count == 3
        items = [c.kwargs['Item'] for c in bw.put_item.call_args_list]
        assert items == [{'pk': 'a'}, {'pk': 'b'}, {'pk': 'c'}]

    def test_generator_returning_dict_wrapped_in_list(self, monkeypatch):
        """Lines 156-157: if generate() returns a dict, it gets wrapped in a list."""
        rl_instance = MagicMock()
        session = MagicMock()
        rl_instance.get_session.return_value = session
        table = MagicMock()
        bw = MagicMock()
        bw.__enter__ = MagicMock(return_value=bw)
        bw.__exit__ = MagicMock(return_value=False)
        table.batch_writer.return_value = bw
        session.resource.return_value.Table.return_value = table
        monkeypatch.setattr(fill_module, 'RateLimiterWorker', MagicMock(return_value=rl_instance))

        # generate returns a dict (not a list)
        generate = MagicMock(return_value={'pk': 'single_item'})
        total_acc = MagicMock()

        fill_module._fill_data({}, 'tbl', 1, generate, total_acc, MagicMock(), MagicMock(), ['pk'])

        bw.put_item.assert_called_once_with(Item={'pk': 'single_item'})
        total_acc.add.assert_called_with(1)

    def test_stops_at_num_items_limit(self, monkeypatch):
        """Lines 159-160: breaks when local_count >= num_items even mid-collection."""
        rl_instance = MagicMock()
        session = MagicMock()
        rl_instance.get_session.return_value = session
        table = MagicMock()
        bw = MagicMock()
        bw.__enter__ = MagicMock(return_value=bw)
        bw.__exit__ = MagicMock(return_value=False)
        table.batch_writer.return_value = bw
        session.resource.return_value.Table.return_value = table
        monkeypatch.setattr(fill_module, 'RateLimiterWorker', MagicMock(return_value=rl_instance))

        # generate returns 5 items, but we only want 2
        generate = MagicMock(return_value=[{'pk': '1'}, {'pk': '2'}, {'pk': '3'}, {'pk': '4'}, {'pk': '5'}])
        total_acc = MagicMock()

        fill_module._fill_data({}, 'tbl', 2, generate, total_acc, MagicMock(), MagicMock(), ['pk'])

        assert bw.put_item.call_count == 2, "only 2 items inserted despite 5 available"
        total_acc.add.assert_called_with(2)

    def test_multiple_generate_calls_until_num_items(self, monkeypatch):
        """Line 153: while loop keeps calling generate() until local_count >= num_items."""
        rl_instance = MagicMock()
        session = MagicMock()
        rl_instance.get_session.return_value = session
        table = MagicMock()
        bw = MagicMock()
        bw.__enter__ = MagicMock(return_value=bw)
        bw.__exit__ = MagicMock(return_value=False)
        table.batch_writer.return_value = bw
        session.resource.return_value.Table.return_value = table
        monkeypatch.setattr(fill_module, 'RateLimiterWorker', MagicMock(return_value=rl_instance))

        # generate returns 2 items each time; we want 5 total → 3 calls needed
        generate = MagicMock(return_value=[{'pk': 'a'}, {'pk': 'b'}])
        total_acc = MagicMock()

        fill_module._fill_data({}, 'tbl', 5, generate, total_acc, MagicMock(), MagicMock(), ['pk'])

        assert generate.call_count == 3, "3 generate calls to get 5 items (2+2+1)"
        assert bw.put_item.call_count == 5
        total_acc.add.assert_called_with(5)


class TestFillDataErrorHandling:
    """Tests for exception handling in _fill_data (lines 164-177)."""

    def _make_client_error(self, code):
        """Create a botocore ClientError with the given code."""
        return botocore.exceptions.ClientError(
            {'Error': {'Code': code, 'Message': f'{code} happened'}},
            'PutItem'
        )

    def test_inner_throttle_logs_and_reraises(self, monkeypatch, caplog):
        """Lines 164-167: inner ClientError with throttle code logs then re-raises to outer handler."""
        rl_instance = MagicMock()
        session = MagicMock()
        rl_instance.get_session.return_value = session
        table = MagicMock()
        bw = MagicMock()
        bw.__enter__ = MagicMock(return_value=bw)
        bw.__exit__ = MagicMock(return_value=False)
        table.batch_writer.return_value = bw
        session.resource.return_value.Table.return_value = table
        monkeypatch.setattr(fill_module, 'RateLimiterWorker', MagicMock(return_value=rl_instance))
        monkeypatch.setattr(fill_module, 'get_error_code',
                            lambda e: e.response['Error']['Code'])
        monkeypatch.setattr(fill_module, 'get_error_message',
                            lambda e: e.response['Error']['Message'])

        # generate raises throttle on first call
        err = self._make_client_error('ProvisionedThroughputExceededException')
        generate = MagicMock(side_effect=err)
        error_acc = MagicMock()

        import logging
        with caplog.at_level(logging.INFO, logger='load_export'):
            fill_module._fill_data({}, 'tbl', 5, generate, MagicMock(), error_acc, MagicMock(), ['pk'])

        # Inner handler logs 'Persistent throttling, loop again...'
        assert any('Persistent throttling' in m and 'loop again' in m for m in caplog.messages), \
            "inner throttle handler logs before re-raising"
        # Outer throttle handler catches re-raise and logs 'give up'
        assert any('give up' in m for m in caplog.messages), \
            "outer throttle handler catches the re-raise"

    def test_outer_throttle_logs_give_up(self, monkeypatch, caplog):
        """Lines 169-171: outer throttle logs and does NOT add to error_accumulator."""
        rl_instance = MagicMock()
        session = MagicMock()
        rl_instance.get_session.return_value = session
        table = MagicMock()

        err = self._make_client_error('ProvisionedThroughputExceededException')
        # batch_writer __exit__ raises the error (simulating batch flush throttle)
        bw = MagicMock()
        bw.__enter__ = MagicMock(return_value=bw)
        bw.__exit__ = MagicMock(side_effect=err)
        table.batch_writer.return_value = bw
        session.resource.return_value.Table.return_value = table
        monkeypatch.setattr(fill_module, 'RateLimiterWorker', MagicMock(return_value=rl_instance))
        monkeypatch.setattr(fill_module, 'get_error_code',
                            lambda e: e.response['Error']['Code'])

        generate = MagicMock(return_value=[{'pk': 'x'}])
        error_acc = MagicMock()

        import logging
        with caplog.at_level(logging.INFO, logger='load_export'):
            fill_module._fill_data({}, 'tbl', 1, generate, MagicMock(), error_acc, MagicMock(), ['pk'])

        assert any('give up' in m for m in caplog.messages)
        error_acc.add.assert_not_called(), \
            "throttle at batch_writer exit doesn't add to error_accumulator"

    def test_validation_exception_adds_schema_error(self, monkeypatch):
        """Lines 172-173: ValidationException → error_accumulator with schema message."""
        rl_instance = MagicMock()
        session = MagicMock()
        rl_instance.get_session.return_value = session
        table = MagicMock()
        bw = MagicMock()
        bw.__enter__ = MagicMock(return_value=bw)
        bw.__exit__ = MagicMock(return_value=False)
        table.batch_writer.return_value = bw
        session.resource.return_value.Table.return_value = table
        monkeypatch.setattr(fill_module, 'RateLimiterWorker', MagicMock(return_value=rl_instance))
        monkeypatch.setattr(fill_module, 'get_error_code',
                            lambda e: e.response['Error']['Code'])
        monkeypatch.setattr(fill_module, 'get_error_message',
                            lambda e: e.response['Error']['Message'])

        err = self._make_client_error('ValidationException')
        generate = MagicMock(side_effect=err)
        error_acc = MagicMock()

        fill_module._fill_data({}, 'tbl', 5, generate, MagicMock(), error_acc, MagicMock(), ['pk'])

        error_acc.add.assert_called_once()
        msg = error_acc.add.call_args.args[0]
        assert isinstance(msg, list) and len(msg) == 1
        assert 'Schema validation error' in msg[0]
        assert "ValidationException happened" in msg[0]

    def test_other_client_error_adds_generic_error(self, monkeypatch):
        """Lines 174-175: non-throttle, non-validation → generic error message."""
        rl_instance = MagicMock()
        session = MagicMock()
        rl_instance.get_session.return_value = session
        table = MagicMock()
        bw = MagicMock()
        bw.__enter__ = MagicMock(return_value=bw)
        bw.__exit__ = MagicMock(return_value=False)
        table.batch_writer.return_value = bw
        session.resource.return_value.Table.return_value = table
        monkeypatch.setattr(fill_module, 'RateLimiterWorker', MagicMock(return_value=rl_instance))
        monkeypatch.setattr(fill_module, 'get_error_code',
                            lambda e: e.response['Error']['Code'])
        monkeypatch.setattr(fill_module, 'get_error_message',
                            lambda e: e.response['Error']['Message'])

        err = self._make_client_error('InternalServerError')
        generate = MagicMock(side_effect=err)
        error_acc = MagicMock()

        fill_module._fill_data({}, 'tbl', 5, generate, MagicMock(), error_acc, MagicMock(), ['pk'])

        error_acc.add.assert_called_once()
        msg = error_acc.add.call_args.args[0]
        assert 'Error during writing' in msg[0]

    def test_rate_limiter_shutdown_in_finally(self, monkeypatch):
        """Lines 176-177: rate_limiter_worker.shutdown() always called."""
        rl_instance = MagicMock()
        session = MagicMock()
        rl_instance.get_session.return_value = session
        table = MagicMock()
        bw = MagicMock()
        bw.__enter__ = MagicMock(return_value=bw)
        bw.__exit__ = MagicMock(return_value=False)
        table.batch_writer.return_value = bw
        session.resource.return_value.Table.return_value = table
        monkeypatch.setattr(fill_module, 'RateLimiterWorker', MagicMock(return_value=rl_instance))
        monkeypatch.setattr(fill_module, 'get_error_code',
                            lambda e: e.response['Error']['Code'])
        monkeypatch.setattr(fill_module, 'get_error_message',
                            lambda e: e.response['Error']['Message'])

        err = self._make_client_error('InternalServerError')
        generate = MagicMock(side_effect=err)

        fill_module._fill_data({}, 'tbl', 5, generate, MagicMock(), MagicMock(), MagicMock(), ['pk'])

        rl_instance.shutdown.assert_called_once()


class TestFillDataReturn:
    """Tests for return value and accumulator update (lines 179-180)."""

    def test_returns_local_count(self, monkeypatch):
        """Line 180: returns local_count after successful insertion."""
        table, bw, generate, total_acc, error_acc, rl = _make_fill_data_deps(
            monkeypatch, generate_items=[{'pk': 'a'}, {'pk': 'b'}])

        result = fill_module._fill_data({}, 'tbl', 4, generate, total_acc, error_acc, MagicMock(), ['pk'])

        assert result == 4, "4 items inserted across multiple generate() calls"
        total_acc.add.assert_called_with(4)

    def test_returns_zero_on_immediate_error(self, monkeypatch):
        """Line 180: returns 0 when error occurs before any insertion."""
        rl_instance = MagicMock()
        session = MagicMock()
        rl_instance.get_session.return_value = session
        table = MagicMock()
        bw = MagicMock()
        bw.__enter__ = MagicMock(return_value=bw)
        bw.__exit__ = MagicMock(return_value=False)
        table.batch_writer.return_value = bw
        session.resource.return_value.Table.return_value = table
        monkeypatch.setattr(fill_module, 'RateLimiterWorker', MagicMock(return_value=rl_instance))
        monkeypatch.setattr(fill_module, 'get_error_code',
                            lambda e: e.response['Error']['Code'])
        monkeypatch.setattr(fill_module, 'get_error_message',
                            lambda e: e.response['Error']['Message'])

        err = botocore.exceptions.ClientError(
            {'Error': {'Code': 'InternalServerError', 'Message': 'fail'}}, 'Op')
        generate = MagicMock(side_effect=err)
        total_acc = MagicMock()

        result = fill_module._fill_data({}, 'tbl', 5, generate, total_acc, MagicMock(), MagicMock(), ['pk'])

        assert result == 0, "no items inserted when error fires on first generate"
        total_acc.add.assert_called_with(0)

    def test_accumulator_records_partial_count_on_mid_batch_error(self, monkeypatch):
        """Line 179: total_inserted_accumulator.add(local_count) even on partial fill."""
        rl_instance = MagicMock()
        session = MagicMock()
        rl_instance.get_session.return_value = session
        table = MagicMock()
        bw = MagicMock()
        bw.__enter__ = MagicMock(return_value=bw)
        bw.__exit__ = MagicMock(return_value=False)
        table.batch_writer.return_value = bw
        session.resource.return_value.Table.return_value = table
        monkeypatch.setattr(fill_module, 'RateLimiterWorker', MagicMock(return_value=rl_instance))
        monkeypatch.setattr(fill_module, 'get_error_code',
                            lambda e: e.response['Error']['Code'])
        monkeypatch.setattr(fill_module, 'get_error_message',
                            lambda e: e.response['Error']['Message'])

        # First call succeeds (2 items), second call throws
        err = botocore.exceptions.ClientError(
            {'Error': {'Code': 'InternalServerError', 'Message': 'fail'}}, 'Op')
        generate = MagicMock(side_effect=[[{'pk': '1'}, {'pk': '2'}], err])
        total_acc = MagicMock()

        result = fill_module._fill_data({}, 'tbl', 5, generate, total_acc, MagicMock(), MagicMock(), ['pk'])

        assert result == 2, "2 items inserted before error"
        total_acc.add.assert_called_with(2)


class TestFillDataMonitorOptions:
    """Tests for monitor_options pass-through to RateLimiterWorker (line 132-135)."""

    def test_monitor_options_splatted_into_worker(self, monkeypatch):
        """Line 134: **monitor_options passed to RateLimiterWorker constructor."""
        rl_class = MagicMock()
        rl_instance = MagicMock()
        session = MagicMock()
        rl_instance.get_session.return_value = session
        table = MagicMock()
        bw = MagicMock()
        bw.__enter__ = MagicMock(return_value=bw)
        bw.__exit__ = MagicMock(return_value=False)
        table.batch_writer.return_value = bw
        session.resource.return_value.Table.return_value = table
        rl_class.return_value = rl_instance
        monkeypatch.setattr(fill_module, 'RateLimiterWorker', rl_class)

        generate = MagicMock(return_value=[{'pk': 'x'}])
        monitor_opts = {'monitor_table': 'tbl', 'custom_key': 'val'}

        fill_module._fill_data(monitor_opts, 'tbl', 1, generate, MagicMock(), MagicMock(), MagicMock(), ['pk'])

        kwargs = rl_class.call_args.kwargs
        assert kwargs['monitor_table'] == 'tbl'
        assert kwargs['custom_key'] == 'val'


# --- ListAccumulator (from shared.errors, imported via star import) ----------

class TestListAccumulator:
    """fill imports ListAccumulator via `from shared.errors import *`.
    Tests confirm the class is accessible and behaves correctly (lines from errors.py)."""

    def test_zero_returns_empty_list(self):
        """ListAccumulator.zero() always returns []."""
        acc = fill_module.ListAccumulator()
        assert acc.zero(['anything']) == []
        assert acc.zero(None) == []

    def test_addInPlace_extends_and_returns(self):
        """ListAccumulator.addInPlace extends first list with second."""
        acc = fill_module.ListAccumulator()
        a = ['err1']
        result = acc.addInPlace(a, ['err2', 'err3'])
        assert result is a
        assert a == ['err1', 'err2', 'err3']


# --- Module constants --------------------------------------------------------

class TestModuleConstants:
    """Verify module-level constants are accessible (lines 27-28)."""

    def test_throttle_exception_constant(self):
        """Line 27: DYNAMO_DB_THROTTLE_EXCEPTION."""
        assert fill_module.DYNAMO_DB_THROTTLE_EXCEPTION == 'ProvisionedThroughputExceededException'

    def test_validation_exception_constant(self):
        """Line 28: DYNAMO_DB_VALIDATION_EXCEPTION."""
        assert fill_module.DYNAMO_DB_VALIDATION_EXCEPTION == 'ValidationException'
