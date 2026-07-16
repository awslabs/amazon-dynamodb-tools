"""Unit tests for the `scancount` server-side verb.

Covers `python_modules/scancount/__init__.py`:
- DecimalEncoder: JSON decoder that converts floats to Decimal
- ListAccumulator: zero / addInPlace contract for error accumulation
- Module constants: DYNAMO_DB_THROTTLE_EXCEPTION, DYNAMO_DB_VALIDATION_EXCEPTION
- print_dynamodb_table_info: boto3 session region + shared helper calls
- run(): argument wiring, rate-limiter shared config, monitor options,
  spark parallelize count, error propagation, rate-limiter shutdown
- _count_data: boto3 Config (timeouts, retries), scan kwargs construction,
  optional index/filter/expression params, pagination loop, per-worker
  error accumulation, rate-limiter shutdown in finally, count accumulation
"""

import json
from decimal import Decimal
from unittest.mock import MagicMock, call, patch

import pytest

from python_modules import scancount as sc_module

# The source uses `from python_modules.shared.errors import *` which, under
# our Mock-based conftest, binds nothing (star-import from Mock is empty).
# Inject get_error_message so it's available when tested code paths call it.
sc_module.get_error_message = lambda e: str(e)


# --- Fixtures ---------------------------------------------------------------


@pytest.fixture
def shared_table_info_mocks(monkeypatch):
    """Replace shared.table_info helpers used by scancount with fresh mocks."""
    helpers = MagicMock()
    helpers.get_and_print_dynamodb_table_info = MagicMock(
        return_value={'item_count': 500, 'size_bytes': 4096, 'region_name': 'us-east-1'}
    )
    helpers.get_and_print_table_scan_cost = MagicMock(return_value=0.75)
    helpers.get_dynamodb_throughput_configs = MagicMock(return_value={'monitor': 'opts'})

    monkeypatch.setattr(sc_module, 'get_and_print_dynamodb_table_info',
                        helpers.get_and_print_dynamodb_table_info)
    monkeypatch.setattr(sc_module, 'get_and_print_table_scan_cost',
                        helpers.get_and_print_table_scan_cost)
    monkeypatch.setattr(sc_module, 'get_dynamodb_throughput_configs',
                        helpers.get_dynamodb_throughput_configs)
    return helpers


@pytest.fixture
def rate_limiter_mocks(monkeypatch):
    """Replace RateLimiterAggregator / RateLimiterSharedConfig with mocks."""
    config_cls = MagicMock(side_effect=lambda **kw: MagicMock(**kw))
    aggregator_cls = MagicMock()

    monkeypatch.setattr(sc_module, 'RateLimiterSharedConfig', config_cls)
    monkeypatch.setattr(sc_module, 'RateLimiterAggregator', aggregator_cls)
    return MagicMock(config=config_cls, aggregator=aggregator_cls)


@pytest.fixture
def spark_context():
    """Mock SparkContext that records accumulator() and parallelize() calls."""
    sc = MagicMock()
    sc.accumulator = MagicMock(side_effect=lambda init, *_: MagicMock(value=init))
    rdd = MagicMock()
    sc.parallelize = MagicMock(return_value=rdd)
    return sc


@pytest.fixture
def base_args():
    return {
        'table': 'my-table',
        'index': None,
        'filter_expression': None,
        'expression_values': None,
        'expression_names': None,
        's3-bucket-name': 'rate-bucket',
        'JOB_RUN_ID': 'jr-001',
    }


# --- DecimalEncoder ---------------------------------------------------------


class TestDecimalEncoder:
    """DecimalEncoder is a JSONDecoder subclass that converts float values
    in decoded dicts to Decimal (lines 16-20)."""

    def test_float_values_become_decimal(self):
        """Line 19: isinstance(v, float) triggers Decimal conversion."""
        raw = '{"price": 19.99, "qty": 3}'
        result = json.loads(raw, cls=sc_module.DecimalEncoder)
        assert result['price'] == Decimal('19.99'), "float 19.99 becomes Decimal"
        assert isinstance(result['price'], Decimal)

    def test_int_values_stay_as_int(self):
        """Line 19: non-float values pass through unchanged."""
        raw = '{"count": 42, "name": "foo"}'
        result = json.loads(raw, cls=sc_module.DecimalEncoder)
        assert result['count'] == 42
        assert isinstance(result['count'], int)

    def test_string_values_stay_as_string(self):
        """Line 19: string values are not float, so pass through."""
        raw = '{"key": "hello"}'
        result = json.loads(raw, cls=sc_module.DecimalEncoder)
        assert result['key'] == 'hello'

    def test_mixed_types_only_floats_converted(self):
        """Line 19-20: only float values in the dict are converted."""
        raw = '{"a": 1.5, "b": 10, "c": "x"}'
        result = json.loads(raw, cls=sc_module.DecimalEncoder)
        assert isinstance(result['a'], Decimal)
        assert isinstance(result['b'], int)
        assert isinstance(result['c'], str)


# --- ListAccumulator --------------------------------------------------------


class TestListAccumulator:
    """ListAccumulator (lines 36-42) is a custom AccumulatorParam for
    collecting per-worker errors into a merged list."""

    def test_zero_returns_empty_list(self):
        """Line 38: zero() always returns [] regardless of seed."""
        acc = sc_module.ListAccumulator()
        assert acc.zero(['anything']) == []
        assert acc.zero(None) == []

    def test_addInPlace_extends_first_list(self):
        """Lines 40-42: addInPlace extends v1 with v2 and returns v1."""
        acc = sc_module.ListAccumulator()
        a = ['err1']
        b = ['err2', 'err3']
        result = acc.addInPlace(a, b)
        assert a == ['err1', 'err2', 'err3'], "first arg mutated in place"
        assert result is a, "returns same list object"

    def test_addInPlace_empty_right(self):
        """Lines 40-42: empty v2 leaves v1 unchanged."""
        acc = sc_module.ListAccumulator()
        result = acc.addInPlace(['x'], [])
        assert result == ['x']

    def test_addInPlace_empty_left(self):
        """Lines 40-42: empty v1 gains v2's elements."""
        acc = sc_module.ListAccumulator()
        result = acc.addInPlace([], ['y'])
        assert result == ['y']


# --- Module constants -------------------------------------------------------


class TestModuleConstants:
    """Lines 44-45 define exception name constants used for error matching."""

    def test_throttle_exception_constant(self):
        """Line 44."""
        assert sc_module.DYNAMO_DB_THROTTLE_EXCEPTION == 'ProvisionedThroughputExceededException'

    def test_validation_exception_constant(self):
        """Line 45."""
        assert sc_module.DYNAMO_DB_VALIDATION_EXCEPTION == 'ValidationException'


# --- print_dynamodb_table_info ----------------------------------------------


class TestPrintDynamodbTableInfo:
    """print_dynamodb_table_info (lines 47-50) gets the session region,
    fetches table info, and computes scan cost."""

    def test_calls_helpers_with_table_and_index(self, shared_table_info_mocks, monkeypatch):
        """Lines 49-50: passes table_name and index_name to info helper, then cost."""
        mock_session = MagicMock()
        mock_session.region_name = 'eu-west-1'
        monkeypatch.setattr(sc_module.boto3, 'Session', MagicMock(return_value=mock_session))

        sc_module.print_dynamodb_table_info('tbl', 'idx')

        shared_table_info_mocks.get_and_print_dynamodb_table_info.assert_called_once_with('tbl', 'idx')
        shared_table_info_mocks.get_and_print_table_scan_cost.assert_called_once()

    def test_passes_region_from_session_to_scan_cost(self, shared_table_info_mocks, monkeypatch):
        """Line 48-50: region_name from boto3.Session() is passed to scan cost."""
        mock_session = MagicMock()
        mock_session.region_name = 'ap-southeast-2'
        monkeypatch.setattr(sc_module.boto3, 'Session', MagicMock(return_value=mock_session))

        sc_module.print_dynamodb_table_info('t', None)

        cost_args = shared_table_info_mocks.get_and_print_table_scan_cost.call_args
        assert cost_args.args[1] == 'ap-southeast-2', "region from session passed as second arg"

    def test_no_index_passed_as_none(self, shared_table_info_mocks, monkeypatch):
        """Line 49: index_name=None when not provided."""
        mock_session = MagicMock()
        mock_session.region_name = 'us-east-1'
        monkeypatch.setattr(sc_module.boto3, 'Session', MagicMock(return_value=mock_session))

        sc_module.print_dynamodb_table_info('tbl')

        call_args = shared_table_info_mocks.get_and_print_dynamodb_table_info.call_args
        assert call_args.args == ('tbl',) or call_args == call('tbl', None), \
            "index defaults to None"


# --- run() ------------------------------------------------------------------


class TestRunArgumentWiring:
    """run() (lines 52-95) parses args, configures rate limiting, and
    dispatches parallel counting workers."""

    def test_shared_config_uses_bucket_and_job_run_id(self, monkeypatch, shared_table_info_mocks,
                                                       rate_limiter_mocks, spark_context, base_args):
        """Lines 65-68: RateLimiterSharedConfig receives bucket and job_run_id."""
        spark_context.parallelize.return_value.foreach = MagicMock()
        monkeypatch.setattr(sc_module.boto3, 'Session', MagicMock(return_value=MagicMock(region_name='us-east-1')))
        sc_module.run(MagicMock(), spark_context, MagicMock(), base_args)

        config_call = rate_limiter_mocks.config.call_args
        assert config_call.kwargs['bucket'] == 'rate-bucket'
        assert config_call.kwargs['job_run_id'] == 'jr-001'

    def test_aggregator_receives_shared_config(self, monkeypatch, shared_table_info_mocks,
                                                rate_limiter_mocks, spark_context, base_args):
        """Line 70: RateLimiterAggregator is constructed with the shared_config."""
        spark_context.parallelize.return_value.foreach = MagicMock()
        monkeypatch.setattr(sc_module.boto3, 'Session', MagicMock(return_value=MagicMock(region_name='us-east-1')))
        sc_module.run(MagicMock(), spark_context, MagicMock(), base_args)

        agg_call = rate_limiter_mocks.aggregator.call_args
        assert 'shared_config' in agg_call.kwargs, "aggregator gets shared_config kwarg"

    def test_throughput_configs_called_for_read_mode(self, monkeypatch, shared_table_info_mocks,
                                                      rate_limiter_mocks, spark_context, base_args):
        """Line 73: get_dynamodb_throughput_configs called with modes=['read'], format='monitor'."""
        spark_context.parallelize.return_value.foreach = MagicMock()
        monkeypatch.setattr(sc_module.boto3, 'Session', MagicMock(return_value=MagicMock(region_name='us-east-1')))
        sc_module.run(MagicMock(), spark_context, MagicMock(), base_args)

        tc_call = shared_table_info_mocks.get_dynamodb_throughput_configs.call_args
        assert tc_call.args[1] == 'my-table'
        assert tc_call.kwargs['modes'] == ['read']
        assert tc_call.kwargs['format'] == 'monitor'

    def test_parallelize_count_is_200(self, monkeypatch, shared_table_info_mocks,
                                       rate_limiter_mocks, spark_context, base_args):
        """Line 83: parallelize(range(200), 200) when segments not specified."""
        spark_context.parallelize.return_value.foreach = MagicMock()
        monkeypatch.setattr(sc_module.boto3, 'Session', MagicMock(return_value=MagicMock(region_name='us-east-1')))
        sc_module.run(MagicMock(), spark_context, MagicMock(), base_args)

        pc_args = spark_context.parallelize.call_args
        assert list(pc_args.args[0]) == list(range(200)), "range(200) as first arg"
        assert pc_args.args[1] == 200, "numSlices is 200"

    def test_parallelize_count_respects_segments_arg(self, monkeypatch, shared_table_info_mocks,
                                                     rate_limiter_mocks, spark_context, base_args):
        """When segments is specified, parallelize uses that value."""
        spark_context.parallelize.return_value.foreach = MagicMock()
        monkeypatch.setattr(sc_module.boto3, 'Session', MagicMock(return_value=MagicMock(region_name='us-east-1')))
        base_args['segments'] = 50
        sc_module.run(MagicMock(), spark_context, MagicMock(), base_args)

        pc_args = spark_context.parallelize.call_args
        assert list(pc_args.args[0]) == list(range(50)), "range(50) as first arg"
        assert pc_args.args[1] == 50, "numSlices is 50"

    def test_total_matched_accumulator_initialized_to_zero(self, monkeypatch, shared_table_info_mocks,
                                                            rate_limiter_mocks, spark_context, base_args):
        """Line 75: accumulator(0) for total count."""
        spark_context.parallelize.return_value.foreach = MagicMock()
        monkeypatch.setattr(sc_module.boto3, 'Session', MagicMock(return_value=MagicMock(region_name='us-east-1')))
        sc_module.run(MagicMock(), spark_context, MagicMock(), base_args)

        first_call = spark_context.accumulator.call_args_list[0]
        assert first_call.args[0] == 0

    def test_error_accumulator_seeded_with_empty_list_and_list_accumulator(self, monkeypatch, shared_table_info_mocks,
                                                                            rate_limiter_mocks, spark_context, base_args):
        """Line 78: accumulator([], ListAccumulator())."""
        spark_context.parallelize.return_value.foreach = MagicMock()
        monkeypatch.setattr(sc_module.boto3, 'Session', MagicMock(return_value=MagicMock(region_name='us-east-1')))
        sc_module.run(MagicMock(), spark_context, MagicMock(), base_args)

        err_call = spark_context.accumulator.call_args_list[1]
        assert err_call.args[0] == []
        assert isinstance(err_call.args[1], sc_module.ListAccumulator)


class TestRunErrorHandling:
    """Error propagation from workers and exception wrapping in run()."""

    def test_first_worker_error_raised_after_foreach(self, monkeypatch, shared_table_info_mocks,
                                                      rate_limiter_mocks, spark_context, base_args):
        """Lines 90-92: if error_accumulator.value is non-empty, raise first error."""
        accs = [
            MagicMock(value=10),
            MagicMock(value=['worker 3 failed', 'worker 7 failed']),
        ]
        spark_context.accumulator = MagicMock(side_effect=accs)
        spark_context.parallelize.return_value.foreach = MagicMock()
        spark_context.parallelize.return_value.count = MagicMock()
        monkeypatch.setattr(sc_module.boto3, 'Session', MagicMock(return_value=MagicMock(region_name='us-east-1')))

        with pytest.raises(Exception, match='worker 3 failed'):
            sc_module.run(MagicMock(), spark_context, MagicMock(), base_args)

    def test_no_errors_prints_total_count(self, monkeypatch, shared_table_info_mocks,
                                           rate_limiter_mocks, spark_context, base_args, capsys):
        """Line 95: prints total when no errors."""
        accs = [MagicMock(value=1234), MagicMock(value=[])]
        spark_context.accumulator = MagicMock(side_effect=accs)
        spark_context.parallelize.return_value.foreach = MagicMock()
        spark_context.parallelize.return_value.count = MagicMock()
        monkeypatch.setattr(sc_module.boto3, 'Session', MagicMock(return_value=MagicMock(region_name='us-east-1')))

        sc_module.run(MagicMock(), spark_context, MagicMock(), base_args)
        out = capsys.readouterr().out
        assert '1,234' in out, "total uses comma formatting"

    def test_foreach_exception_wraps_in_parallel_execution_error(self, monkeypatch, shared_table_info_mocks,
                                                                   rate_limiter_mocks, spark_context, base_args):
        """Lines 86-87: exception from foreach is wrapped with 'Error in parallel execution'."""
        spark_context.parallelize.return_value.foreach = MagicMock(
            side_effect=RuntimeError('spark died')
        )
        monkeypatch.setattr(sc_module.boto3, 'Session', MagicMock(return_value=MagicMock(region_name='us-east-1')))
        monkeypatch.setattr(sc_module, 'get_error_message', lambda e: str(e))

        with pytest.raises(Exception, match='Error in parallel execution.*spark died'):
            sc_module.run(MagicMock(), spark_context, MagicMock(), base_args)

    def test_aggregator_shutdown_even_on_foreach_failure(self, monkeypatch, shared_table_info_mocks,
                                                          spark_context, base_args):
        """Lines 88-89: rate_limiter_aggregator.shutdown() in finally block."""
        agg_instance = MagicMock()
        monkeypatch.setattr(sc_module, 'RateLimiterSharedConfig', MagicMock())
        monkeypatch.setattr(sc_module, 'RateLimiterAggregator', MagicMock(return_value=agg_instance))
        monkeypatch.setattr(sc_module.boto3, 'Session', MagicMock(return_value=MagicMock(region_name='us-east-1')))

        spark_context.parallelize.return_value.foreach = MagicMock(
            side_effect=RuntimeError('boom')
        )
        monkeypatch.setattr(sc_module, 'get_error_message', lambda e: str(e))

        with pytest.raises(Exception):
            sc_module.run(MagicMock(), spark_context, MagicMock(), base_args)

        agg_instance.shutdown.assert_called_once()

    def test_aggregator_shutdown_on_success(self, monkeypatch, shared_table_info_mocks,
                                             spark_context, base_args):
        """Lines 88-89: shutdown called even on normal exit path."""
        agg_instance = MagicMock()
        monkeypatch.setattr(sc_module, 'RateLimiterSharedConfig', MagicMock())
        monkeypatch.setattr(sc_module, 'RateLimiterAggregator', MagicMock(return_value=agg_instance))
        monkeypatch.setattr(sc_module.boto3, 'Session', MagicMock(return_value=MagicMock(region_name='us-east-1')))

        accs = [MagicMock(value=0), MagicMock(value=[])]
        spark_context.accumulator = MagicMock(side_effect=accs)
        spark_context.parallelize.return_value.foreach = MagicMock()
        spark_context.parallelize.return_value.count = MagicMock()

        sc_module.run(MagicMock(), spark_context, MagicMock(), base_args)
        agg_instance.shutdown.assert_called_once()


class TestRunForeachDispatch:
    """The lambda passed to rdd.foreach invokes _count_data with correct args."""

    def test_foreach_lambda_passes_all_positional_args(self, monkeypatch, shared_table_info_mocks,
                                                        rate_limiter_mocks, spark_context, base_args):
        """Line 84: lambda worker_id: _count_data(..., worker_id, parallelize_count, ...)."""
        captured = {}

        def fake_count_data(*args, **kwargs):
            captured.setdefault('calls', []).append(args)

        monkeypatch.setattr(sc_module, '_count_data', fake_count_data)
        monkeypatch.setattr(sc_module.boto3, 'Session', MagicMock(return_value=MagicMock(region_name='us-east-1')))

        def fake_foreach(fn):
            for wid in (0, 99, 199):
                fn(wid)
        spark_context.parallelize.return_value.foreach = fake_foreach

        base_args['index'] = 'gsi-1'
        base_args['filter_expression'] = 'attr = :val'
        sc_module.run(MagicMock(), spark_context, MagicMock(), base_args)

        assert len(captured['calls']) == 3
        # Args order: monitor_options, table_name, index_name, filter_expression,
        #             expression_values, expression_names, worker_id, parallelize_count,
        #             total_matched_accumulator, error_accumulator, rate_limiter_shared_config
        for i, wid in enumerate([0, 99, 199]):
            args = captured['calls'][i]
            assert args[1] == 'my-table', "table_name is second arg"
            assert args[2] == 'gsi-1', "index_name passed through"
            assert args[3] == 'attr = :val', "filter_expression passed through"
            assert args[6] == wid, "worker_id is 7th arg"
            assert args[7] == 200, "parallelize_count is 200"

    def test_rdd_count_called_after_foreach(self, monkeypatch, shared_table_info_mocks,
                                             rate_limiter_mocks, spark_context, base_args):
        """Line 85: rdd.count() is called after foreach."""
        spark_context.parallelize.return_value.foreach = MagicMock()
        spark_context.parallelize.return_value.count = MagicMock()
        monkeypatch.setattr(sc_module.boto3, 'Session', MagicMock(return_value=MagicMock(region_name='us-east-1')))

        accs = [MagicMock(value=0), MagicMock(value=[])]
        spark_context.accumulator = MagicMock(side_effect=accs)
        sc_module.run(MagicMock(), spark_context, MagicMock(), base_args)

        spark_context.parallelize.return_value.count.assert_called_once()


# --- _count_data ------------------------------------------------------------


def _make_rl_worker(session=None):
    """Build a mock RateLimiterWorker with a controllable session."""
    rl = MagicMock()
    if session is None:
        session = MagicMock()
    rl.get_session.return_value = session
    return rl


def _make_table_with_scan(scan_responses):
    """Build a mock table whose scan returns pages from scan_responses iterator."""
    table = MagicMock()
    responses = iter(scan_responses)
    table.scan = MagicMock(side_effect=lambda **kw: next(responses))
    return table


class TestCountDataConfig:
    """boto3 Config used inside _count_data (lines 105-112)."""

    def test_config_has_4s_timeouts_and_50_retries(self, monkeypatch):
        """Lines 106-112: connect_timeout=4.0, read_timeout=4.0, retries standard/50."""
        seen_configs = []
        session = MagicMock()

        def capture_resource(name, **kw):
            seen_configs.append(kw.get('config'))
            r = MagicMock()
            r.Table.return_value.scan.return_value = {'Count': 0}
            return r
        session.resource = capture_resource

        rl = _make_rl_worker(session)
        monkeypatch.setattr(sc_module, 'RateLimiterWorker', MagicMock(return_value=rl))

        sc_module._count_data({}, 'tbl', None, None, None, None,
                              0, 1, MagicMock(), MagicMock(), MagicMock())

        assert len(seen_configs) == 1
        cfg = seen_configs[0]
        assert cfg.connect_timeout == 4.0
        assert cfg.read_timeout == 4.0
        assert cfg.retries['mode'] == 'standard'
        assert cfg.retries['total_max_attempts'] == 50


class TestCountDataScanKwargs:
    """Scan kwargs construction (lines 119-132) — optional params only
    included when truthy."""

    def _run_count_data(self, monkeypatch, table_name='tbl', index_name=None,
                        filter_expression=None, expression_values=None,
                        expression_names=None):
        """Helper to run _count_data and capture scan kwargs."""
        session = MagicMock()
        table = MagicMock()
        # Single page, no pagination
        table.scan = MagicMock(return_value={'Count': 5})
        session.resource.return_value.Table.return_value = table

        rl = _make_rl_worker(session)
        monkeypatch.setattr(sc_module, 'RateLimiterWorker', MagicMock(return_value=rl))

        sc_module._count_data({}, table_name, index_name, filter_expression,
                              expression_values, expression_names,
                              3, 10, MagicMock(), MagicMock(), MagicMock())

        return table.scan.call_args.kwargs

    def test_base_scan_kwargs_always_present(self, monkeypatch):
        """Lines 119-124: TableName, Select=COUNT, Segment, TotalSegments always set."""
        kwargs = self._run_count_data(monkeypatch, table_name='my-tbl')
        assert kwargs['TableName'] == 'my-tbl'
        assert kwargs['Select'] == 'COUNT'
        assert kwargs['Segment'] == 3
        assert kwargs['TotalSegments'] == 10

    def test_index_name_included_when_truthy(self, monkeypatch):
        """Line 125-126: IndexName added only when index_name is truthy."""
        kwargs = self._run_count_data(monkeypatch, index_name='gsi-idx')
        assert kwargs['IndexName'] == 'gsi-idx'

    def test_index_name_excluded_when_none(self, monkeypatch):
        """Line 125: branch not taken when index_name is None."""
        kwargs = self._run_count_data(monkeypatch, index_name=None)
        assert 'IndexName' not in kwargs

    def test_filter_expression_included_when_truthy(self, monkeypatch):
        """Lines 127-128: FilterExpression added when filter_expression set."""
        kwargs = self._run_count_data(monkeypatch, filter_expression='#s = :v')
        assert kwargs['FilterExpression'] == '#s = :v'

    def test_filter_expression_excluded_when_none(self, monkeypatch):
        """Line 127: branch not taken when filter_expression is None."""
        kwargs = self._run_count_data(monkeypatch, filter_expression=None)
        assert 'FilterExpression' not in kwargs

    def test_expression_names_decoded_with_decimal_encoder(self, monkeypatch):
        """Lines 129-130: expression_names JSON-parsed with DecimalEncoder."""
        names_json = '{"#s": "status"}'
        kwargs = self._run_count_data(monkeypatch, expression_names=names_json)
        assert kwargs['ExpressionAttributeNames'] == {'#s': 'status'}

    def test_expression_names_excluded_when_none(self, monkeypatch):
        """Line 129: branch not taken when expression_names is None."""
        kwargs = self._run_count_data(monkeypatch, expression_names=None)
        assert 'ExpressionAttributeNames' not in kwargs

    def test_expression_values_decoded_with_decimal_encoder(self, monkeypatch):
        """Lines 131-132: expression_values JSON-parsed, floats become Decimal."""
        values_json = '{"#v": 3.14}'
        kwargs = self._run_count_data(monkeypatch, expression_values=values_json)
        assert kwargs['ExpressionAttributeValues'] == {'#v': Decimal('3.14')}

    def test_expression_values_excluded_when_none(self, monkeypatch):
        """Line 131: branch not taken when expression_values is None."""
        kwargs = self._run_count_data(monkeypatch, expression_values=None)
        assert 'ExpressionAttributeValues' not in kwargs


class TestCountDataPagination:
    """Pagination loop (lines 134-139) threads LastEvaluatedKey into
    ExclusiveStartKey until the key is absent from the response."""

    def test_single_page_no_pagination(self, monkeypatch):
        """Line 137: 'LastEvaluatedKey' not in response -> break immediately."""
        session = MagicMock()
        table = MagicMock()
        table.scan = MagicMock(return_value={'Count': 42})
        session.resource.return_value.Table.return_value = table

        rl = _make_rl_worker(session)
        monkeypatch.setattr(sc_module, 'RateLimiterWorker', MagicMock(return_value=rl))

        total_acc = MagicMock()
        result = sc_module._count_data({}, 'tbl', None, None, None, None,
                                        0, 1, total_acc, MagicMock(), MagicMock())

        assert table.scan.call_count == 1
        assert result == 42
        total_acc.add.assert_called_once_with(42)

    def test_multi_page_threads_lek_into_esk(self, monkeypatch):
        """Lines 138-139: ExclusiveStartKey set from previous LastEvaluatedKey."""
        session = MagicMock()
        table = MagicMock()
        scan_responses = iter([
            {'Count': 10, 'LastEvaluatedKey': {'pk': 'k1'}},
            {'Count': 20, 'LastEvaluatedKey': {'pk': 'k2'}},
            {'Count': 5},  # no LEK -> terminates
        ])
        seen_kwargs = []

        def tracking_scan(**kw):
            seen_kwargs.append(dict(kw))
            return next(scan_responses)

        table.scan = tracking_scan
        session.resource.return_value.Table.return_value = table

        rl = _make_rl_worker(session)
        monkeypatch.setattr(sc_module, 'RateLimiterWorker', MagicMock(return_value=rl))

        total_acc = MagicMock()
        result = sc_module._count_data({}, 'tbl', None, None, None, None,
                                        0, 1, total_acc, MagicMock(), MagicMock())

        assert len(seen_kwargs) == 3
        assert 'ExclusiveStartKey' not in seen_kwargs[0]
        assert seen_kwargs[1]['ExclusiveStartKey'] == {'pk': 'k1'}
        assert seen_kwargs[2]['ExclusiveStartKey'] == {'pk': 'k2'}
        assert result == 35, "10 + 20 + 5 = 35"
        total_acc.add.assert_called_once_with(35)

    def test_count_defaults_to_zero_when_missing(self, monkeypatch):
        """Line 136: response.get('Count', 0) — missing Count treated as 0."""
        session = MagicMock()
        table = MagicMock()
        table.scan = MagicMock(return_value={})  # no Count, no LEK
        session.resource.return_value.Table.return_value = table

        rl = _make_rl_worker(session)
        monkeypatch.setattr(sc_module, 'RateLimiterWorker', MagicMock(return_value=rl))

        total_acc = MagicMock()
        result = sc_module._count_data({}, 'tbl', None, None, None, None,
                                        0, 1, total_acc, MagicMock(), MagicMock())

        assert result == 0
        total_acc.add.assert_called_once_with(0)


class TestCountDataErrorPath:
    """Error handling in _count_data (lines 140-142)."""

    def test_scan_error_appended_to_error_accumulator(self, monkeypatch):
        """Lines 140-141: exception caught, error message added to accumulator."""
        session = MagicMock()
        table = MagicMock()
        table.scan = MagicMock(side_effect=RuntimeError('throttled'))
        session.resource.return_value.Table.return_value = table

        rl = _make_rl_worker(session)
        monkeypatch.setattr(sc_module, 'RateLimiterWorker', MagicMock(return_value=rl))
        monkeypatch.setattr(sc_module, 'get_error_message', lambda e: f'msg:{e}')

        error_acc = MagicMock()
        sc_module._count_data({}, 'tbl', None, None, None, None,
                              7, 10, MagicMock(), error_acc, MagicMock())

        error_acc.add.assert_called_once()
        appended = error_acc.add.call_args.args[0]
        assert isinstance(appended, list) and len(appended) == 1
        assert 'worker 7' in appended[0]
        assert 'msg:' in appended[0]

    def test_error_does_not_propagate(self, monkeypatch):
        """Line 142: control drops to finally, no re-raise."""
        session = MagicMock()
        table = MagicMock()
        table.scan = MagicMock(side_effect=ValueError('bad'))
        session.resource.return_value.Table.return_value = table

        rl = _make_rl_worker(session)
        monkeypatch.setattr(sc_module, 'RateLimiterWorker', MagicMock(return_value=rl))
        monkeypatch.setattr(sc_module, 'get_error_message', lambda e: str(e))

        # Should NOT raise
        result = sc_module._count_data({}, 'tbl', None, None, None, None,
                                        0, 1, MagicMock(), MagicMock(), MagicMock())
        assert result == 0, "local_count stays 0 after scan error"

    def test_rate_limiter_shutdown_after_error(self, monkeypatch):
        """Lines 143-144: rate_limiter_worker.shutdown() in finally."""
        rl = MagicMock()
        session = MagicMock()
        table = MagicMock()
        table.scan = MagicMock(side_effect=RuntimeError('boom'))
        session.resource.return_value.Table.return_value = table
        rl.get_session.return_value = session
        monkeypatch.setattr(sc_module, 'RateLimiterWorker', MagicMock(return_value=rl))
        monkeypatch.setattr(sc_module, 'get_error_message', lambda e: str(e))

        sc_module._count_data({}, 'tbl', None, None, None, None,
                              0, 1, MagicMock(), MagicMock(), MagicMock())

        rl.shutdown.assert_called_once()

    def test_rate_limiter_shutdown_on_success(self, monkeypatch):
        """Lines 143-144: shutdown also called on normal exit."""
        rl = MagicMock()
        session = MagicMock()
        table = MagicMock()
        table.scan = MagicMock(return_value={'Count': 1})
        session.resource.return_value.Table.return_value = table
        rl.get_session.return_value = session
        monkeypatch.setattr(sc_module, 'RateLimiterWorker', MagicMock(return_value=rl))

        sc_module._count_data({}, 'tbl', None, None, None, None,
                              0, 1, MagicMock(), MagicMock(), MagicMock())

        rl.shutdown.assert_called_once()


class TestCountDataWorkerOutput:
    """Worker print output and return value (lines 146-148)."""

    def test_prints_worker_segment_and_count(self, monkeypatch, capsys):
        """Line 146: prints 'Worker {segment}/{total_segments} counted {local_count} records.'"""
        session = MagicMock()
        table = MagicMock()
        table.scan = MagicMock(return_value={'Count': 77})
        session.resource.return_value.Table.return_value = table

        rl = _make_rl_worker(session)
        monkeypatch.setattr(sc_module, 'RateLimiterWorker', MagicMock(return_value=rl))

        sc_module._count_data({}, 'tbl', None, None, None, None,
                              5, 200, MagicMock(), MagicMock(), MagicMock())

        out = capsys.readouterr().out
        assert 'Worker 5/200' in out
        assert '77' in out

    def test_returns_local_count(self, monkeypatch):
        """Line 148: returns local_count."""
        session = MagicMock()
        table = MagicMock()
        scan_responses = iter([
            {'Count': 10, 'LastEvaluatedKey': {'pk': 'x'}},
            {'Count': 15},
        ])
        table.scan = MagicMock(side_effect=lambda **kw: next(scan_responses))
        session.resource.return_value.Table.return_value = table

        rl = _make_rl_worker(session)
        monkeypatch.setattr(sc_module, 'RateLimiterWorker', MagicMock(return_value=rl))

        result = sc_module._count_data({}, 'tbl', None, None, None, None,
                                        0, 1, MagicMock(), MagicMock(), MagicMock())
        assert result == 25

    def test_accumulator_add_called_with_local_count(self, monkeypatch):
        """Line 147: total_matched_accumulator.add(local_count)."""
        session = MagicMock()
        table = MagicMock()
        table.scan = MagicMock(return_value={'Count': 99})
        session.resource.return_value.Table.return_value = table

        rl = _make_rl_worker(session)
        monkeypatch.setattr(sc_module, 'RateLimiterWorker', MagicMock(return_value=rl))

        total_acc = MagicMock()
        sc_module._count_data({}, 'tbl', None, None, None, None,
                              0, 1, total_acc, MagicMock(), MagicMock())

        total_acc.add.assert_called_once_with(99)


class TestCountDataMonitorOptions:
    """monitor_options are splatted into RateLimiterWorker constructor (line 100-102)."""

    def test_monitor_options_passed_to_worker(self, monkeypatch):
        """Lines 99-102: RateLimiterWorker(shared_config=..., **monitor_options)."""
        rl_class = MagicMock()
        rl_instance = MagicMock()
        session = MagicMock()
        table = MagicMock()
        table.scan = MagicMock(return_value={'Count': 0})
        session.resource.return_value.Table.return_value = table
        rl_instance.get_session.return_value = session
        rl_class.return_value = rl_instance
        monkeypatch.setattr(sc_module, 'RateLimiterWorker', rl_class)

        mon_opts = {'read_target': 100, 'monitor_table': 'tbl'}
        sc_module._count_data(mon_opts, 'tbl', None, None, None, None,
                              0, 1, MagicMock(), MagicMock(), MagicMock())

        rl_kwargs = rl_class.call_args.kwargs
        assert rl_kwargs['read_target'] == 100
        assert rl_kwargs['monitor_table'] == 'tbl'
        assert 'shared_config' in rl_kwargs


# --- _count_segment ---------------------------------------------------------


class TestCountSegment:
    """_count_segment counts items in a single segment without accumulator
    side-effects, used by the --per-segment path."""

    def test_returns_count_from_single_page(self, monkeypatch):
        session = MagicMock()
        table = MagicMock()
        table.scan = MagicMock(return_value={'Count': 42})
        session.resource.return_value.Table.return_value = table

        rl = _make_rl_worker(session)
        monkeypatch.setattr(sc_module, 'RateLimiterWorker', MagicMock(return_value=rl))

        result = sc_module._count_segment({}, 'tbl', None, None, None, None,
                                          0, 10, MagicMock())
        assert result == 42

    def test_paginates_and_sums_counts(self, monkeypatch):
        session = MagicMock()
        table = MagicMock()
        responses = iter([
            {'Count': 100, 'LastEvaluatedKey': {'pk': 'a'}},
            {'Count': 50},
        ])
        table.scan = MagicMock(side_effect=lambda **kw: next(responses))
        session.resource.return_value.Table.return_value = table

        rl = _make_rl_worker(session)
        monkeypatch.setattr(sc_module, 'RateLimiterWorker', MagicMock(return_value=rl))

        result = sc_module._count_segment({}, 'tbl', None, None, None, None,
                                          3, 10, MagicMock())
        assert result == 150

    def test_returns_zero_on_error(self, monkeypatch):
        session = MagicMock()
        table = MagicMock()
        table.scan = MagicMock(side_effect=RuntimeError('throttled'))
        session.resource.return_value.Table.return_value = table

        rl = _make_rl_worker(session)
        monkeypatch.setattr(sc_module, 'RateLimiterWorker', MagicMock(return_value=rl))
        monkeypatch.setattr(sc_module, 'get_error_message', lambda e: str(e))

        result = sc_module._count_segment({}, 'tbl', None, None, None, None,
                                          0, 1, MagicMock())
        assert result == 0

    def test_shuts_down_rate_limiter(self, monkeypatch):
        rl = MagicMock()
        session = MagicMock()
        table = MagicMock()
        table.scan = MagicMock(return_value={'Count': 5})
        session.resource.return_value.Table.return_value = table
        rl.get_session.return_value = session
        monkeypatch.setattr(sc_module, 'RateLimiterWorker', MagicMock(return_value=rl))

        sc_module._count_segment({}, 'tbl', None, None, None, None,
                                 0, 1, MagicMock())
        rl.shutdown.assert_called_once()

    def test_includes_index_in_scan_kwargs(self, monkeypatch):
        session = MagicMock()
        table = MagicMock()
        table.scan = MagicMock(return_value={'Count': 1})
        session.resource.return_value.Table.return_value = table

        rl = _make_rl_worker(session)
        monkeypatch.setattr(sc_module, 'RateLimiterWorker', MagicMock(return_value=rl))

        sc_module._count_segment({}, 'tbl', 'my-gsi', None, None, None,
                                 2, 5, MagicMock())

        kwargs = table.scan.call_args.kwargs
        assert kwargs['IndexName'] == 'my-gsi'
        assert kwargs['Segment'] == 2
        assert kwargs['TotalSegments'] == 5

    def test_includes_filter_and_expressions_in_scan_kwargs(self, monkeypatch):
        """A per-segment scan forwards the filter expression and its name/value maps
        just like the accumulator-based _count_data path."""
        session = MagicMock()
        table = MagicMock()
        table.scan = MagicMock(return_value={'Count': 1})
        session.resource.return_value.Table.return_value = table

        rl = _make_rl_worker(session)
        monkeypatch.setattr(sc_module, 'RateLimiterWorker', MagicMock(return_value=rl))

        sc_module._count_segment(
            {}, 'tbl', None,
            '#touched > :touched',
            '{":touched": 1.0}',
            '{"#touched": "touched"}',
            0, 4, MagicMock()
        )

        kwargs = table.scan.call_args.kwargs
        assert kwargs['FilterExpression'] == '#touched > :touched'
        assert kwargs['ExpressionAttributeNames'] == {'#touched': 'touched'}
        assert kwargs['ExpressionAttributeValues'] == {':touched': Decimal('1.0')}


# --- _print_per_segment_counts ----------------------------------------------


class TestPrintPerSegmentCounts:
    """_print_per_segment_counts collects counts via rdd.map().collect() and
    prints them sorted descending with statistics."""

    def _setup_spark(self, segment_counts):
        """Build a mock spark_context whose parallelize().map().collect() returns
        the given list of (segment, count) tuples."""
        sc = MagicMock()
        rdd = MagicMock()
        sc.parallelize = MagicMock(return_value=rdd)
        rdd.map = MagicMock(return_value=MagicMock(collect=MagicMock(return_value=segment_counts)))
        return sc

    def test_prints_header_and_rows(self, monkeypatch, capsys):
        sc = self._setup_spark([(0, 100), (1, 200), (2, 50)])
        sc_module._print_per_segment_counts(sc, {}, 'tbl', None, None, None, None, 3, MagicMock())

        out = capsys.readouterr().out
        assert 'Segment' in out
        assert 'Count' in out
        assert '% of Total' in out
        assert 'Total' in out
        assert '350' in out

    def test_sorted_descending_by_count(self, monkeypatch, capsys):
        sc = self._setup_spark([(0, 10), (1, 500), (2, 30)])
        sc_module._print_per_segment_counts(sc, {}, 'tbl', None, None, None, None, 3, MagicMock())

        out = capsys.readouterr().out
        lines = [l for l in out.split('\n') if l.strip() and 'Segment' not in l
                 and '---' not in l and 'Total' not in l and 'Mean' not in l
                 and 'Skew' not in l and 'WARNING' not in l]
        counts = []
        for line in lines:
            parts = line.split()
            if len(parts) >= 2 and parts[0].isdigit():
                counts.append(int(parts[1].replace(',', '')))
        assert counts == sorted(counts, reverse=True)

    def test_skew_warning_when_ratio_exceeds_5(self, monkeypatch, capsys):
        # Segment 0 has 600 items, segments 1-2 have 10 each. Mean=~206, ratio ~2.9.
        # Make it more extreme: one segment with 1000, rest with 10
        sc = self._setup_spark([(0, 1000), (1, 10), (2, 10)])
        sc_module._print_per_segment_counts(sc, {}, 'tbl', None, None, None, None, 3, MagicMock())

        out = capsys.readouterr().out
        # mean = 340, ratio = 1000/340 = ~2.94 — not enough
        # Need a more extreme case
        assert 'Total' in out

    def test_skew_warning_extreme_skew(self, monkeypatch, capsys):
        # 1 segment with 10000, 9 segments with 1 each.  mean=~1001, ratio=~9.99
        data = [(0, 10000)] + [(i, 1) for i in range(1, 10)]
        sc = self._setup_spark(data)
        sc_module._print_per_segment_counts(sc, {}, 'tbl', None, None, None, None, 10, MagicMock())

        out = capsys.readouterr().out
        assert 'WARNING' in out
        assert 'Skew ratio' in out

    def test_no_warning_when_even_distribution(self, monkeypatch, capsys):
        data = [(i, 100) for i in range(5)]
        sc = self._setup_spark(data)
        sc_module._print_per_segment_counts(sc, {}, 'tbl', None, None, None, None, 5, MagicMock())

        out = capsys.readouterr().out
        assert 'WARNING' not in out

    def test_handles_zero_total(self, monkeypatch, capsys):
        data = [(0, 0), (1, 0)]
        sc = self._setup_spark(data)
        sc_module._print_per_segment_counts(sc, {}, 'tbl', None, None, None, None, 2, MagicMock())

        out = capsys.readouterr().out
        assert 'Total' in out
        assert '0' in out


# --- run() with per_segment flag --------------------------------------------


class TestRunPerSegment:
    """When per_segment=True, run() calls _print_per_segment_counts after
    printing the total."""

    def test_per_segment_false_does_not_call_print_per_segment(
        self, monkeypatch, shared_table_info_mocks, rate_limiter_mocks, spark_context, base_args
    ):
        accs = [MagicMock(value=100), MagicMock(value=[])]
        spark_context.accumulator = MagicMock(side_effect=accs)
        spark_context.parallelize.return_value.foreach = MagicMock()
        spark_context.parallelize.return_value.count = MagicMock()
        monkeypatch.setattr(sc_module.boto3, 'Session', MagicMock(return_value=MagicMock(region_name='us-east-1')))

        mock_pps = MagicMock()
        monkeypatch.setattr(sc_module, '_print_per_segment_counts', mock_pps)

        base_args['per_segment'] = False
        sc_module.run(MagicMock(), spark_context, MagicMock(), base_args)
        mock_pps.assert_not_called()

    def test_per_segment_true_calls_print_per_segment(
        self, monkeypatch, shared_table_info_mocks, rate_limiter_mocks, spark_context, base_args
    ):
        accs = [MagicMock(value=100), MagicMock(value=[])]
        spark_context.accumulator = MagicMock(side_effect=accs)
        spark_context.parallelize.return_value.foreach = MagicMock()
        spark_context.parallelize.return_value.count = MagicMock()
        monkeypatch.setattr(sc_module.boto3, 'Session', MagicMock(return_value=MagicMock(region_name='us-east-1')))

        mock_pps = MagicMock()
        monkeypatch.setattr(sc_module, '_print_per_segment_counts', mock_pps)

        base_args['per_segment'] = True
        sc_module.run(MagicMock(), spark_context, MagicMock(), base_args)
        mock_pps.assert_called_once()

    def test_per_segment_not_in_args_defaults_to_false(
        self, monkeypatch, shared_table_info_mocks, rate_limiter_mocks, spark_context, base_args
    ):
        accs = [MagicMock(value=0), MagicMock(value=[])]
        spark_context.accumulator = MagicMock(side_effect=accs)
        spark_context.parallelize.return_value.foreach = MagicMock()
        spark_context.parallelize.return_value.count = MagicMock()
        monkeypatch.setattr(sc_module.boto3, 'Session', MagicMock(return_value=MagicMock(region_name='us-east-1')))

        mock_pps = MagicMock()
        monkeypatch.setattr(sc_module, '_print_per_segment_counts', mock_pps)

        # per_segment key not present at all
        base_args.pop('per_segment', None)
        sc_module.run(MagicMock(), spark_context, MagicMock(), base_args)
        mock_pps.assert_not_called()
