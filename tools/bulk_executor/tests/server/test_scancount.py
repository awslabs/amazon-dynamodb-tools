"""Unit tests for the `scancount` server-side verb.

Covers `python_modules/scancount/__init__.py`:
- DecimalEncoder: JSON decoder that converts floats to Decimal
- ListAccumulator: zero / addInPlace contract for error accumulation
- Module constants: DYNAMO_DB_THROTTLE_EXCEPTION, DYNAMO_DB_VALIDATION_EXCEPTION
- print_dynamodb_table_info: boto3 session region + shared helper calls
- run(): argument wiring, rate-limiter shared config, monitor options,
  spark parallelize count, single-pass map/collect of per-segment counts,
  total = sum of collected counts, error propagation, rate-limiter shutdown
- _count_data: boto3 Config (timeouts, retries), scan kwargs construction,
  optional index/filter/expression params, pagination loop, per-worker
  error accumulation, rate-limiter shutdown in finally, count return value
- _print_per_segment_counts: sorts/formats the already-collected counts
  (no second scan) with % of total, mean, and skew-ratio warning
- _format_percent: fraction -> percent string with no trailing zeros
- _print_sampled_estimate: extrapolated total + 95% CI from a segment sample,
  finite-population correction, k<2 and empty-sample edge cases, skew note
- run() with --sample-fraction: scans a random subset of segments, reports the
  true fraction, forwards it to the cost helper, and prints the estimate
"""

import json
from decimal import Decimal
from unittest.mock import MagicMock, call, patch

import pytest

from python_modules import scancount as sc_module
from python_modules.shared import worker_errors

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
    """Mock SparkContext that records accumulator() and parallelize() calls.

    parallelize().map().collect() returns an empty list by default; tests that
    care about the collected per-segment counts override the collect() return.
    """
    sc = MagicMock()
    sc.accumulator = MagicMock(side_effect=lambda init, *_: MagicMock(value=init))
    rdd = MagicMock()
    rdd.map = MagicMock(return_value=MagicMock(collect=MagicMock(return_value=[])))
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
    """run() parses args, configures rate limiting, and dispatches a single
    parallel segmented scan via rdd.map().collect()."""

    def test_shared_config_uses_bucket_and_job_run_id(self, monkeypatch, shared_table_info_mocks,
                                                       rate_limiter_mocks, spark_context, base_args):
        """RateLimiterSharedConfig receives bucket and job_run_id."""
        monkeypatch.setattr(sc_module.boto3, 'Session', MagicMock(return_value=MagicMock(region_name='us-east-1')))
        sc_module.run(MagicMock(), spark_context, MagicMock(), base_args)

        config_call = rate_limiter_mocks.config.call_args
        assert config_call.kwargs['bucket'] == 'rate-bucket'
        assert config_call.kwargs['job_run_id'] == 'jr-001'

    def test_aggregator_receives_shared_config(self, monkeypatch, shared_table_info_mocks,
                                                rate_limiter_mocks, spark_context, base_args):
        """RateLimiterAggregator is constructed with the shared_config."""
        monkeypatch.setattr(sc_module.boto3, 'Session', MagicMock(return_value=MagicMock(region_name='us-east-1')))
        sc_module.run(MagicMock(), spark_context, MagicMock(), base_args)

        agg_call = rate_limiter_mocks.aggregator.call_args
        assert 'shared_config' in agg_call.kwargs, "aggregator gets shared_config kwarg"

    def test_throughput_configs_called_for_read_mode(self, monkeypatch, shared_table_info_mocks,
                                                      rate_limiter_mocks, spark_context, base_args):
        """get_dynamodb_throughput_configs called with modes=['read'], format='monitor'."""
        monkeypatch.setattr(sc_module.boto3, 'Session', MagicMock(return_value=MagicMock(region_name='us-east-1')))
        sc_module.run(MagicMock(), spark_context, MagicMock(), base_args)

        tc_call = shared_table_info_mocks.get_dynamodb_throughput_configs.call_args
        assert tc_call.args[1] == 'my-table'
        assert tc_call.kwargs['modes'] == ['read']
        assert tc_call.kwargs['format'] == 'monitor'

    def test_parallelize_count_is_200(self, monkeypatch, shared_table_info_mocks,
                                       rate_limiter_mocks, spark_context, base_args):
        """parallelize(range(200), 200) when segments not specified."""
        monkeypatch.setattr(sc_module.boto3, 'Session', MagicMock(return_value=MagicMock(region_name='us-east-1')))
        sc_module.run(MagicMock(), spark_context, MagicMock(), base_args)

        pc_args = spark_context.parallelize.call_args
        assert list(pc_args.args[0]) == list(range(200)), "range(200) as first arg"
        assert pc_args.args[1] == 200, "numSlices is 200"

    def test_parallelize_count_respects_segments_arg(self, monkeypatch, shared_table_info_mocks,
                                                     rate_limiter_mocks, spark_context, base_args):
        """When segments is specified, parallelize uses that value."""
        monkeypatch.setattr(sc_module.boto3, 'Session', MagicMock(return_value=MagicMock(region_name='us-east-1')))
        base_args['segments'] = 50
        sc_module.run(MagicMock(), spark_context, MagicMock(), base_args)

        pc_args = spark_context.parallelize.call_args
        assert list(pc_args.args[0]) == list(range(50)), "range(50) as first arg"
        assert pc_args.args[1] == 50, "numSlices is 50"

    def test_only_error_accumulator_created(self, monkeypatch, shared_table_info_mocks,
                                             rate_limiter_mocks, spark_context, base_args):
        """The single-pass design keeps only the error accumulator — the total
        is now summed from the collected segment counts, not an accumulator."""
        monkeypatch.setattr(sc_module.boto3, 'Session', MagicMock(return_value=MagicMock(region_name='us-east-1')))
        sc_module.run(MagicMock(), spark_context, MagicMock(), base_args)

        assert spark_context.accumulator.call_count == 1, "only the error accumulator is created"
        err_call = spark_context.accumulator.call_args_list[0]
        assert err_call.args[0] == []
        assert isinstance(err_call.args[1], sc_module.ListAccumulator)


class TestRunTotalFromCollectedCounts:
    """The grand total is the sum of the (segment, count) pairs returned by
    rdd.map().collect() — no second scan, no total accumulator."""

    def _collect_returns(self, spark_context, pairs):
        spark_context.parallelize.return_value.map.return_value.collect = MagicMock(
            return_value=pairs
        )

    def test_total_is_sum_of_segment_counts(self, monkeypatch, shared_table_info_mocks,
                                             rate_limiter_mocks, spark_context, base_args, capsys):
        """Total printed is the sum of collected counts, comma-formatted."""
        self._collect_returns(spark_context, [(0, 1000), (1, 200), (2, 34)])
        monkeypatch.setattr(sc_module.boto3, 'Session', MagicMock(return_value=MagicMock(region_name='us-east-1')))

        sc_module.run(MagicMock(), spark_context, MagicMock(), base_args)
        out = capsys.readouterr().out
        assert 'Total records counted: 1,234' in out, "1000+200+34 = 1,234"

    def test_empty_collect_totals_zero(self, monkeypatch, shared_table_info_mocks,
                                       rate_limiter_mocks, spark_context, base_args, capsys):
        """No segments returned -> total is 0 (sum of empty)."""
        self._collect_returns(spark_context, [])
        monkeypatch.setattr(sc_module.boto3, 'Session', MagicMock(return_value=MagicMock(region_name='us-east-1')))

        sc_module.run(MagicMock(), spark_context, MagicMock(), base_args)
        out = capsys.readouterr().out
        assert 'Total records counted: 0' in out


class TestRunErrorHandling:
    """Error propagation from workers and exception wrapping in run()."""

    def test_first_worker_error_raised_after_collect(self, monkeypatch, shared_table_info_mocks,
                                                      rate_limiter_mocks, spark_context, base_args):
        """If error_accumulator.value is non-empty, raise the first error."""
        spark_context.accumulator = MagicMock(
            return_value=MagicMock(value=['worker 3 failed', 'worker 7 failed'])
        )
        monkeypatch.setattr(sc_module.boto3, 'Session', MagicMock(return_value=MagicMock(region_name='us-east-1')))

        with pytest.raises(Exception, match='worker 3 failed'):
            sc_module.run(MagicMock(), spark_context, MagicMock(), base_args)

    def test_map_exception_wraps_in_parallel_execution_error(self, monkeypatch, shared_table_info_mocks,
                                                              rate_limiter_mocks, spark_context, base_args):
        """An exception from the map/collect is wrapped with 'Error in parallel execution'."""
        spark_context.parallelize.return_value.map = MagicMock(
            side_effect=RuntimeError('spark died')
        )
        monkeypatch.setattr(sc_module.boto3, 'Session', MagicMock(return_value=MagicMock(region_name='us-east-1')))
        monkeypatch.setattr(sc_module, 'get_error_message', lambda e: str(e))

        with pytest.raises(Exception, match='Error in parallel execution.*spark died'):
            sc_module.run(MagicMock(), spark_context, MagicMock(), base_args)

    def test_aggregator_shutdown_even_on_map_failure(self, monkeypatch, shared_table_info_mocks,
                                                      spark_context, base_args):
        """rate_limiter_aggregator.shutdown() runs in the finally block on failure."""
        agg_instance = MagicMock()
        monkeypatch.setattr(sc_module, 'RateLimiterSharedConfig', MagicMock())
        monkeypatch.setattr(sc_module, 'RateLimiterAggregator', MagicMock(return_value=agg_instance))
        monkeypatch.setattr(sc_module.boto3, 'Session', MagicMock(return_value=MagicMock(region_name='us-east-1')))

        spark_context.parallelize.return_value.map = MagicMock(
            side_effect=RuntimeError('boom')
        )
        monkeypatch.setattr(sc_module, 'get_error_message', lambda e: str(e))

        with pytest.raises(Exception):
            sc_module.run(MagicMock(), spark_context, MagicMock(), base_args)

        agg_instance.shutdown.assert_called_once()

    def test_aggregator_shutdown_on_success(self, monkeypatch, shared_table_info_mocks,
                                             rate_limiter_mocks, spark_context, base_args):
        """shutdown called even on the normal exit path."""
        agg_instance = MagicMock()
        monkeypatch.setattr(sc_module, 'RateLimiterAggregator', MagicMock(return_value=agg_instance))
        monkeypatch.setattr(sc_module.boto3, 'Session', MagicMock(return_value=MagicMock(region_name='us-east-1')))

        sc_module.run(MagicMock(), spark_context, MagicMock(), base_args)
        agg_instance.shutdown.assert_called_once()


class TestRunMapDispatch:
    """The lambda passed to rdd.map invokes _count_data with correct args and
    pairs each result with its worker_id."""

    def test_map_lambda_passes_all_positional_args(self, monkeypatch, shared_table_info_mocks,
                                                    rate_limiter_mocks, spark_context, base_args):
        """lambda worker_id: (worker_id, _count_data(..., worker_id, parallelize_count, ...))."""
        captured = {}

        def fake_count_data(*args, **kwargs):
            captured.setdefault('calls', []).append(args)
            return 5

        monkeypatch.setattr(sc_module, '_count_data', fake_count_data)
        monkeypatch.setattr(sc_module.boto3, 'Session', MagicMock(return_value=MagicMock(region_name='us-east-1')))

        def fake_map(fn):
            return MagicMock(collect=MagicMock(return_value=[fn(wid) for wid in (0, 99, 199)]))
        spark_context.parallelize.return_value.map = fake_map

        base_args['index'] = 'gsi-1'
        base_args['filter_expression'] = 'attr = :val'
        sc_module.run(MagicMock(), spark_context, MagicMock(), base_args)

        assert len(captured['calls']) == 3
        # Args order: monitor_options, table_name, index_name, filter_expression,
        #             expression_values, expression_names, worker_id, parallelize_count,
        #             error_accumulator, rate_limiter_shared_config
        for i, wid in enumerate([0, 99, 199]):
            args = captured['calls'][i]
            assert args[1] == 'my-table', "table_name is second arg"
            assert args[2] == 'gsi-1', "index_name passed through"
            assert args[3] == 'attr = :val', "filter_expression passed through"
            assert args[6] == wid, "worker_id is 7th arg"
            assert args[7] == 200, "parallelize_count is 200"

    def test_map_result_paired_with_worker_id(self, monkeypatch, shared_table_info_mocks,
                                              rate_limiter_mocks, spark_context, base_args, capsys):
        """Each collected element is (worker_id, count); the total sums the counts."""
        monkeypatch.setattr(sc_module, '_count_data',
                            lambda *a, **k: {0: 7, 1: 3}[a[6]])
        monkeypatch.setattr(sc_module.boto3, 'Session', MagicMock(return_value=MagicMock(region_name='us-east-1')))

        def fake_map(fn):
            return MagicMock(collect=MagicMock(return_value=[fn(wid) for wid in (0, 1)]))
        spark_context.parallelize.return_value.map = fake_map
        base_args['segments'] = 2

        sc_module.run(MagicMock(), spark_context, MagicMock(), base_args)
        out = capsys.readouterr().out
        assert 'Total records counted: 10' in out, "7 + 3 = 10"


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
    """boto3 Config used inside _count_data."""

    def test_config_has_4s_timeouts_and_50_retries(self, monkeypatch):
        """connect_timeout=4.0, read_timeout=4.0, retries standard/50."""
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
                              0, 1, MagicMock(), MagicMock())

        assert len(seen_configs) == 1
        cfg = seen_configs[0]
        assert cfg.connect_timeout == 4.0
        assert cfg.read_timeout == 4.0
        assert cfg.retries['mode'] == 'standard'
        assert cfg.retries['total_max_attempts'] == 50


class TestCountDataScanKwargs:
    """Scan kwargs construction — optional params only included when truthy."""

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
                              3, 10, MagicMock(), MagicMock())

        return table.scan.call_args.kwargs

    def test_base_scan_kwargs_always_present(self, monkeypatch):
        """Select=COUNT, Segment, TotalSegments always set (not TableName — the
        Table resource already knows its name)."""
        kwargs = self._run_count_data(monkeypatch, table_name='my-tbl')
        assert 'TableName' not in kwargs
        assert kwargs['Select'] == 'COUNT'
        assert kwargs['Segment'] == 3
        assert kwargs['TotalSegments'] == 10

    def test_index_name_included_when_truthy(self, monkeypatch):
        """IndexName added only when index_name is truthy."""
        kwargs = self._run_count_data(monkeypatch, index_name='gsi-idx')
        assert kwargs['IndexName'] == 'gsi-idx'

    def test_index_name_excluded_when_none(self, monkeypatch):
        """Branch not taken when index_name is None."""
        kwargs = self._run_count_data(monkeypatch, index_name=None)
        assert 'IndexName' not in kwargs

    def test_filter_expression_included_when_truthy(self, monkeypatch):
        """FilterExpression added when filter_expression set."""
        kwargs = self._run_count_data(monkeypatch, filter_expression='#s = :v')
        assert kwargs['FilterExpression'] == '#s = :v'

    def test_filter_expression_excluded_when_none(self, monkeypatch):
        """Branch not taken when filter_expression is None."""
        kwargs = self._run_count_data(monkeypatch, filter_expression=None)
        assert 'FilterExpression' not in kwargs

    def test_expression_names_decoded_with_decimal_encoder(self, monkeypatch):
        """expression_names JSON-parsed with DecimalEncoder."""
        names_json = '{"#s": "status"}'
        kwargs = self._run_count_data(monkeypatch, expression_names=names_json)
        assert kwargs['ExpressionAttributeNames'] == {'#s': 'status'}

    def test_expression_names_excluded_when_none(self, monkeypatch):
        """Branch not taken when expression_names is None."""
        kwargs = self._run_count_data(monkeypatch, expression_names=None)
        assert 'ExpressionAttributeNames' not in kwargs

    def test_expression_values_decoded_with_decimal_encoder(self, monkeypatch):
        """expression_values JSON-parsed, floats become Decimal."""
        values_json = '{"#v": 3.14}'
        kwargs = self._run_count_data(monkeypatch, expression_values=values_json)
        assert kwargs['ExpressionAttributeValues'] == {'#v': Decimal('3.14')}

    def test_expression_values_excluded_when_none(self, monkeypatch):
        """Branch not taken when expression_values is None."""
        kwargs = self._run_count_data(monkeypatch, expression_values=None)
        assert 'ExpressionAttributeValues' not in kwargs


class TestCountDataPagination:
    """Pagination loop threads LastEvaluatedKey into ExclusiveStartKey until
    the key is absent from the response."""

    def test_single_page_no_pagination(self, monkeypatch):
        """'LastEvaluatedKey' not in response -> break immediately."""
        session = MagicMock()
        table = MagicMock()
        table.scan = MagicMock(return_value={'Count': 42})
        session.resource.return_value.Table.return_value = table

        rl = _make_rl_worker(session)
        monkeypatch.setattr(sc_module, 'RateLimiterWorker', MagicMock(return_value=rl))

        result = sc_module._count_data({}, 'tbl', None, None, None, None,
                                        0, 1, MagicMock(), MagicMock())

        assert table.scan.call_count == 1
        assert result == 42

    def test_multi_page_threads_lek_into_esk(self, monkeypatch):
        """ExclusiveStartKey set from previous LastEvaluatedKey."""
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

        result = sc_module._count_data({}, 'tbl', None, None, None, None,
                                        0, 1, MagicMock(), MagicMock())

        assert len(seen_kwargs) == 3
        assert 'ExclusiveStartKey' not in seen_kwargs[0]
        assert seen_kwargs[1]['ExclusiveStartKey'] == {'pk': 'k1'}
        assert seen_kwargs[2]['ExclusiveStartKey'] == {'pk': 'k2'}
        assert result == 35, "10 + 20 + 5 = 35"

    def test_count_defaults_to_zero_when_missing(self, monkeypatch):
        """response.get('Count', 0) — missing Count treated as 0."""
        session = MagicMock()
        table = MagicMock()
        table.scan = MagicMock(return_value={})  # no Count, no LEK
        session.resource.return_value.Table.return_value = table

        rl = _make_rl_worker(session)
        monkeypatch.setattr(sc_module, 'RateLimiterWorker', MagicMock(return_value=rl))

        result = sc_module._count_data({}, 'tbl', None, None, None, None,
                                        0, 1, MagicMock(), MagicMock())

        assert result == 0


class TestCountDataErrorPath:
    """Error handling in _count_data."""

    def test_scan_error_appended_to_error_accumulator(self, monkeypatch):
        """Exception caught, error message added to accumulator."""
        session = MagicMock()
        table = MagicMock()
        table.scan = MagicMock(side_effect=RuntimeError('throttled'))
        session.resource.return_value.Table.return_value = table

        rl = _make_rl_worker(session)
        monkeypatch.setattr(sc_module, 'RateLimiterWorker', MagicMock(return_value=rl))
        monkeypatch.setattr(sc_module, 'get_error_message', lambda e: f'msg:{e}')
        # shared.errors is a Mock in tests/server, so patch where worker_errors reads it.
        monkeypatch.setattr(worker_errors, 'get_error_message', str)

        error_acc = MagicMock()
        sc_module._count_data({}, 'tbl', None, None, None, None,
                              7, 10, error_acc, MagicMock())

        error_acc.add.assert_called_once()
        appended = error_acc.add.call_args.args[0]
        assert isinstance(appended, list) and len(appended) == 1
        message, detail = appended[0]
        assert 'worker 7' in message
        assert 'throttled' in message
        assert 'Traceback' in detail, "an unexpected failure carries the worker traceback"

    def test_error_does_not_propagate(self, monkeypatch):
        """Control drops to finally, no re-raise; count stays 0."""
        session = MagicMock()
        table = MagicMock()
        table.scan = MagicMock(side_effect=ValueError('bad'))
        session.resource.return_value.Table.return_value = table

        rl = _make_rl_worker(session)
        monkeypatch.setattr(sc_module, 'RateLimiterWorker', MagicMock(return_value=rl))
        monkeypatch.setattr(sc_module, 'get_error_message', lambda e: str(e))

        # Should NOT raise
        result = sc_module._count_data({}, 'tbl', None, None, None, None,
                                        0, 1, MagicMock(), MagicMock())
        assert result == 0, "local_count stays 0 after scan error"

    def test_rate_limiter_shutdown_after_error(self, monkeypatch):
        """rate_limiter_worker.shutdown() in finally."""
        rl = MagicMock()
        session = MagicMock()
        table = MagicMock()
        table.scan = MagicMock(side_effect=RuntimeError('boom'))
        session.resource.return_value.Table.return_value = table
        rl.get_session.return_value = session
        monkeypatch.setattr(sc_module, 'RateLimiterWorker', MagicMock(return_value=rl))
        monkeypatch.setattr(sc_module, 'get_error_message', lambda e: str(e))

        sc_module._count_data({}, 'tbl', None, None, None, None,
                              0, 1, MagicMock(), MagicMock())

        rl.shutdown.assert_called_once()

    def test_rate_limiter_shutdown_on_success(self, monkeypatch):
        """shutdown also called on normal exit."""
        rl = MagicMock()
        session = MagicMock()
        table = MagicMock()
        table.scan = MagicMock(return_value={'Count': 1})
        session.resource.return_value.Table.return_value = table
        rl.get_session.return_value = session
        monkeypatch.setattr(sc_module, 'RateLimiterWorker', MagicMock(return_value=rl))

        sc_module._count_data({}, 'tbl', None, None, None, None,
                              0, 1, MagicMock(), MagicMock())

        rl.shutdown.assert_called_once()


class TestCountDataWorkerOutput:
    """Worker print output and return value."""

    def test_prints_worker_segment_and_count(self, monkeypatch, capsys):
        """Prints 'Worker {segment}/{total_segments} counted {local_count} records.'"""
        session = MagicMock()
        table = MagicMock()
        table.scan = MagicMock(return_value={'Count': 77})
        session.resource.return_value.Table.return_value = table

        rl = _make_rl_worker(session)
        monkeypatch.setattr(sc_module, 'RateLimiterWorker', MagicMock(return_value=rl))

        sc_module._count_data({}, 'tbl', None, None, None, None,
                              5, 200, MagicMock(), MagicMock())

        out = capsys.readouterr().out
        assert 'Worker 5/200' in out
        assert '77' in out

    def test_returns_local_count(self, monkeypatch):
        """Returns local_count."""
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
                                        0, 1, MagicMock(), MagicMock())
        assert result == 25


class TestCountDataMonitorOptions:
    """monitor_options are splatted into RateLimiterWorker constructor."""

    def test_monitor_options_passed_to_worker(self, monkeypatch):
        """RateLimiterWorker(shared_config=..., **monitor_options)."""
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
                              0, 1, MagicMock(), MagicMock())

        rl_kwargs = rl_class.call_args.kwargs
        assert rl_kwargs['read_target'] == 100
        assert rl_kwargs['monitor_table'] == 'tbl'
        assert 'shared_config' in rl_kwargs


# --- _print_per_segment_counts ----------------------------------------------


class TestPrintPerSegmentCounts:
    """_print_per_segment_counts formats the already-collected (segment, count)
    list — it sorts descending and reports statistics without scanning again."""

    def test_prints_header_and_rows(self, capsys):
        sc_module._print_per_segment_counts([(0, 100), (1, 200), (2, 50)], 3)

        out = capsys.readouterr().out
        assert 'Segment' in out
        assert 'Count' in out
        assert '% of Total' in out
        assert 'Total' in out
        assert '350' in out

    def test_does_not_scan_again(self, capsys):
        """Pure formatting: given a list, it must not touch Spark or DynamoDB.
        The signature takes no spark_context / rate-limiter, so there is nothing
        to scan with — this asserts the count is derived from the passed list."""
        sc_module._print_per_segment_counts([(0, 100), (1, 200)], 2)
        out = capsys.readouterr().out
        assert '300' in out, "total is the sum of the supplied counts"

    def test_sorted_descending_by_count(self, capsys):
        sc_module._print_per_segment_counts([(0, 10), (1, 500), (2, 30)], 3)

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

    def test_skew_warning_extreme_skew(self, capsys):
        # 1 segment with 10000, 9 segments with 1 each.  mean=~1001, ratio=~9.99
        data = [(0, 10000)] + [(i, 1) for i in range(1, 10)]
        sc_module._print_per_segment_counts(data, 10)

        out = capsys.readouterr().out
        assert 'WARNING' in out
        assert 'Skew ratio' in out

    def test_no_warning_when_even_distribution(self, capsys):
        data = [(i, 100) for i in range(5)]
        sc_module._print_per_segment_counts(data, 5)

        out = capsys.readouterr().out
        assert 'WARNING' not in out

    def test_handles_zero_total(self, capsys):
        data = [(0, 0), (1, 0)]
        sc_module._print_per_segment_counts(data, 2)

        out = capsys.readouterr().out
        assert 'Total' in out
        assert '0' in out


# --- run() with per_segment flag --------------------------------------------


class TestRunPerSegment:
    """When per_segment=True, run() passes the collected segment counts to
    _print_per_segment_counts after printing the total."""

    def test_per_segment_false_does_not_call_print_per_segment(
        self, monkeypatch, shared_table_info_mocks, rate_limiter_mocks, spark_context, base_args
    ):
        monkeypatch.setattr(sc_module.boto3, 'Session', MagicMock(return_value=MagicMock(region_name='us-east-1')))

        mock_pps = MagicMock()
        monkeypatch.setattr(sc_module, '_print_per_segment_counts', mock_pps)

        base_args['per_segment'] = False
        sc_module.run(MagicMock(), spark_context, MagicMock(), base_args)
        mock_pps.assert_not_called()

    def test_per_segment_true_passes_collected_counts(
        self, monkeypatch, shared_table_info_mocks, rate_limiter_mocks, spark_context, base_args
    ):
        collected = [(0, 100), (1, 50)]
        spark_context.parallelize.return_value.map.return_value.collect = MagicMock(
            return_value=collected
        )
        monkeypatch.setattr(sc_module.boto3, 'Session', MagicMock(return_value=MagicMock(region_name='us-east-1')))

        mock_pps = MagicMock()
        monkeypatch.setattr(sc_module, '_print_per_segment_counts', mock_pps)

        base_args['per_segment'] = True
        sc_module.run(MagicMock(), spark_context, MagicMock(), base_args)

        mock_pps.assert_called_once()
        pps_args = mock_pps.call_args.args
        assert pps_args[0] == collected, "the already-collected counts are reused (no re-scan)"
        assert pps_args[1] == 200, "parallelize_count passed through"

    def test_per_segment_not_in_args_defaults_to_false(
        self, monkeypatch, shared_table_info_mocks, rate_limiter_mocks, spark_context, base_args
    ):
        monkeypatch.setattr(sc_module.boto3, 'Session', MagicMock(return_value=MagicMock(region_name='us-east-1')))

        mock_pps = MagicMock()
        monkeypatch.setattr(sc_module, '_print_per_segment_counts', mock_pps)

        # per_segment key not present at all
        base_args.pop('per_segment', None)
        sc_module.run(MagicMock(), spark_context, MagicMock(), base_args)
        mock_pps.assert_not_called()


# --- _format_percent --------------------------------------------------------


class TestFormatPercent:
    """_format_percent renders a fraction as a percent trimmed to at most 3
    decimals with no trailing zeros."""

    def test_clean_ten_percent(self):
        assert sc_module._format_percent(0.1) == '10%'

    def test_half(self):
        assert sc_module._format_percent(0.5) == '50%'

    def test_full(self):
        assert sc_module._format_percent(1.0) == '100%'

    def test_repeating_fraction_capped_at_three_decimals(self):
        # 1/15 = 6.666...% -> 6.667% (rounded to 3 decimals), never a long tail
        assert sc_module._format_percent(1 / 15) == '6.667%'

    def test_trailing_zeros_stripped_after_rounding(self):
        # 4/201 = 1.99004...% -> rounds to 1.990% -> trailing zero stripped
        assert sc_module._format_percent(4 / 201) == '1.99%'

    def test_small_fraction(self):
        assert sc_module._format_percent(0.005) == '0.5%'


# --- _print_sampled_estimate ------------------------------------------------


class TestPrintSampledEstimate:
    """_print_sampled_estimate extrapolates a full-table count from a sample of
    segments and prints a 95% confidence interval."""

    def test_reports_sampled_count_and_segment_ratio(self, capsys):
        # 3 segments sampled out of 200
        counts = [(0, 100), (5, 110), (9, 90)]
        sc_module._print_sampled_estimate(counts, 200, 300)
        out = capsys.readouterr().out
        assert 'Sampled count:' in out
        assert '300' in out
        assert 'across 3 of 200 segments' in out

    def test_estimate_is_total_segments_times_mean(self, capsys):
        # mean = 100 across 3 sampled, N=200 -> estimate 20,000
        counts = [(0, 100), (1, 100), (2, 100)]
        sc_module._print_sampled_estimate(counts, 200, 300)
        out = capsys.readouterr().out
        assert 'Estimated total:' in out
        assert '20,000' in out

    def test_even_sample_has_zero_margin(self, capsys):
        """Identical segment counts -> stddev 0 -> ± 0.0% and a tight CI."""
        counts = [(i, 50) for i in range(10)]
        sc_module._print_sampled_estimate(counts, 200, 500)
        out = capsys.readouterr().out
        assert '± 0.0%' in out
        assert '95% CI' in out

    def test_confidence_interval_present_for_varied_sample(self, capsys):
        counts = [(0, 10), (1, 90), (2, 40), (3, 60)]
        sc_module._print_sampled_estimate(counts, 200, 200)
        out = capsys.readouterr().out
        assert '95% CI' in out
        assert '±' in out

    def test_single_segment_no_confidence_interval(self, capsys):
        """k=1 cannot yield a stddev, so no CI — an explicit note instead."""
        counts = [(7, 123)]
        sc_module._print_sampled_estimate(counts, 200, 123)
        out = capsys.readouterr().out
        assert 'Estimated total:' in out
        assert '95% CI' not in out
        assert 'too small for a confidence interval' in out

    def test_high_skew_note_when_cv_exceeds_one(self, capsys):
        """A coefficient of variation > 1 across segments prints the skew note,
        including the --per-segment hint when the table was not already shown."""
        # one hot segment dwarfs the rest -> cv > 1
        counts = [(0, 10000)] + [(i, 1) for i in range(1, 6)]
        sc_module._print_sampled_estimate(counts, 200, 10005)
        out = capsys.readouterr().out
        assert 'data skew' in out
        assert 'Add --per-segment to inspect it.' in out

    def test_low_skew_note_when_even(self, capsys):
        counts = [(i, 100) for i in range(5)]
        sc_module._print_sampled_estimate(counts, 200, 500)
        out = capsys.readouterr().out
        assert 'assumes segment sizes are representative' in out
        assert 'Add --per-segment to inspect it.' in out

    def test_high_skew_note_omits_hint_when_per_segment_shown(self, capsys):
        """When the per-segment table was already printed, the estimate does not
        tell the user to add --per-segment (it is already there)."""
        counts = [(0, 10000)] + [(i, 1) for i in range(1, 6)]
        sc_module._print_sampled_estimate(counts, 200, 10005, per_segment_shown=True)
        out = capsys.readouterr().out
        assert 'data skew' in out
        assert '--per-segment' not in out

    def test_low_skew_note_omits_hint_when_per_segment_shown(self, capsys):
        counts = [(i, 100) for i in range(5)]
        sc_module._print_sampled_estimate(counts, 200, 500, per_segment_shown=True)
        out = capsys.readouterr().out
        assert 'assumes segment sizes are representative' in out
        assert '--per-segment' not in out

    def test_all_empty_segments_reports_zero_estimate(self, capsys):
        """Every sampled segment empty -> estimate 0, explicit empty note, no CI."""
        counts = [(0, 0), (1, 0), (2, 0)]
        sc_module._print_sampled_estimate(counts, 200, 0)
        out = capsys.readouterr().out
        assert 'Estimated total:' in out
        assert 'every sampled segment was empty' in out
        assert '95% CI' not in out

    def test_ci_lower_bound_floored_at_zero(self, capsys):
        """A wide margin on a small estimate must not print a negative lower bound."""
        # tiny estimate, huge spread -> margin exceeds estimate
        counts = [(0, 0), (1, 0), (2, 30)]
        sc_module._print_sampled_estimate(counts, 200, 30)
        out = capsys.readouterr().out
        # find the CI line
        ci_line = next(l for l in out.split('\n') if '95% CI' in l)
        low = ci_line.split('95% CI:')[1].split('–')[0].strip()
        assert not low.startswith('-'), "lower bound floored at 0, never negative"


# --- run() with --sample-fraction -------------------------------------------


class TestRunSampling:
    """When sample_fraction < 1.0, run() scans a random subset of segments and
    prints an extrapolated estimate instead of an exact total."""

    def _collect_returns(self, spark_context, pairs):
        spark_context.parallelize.return_value.map.return_value.collect = MagicMock(
            return_value=pairs
        )

    def test_sampling_parallelizes_subset_of_segments(self, monkeypatch, shared_table_info_mocks,
                                                       rate_limiter_mocks, spark_context, base_args):
        """0.1 of 200 segments -> 20 indices parallelized (numSlices = 20)."""
        self._collect_returns(spark_context, [(i, 10) for i in range(20)])
        monkeypatch.setattr(sc_module.boto3, 'Session', MagicMock(return_value=MagicMock(region_name='us-east-1')))

        base_args['sample_fraction'] = 0.1
        sc_module.run(MagicMock(), spark_context, MagicMock(), base_args)

        pc_args = spark_context.parallelize.call_args
        assert len(list(pc_args.args[0])) == 20, "20 of 200 segments scanned"
        assert pc_args.args[1] == 20, "numSlices matches sampled count"

    def test_sampling_prints_banner_with_true_fraction(self, monkeypatch, shared_table_info_mocks,
                                                        rate_limiter_mocks, spark_context, base_args, capsys):
        self._collect_returns(spark_context, [(i, 10) for i in range(20)])
        monkeypatch.setattr(sc_module.boto3, 'Session', MagicMock(return_value=MagicMock(region_name='us-east-1')))

        base_args['sample_fraction'] = 0.1
        sc_module.run(MagicMock(), spark_context, MagicMock(), base_args)
        out = capsys.readouterr().out
        assert 'Sampling 10% of segments (20 of 200 total)' in out

    def test_true_fraction_forwarded_to_cost_helper(self, monkeypatch, rate_limiter_mocks,
                                                     spark_context, base_args):
        """The reduced scan cost must reflect the sampled fraction, not a full scan."""
        self._collect_returns(spark_context, [(i, 10) for i in range(20)])
        monkeypatch.setattr(sc_module.boto3, 'Session', MagicMock(return_value=MagicMock(region_name='us-east-1')))
        monkeypatch.setattr(sc_module, 'get_and_print_dynamodb_table_info', MagicMock(return_value={}))
        monkeypatch.setattr(sc_module, 'get_dynamodb_throughput_configs', MagicMock(return_value={}))
        cost = MagicMock(return_value=0.0)
        monkeypatch.setattr(sc_module, 'get_and_print_table_scan_cost', cost)

        base_args['sample_fraction'] = 0.1
        sc_module.run(MagicMock(), spark_context, MagicMock(), base_args)

        assert cost.call_args.kwargs['fraction'] == 20 / 200

    def test_sampling_prints_estimate_not_exact_total(self, monkeypatch, shared_table_info_mocks,
                                                       rate_limiter_mocks, spark_context, base_args, capsys):
        self._collect_returns(spark_context, [(i, 100) for i in range(20)])
        monkeypatch.setattr(sc_module.boto3, 'Session', MagicMock(return_value=MagicMock(region_name='us-east-1')))

        base_args['sample_fraction'] = 0.1
        sc_module.run(MagicMock(), spark_context, MagicMock(), base_args)
        out = capsys.readouterr().out
        assert 'Estimated total:' in out
        assert 'Total records counted:' not in out, "exact-total line suppressed while sampling"

    def test_sampling_with_per_segment_prints_both_table_and_estimate(
        self, monkeypatch, shared_table_info_mocks, rate_limiter_mocks, spark_context, base_args
    ):
        self._collect_returns(spark_context, [(i, 100) for i in range(20)])
        monkeypatch.setattr(sc_module.boto3, 'Session', MagicMock(return_value=MagicMock(region_name='us-east-1')))

        mock_pps = MagicMock()
        mock_est = MagicMock()
        monkeypatch.setattr(sc_module, '_print_per_segment_counts', mock_pps)
        monkeypatch.setattr(sc_module, '_print_sampled_estimate', mock_est)

        base_args['sample_fraction'] = 0.1
        base_args['per_segment'] = True
        sc_module.run(MagicMock(), spark_context, MagicMock(), base_args)

        mock_pps.assert_called_once()
        mock_est.assert_called_once()
        # per-segment table is scoped to the sampled segments (20), not all 200
        assert mock_pps.call_args.args[1] == 20
        # the estimate is told the table was already shown, so it drops the hint
        assert mock_est.call_args.kwargs['per_segment_shown'] is True

    def test_sampling_without_per_segment_skips_table(self, monkeypatch, shared_table_info_mocks,
                                                       rate_limiter_mocks, spark_context, base_args):
        self._collect_returns(spark_context, [(i, 100) for i in range(20)])
        monkeypatch.setattr(sc_module.boto3, 'Session', MagicMock(return_value=MagicMock(region_name='us-east-1')))

        mock_pps = MagicMock()
        mock_est = MagicMock()
        monkeypatch.setattr(sc_module, '_print_per_segment_counts', mock_pps)
        monkeypatch.setattr(sc_module, '_print_sampled_estimate', mock_est)

        base_args['sample_fraction'] = 0.1
        sc_module.run(MagicMock(), spark_context, MagicMock(), base_args)

        mock_pps.assert_not_called()
        mock_est.assert_called_once()
        # table was not shown, so the estimate keeps the "Add --per-segment" hint
        assert mock_est.call_args.kwargs['per_segment_shown'] is False

    def test_sample_size_floored_at_one(self, monkeypatch, shared_table_info_mocks,
                                        rate_limiter_mocks, spark_context, base_args):
        """A fraction that rounds below one segment still scans a single segment."""
        self._collect_returns(spark_context, [(0, 5)])
        monkeypatch.setattr(sc_module.boto3, 'Session', MagicMock(return_value=MagicMock(region_name='us-east-1')))

        base_args['segments'] = 5
        base_args['sample_fraction'] = 0.01  # int(5 * 0.01) == 0 -> max(1, 0) == 1
        sc_module.run(MagicMock(), spark_context, MagicMock(), base_args)

        pc_args = spark_context.parallelize.call_args
        assert len(list(pc_args.args[0])) == 1

    def test_full_scan_default_is_not_sampling(self, monkeypatch, shared_table_info_mocks,
                                               rate_limiter_mocks, spark_context, base_args, capsys):
        """sample_fraction defaulting to 1.0 keeps the exact-total path."""
        self._collect_returns(spark_context, [(i, 10) for i in range(200)])
        monkeypatch.setattr(sc_module.boto3, 'Session', MagicMock(return_value=MagicMock(region_name='us-east-1')))

        # no sample_fraction key -> default 1.0
        sc_module.run(MagicMock(), spark_context, MagicMock(), base_args)
        out = capsys.readouterr().out
        assert 'Total records counted:' in out
        assert 'Sampling' not in out
        assert 'Estimated total:' not in out
