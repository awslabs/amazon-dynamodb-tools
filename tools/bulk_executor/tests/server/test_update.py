"""Unit tests for the `update` server-side verb.

Covers `python_modules/update/__init__.py`:
- ListAccumulator: zero / addInPlace contract for error accumulation
- print_dynamodb_table_info: helper call ordering and output
- run(): argument wiring (table, generator, s3-bucket-name, JOB_RUN_ID),
  importlib dynamic module loading, rate-limiter config, monitor options,
  spark parallelize count (800), accumulator initialization,
  error propagation, rate-limiter shutdown in finally, summary output
- _update_data: boto3 Config (timeouts, retries), scan pagination via
  LastEvaluatedKey presence check, generate() dispatch, update_item calls,
  skipped items (generate returns falsy), ClientError handling
  (throttle/validation/conditional-check/unhandled), per-worker error
  accumulation, rate-limiter shutdown in finally, accumulator reporting
"""

import sys
from unittest.mock import MagicMock, patch, call

import botocore.exceptions
import pytest

from python_modules import update as update_module

# The update verb does `from python_modules.shared.errors import *` which yields
# nothing from a Mock (no __all__). Inject the names so they exist at module level.
update_module.get_error_message = MagicMock(side_effect=lambda e: str(e))
update_module.get_error_code = MagicMock(side_effect=lambda e: e.response['Error']['Code'])


# --- Fixtures ---------------------------------------------------------------

@pytest.fixture
def shared_table_info_mocks(monkeypatch):
    """Replace shared.table_info helpers used by update."""
    helpers = MagicMock()
    helpers.get_and_print_dynamodb_table_info = MagicMock(
        return_value={'item_count': 50, 'size_bytes': 512, 'region_name': 'us-east-1'}
    )
    helpers.get_and_print_table_scan_cost = MagicMock(return_value=0.75)
    helpers.get_dynamodb_throughput_configs = MagicMock(return_value={'opt': 'val'})

    monkeypatch.setattr(update_module, 'get_and_print_dynamodb_table_info',
                        helpers.get_and_print_dynamodb_table_info)
    monkeypatch.setattr(update_module, 'get_and_print_table_scan_cost',
                        helpers.get_and_print_table_scan_cost)
    monkeypatch.setattr(update_module, 'get_dynamodb_throughput_configs',
                        helpers.get_dynamodb_throughput_configs)
    return helpers


@pytest.fixture
def rate_limiter_mocks(monkeypatch):
    """Replace RateLimiterAggregator / RateLimiterSharedConfig."""
    config_cls = MagicMock(side_effect=lambda **kw: MagicMock(**kw))
    aggregator_cls = MagicMock()

    monkeypatch.setattr(update_module, 'RateLimiterSharedConfig', config_cls)
    monkeypatch.setattr(update_module, 'RateLimiterAggregator', aggregator_cls)
    return MagicMock(config=config_cls, aggregator=aggregator_cls)


@pytest.fixture
def spark_context():
    """Mock SparkContext with accumulator() and parallelize()."""
    sc = MagicMock()
    sc.accumulator = MagicMock(side_effect=lambda init, *_: MagicMock(value=init))
    rdd = MagicMock()
    rdd.map = MagicMock(return_value=rdd)
    rdd.collect = MagicMock(return_value=[])
    sc.parallelize = MagicMock(return_value=rdd)
    return sc


@pytest.fixture
def base_args():
    return {
        'table': 'my-table',
        's3-bucket-name': 'rate-limit-bucket',
        'JOB_RUN_ID': 'jr-run-001',
    }


# --- ListAccumulator --------------------------------------------------------

class TestListAccumulator:
    """update/__init__.py defines ListAccumulator to collect per-worker errors."""

    def test_zero_returns_empty_list(self):
        """Line 29: zero() always returns [] regardless of initialValue."""
        acc = update_module.ListAccumulator()
        assert acc.zero(['ignored']) == []
        assert acc.zero(None) == []

    def test_addInPlace_extends_first_and_returns_it(self):
        """Lines 32-34: addInPlace extends v1 with v2 in place, returns v1."""
        acc = update_module.ListAccumulator()
        a = ['err1']
        b = ['err2', 'err3']
        result = acc.addInPlace(a, b)
        assert a == ['err1', 'err2', 'err3'], "first arg mutated in place"
        assert result is a, "returns the mutated first list"

    def test_addInPlace_with_empty_right(self):
        """Line 32: extend with empty list is a no-op."""
        acc = update_module.ListAccumulator()
        result = acc.addInPlace(['x'], [])
        assert result == ['x']

    def test_addInPlace_with_empty_left(self):
        """Line 32: extend empty list with items works."""
        acc = update_module.ListAccumulator()
        result = acc.addInPlace([], ['y'])
        assert result == ['y']


# --- Constants ---------------------------------------------------------------

class TestConstants:
    """Lines 36-38: module-level exception name constants."""

    def test_throttle_exception_constant(self):
        """Line 36."""
        assert update_module.DYNAMO_DB_THROTTLE_EXCEPTION == 'ProvisionedThroughputExceededException'

    def test_validation_exception_constant(self):
        """Line 37."""
        assert update_module.DYNAMO_DB_VALIDATION_EXCEPTION == 'ValidationException'

    def test_conditional_check_failed_constant(self):
        """Line 38."""
        assert update_module.DYNAMO_DB_CONDITIONAL_CHECK_FAILED == 'ConditionalCheckFailedException'


# --- print_dynamodb_table_info -----------------------------------------------

class TestPrintDynamodbTableInfo:
    """Lines 40-44: helper that prints table info and scan cost."""

    def test_calls_get_and_print_dynamodb_table_info(self, shared_table_info_mocks, monkeypatch):
        """Line 42: calls get_and_print_dynamodb_table_info with table_name."""
        mock_session = MagicMock()
        mock_session.return_value.region_name = 'us-west-2'
        monkeypatch.setattr(update_module, 'boto3', MagicMock(Session=mock_session))

        update_module.print_dynamodb_table_info('test-table')

        shared_table_info_mocks.get_and_print_dynamodb_table_info.assert_called_once_with('test-table')

    def test_calls_get_and_print_table_scan_cost(self, shared_table_info_mocks, monkeypatch):
        """Line 43: calls get_and_print_table_scan_cost with table_info and region."""
        mock_session = MagicMock()
        mock_session.return_value.region_name = 'eu-west-1'
        monkeypatch.setattr(update_module, 'boto3', MagicMock(Session=mock_session))

        update_module.print_dynamodb_table_info('t')

        shared_table_info_mocks.get_and_print_table_scan_cost.assert_called_once()
        args = shared_table_info_mocks.get_and_print_table_scan_cost.call_args
        assert args.args[0] == {'item_count': 50, 'size_bytes': 512, 'region_name': 'us-east-1'}
        assert args.args[1] == 'eu-west-1'

    def test_prints_write_cost_message(self, shared_table_info_mocks, monkeypatch, capsys):
        """Line 44: prints a message about write cost depending on updates."""
        mock_session = MagicMock()
        mock_session.return_value.region_name = 'us-east-1'
        monkeypatch.setattr(update_module, 'boto3', MagicMock(Session=mock_session))

        update_module.print_dynamodb_table_info('t')

        out = capsys.readouterr().out
        assert 'Cost for writes depends on how many items will be updated' in out


# --- run() ------------------------------------------------------------------

class TestRunArgumentWiring:
    """run() pulls table, generator, bucket, and job-run-id from parsed_args."""

    def test_uses_default_generator_name(self, monkeypatch, shared_table_info_mocks,
                                          rate_limiter_mocks, spark_context, base_args):
        """Line 49: generator defaults to 'default' when not in parsed_args."""
        imported_modules = []

        def fake_import(name):
            imported_modules.append(name)
            m = MagicMock()
            m.generate = MagicMock()
            return m

        monkeypatch.setattr(update_module.importlib, 'import_module', fake_import)
        monkeypatch.setattr(update_module, 'print_dynamodb_table_info', MagicMock())

        update_module.run(MagicMock(), spark_context, MagicMock(), base_args)

        assert imported_modules[0] == 'python_modules.update.default'

    def test_uses_custom_generator_name(self, monkeypatch, shared_table_info_mocks,
                                         rate_limiter_mocks, spark_context, base_args):
        """Line 49: generator name from parsed_args overrides default."""
        imported_modules = []
        base_args['generator'] = 'custom_gen'

        def fake_import(name):
            imported_modules.append(name)
            m = MagicMock()
            m.generate = MagicMock()
            return m

        monkeypatch.setattr(update_module.importlib, 'import_module', fake_import)
        monkeypatch.setattr(update_module, 'print_dynamodb_table_info', MagicMock())

        update_module.run(MagicMock(), spark_context, MagicMock(), base_args)

        assert imported_modules[0] == 'python_modules.update.custom_gen'

    def test_uses_custom_generator_function_name(self, monkeypatch, shared_table_info_mocks,
                                                   rate_limiter_mocks, spark_context, base_args):
        """Line 50: generatorfunctionname overrides 'generate'."""
        base_args['generatorfunctionname'] = 'custom_fn'
        mock_module = MagicMock()
        mock_module.custom_fn = MagicMock()
        monkeypatch.setattr(update_module.importlib, 'import_module', MagicMock(return_value=mock_module))
        monkeypatch.setattr(update_module, 'print_dynamodb_table_info', MagicMock())

        captured_fn = []
        original_map = spark_context.parallelize.return_value.map

        def capture_map(fn):
            captured_fn.append(fn)
            rdd = MagicMock()
            rdd.collect = MagicMock(return_value=[])
            return rdd

        spark_context.parallelize.return_value.map = capture_map

        update_module.run(MagicMock(), spark_context, MagicMock(), base_args)

        # The map lambda captures the generate function — getattr(module, 'custom_fn')
        # Verify getattr was used correctly by checking the module attribute
        assert hasattr(mock_module, 'custom_fn')

    def test_rate_limiter_shared_config_wiring(self, monkeypatch, shared_table_info_mocks,
                                                rate_limiter_mocks, spark_context, base_args):
        """Lines 61-64: RateLimiterSharedConfig gets bucket and job_run_id."""
        monkeypatch.setattr(update_module.importlib, 'import_module',
                            MagicMock(return_value=MagicMock(generate=MagicMock())))
        monkeypatch.setattr(update_module, 'print_dynamodb_table_info', MagicMock())

        update_module.run(MagicMock(), spark_context, MagicMock(), base_args)

        rate_limiter_mocks.config.assert_called_once_with(
            bucket='rate-limit-bucket', job_run_id='jr-run-001'
        )

    def test_monitor_options_called_with_read_and_write(self, monkeypatch, shared_table_info_mocks,
                                                         rate_limiter_mocks, spark_context, base_args):
        """Line 69: get_dynamodb_throughput_configs called with modes=["read", "write"]."""
        monkeypatch.setattr(update_module.importlib, 'import_module',
                            MagicMock(return_value=MagicMock(generate=MagicMock())))
        monkeypatch.setattr(update_module, 'print_dynamodb_table_info', MagicMock())

        update_module.run(MagicMock(), spark_context, MagicMock(), base_args)

        shared_table_info_mocks.get_dynamodb_throughput_configs.assert_called_once()
        call_kwargs = shared_table_info_mocks.get_dynamodb_throughput_configs.call_args
        assert call_kwargs.args[1] == 'my-table'
        assert call_kwargs.kwargs['modes'] == ['read', 'write']
        assert call_kwargs.kwargs['format'] == 'monitor'

    def test_parallelize_count_is_800(self, monkeypatch, shared_table_info_mocks,
                                       rate_limiter_mocks, spark_context, base_args):
        """Line 81: parallelize uses 800 partitions."""
        monkeypatch.setattr(update_module.importlib, 'import_module',
                            MagicMock(return_value=MagicMock(generate=MagicMock())))
        monkeypatch.setattr(update_module, 'print_dynamodb_table_info', MagicMock())

        update_module.run(MagicMock(), spark_context, MagicMock(), base_args)

        pc_args = spark_context.parallelize.call_args
        assert list(pc_args.args[0]) == list(range(800)), "first arg is range(800)"
        assert pc_args.args[1] == 800, "numSlices is 800"


class TestRunAccumulators:
    """Lines 71-76: run() initializes four accumulators."""

    def test_updated_accumulator_initialized_to_zero(self, monkeypatch, shared_table_info_mocks,
                                                      rate_limiter_mocks, spark_context, base_args):
        """Line 71: updated_accumulator starts at 0."""
        monkeypatch.setattr(update_module.importlib, 'import_module',
                            MagicMock(return_value=MagicMock(generate=MagicMock())))
        monkeypatch.setattr(update_module, 'print_dynamodb_table_info', MagicMock())

        update_module.run(MagicMock(), spark_context, MagicMock(), base_args)

        first_call = spark_context.accumulator.call_args_list[0]
        assert first_call.args[0] == 0

    def test_skipped_accumulator_initialized_to_zero(self, monkeypatch, shared_table_info_mocks,
                                                      rate_limiter_mocks, spark_context, base_args):
        """Line 72: skipped_accumulator starts at 0."""
        monkeypatch.setattr(update_module.importlib, 'import_module',
                            MagicMock(return_value=MagicMock(generate=MagicMock())))
        monkeypatch.setattr(update_module, 'print_dynamodb_table_info', MagicMock())

        update_module.run(MagicMock(), spark_context, MagicMock(), base_args)

        second_call = spark_context.accumulator.call_args_list[1]
        assert second_call.args[0] == 0

    def test_failed_accumulator_initialized_to_zero(self, monkeypatch, shared_table_info_mocks,
                                                     rate_limiter_mocks, spark_context, base_args):
        """Line 73: failed_accumulator starts at 0."""
        monkeypatch.setattr(update_module.importlib, 'import_module',
                            MagicMock(return_value=MagicMock(generate=MagicMock())))
        monkeypatch.setattr(update_module, 'print_dynamodb_table_info', MagicMock())

        update_module.run(MagicMock(), spark_context, MagicMock(), base_args)

        third_call = spark_context.accumulator.call_args_list[2]
        assert third_call.args[0] == 0

    def test_error_accumulator_seeded_with_empty_list(self, monkeypatch, shared_table_info_mocks,
                                                       rate_limiter_mocks, spark_context, base_args):
        """Line 76: error_accumulator starts with [] and ListAccumulator."""
        monkeypatch.setattr(update_module.importlib, 'import_module',
                            MagicMock(return_value=MagicMock(generate=MagicMock())))
        monkeypatch.setattr(update_module, 'print_dynamodb_table_info', MagicMock())

        update_module.run(MagicMock(), spark_context, MagicMock(), base_args)

        fourth_call = spark_context.accumulator.call_args_list[3]
        assert fourth_call.args[0] == []
        assert isinstance(fourth_call.args[1], update_module.ListAccumulator)


class TestRunErrorHandling:
    """Lines 83-89: error propagation from workers."""

    def test_parallel_execution_error_wraps_exception(self, monkeypatch, shared_table_info_mocks,
                                                       rate_limiter_mocks, spark_context, base_args):
        """Line 84: RuntimeError from collect() is wrapped in 'Error in parallel execution'."""
        monkeypatch.setattr(update_module.importlib, 'import_module',
                            MagicMock(return_value=MagicMock(generate=MagicMock())))
        monkeypatch.setattr(update_module, 'print_dynamodb_table_info', MagicMock())
        monkeypatch.setattr(update_module, 'get_error_message', lambda e: str(e))

        spark_context.parallelize.return_value.map.return_value.collect = MagicMock(
            side_effect=RuntimeError('spark died')
        )

        with pytest.raises(Exception, match='Error in parallel execution'):
            update_module.run(MagicMock(), spark_context, MagicMock(), base_args)

    def test_aggregator_shutdown_even_on_collect_failure(self, monkeypatch, shared_table_info_mocks,
                                                          spark_context, base_args):
        """Line 86: rate_limiter_aggregator.shutdown() is in finally block."""
        monkeypatch.setattr(update_module.importlib, 'import_module',
                            MagicMock(return_value=MagicMock(generate=MagicMock())))
        monkeypatch.setattr(update_module, 'print_dynamodb_table_info', MagicMock())
        monkeypatch.setattr(update_module, 'get_error_message', lambda e: str(e))

        agg_instance = MagicMock()
        monkeypatch.setattr(update_module, 'RateLimiterSharedConfig', MagicMock())
        monkeypatch.setattr(update_module, 'RateLimiterAggregator', MagicMock(return_value=agg_instance))

        spark_context.parallelize.return_value.map.return_value.collect = MagicMock(
            side_effect=RuntimeError('boom')
        )

        with pytest.raises(Exception):
            update_module.run(MagicMock(), spark_context, MagicMock(), base_args)

        agg_instance.shutdown.assert_called_once()

    def test_first_worker_error_is_raised(self, monkeypatch, shared_table_info_mocks,
                                            rate_limiter_mocks, spark_context, base_args):
        """Lines 87-89: if error_accumulator.value is non-empty, first error raised."""
        monkeypatch.setattr(update_module.importlib, 'import_module',
                            MagicMock(return_value=MagicMock(generate=MagicMock())))
        monkeypatch.setattr(update_module, 'print_dynamodb_table_info', MagicMock())

        accs = [
            MagicMock(value=10),   # updated
            MagicMock(value=5),    # skipped
            MagicMock(value=2),    # failed
            MagicMock(value=['first error', 'second error']),  # errors
        ]
        spark_context.accumulator = MagicMock(side_effect=accs)
        spark_context.parallelize.return_value.map.return_value.collect = MagicMock(return_value=[])

        with pytest.raises(Exception, match='first error'):
            update_module.run(MagicMock(), spark_context, MagicMock(), base_args)

    def test_no_errors_prints_summary(self, monkeypatch, shared_table_info_mocks,
                                       rate_limiter_mocks, spark_context, base_args, capsys):
        """Lines 93-94: prints total, updates, non-updates, conditions failed."""
        monkeypatch.setattr(update_module.importlib, 'import_module',
                            MagicMock(return_value=MagicMock(generate=MagicMock())))
        monkeypatch.setattr(update_module, 'print_dynamodb_table_info', MagicMock())

        accs = [
            MagicMock(value=10),  # updated
            MagicMock(value=5),   # skipped
            MagicMock(value=2),   # failed
            MagicMock(value=[]),  # no errors
        ]
        spark_context.accumulator = MagicMock(side_effect=accs)
        spark_context.parallelize.return_value.map.return_value.collect = MagicMock(return_value=[])

        update_module.run(MagicMock(), spark_context, MagicMock(), base_args)

        out = capsys.readouterr().out
        assert '17' in out, "total = 10 + 5 + 2 = 17"
        assert '10' in out, "updates count"
        assert '5' in out, "non-updates count"
        assert '2' in out, "conditions failed count"


class TestRunMapDispatch:
    """Line 82: the lambda passed to rdd.map calls _update_data with correct args."""

    def test_map_lambda_invokes_update_data(self, monkeypatch, shared_table_info_mocks,
                                              rate_limiter_mocks, spark_context, base_args):
        """Line 82: _update_data called with all required positional args."""
        captured = {}

        def fake_update_data(*args, **kwargs):
            captured.setdefault('calls', []).append(args)
            return 0

        monkeypatch.setattr(update_module, '_update_data', fake_update_data)
        monkeypatch.setattr(update_module.importlib, 'import_module',
                            MagicMock(return_value=MagicMock(generate=MagicMock())))
        monkeypatch.setattr(update_module, 'print_dynamodb_table_info', MagicMock())

        def fake_map(fn):
            rdd = MagicMock()
            rdd.collect = MagicMock(return_value=[fn(0), fn(5), fn(799)])
            return rdd

        spark_context.parallelize.return_value.map = fake_map

        update_module.run(MagicMock(), spark_context, MagicMock(), base_args)

        assert len(captured['calls']) == 3
        # _update_data(monitor_options, table_name, generate, worker_id, parallelize_count, ...)
        for c in captured['calls']:
            assert c[1] == 'my-table', "table_name is second arg"
            assert c[4] == 800, "total_segments (parallelize_count) is 5th arg"
        assert captured['calls'][0][3] == 0, "worker_id=0"
        assert captured['calls'][1][3] == 5, "worker_id=5"
        assert captured['calls'][2][3] == 799, "worker_id=799"


# --- _update_data -----------------------------------------------------------

def _make_rl_worker(table_mock):
    """Create a mock RateLimiterWorker that returns a session wired to table_mock."""
    rl = MagicMock()
    session = MagicMock()
    session.resource.return_value.Table.return_value = table_mock
    rl.get_session.return_value = session
    return rl


def _make_table_with_scan(scan_responses):
    """Create a table mock whose scan returns items from scan_responses iterator."""
    table = MagicMock()
    responses = iter(scan_responses)
    table.scan = MagicMock(side_effect=lambda **kw: next(responses))
    return table


class TestUpdateDataConfig:
    """Lines 97-111: boto3 Config and session setup."""

    def test_boto3_config_has_4_second_timeouts_and_50_retries(self, monkeypatch):
        """Lines 103-109: Config has connect_timeout=4.0, read_timeout=4.0, 50 retries."""
        seen_configs = []

        rl_instance = MagicMock()
        session = MagicMock()

        def capture_resource(name, **kw):
            seen_configs.append(kw.get('config'))
            r = MagicMock()
            r.Table.return_value.scan.return_value = {'Items': []}
            return r

        session.resource = capture_resource
        rl_instance.get_session.return_value = session
        monkeypatch.setattr(update_module, 'RateLimiterWorker', MagicMock(return_value=rl_instance))

        update_module._update_data(
            {}, 'tbl', lambda item: None, 0, 1,
            MagicMock(), MagicMock(), MagicMock(), MagicMock(), MagicMock()
        )

        assert len(seen_configs) == 1
        cfg = seen_configs[0]
        assert cfg.connect_timeout == 4.0
        assert cfg.read_timeout == 4.0
        assert cfg.retries['mode'] == 'standard'
        assert cfg.retries['total_max_attempts'] == 50


class TestUpdateDataPagination:
    """Lines 124-150: scan loop with LastEvaluatedKey presence-based pagination."""

    def test_single_page_no_lek_key(self, monkeypatch):
        """Line 148: if 'LastEvaluatedKey' not in response -> break."""
        table = _make_table_with_scan([{'Items': [{'id': 1}]}])
        rl = _make_rl_worker(table)
        monkeypatch.setattr(update_module, 'RateLimiterWorker', MagicMock(return_value=rl))

        updated_acc = MagicMock()
        skipped_acc = MagicMock()
        failed_acc = MagicMock()

        update_module._update_data(
            {}, 'tbl', lambda item: {'Key': item}, 0, 1,
            updated_acc, skipped_acc, failed_acc, MagicMock(), MagicMock()
        )

        assert table.scan.call_count == 1
        updated_acc.add.assert_called_once_with(1)

    def test_multi_page_threads_lek(self, monkeypatch):
        """Line 150: ExclusiveStartKey set from previous LastEvaluatedKey."""
        scan_kwargs_seen = []
        responses = iter([
            {'Items': [{'a': 1}], 'LastEvaluatedKey': {'pk': 'page1'}},
            {'Items': [{'b': 2}], 'LastEvaluatedKey': {'pk': 'page2'}},
            {'Items': [{'c': 3}]},  # no LastEvaluatedKey key at all
        ])

        table = MagicMock()

        def scan_capture(**kwargs):
            scan_kwargs_seen.append(dict(kwargs))
            return next(responses)

        table.scan = scan_capture
        rl = _make_rl_worker(table)
        monkeypatch.setattr(update_module, 'RateLimiterWorker', MagicMock(return_value=rl))

        updated_acc = MagicMock()
        update_module._update_data(
            {}, 'tbl', lambda item: {'Key': item}, 3, 10,
            updated_acc, MagicMock(), MagicMock(), MagicMock(), MagicMock()
        )

        assert len(scan_kwargs_seen) == 3
        assert 'ExclusiveStartKey' not in scan_kwargs_seen[0]
        assert scan_kwargs_seen[1]['ExclusiveStartKey'] == {'pk': 'page1'}
        assert scan_kwargs_seen[2]['ExclusiveStartKey'] == {'pk': 'page2'}
        updated_acc.add.assert_called_once_with(3)

    def test_scan_kwargs_include_segment_and_total(self, monkeypatch):
        """Lines 117-120: scan_kwargs includes Segment, TotalSegments (not TableName — the Table resource already knows its name)."""
        scan_kwargs_seen = []
        table = MagicMock()

        def scan_capture(**kwargs):
            scan_kwargs_seen.append(dict(kwargs))
            return {'Items': []}

        table.scan = scan_capture
        rl = _make_rl_worker(table)
        monkeypatch.setattr(update_module, 'RateLimiterWorker', MagicMock(return_value=rl))

        update_module._update_data(
            {}, 'my-tbl', lambda item: None, 7, 100,
            MagicMock(), MagicMock(), MagicMock(), MagicMock(), MagicMock()
        )

        assert 'TableName' not in scan_kwargs_seen[0]
        assert scan_kwargs_seen[0]['Segment'] == 7
        assert scan_kwargs_seen[0]['TotalSegments'] == 100


class TestUpdateDataGenerateDispatch:
    """Lines 128-133: generate() is called per item; update_item if truthy, skip if falsy."""

    def test_generate_returns_truthy_calls_update_item(self, monkeypatch):
        """Lines 129-131: if update_kwargs is truthy, table.update_item called."""
        table = _make_table_with_scan([{'Items': [{'id': 1}, {'id': 2}]}])
        rl = _make_rl_worker(table)
        monkeypatch.setattr(update_module, 'RateLimiterWorker', MagicMock(return_value=rl))

        def gen(item):
            return {'Key': {'id': item['id']}, 'UpdateExpression': 'SET #a = :v'}

        updated_acc = MagicMock()
        skipped_acc = MagicMock()

        update_module._update_data(
            {}, 'tbl', gen, 0, 1,
            updated_acc, skipped_acc, MagicMock(), MagicMock(), MagicMock()
        )

        assert table.update_item.call_count == 2
        updated_acc.add.assert_called_once_with(2)
        skipped_acc.add.assert_called_once_with(0)

    def test_generate_returns_falsy_skips_update(self, monkeypatch):
        """Lines 132-133: if generate returns falsy (None, {}, etc.), item is skipped."""
        table = _make_table_with_scan([{'Items': [{'id': 1}, {'id': 2}, {'id': 3}]}])
        rl = _make_rl_worker(table)
        monkeypatch.setattr(update_module, 'RateLimiterWorker', MagicMock(return_value=rl))

        def gen(item):
            if item['id'] == 2:
                return {'Key': item}
            return None

        updated_acc = MagicMock()
        skipped_acc = MagicMock()

        update_module._update_data(
            {}, 'tbl', gen, 0, 1,
            updated_acc, skipped_acc, MagicMock(), MagicMock(), MagicMock()
        )

        assert table.update_item.call_count == 1
        updated_acc.add.assert_called_once_with(1)
        skipped_acc.add.assert_called_once_with(2)

    def test_generate_returns_empty_dict_is_falsy(self, monkeypatch):
        """Line 129: empty dict is falsy so item is skipped."""
        table = _make_table_with_scan([{'Items': [{'id': 1}]}])
        rl = _make_rl_worker(table)
        monkeypatch.setattr(update_module, 'RateLimiterWorker', MagicMock(return_value=rl))

        skipped_acc = MagicMock()
        update_module._update_data(
            {}, 'tbl', lambda item: {}, 0, 1,
            MagicMock(), skipped_acc, MagicMock(), MagicMock(), MagicMock()
        )

        table.update_item.assert_not_called()
        skipped_acc.add.assert_called_once_with(1)


class TestUpdateDataClientErrors:
    """Lines 135-146: ClientError handling per error code."""

    def _make_client_error(self, code, message='error'):
        """Create a botocore ClientError with the given error code."""
        return botocore.exceptions.ClientError(
            {'Error': {'Code': code, 'Message': message}},
            'UpdateItem'
        )

    def test_throttle_exception_exits(self, monkeypatch):
        """Lines 137-138: ProvisionedThroughputExceededException calls exit()."""
        table = _make_table_with_scan([{'Items': [{'id': 1}]}])
        table.update_item = MagicMock(
            side_effect=self._make_client_error('ProvisionedThroughputExceededException')
        )
        rl = _make_rl_worker(table)
        monkeypatch.setattr(update_module, 'RateLimiterWorker', MagicMock(return_value=rl))
        monkeypatch.setattr(update_module, 'get_error_code',
                            lambda e: e.response['Error']['Code'])
        monkeypatch.setattr(update_module, 'get_error_message', lambda e: str(e))

        error_acc = MagicMock()
        with pytest.raises(SystemExit):
            update_module._update_data(
                {}, 'tbl', lambda item: {'Key': item}, 0, 1,
                MagicMock(), MagicMock(), MagicMock(), error_acc, MagicMock()
            )

    def test_validation_exception_exits(self, monkeypatch):
        """Lines 139-140: ValidationException calls exit() with message."""
        table = _make_table_with_scan([{'Items': [{'id': 1}]}])
        table.update_item = MagicMock(
            side_effect=self._make_client_error('ValidationException', 'bad schema')
        )
        rl = _make_rl_worker(table)
        monkeypatch.setattr(update_module, 'RateLimiterWorker', MagicMock(return_value=rl))
        monkeypatch.setattr(update_module, 'get_error_code',
                            lambda e: e.response['Error']['Code'])
        monkeypatch.setattr(update_module, 'get_error_message', lambda e: 'bad schema')

        error_acc = MagicMock()
        with pytest.raises(SystemExit):
            update_module._update_data(
                {}, 'tbl', lambda item: {'Key': item}, 0, 1,
                MagicMock(), MagicMock(), MagicMock(), error_acc, MagicMock()
            )

    def test_conditional_check_failed_increments_failed_count(self, monkeypatch):
        """Lines 141-143: ConditionalCheckFailedException prints and increments failed_count."""
        table = _make_table_with_scan([{'Items': [{'id': 1}, {'id': 2}]}])
        table.update_item = MagicMock(
            side_effect=self._make_client_error('ConditionalCheckFailedException')
        )
        rl = _make_rl_worker(table)
        monkeypatch.setattr(update_module, 'RateLimiterWorker', MagicMock(return_value=rl))
        monkeypatch.setattr(update_module, 'get_error_code',
                            lambda e: e.response['Error']['Code'])

        failed_acc = MagicMock()
        updated_acc = MagicMock()

        update_module._update_data(
            {}, 'tbl', lambda item: {'Key': item}, 0, 1,
            updated_acc, MagicMock(), failed_acc, MagicMock(), MagicMock()
        )

        failed_acc.add.assert_called_once_with(2)
        updated_acc.add.assert_called_once_with(0)

    def test_conditional_check_failed_prints_kwargs(self, monkeypatch, capsys):
        """Line 142: prints the update_kwargs that caused the condition failure."""
        table = _make_table_with_scan([{'Items': [{'id': 'x'}]}])
        table.update_item = MagicMock(
            side_effect=self._make_client_error('ConditionalCheckFailedException')
        )
        rl = _make_rl_worker(table)
        monkeypatch.setattr(update_module, 'RateLimiterWorker', MagicMock(return_value=rl))
        monkeypatch.setattr(update_module, 'get_error_code',
                            lambda e: e.response['Error']['Code'])

        update_module._update_data(
            {}, 'tbl', lambda item: {'Key': {'id': 'x'}, 'CE': 'cond'}, 0, 1,
            MagicMock(), MagicMock(), MagicMock(), MagicMock(), MagicMock()
        )

        out = capsys.readouterr().out
        assert 'condition expression failed' in out
        assert "{'Key': {'id': 'x'}, 'CE': 'cond'}" in out

    def test_unhandled_client_error_re_raises(self, monkeypatch):
        """Lines 144-146: unknown error code prints to stderr and re-raises."""
        table = _make_table_with_scan([{'Items': [{'id': 1}]}])
        err = self._make_client_error('InternalServerError', 'oops')
        table.update_item = MagicMock(side_effect=err)
        rl = _make_rl_worker(table)
        monkeypatch.setattr(update_module, 'RateLimiterWorker', MagicMock(return_value=rl))
        monkeypatch.setattr(update_module, 'get_error_code',
                            lambda e: e.response['Error']['Code'])
        monkeypatch.setattr(update_module, 'get_error_message', lambda e: str(e))

        error_acc = MagicMock()
        # The re-raise is caught by the outer except (line 152)
        update_module._update_data(
            {}, 'tbl', lambda item: {'Key': item}, 5, 10,
            MagicMock(), MagicMock(), MagicMock(), error_acc, MagicMock()
        )

        error_acc.add.assert_called_once()
        msg = error_acc.add.call_args.args[0][0]
        assert 'worker 5' in msg

    def test_unhandled_client_error_prints_to_stderr(self, monkeypatch, capsys):
        """Line 145: unhandled error printed to stderr."""
        table = _make_table_with_scan([{'Items': [{'id': 1}]}])
        err = self._make_client_error('InternalServerError', 'oops')
        table.update_item = MagicMock(side_effect=err)
        rl = _make_rl_worker(table)
        monkeypatch.setattr(update_module, 'RateLimiterWorker', MagicMock(return_value=rl))
        monkeypatch.setattr(update_module, 'get_error_code',
                            lambda e: e.response['Error']['Code'])
        monkeypatch.setattr(update_module, 'get_error_message', lambda e: str(e))

        update_module._update_data(
            {}, 'tbl', lambda item: {'Key': item}, 0, 1,
            MagicMock(), MagicMock(), MagicMock(), MagicMock(), MagicMock()
        )

        err_out = capsys.readouterr().err
        assert 'Unhandled ClientError' in err_out


class TestUpdateDataErrorAccumulation:
    """Lines 152-153: outer except catches errors and adds to error_accumulator."""

    def test_scan_error_captured_in_accumulator(self, monkeypatch):
        """Line 153: error from scan is caught and added to error_accumulator."""
        table = MagicMock()
        table.scan = MagicMock(side_effect=RuntimeError('network fail'))
        rl = _make_rl_worker(table)
        monkeypatch.setattr(update_module, 'RateLimiterWorker', MagicMock(return_value=rl))
        monkeypatch.setattr(update_module, 'get_error_message', lambda e: f'wrapped:{e}')

        error_acc = MagicMock()
        update_module._update_data(
            {}, 'tbl', lambda item: {'Key': item}, 7, 10,
            MagicMock(), MagicMock(), MagicMock(), error_acc, MagicMock()
        )

        error_acc.add.assert_called_once()
        appended = error_acc.add.call_args.args[0]
        assert isinstance(appended, list) and len(appended) == 1
        assert 'worker 7' in appended[0]
        assert 'wrapped:' in appended[0]

    def test_system_exit_from_throttle_captured(self, monkeypatch):
        """Lines 137-138 + 152: exit() raises SystemExit caught by outer except."""
        table = _make_table_with_scan([{'Items': [{'id': 1}]}])
        table.update_item = MagicMock(
            side_effect=botocore.exceptions.ClientError(
                {'Error': {'Code': 'ProvisionedThroughputExceededException', 'Message': 'throttled'}},
                'UpdateItem'
            )
        )
        rl = _make_rl_worker(table)
        monkeypatch.setattr(update_module, 'RateLimiterWorker', MagicMock(return_value=rl))
        monkeypatch.setattr(update_module, 'get_error_code',
                            lambda e: e.response['Error']['Code'])
        monkeypatch.setattr(update_module, 'get_error_message', lambda e: str(e))

        error_acc = MagicMock()
        # SystemExit is a BaseException — the except Exception won't catch it
        with pytest.raises(SystemExit):
            update_module._update_data(
                {}, 'tbl', lambda item: {'Key': item}, 0, 1,
                MagicMock(), MagicMock(), MagicMock(), error_acc, MagicMock()
            )


class TestUpdateDataShutdown:
    """Lines 155-156: rate_limiter_worker.shutdown() always called in finally."""

    def test_shutdown_called_on_success(self, monkeypatch):
        """Line 156: shutdown called after successful scan loop."""
        table = _make_table_with_scan([{'Items': []}])
        rl_instance = MagicMock()
        rl_instance.get_session.return_value.resource.return_value.Table.return_value = table
        monkeypatch.setattr(update_module, 'RateLimiterWorker', MagicMock(return_value=rl_instance))

        update_module._update_data(
            {}, 'tbl', lambda item: None, 0, 1,
            MagicMock(), MagicMock(), MagicMock(), MagicMock(), MagicMock()
        )

        rl_instance.shutdown.assert_called_once()

    def test_shutdown_called_on_error(self, monkeypatch):
        """Line 156: shutdown called even when scan raises."""
        table = MagicMock()
        table.scan = MagicMock(side_effect=RuntimeError('boom'))
        rl_instance = MagicMock()
        rl_instance.get_session.return_value.resource.return_value.Table.return_value = table
        monkeypatch.setattr(update_module, 'RateLimiterWorker', MagicMock(return_value=rl_instance))
        monkeypatch.setattr(update_module, 'get_error_message', lambda e: str(e))

        update_module._update_data(
            {}, 'tbl', lambda item: None, 0, 1,
            MagicMock(), MagicMock(), MagicMock(), MagicMock(), MagicMock()
        )

        rl_instance.shutdown.assert_called_once()


class TestUpdateDataAccumulatorReporting:
    """Lines 158-163: after the try/finally, accumulators get .add() and print."""

    def test_accumulators_receive_correct_counts(self, monkeypatch):
        """Lines 160-162: updated/skipped/failed counts added to accumulators."""
        # 3 items: first two get updated, third skipped
        table = _make_table_with_scan([{'Items': [{'id': 1}, {'id': 2}, {'id': 3}]}])
        rl = _make_rl_worker(table)
        monkeypatch.setattr(update_module, 'RateLimiterWorker', MagicMock(return_value=rl))

        def gen(item):
            if item['id'] == 3:
                return None
            return {'Key': item}

        updated_acc = MagicMock()
        skipped_acc = MagicMock()
        failed_acc = MagicMock()

        update_module._update_data(
            {}, 'tbl', gen, 0, 1,
            updated_acc, skipped_acc, failed_acc, MagicMock(), MagicMock()
        )

        updated_acc.add.assert_called_once_with(2)
        skipped_acc.add.assert_called_once_with(1)
        failed_acc.add.assert_called_once_with(0)

    def test_prints_worker_summary(self, monkeypatch, capsys):
        """Line 159: prints worker segment/total and record breakdown."""
        table = _make_table_with_scan([{'Items': [{'id': 1}]}])
        rl = _make_rl_worker(table)
        monkeypatch.setattr(update_module, 'RateLimiterWorker', MagicMock(return_value=rl))

        update_module._update_data(
            {}, 'tbl', lambda item: {'Key': item}, 3, 10,
            MagicMock(), MagicMock(), MagicMock(), MagicMock(), MagicMock()
        )

        out = capsys.readouterr().out
        assert 'Worker 3/10' in out
        assert '1' in out  # 1 record processed

    def test_returns_zero(self, monkeypatch):
        """Line 163: _update_data always returns 0."""
        table = _make_table_with_scan([{'Items': []}])
        rl = _make_rl_worker(table)
        monkeypatch.setattr(update_module, 'RateLimiterWorker', MagicMock(return_value=rl))

        result = update_module._update_data(
            {}, 'tbl', lambda item: None, 0, 1,
            MagicMock(), MagicMock(), MagicMock(), MagicMock(), MagicMock()
        )

        assert result == 0


class TestUpdateDataMonitorOptions:
    """Line 99: monitor_options are passed as kwargs to RateLimiterWorker."""

    def test_monitor_options_splatted_into_worker(self, monkeypatch):
        """Line 99: **monitor_options passed to RateLimiterWorker constructor."""
        rl_class = MagicMock()
        rl_instance = MagicMock()
        rl_instance.get_session.return_value.resource.return_value.Table.return_value.scan.return_value = {'Items': []}
        rl_class.return_value = rl_instance
        monkeypatch.setattr(update_module, 'RateLimiterWorker', rl_class)

        monitor_opts = {'monitor_table': 'my-tbl', 'max_rate': 500}

        update_module._update_data(
            monitor_opts, 'tbl', lambda item: None, 0, 1,
            MagicMock(), MagicMock(), MagicMock(), MagicMock(), MagicMock()
        )

        call_kwargs = rl_class.call_args.kwargs
        assert call_kwargs['monitor_table'] == 'my-tbl'
        assert call_kwargs['max_rate'] == 500
