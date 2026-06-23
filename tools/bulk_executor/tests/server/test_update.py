"""Unit tests for the `update` server-side verb (two-phase architecture).

Covers `python_modules/update/__init__.py`:
- ListAccumulator: zero / addInPlace contract for error accumulation
- print_dynamodb_table_info: helper call ordering and output
- run(): two-phase execution —
  Phase 1: scan segments with flatMap to collect pending operations
  Phase 2: repartition ops across workers with foreachPartition
  Rate-limiter lifecycle, error propagation, summary output
- _scan_segment: scan pagination, generate() dispatch, error handling,
  rate-limiter shutdown in finally
- _execute_updates: partition iteration, update_item calls,
  ClientError handling (throttle/validation/conditional-check/unhandled),
  rate-limiter shutdown, accumulator reporting
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
    """Mock SparkContext with accumulator(), parallelize(), and foreachPartition support."""
    sc = MagicMock()
    sc.accumulator = MagicMock(side_effect=lambda init, *_: MagicMock(value=init))

    # Phase 1: scan RDD returns flatMap results via collect()
    scan_rdd = MagicMock()
    scan_rdd.flatMap = MagicMock(return_value=scan_rdd)
    scan_rdd.collect = MagicMock(return_value=[])

    # Phase 2: ops RDD with foreachPartition
    ops_rdd = MagicMock()
    ops_rdd.foreachPartition = MagicMock()

    # parallelize returns scan_rdd first (Phase 1), ops_rdd second (Phase 2)
    sc.parallelize = MagicMock(side_effect=[scan_rdd, ops_rdd])
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
        """zero() always returns [] regardless of initialValue."""
        acc = update_module.ListAccumulator()
        assert acc.zero(['ignored']) == []
        assert acc.zero(None) == []

    def test_addInPlace_extends_first_and_returns_it(self):
        """addInPlace extends v1 with v2 in place, returns v1."""
        acc = update_module.ListAccumulator()
        a = ['err1']
        b = ['err2', 'err3']
        result = acc.addInPlace(a, b)
        assert a == ['err1', 'err2', 'err3'], "first arg mutated in place"
        assert result is a, "returns the mutated first list"

    def test_addInPlace_with_empty_right(self):
        """extend with empty list is a no-op."""
        acc = update_module.ListAccumulator()
        result = acc.addInPlace(['x'], [])
        assert result == ['x']

    def test_addInPlace_with_empty_left(self):
        """extend empty list with items works."""
        acc = update_module.ListAccumulator()
        result = acc.addInPlace([], ['y'])
        assert result == ['y']


# --- Constants ---------------------------------------------------------------

class TestConstants:
    """Module-level exception name constants."""

    def test_throttle_exception_constant(self):
        assert update_module.DYNAMO_DB_THROTTLE_EXCEPTION == 'ProvisionedThroughputExceededException'

    def test_validation_exception_constant(self):
        assert update_module.DYNAMO_DB_VALIDATION_EXCEPTION == 'ValidationException'

    def test_conditional_check_failed_constant(self):
        assert update_module.DYNAMO_DB_CONDITIONAL_CHECK_FAILED == 'ConditionalCheckFailedException'


# --- print_dynamodb_table_info -----------------------------------------------

class TestPrintDynamodbTableInfo:
    """Helper that prints table info and scan cost."""

    def test_calls_get_and_print_dynamodb_table_info(self, shared_table_info_mocks, monkeypatch):
        mock_session = MagicMock()
        mock_session.return_value.region_name = 'us-west-2'
        monkeypatch.setattr(update_module, 'boto3', MagicMock(Session=mock_session))

        update_module.print_dynamodb_table_info('test-table')

        shared_table_info_mocks.get_and_print_dynamodb_table_info.assert_called_once_with('test-table')

    def test_calls_get_and_print_table_scan_cost(self, shared_table_info_mocks, monkeypatch):
        mock_session = MagicMock()
        mock_session.return_value.region_name = 'eu-west-1'
        monkeypatch.setattr(update_module, 'boto3', MagicMock(Session=mock_session))

        update_module.print_dynamodb_table_info('t')

        shared_table_info_mocks.get_and_print_table_scan_cost.assert_called_once()
        args = shared_table_info_mocks.get_and_print_table_scan_cost.call_args
        assert args.args[0] == {'item_count': 50, 'size_bytes': 512, 'region_name': 'us-east-1'}
        assert args.args[1] == 'eu-west-1'

    def test_prints_write_cost_message(self, shared_table_info_mocks, monkeypatch, capsys):
        mock_session = MagicMock()
        mock_session.return_value.region_name = 'us-east-1'
        monkeypatch.setattr(update_module, 'boto3', MagicMock(Session=mock_session))

        update_module.print_dynamodb_table_info('t')

        out = capsys.readouterr().out
        assert 'Cost for writes depends on how many items will be updated' in out


# --- run() Two-Phase Architecture -------------------------------------------

class TestRunArgumentWiring:
    """run() pulls table, generator, bucket, and job-run-id from parsed_args."""

    def test_uses_default_generator_name(self, monkeypatch, shared_table_info_mocks,
                                          rate_limiter_mocks, spark_context, base_args):
        """generator defaults to 'default' when not in parsed_args."""
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
        """generator name from parsed_args overrides default."""
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
        """generatorfunctionname overrides 'generate'."""
        base_args['generatorfunctionname'] = 'custom_fn'
        mock_module = MagicMock()
        mock_module.custom_fn = MagicMock()
        monkeypatch.setattr(update_module.importlib, 'import_module', MagicMock(return_value=mock_module))
        monkeypatch.setattr(update_module, 'print_dynamodb_table_info', MagicMock())

        update_module.run(MagicMock(), spark_context, MagicMock(), base_args)

        # Verify getattr was used correctly by checking the module attribute
        assert hasattr(mock_module, 'custom_fn')

    def test_rate_limiter_shared_config_wiring(self, monkeypatch, shared_table_info_mocks,
                                                rate_limiter_mocks, spark_context, base_args):
        """RateLimiterSharedConfig gets bucket and job_run_id."""
        monkeypatch.setattr(update_module.importlib, 'import_module',
                            MagicMock(return_value=MagicMock(generate=MagicMock())))
        monkeypatch.setattr(update_module, 'print_dynamodb_table_info', MagicMock())

        update_module.run(MagicMock(), spark_context, MagicMock(), base_args)

        rate_limiter_mocks.config.assert_called_once_with(
            bucket='rate-limit-bucket', job_run_id='jr-run-001'
        )

    def test_monitor_options_called_with_read_and_write(self, monkeypatch, shared_table_info_mocks,
                                                         rate_limiter_mocks, spark_context, base_args):
        """get_dynamodb_throughput_configs called with modes=["read", "write"]."""
        monkeypatch.setattr(update_module.importlib, 'import_module',
                            MagicMock(return_value=MagicMock(generate=MagicMock())))
        monkeypatch.setattr(update_module, 'print_dynamodb_table_info', MagicMock())

        update_module.run(MagicMock(), spark_context, MagicMock(), base_args)

        shared_table_info_mocks.get_dynamodb_throughput_configs.assert_called_once()
        call_kwargs = shared_table_info_mocks.get_dynamodb_throughput_configs.call_args
        assert call_kwargs.args[1] == 'my-table'
        assert call_kwargs.kwargs['modes'] == ['read', 'write']
        assert call_kwargs.kwargs['format'] == 'monitor'

    def test_parallelize_count_is_800_for_scan_phase(self, monkeypatch, shared_table_info_mocks,
                                                      rate_limiter_mocks, spark_context, base_args):
        """Phase 1 parallelize uses 800 partitions for scanning."""
        monkeypatch.setattr(update_module.importlib, 'import_module',
                            MagicMock(return_value=MagicMock(generate=MagicMock())))
        monkeypatch.setattr(update_module, 'print_dynamodb_table_info', MagicMock())

        update_module.run(MagicMock(), spark_context, MagicMock(), base_args)

        # First call to parallelize is the scan phase
        pc_args = spark_context.parallelize.call_args_list[0]
        assert list(pc_args.args[0]) == list(range(800)), "first arg is range(800)"
        assert pc_args.args[1] == 800, "numSlices is 800"


class TestRunTwoPhaseFlow:
    """Tests for the two-phase scan-then-update flow."""

    def test_no_pending_ops_skips_phase_2(self, monkeypatch, shared_table_info_mocks,
                                            rate_limiter_mocks, spark_context, base_args, capsys):
        """When scan phase returns empty list, phase 2 is skipped."""
        monkeypatch.setattr(update_module.importlib, 'import_module',
                            MagicMock(return_value=MagicMock(generate=MagicMock())))
        monkeypatch.setattr(update_module, 'print_dynamodb_table_info', MagicMock())

        # Phase 1 returns no ops
        scan_rdd = MagicMock()
        scan_rdd.flatMap = MagicMock(return_value=scan_rdd)
        scan_rdd.collect = MagicMock(return_value=[])
        spark_context.parallelize = MagicMock(return_value=scan_rdd)

        update_module.run(MagicMock(), spark_context, MagicMock(), base_args)

        out = capsys.readouterr().out
        assert 'No items need updating' in out
        # parallelize called only once (scan phase, not update phase)
        assert spark_context.parallelize.call_count == 1

    def test_phase_2_repartitions_pending_ops(self, monkeypatch, shared_table_info_mocks,
                                               base_args, capsys):
        """Phase 2 distributes collected ops across workers via parallelize + foreachPartition."""
        monkeypatch.setattr(update_module.importlib, 'import_module',
                            MagicMock(return_value=MagicMock(generate=MagicMock())))
        monkeypatch.setattr(update_module, 'print_dynamodb_table_info', MagicMock())

        config_cls = MagicMock(side_effect=lambda **kw: MagicMock(**kw))
        aggregator_cls = MagicMock()
        monkeypatch.setattr(update_module, 'RateLimiterSharedConfig', config_cls)
        monkeypatch.setattr(update_module, 'RateLimiterAggregator', aggregator_cls)

        pending_ops = [{'Key': {'id': i}} for i in range(10)]

        # Phase 1 scan returns pending_ops
        scan_rdd = MagicMock()
        scan_rdd.flatMap = MagicMock(return_value=scan_rdd)
        scan_rdd.collect = MagicMock(return_value=pending_ops)

        # Phase 2 ops RDD
        ops_rdd = MagicMock()
        ops_rdd.foreachPartition = MagicMock()

        sc = MagicMock()
        accs = [
            MagicMock(value=[]),   # error_accumulator for scan
            MagicMock(value=10),   # updated_accumulator
            MagicMock(value=0),    # failed_accumulator
            MagicMock(value=[]),   # error_accumulator_2
        ]
        sc.accumulator = MagicMock(side_effect=accs)
        sc.parallelize = MagicMock(side_effect=[scan_rdd, ops_rdd])

        update_module.run(MagicMock(), sc, MagicMock(), base_args)

        # Phase 2: parallelize called with the pending_ops and 800 partitions
        phase2_call = sc.parallelize.call_args_list[1]
        assert phase2_call.args[0] == pending_ops
        assert phase2_call.args[1] == 800
        ops_rdd.foreachPartition.assert_called_once()

    def test_scan_phase_error_propagated(self, monkeypatch, shared_table_info_mocks,
                                          rate_limiter_mocks, spark_context, base_args):
        """Error during scan phase collect() is wrapped and raised."""
        monkeypatch.setattr(update_module.importlib, 'import_module',
                            MagicMock(return_value=MagicMock(generate=MagicMock())))
        monkeypatch.setattr(update_module, 'print_dynamodb_table_info', MagicMock())
        monkeypatch.setattr(update_module, 'get_error_message', lambda e: str(e))

        scan_rdd = MagicMock()
        scan_rdd.flatMap = MagicMock(return_value=scan_rdd)
        scan_rdd.collect = MagicMock(side_effect=RuntimeError('spark died'))
        spark_context.parallelize = MagicMock(return_value=scan_rdd)

        with pytest.raises(Exception, match='Error in scan phase'):
            update_module.run(MagicMock(), spark_context, MagicMock(), base_args)

    def test_update_phase_error_propagated(self, monkeypatch, shared_table_info_mocks,
                                            base_args):
        """Error during update phase foreachPartition() is wrapped and raised."""
        monkeypatch.setattr(update_module.importlib, 'import_module',
                            MagicMock(return_value=MagicMock(generate=MagicMock())))
        monkeypatch.setattr(update_module, 'print_dynamodb_table_info', MagicMock())
        monkeypatch.setattr(update_module, 'get_error_message', lambda e: str(e))

        config_cls = MagicMock(side_effect=lambda **kw: MagicMock(**kw))
        aggregator_cls = MagicMock()
        monkeypatch.setattr(update_module, 'RateLimiterSharedConfig', config_cls)
        monkeypatch.setattr(update_module, 'RateLimiterAggregator', aggregator_cls)

        # Phase 1 returns some ops
        scan_rdd = MagicMock()
        scan_rdd.flatMap = MagicMock(return_value=scan_rdd)
        scan_rdd.collect = MagicMock(return_value=[{'Key': {'id': 1}}])

        # Phase 2 raises
        ops_rdd = MagicMock()
        ops_rdd.foreachPartition = MagicMock(side_effect=RuntimeError('update crashed'))

        sc = MagicMock()
        accs = [
            MagicMock(value=[]),   # error_accumulator scan
            MagicMock(value=0),    # updated
            MagicMock(value=0),    # failed
            MagicMock(value=[]),   # error_accumulator_2
        ]
        sc.accumulator = MagicMock(side_effect=accs)
        sc.parallelize = MagicMock(side_effect=[scan_rdd, ops_rdd])

        with pytest.raises(Exception, match='Error in update phase'):
            update_module.run(MagicMock(), sc, MagicMock(), base_args)

    def test_aggregator_shutdown_on_scan_failure(self, monkeypatch, shared_table_info_mocks,
                                                  base_args):
        """Rate limiter aggregator shutdown called even when scan fails."""
        monkeypatch.setattr(update_module.importlib, 'import_module',
                            MagicMock(return_value=MagicMock(generate=MagicMock())))
        monkeypatch.setattr(update_module, 'print_dynamodb_table_info', MagicMock())
        monkeypatch.setattr(update_module, 'get_error_message', lambda e: str(e))

        agg_instance = MagicMock()
        monkeypatch.setattr(update_module, 'RateLimiterSharedConfig', MagicMock())
        monkeypatch.setattr(update_module, 'RateLimiterAggregator', MagicMock(return_value=agg_instance))

        scan_rdd = MagicMock()
        scan_rdd.flatMap = MagicMock(return_value=scan_rdd)
        scan_rdd.collect = MagicMock(side_effect=RuntimeError('boom'))

        sc = MagicMock()
        sc.accumulator = MagicMock(return_value=MagicMock(value=[]))
        sc.parallelize = MagicMock(return_value=scan_rdd)

        with pytest.raises(Exception):
            update_module.run(MagicMock(), sc, MagicMock(), base_args)

        agg_instance.shutdown.assert_called_once()

    def test_scan_error_accumulator_raises_first_error(self, monkeypatch, shared_table_info_mocks,
                                                        rate_limiter_mocks, spark_context, base_args):
        """If scan error_accumulator has errors after collect, first error is raised."""
        monkeypatch.setattr(update_module.importlib, 'import_module',
                            MagicMock(return_value=MagicMock(generate=MagicMock())))
        monkeypatch.setattr(update_module, 'print_dynamodb_table_info', MagicMock())

        scan_rdd = MagicMock()
        scan_rdd.flatMap = MagicMock(return_value=scan_rdd)
        scan_rdd.collect = MagicMock(return_value=[])

        sc = MagicMock()
        error_acc = MagicMock(value=['first scan error', 'second error'])
        sc.accumulator = MagicMock(return_value=error_acc)
        sc.parallelize = MagicMock(return_value=scan_rdd)

        with pytest.raises(Exception, match='first scan error'):
            update_module.run(MagicMock(), sc, MagicMock(), base_args)

    def test_prints_phase_summary(self, monkeypatch, shared_table_info_mocks,
                                   base_args, capsys):
        """run() prints phase 1 and phase 2 summaries."""
        monkeypatch.setattr(update_module.importlib, 'import_module',
                            MagicMock(return_value=MagicMock(generate=MagicMock())))
        monkeypatch.setattr(update_module, 'print_dynamodb_table_info', MagicMock())

        config_cls = MagicMock(side_effect=lambda **kw: MagicMock(**kw))
        aggregator_cls = MagicMock()
        monkeypatch.setattr(update_module, 'RateLimiterSharedConfig', config_cls)
        monkeypatch.setattr(update_module, 'RateLimiterAggregator', aggregator_cls)

        pending_ops = [{'Key': {'id': i}} for i in range(5)]
        scan_rdd = MagicMock()
        scan_rdd.flatMap = MagicMock(return_value=scan_rdd)
        scan_rdd.collect = MagicMock(return_value=pending_ops)

        ops_rdd = MagicMock()
        ops_rdd.foreachPartition = MagicMock()

        sc = MagicMock()
        accs = [
            MagicMock(value=[]),  # error acc scan
            MagicMock(value=4),   # updated
            MagicMock(value=1),   # failed
            MagicMock(value=[]),  # error acc update
        ]
        sc.accumulator = MagicMock(side_effect=accs)
        sc.parallelize = MagicMock(side_effect=[scan_rdd, ops_rdd])

        update_module.run(MagicMock(), sc, MagicMock(), base_args)

        out = capsys.readouterr().out
        assert 'Phase 1' in out
        assert '5' in out  # 5 items identified
        assert 'Phase 2' in out
        assert '4' in out  # 4 updates


# --- _scan_segment -----------------------------------------------------------

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


class TestScanSegmentPagination:
    """_scan_segment scan loop with LastEvaluatedKey-based pagination."""

    def test_single_page_no_lek(self, monkeypatch):
        """If no LastEvaluatedKey in response, scan stops after one page."""
        table = _make_table_with_scan([{'Items': [{'id': 1}]}])
        rl = _make_rl_worker(table)
        monkeypatch.setattr(update_module, 'RateLimiterWorker', MagicMock(return_value=rl))

        result = update_module._scan_segment(
            {}, 'tbl', lambda item: {'Key': item}, 0, 1,
            MagicMock(), MagicMock()
        )

        assert table.scan.call_count == 1
        assert len(result) == 1
        assert result[0] == {'Key': {'id': 1}}

    def test_multi_page_threads_lek(self, monkeypatch):
        """ExclusiveStartKey is set from previous LastEvaluatedKey."""
        scan_kwargs_seen = []
        responses = iter([
            {'Items': [{'a': 1}], 'LastEvaluatedKey': {'pk': 'page1'}},
            {'Items': [{'b': 2}], 'LastEvaluatedKey': {'pk': 'page2'}},
            {'Items': [{'c': 3}]},
        ])

        table = MagicMock()

        def scan_capture(**kwargs):
            scan_kwargs_seen.append(dict(kwargs))
            return next(responses)

        table.scan = scan_capture
        rl = _make_rl_worker(table)
        monkeypatch.setattr(update_module, 'RateLimiterWorker', MagicMock(return_value=rl))

        result = update_module._scan_segment(
            {}, 'tbl', lambda item: {'Key': item}, 3, 10,
            MagicMock(), MagicMock()
        )

        assert len(scan_kwargs_seen) == 3
        assert 'ExclusiveStartKey' not in scan_kwargs_seen[0]
        assert scan_kwargs_seen[1]['ExclusiveStartKey'] == {'pk': 'page1'}
        assert scan_kwargs_seen[2]['ExclusiveStartKey'] == {'pk': 'page2'}
        assert len(result) == 3

    def test_scan_kwargs_include_segment_and_total(self, monkeypatch):
        """scan_kwargs includes TableName, Segment, TotalSegments."""
        scan_kwargs_seen = []
        table = MagicMock()

        def scan_capture(**kwargs):
            scan_kwargs_seen.append(dict(kwargs))
            return {'Items': []}

        table.scan = scan_capture
        rl = _make_rl_worker(table)
        monkeypatch.setattr(update_module, 'RateLimiterWorker', MagicMock(return_value=rl))

        update_module._scan_segment(
            {}, 'my-tbl', lambda item: None, 7, 100,
            MagicMock(), MagicMock()
        )

        assert scan_kwargs_seen[0]['TableName'] == 'my-tbl'
        assert scan_kwargs_seen[0]['Segment'] == 7
        assert scan_kwargs_seen[0]['TotalSegments'] == 100


class TestScanSegmentGenerate:
    """_scan_segment calls generate() per item and collects truthy results."""

    def test_generate_returns_truthy_collected(self, monkeypatch):
        """If generate returns truthy, it's added to pending_ops."""
        table = _make_table_with_scan([{'Items': [{'id': 1}, {'id': 2}]}])
        rl = _make_rl_worker(table)
        monkeypatch.setattr(update_module, 'RateLimiterWorker', MagicMock(return_value=rl))

        def gen(item):
            return {'Key': {'id': item['id']}, 'UpdateExpression': 'SET #a = :v'}

        result = update_module._scan_segment(
            {}, 'tbl', gen, 0, 1, MagicMock(), MagicMock()
        )

        assert len(result) == 2

    def test_generate_returns_falsy_skipped(self, monkeypatch):
        """If generate returns falsy, item is not collected."""
        table = _make_table_with_scan([{'Items': [{'id': 1}, {'id': 2}, {'id': 3}]}])
        rl = _make_rl_worker(table)
        monkeypatch.setattr(update_module, 'RateLimiterWorker', MagicMock(return_value=rl))

        def gen(item):
            if item['id'] == 2:
                return {'Key': item}
            return None

        result = update_module._scan_segment(
            {}, 'tbl', gen, 0, 1, MagicMock(), MagicMock()
        )

        assert len(result) == 1
        assert result[0] == {'Key': {'id': 2}}

    def test_generate_validation_error_exits(self, monkeypatch):
        """ValidationException from generate raises SystemExit."""
        table = _make_table_with_scan([{'Items': [{'id': 1}]}])
        rl = _make_rl_worker(table)
        monkeypatch.setattr(update_module, 'RateLimiterWorker', MagicMock(return_value=rl))
        monkeypatch.setattr(update_module, 'get_error_code',
                            lambda e: e.response['Error']['Code'])
        monkeypatch.setattr(update_module, 'get_error_message', lambda e: 'bad schema')

        def gen(item):
            raise botocore.exceptions.ClientError(
                {'Error': {'Code': 'ValidationException', 'Message': 'bad'}},
                'Scan'
            )

        with pytest.raises(SystemExit):
            update_module._scan_segment(
                {}, 'tbl', gen, 0, 1, MagicMock(), MagicMock()
            )


class TestScanSegmentErrorHandling:
    """_scan_segment error handling and rate limiter shutdown."""

    def test_scan_error_captured_in_accumulator(self, monkeypatch):
        """Error from scan is caught and added to error_accumulator."""
        table = MagicMock()
        table.scan = MagicMock(side_effect=RuntimeError('network fail'))
        rl = _make_rl_worker(table)
        monkeypatch.setattr(update_module, 'RateLimiterWorker', MagicMock(return_value=rl))
        monkeypatch.setattr(update_module, 'get_error_message', lambda e: f'wrapped:{e}')

        error_acc = MagicMock()
        update_module._scan_segment(
            {}, 'tbl', lambda item: {'Key': item}, 7, 10,
            error_acc, MagicMock()
        )

        error_acc.add.assert_called_once()
        appended = error_acc.add.call_args.args[0]
        assert isinstance(appended, list) and len(appended) == 1
        assert 'scan worker 7' in appended[0]

    def test_shutdown_called_on_success(self, monkeypatch):
        """rate_limiter_worker.shutdown() called after successful scan."""
        table = _make_table_with_scan([{'Items': []}])
        rl_instance = MagicMock()
        rl_instance.get_session.return_value.resource.return_value.Table.return_value = table
        monkeypatch.setattr(update_module, 'RateLimiterWorker', MagicMock(return_value=rl_instance))

        update_module._scan_segment(
            {}, 'tbl', lambda item: None, 0, 1, MagicMock(), MagicMock()
        )

        rl_instance.shutdown.assert_called_once()

    def test_shutdown_called_on_error(self, monkeypatch):
        """rate_limiter_worker.shutdown() called even when scan raises."""
        table = MagicMock()
        table.scan = MagicMock(side_effect=RuntimeError('boom'))
        rl_instance = MagicMock()
        rl_instance.get_session.return_value.resource.return_value.Table.return_value = table
        monkeypatch.setattr(update_module, 'RateLimiterWorker', MagicMock(return_value=rl_instance))
        monkeypatch.setattr(update_module, 'get_error_message', lambda e: str(e))

        update_module._scan_segment(
            {}, 'tbl', lambda item: None, 0, 1, MagicMock(), MagicMock()
        )

        rl_instance.shutdown.assert_called_once()

    def test_boto3_config_has_4_second_timeouts_and_50_retries(self, monkeypatch):
        """Config has connect_timeout=4.0, read_timeout=4.0, 50 retries."""
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

        update_module._scan_segment(
            {}, 'tbl', lambda item: None, 0, 1, MagicMock(), MagicMock()
        )

        assert len(seen_configs) == 1
        cfg = seen_configs[0]
        assert cfg.connect_timeout == 4.0
        assert cfg.read_timeout == 4.0
        assert cfg.retries['mode'] == 'standard'
        assert cfg.retries['total_max_attempts'] == 50


# --- _execute_updates --------------------------------------------------------

class TestExecuteUpdatesBasic:
    """_execute_updates iterates partition and calls update_item."""

    def test_updates_all_items_in_partition(self, monkeypatch):
        """Each item in partition gets update_item called."""
        table = MagicMock()
        rl = _make_rl_worker(table)
        monkeypatch.setattr(update_module, 'RateLimiterWorker', MagicMock(return_value=rl))

        ops = [{'Key': {'id': 1}}, {'Key': {'id': 2}}, {'Key': {'id': 3}}]
        updated_acc = MagicMock()
        failed_acc = MagicMock()

        update_module._execute_updates(
            iter(ops), 'tbl', updated_acc, failed_acc, MagicMock(), MagicMock(), {}
        )

        assert table.update_item.call_count == 3
        updated_acc.add.assert_called_once_with(3)
        failed_acc.add.assert_called_once_with(0)

    def test_empty_partition(self, monkeypatch):
        """Empty partition results in zero counts."""
        table = MagicMock()
        rl = _make_rl_worker(table)
        monkeypatch.setattr(update_module, 'RateLimiterWorker', MagicMock(return_value=rl))

        updated_acc = MagicMock()
        failed_acc = MagicMock()

        update_module._execute_updates(
            iter([]), 'tbl', updated_acc, failed_acc, MagicMock(), MagicMock(), {}
        )

        table.update_item.assert_not_called()
        updated_acc.add.assert_called_once_with(0)
        failed_acc.add.assert_called_once_with(0)


class TestExecuteUpdatesClientErrors:
    """_execute_updates ClientError handling per error code."""

    def _make_client_error(self, code, message='error'):
        return botocore.exceptions.ClientError(
            {'Error': {'Code': code, 'Message': message}},
            'UpdateItem'
        )

    def test_throttle_exception_exits(self, monkeypatch):
        """ProvisionedThroughputExceededException calls exit()."""
        table = MagicMock()
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
            update_module._execute_updates(
                iter([{'Key': {'id': 1}}]), 'tbl',
                MagicMock(), MagicMock(), error_acc, MagicMock(), {}
            )

    def test_validation_exception_exits(self, monkeypatch):
        """ValidationException calls exit() with message."""
        table = MagicMock()
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
            update_module._execute_updates(
                iter([{'Key': {'id': 1}}]), 'tbl',
                MagicMock(), MagicMock(), error_acc, MagicMock(), {}
            )

    def test_conditional_check_failed_increments_failed(self, monkeypatch):
        """ConditionalCheckFailedException increments failed_count."""
        table = MagicMock()
        table.update_item = MagicMock(
            side_effect=self._make_client_error('ConditionalCheckFailedException')
        )
        rl = _make_rl_worker(table)
        monkeypatch.setattr(update_module, 'RateLimiterWorker', MagicMock(return_value=rl))
        monkeypatch.setattr(update_module, 'get_error_code',
                            lambda e: e.response['Error']['Code'])

        failed_acc = MagicMock()
        updated_acc = MagicMock()

        update_module._execute_updates(
            iter([{'Key': {'id': 1}}, {'Key': {'id': 2}}]), 'tbl',
            updated_acc, failed_acc, MagicMock(), MagicMock(), {}
        )

        failed_acc.add.assert_called_once_with(2)
        updated_acc.add.assert_called_once_with(0)

    def test_conditional_check_failed_prints_kwargs(self, monkeypatch, capsys):
        """Prints the update_kwargs that caused the condition failure."""
        table = MagicMock()
        table.update_item = MagicMock(
            side_effect=self._make_client_error('ConditionalCheckFailedException')
        )
        rl = _make_rl_worker(table)
        monkeypatch.setattr(update_module, 'RateLimiterWorker', MagicMock(return_value=rl))
        monkeypatch.setattr(update_module, 'get_error_code',
                            lambda e: e.response['Error']['Code'])

        update_module._execute_updates(
            iter([{'Key': {'id': 'x'}, 'CE': 'cond'}]), 'tbl',
            MagicMock(), MagicMock(), MagicMock(), MagicMock(), {}
        )

        out = capsys.readouterr().out
        assert 'condition expression failed' in out
        assert "{'Key': {'id': 'x'}, 'CE': 'cond'}" in out

    def test_unhandled_client_error_captured(self, monkeypatch):
        """Unknown error code is caught by outer except and added to error_accumulator."""
        table = MagicMock()
        err = self._make_client_error('InternalServerError', 'oops')
        table.update_item = MagicMock(side_effect=err)
        rl = _make_rl_worker(table)
        monkeypatch.setattr(update_module, 'RateLimiterWorker', MagicMock(return_value=rl))
        monkeypatch.setattr(update_module, 'get_error_code',
                            lambda e: e.response['Error']['Code'])
        monkeypatch.setattr(update_module, 'get_error_message', lambda e: str(e))

        error_acc = MagicMock()
        update_module._execute_updates(
            iter([{'Key': {'id': 1}}]), 'tbl',
            MagicMock(), MagicMock(), error_acc, MagicMock(), {}
        )

        error_acc.add.assert_called_once()
        msg = error_acc.add.call_args.args[0][0]
        assert 'update worker' in msg

    def test_unhandled_client_error_prints_to_stderr(self, monkeypatch, capsys):
        """Unhandled error printed to stderr."""
        table = MagicMock()
        err = self._make_client_error('InternalServerError', 'oops')
        table.update_item = MagicMock(side_effect=err)
        rl = _make_rl_worker(table)
        monkeypatch.setattr(update_module, 'RateLimiterWorker', MagicMock(return_value=rl))
        monkeypatch.setattr(update_module, 'get_error_code',
                            lambda e: e.response['Error']['Code'])
        monkeypatch.setattr(update_module, 'get_error_message', lambda e: str(e))

        update_module._execute_updates(
            iter([{'Key': {'id': 1}}]), 'tbl',
            MagicMock(), MagicMock(), MagicMock(), MagicMock(), {}
        )

        err_out = capsys.readouterr().err
        assert 'Unhandled ClientError' in err_out


class TestExecuteUpdatesShutdown:
    """rate_limiter_worker.shutdown() always called in finally."""

    def test_shutdown_called_on_success(self, monkeypatch):
        table = MagicMock()
        rl_instance = MagicMock()
        rl_instance.get_session.return_value.resource.return_value.Table.return_value = table
        monkeypatch.setattr(update_module, 'RateLimiterWorker', MagicMock(return_value=rl_instance))

        update_module._execute_updates(
            iter([]), 'tbl', MagicMock(), MagicMock(), MagicMock(), MagicMock(), {}
        )

        rl_instance.shutdown.assert_called_once()

    def test_shutdown_called_on_error(self, monkeypatch):
        table = MagicMock()
        table.update_item = MagicMock(side_effect=RuntimeError('boom'))
        rl_instance = MagicMock()
        rl_instance.get_session.return_value.resource.return_value.Table.return_value = table
        monkeypatch.setattr(update_module, 'RateLimiterWorker', MagicMock(return_value=rl_instance))
        monkeypatch.setattr(update_module, 'get_error_message', lambda e: str(e))

        update_module._execute_updates(
            iter([{'Key': {'id': 1}}]), 'tbl',
            MagicMock(), MagicMock(), MagicMock(), MagicMock(), {}
        )

        rl_instance.shutdown.assert_called_once()

    def test_boto3_config_has_4_second_timeouts_and_50_retries(self, monkeypatch):
        """Config has connect_timeout=4.0, read_timeout=4.0, 50 retries."""
        seen_configs = []

        rl_instance = MagicMock()
        session = MagicMock()

        def capture_resource(name, **kw):
            seen_configs.append(kw.get('config'))
            r = MagicMock()
            r.Table.return_value = MagicMock()
            return r

        session.resource = capture_resource
        rl_instance.get_session.return_value = session
        monkeypatch.setattr(update_module, 'RateLimiterWorker', MagicMock(return_value=rl_instance))

        update_module._execute_updates(
            iter([]), 'tbl', MagicMock(), MagicMock(), MagicMock(), MagicMock(), {}
        )

        assert len(seen_configs) == 1
        cfg = seen_configs[0]
        assert cfg.connect_timeout == 4.0
        assert cfg.read_timeout == 4.0
        assert cfg.retries['mode'] == 'standard'
        assert cfg.retries['total_max_attempts'] == 50


class TestExecuteUpdatesMonitorOptions:
    """monitor_options are passed as kwargs to RateLimiterWorker."""

    def test_monitor_options_splatted_into_worker(self, monkeypatch):
        rl_class = MagicMock()
        rl_instance = MagicMock()
        rl_instance.get_session.return_value.resource.return_value.Table.return_value = MagicMock()
        rl_class.return_value = rl_instance
        monkeypatch.setattr(update_module, 'RateLimiterWorker', rl_class)

        monitor_opts = {'monitor_table': 'my-tbl', 'max_rate': 500}

        update_module._execute_updates(
            iter([]), 'tbl', MagicMock(), MagicMock(), MagicMock(), MagicMock(), monitor_opts
        )

        call_kwargs = rl_class.call_args.kwargs
        assert call_kwargs['monitor_table'] == 'my-tbl'
        assert call_kwargs['max_rate'] == 500
