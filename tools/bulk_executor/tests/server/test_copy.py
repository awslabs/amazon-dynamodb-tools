"""Unit tests for the `copy` server-side verb.

Covers `python_modules/copy.py`:
- ListAccumulator: zero / addInPlace contract used to accumulate errors
- print_dynamodb_table_info: helper call ordering and cost arithmetic
- run(): argument wiring, source/target rate-limiter shared configs,
  monitor option modes, spark parallelize count, error propagation,
  rate-limiter shutdown invariants, total record count
- _copy_data: boto3 Config (timeouts, retries), region resolution
  from ARN vs session, scan pagination, batch_writer usage, per-worker
  error accumulation, rate-limiter shutdown in finally

The existing tests/server/conftest.py mocks awsglue, pyspark, and
shared modules at all resolution paths. These tests build on that.
"""

from unittest.mock import MagicMock, call, patch

import pytest


# --- Module imports -----------------------------------------------------
#
# python_modules.copy depends on heavily-mocked shared.* and pyspark
# modules at import time. The conftest's sys.modules patches must already
# be active when this file is collected, so we import inside fixtures
# where possible. For the module-level constants (ListAccumulator) the
# import has to happen at test-collection time.

from python_modules import copy as copy_module  # noqa: E402


@pytest.fixture
def shared_table_info_mocks(monkeypatch):
    """Replace the four shared.table_info helpers used by copy.run() with
    fresh MagicMocks per test, returning predictable values."""
    helpers = MagicMock()
    helpers.get_and_print_dynamodb_table_info = MagicMock(
        side_effect=[
            {'item_count': 100, 'size_bytes': 1024, 'region_name': 'us-east-1'},
            {'item_count': 100, 'size_bytes': 1024, 'region_name': 'us-east-1'},
        ]
    )
    helpers.get_and_print_table_scan_cost = MagicMock(return_value=1.50)
    helpers.get_and_print_table_copy_write_cost = MagicMock(return_value=2.50)
    helpers.get_dynamodb_throughput_configs = MagicMock(return_value={'opt': 'val'})

    monkeypatch.setattr(copy_module, 'get_and_print_dynamodb_table_info',
                        helpers.get_and_print_dynamodb_table_info)
    monkeypatch.setattr(copy_module, 'get_and_print_table_scan_cost',
                        helpers.get_and_print_table_scan_cost)
    monkeypatch.setattr(copy_module, 'get_and_print_table_copy_write_cost',
                        helpers.get_and_print_table_copy_write_cost)
    monkeypatch.setattr(copy_module, 'get_dynamodb_throughput_configs',
                        helpers.get_dynamodb_throughput_configs)
    return helpers


@pytest.fixture
def rate_limiter_mocks(monkeypatch):
    """Replace RateLimiterAggregator / RateLimiterSharedConfig with mocks
    so we can inspect how copy.run wires source vs target configs."""
    config_cls = MagicMock(side_effect=lambda **kw: MagicMock(spec_set=['bucket', 'job_run_id'], **kw))
    aggregator_cls = MagicMock()

    monkeypatch.setattr(copy_module, 'RateLimiterSharedConfig', config_cls)
    monkeypatch.setattr(copy_module, 'RateLimiterAggregator', aggregator_cls)
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
        'source': 'src-table',
        'target': 'dst-table',
        's3-bucket-name': 'rate-limit-bucket',
        'JOB_RUN_ID': 'jr-abc-123',
    }


# --- ListAccumulator ----------------------------------------------------

class TestListAccumulator:
    """copy.py defines a custom AccumulatorParam to collect per-worker
    errors. The contract: zero() returns a fresh empty list; addInPlace
    extends in-place and returns the merged list."""

    def test_zero_returns_empty_list_regardless_of_seed(self):
        acc = copy_module.ListAccumulator()
        assert acc.zero(['ignored']) == []
        assert acc.zero(None) == []

    def test_addInPlace_extends_first_and_returns_it(self):
        acc = copy_module.ListAccumulator()
        a = ['err1']
        b = ['err2', 'err3']
        result = acc.addInPlace(a, b)
        assert a == ['err1', 'err2', 'err3'], "first arg mutated in place"
        assert result is a, "returns the mutated first list, not a new one"

    def test_addInPlace_with_empty_right_side(self):
        acc = copy_module.ListAccumulator()
        result = acc.addInPlace(['x'], [])
        assert result == ['x']

    def test_addInPlace_with_empty_left_side(self):
        acc = copy_module.ListAccumulator()
        result = acc.addInPlace([], ['y'])
        assert result == ['y']


# --- print_dynamodb_table_info ------------------------------------------

class TestPrintDynamodbTableInfo:
    """The helper prints info for both tables and computes total cost.
    It calls four shared functions in a specific order."""

    def test_calls_helpers_in_order(self, shared_table_info_mocks, capsys):
        copy_module.print_dynamodb_table_info('src', 'dst')

        info_calls = shared_table_info_mocks.get_and_print_dynamodb_table_info.call_args_list
        assert info_calls == [call('src'), call('dst')], \
            "source table info must be fetched before target"

        scan_calls = shared_table_info_mocks.get_and_print_table_scan_cost.call_args_list
        assert len(scan_calls) == 1, "scan cost computed once (for source only)"

        write_calls = shared_table_info_mocks.get_and_print_table_copy_write_cost.call_args_list
        assert len(write_calls) == 1, "write cost computed once (source -> target)"

    def test_prints_total_cost_combining_scan_and_write(self, shared_table_info_mocks, capsys):
        # scan_cost=1.50, write_cost=2.50 in the fixture
        copy_module.print_dynamodb_table_info('src', 'dst')
        out = capsys.readouterr().out
        assert '$4.00' in out, "1.50 scan + 2.50 write must equal 4.00"
        assert 'src' in out and 'dst' in out, "both table names appear in summary"

    def test_write_cost_is_called_with_both_table_infos(self, shared_table_info_mocks):
        copy_module.print_dynamodb_table_info('src', 'dst')
        write_args = shared_table_info_mocks.get_and_print_table_copy_write_cost.call_args
        # Expect (source_info, target_info) positionally
        assert write_args.args[0]['item_count'] == 100, "source info first"
        assert write_args.args[1]['item_count'] == 100, "target info second"


# --- run() --------------------------------------------------------------

class TestRunArgumentWiring:
    """copy.run() pulls source, target, bucket, and job-run-id from
    parsed_args and routes them into per-side rate-limiter shared configs."""

    def test_source_shared_config_uses_source_suffix(self, monkeypatch, shared_table_info_mocks,
                                                     rate_limiter_mocks, spark_context, base_args):
        # Make the foreach a no-op so we don't need to mock _copy_data internals
        spark_context.parallelize.return_value.foreach = MagicMock()
        copy_module.run(MagicMock(), spark_context, MagicMock(), base_args)

        config_calls = rate_limiter_mocks.config.call_args_list
        kwargs_seen = [c.kwargs for c in config_calls]
        assert {'bucket': 'rate-limit-bucket', 'job_run_id': 'jr-abc-123-source'} in kwargs_seen, \
            "source shared config must carry job_run_id with -source suffix"

    def test_target_shared_config_uses_target_suffix(self, monkeypatch, shared_table_info_mocks,
                                                     rate_limiter_mocks, spark_context, base_args):
        spark_context.parallelize.return_value.foreach = MagicMock()
        copy_module.run(MagicMock(), spark_context, MagicMock(), base_args)

        kwargs_seen = [c.kwargs for c in rate_limiter_mocks.config.call_args_list]
        assert {'bucket': 'rate-limit-bucket', 'job_run_id': 'jr-abc-123-target'} in kwargs_seen, \
            "target shared config must carry job_run_id with -target suffix"

    def test_throughput_configs_called_for_read_on_source(self, monkeypatch, shared_table_info_mocks,
                                                           rate_limiter_mocks, spark_context, base_args):
        spark_context.parallelize.return_value.foreach = MagicMock()
        copy_module.run(MagicMock(), spark_context, MagicMock(), base_args)

        calls = shared_table_info_mocks.get_dynamodb_throughput_configs.call_args_list
        # First call: source table, modes=["read"]
        assert calls[0].args[1] == 'src-table'
        assert calls[0].kwargs['modes'] == ['read']
        assert calls[0].kwargs['format'] == 'monitor'

    def test_throughput_configs_called_for_write_on_target(self, monkeypatch, shared_table_info_mocks,
                                                            rate_limiter_mocks, spark_context, base_args):
        spark_context.parallelize.return_value.foreach = MagicMock()
        copy_module.run(MagicMock(), spark_context, MagicMock(), base_args)

        calls = shared_table_info_mocks.get_dynamodb_throughput_configs.call_args_list
        assert calls[1].args[1] == 'dst-table'
        assert calls[1].kwargs['modes'] == ['write']

    def test_parallelize_count_is_400(self, monkeypatch, shared_table_info_mocks,
                                       rate_limiter_mocks, spark_context, base_args):
        spark_context.parallelize.return_value.foreach = MagicMock()
        copy_module.run(MagicMock(), spark_context, MagicMock(), base_args)

        # spark_context.parallelize(range(400), 400)
        pc_args = spark_context.parallelize.call_args
        assert sorted(pc_args.args[0]) == list(range(400)), "segments cover 0..399 (shuffled)"
        assert pc_args.args[1] == 400, "numSlices is 400 — one partition per worker"

    def test_total_matched_accumulator_initialized_to_zero(self, monkeypatch, shared_table_info_mocks,
                                                            rate_limiter_mocks, spark_context, base_args):
        spark_context.parallelize.return_value.foreach = MagicMock()
        copy_module.run(MagicMock(), spark_context, MagicMock(), base_args)

        # First accumulator() call is for total_matched, seeded with 0
        first_call = spark_context.accumulator.call_args_list[0]
        assert first_call.args[0] == 0, "total_matched accumulator starts at 0"

    def test_error_accumulator_seeded_with_empty_list(self, monkeypatch, shared_table_info_mocks,
                                                       rate_limiter_mocks, spark_context, base_args):
        spark_context.parallelize.return_value.foreach = MagicMock()
        copy_module.run(MagicMock(), spark_context, MagicMock(), base_args)

        # Second accumulator call: error_accumulator with [] and a ListAccumulator instance
        err_call = spark_context.accumulator.call_args_list[1]
        assert err_call.args[0] == [], "error accumulator seeded with empty list"
        assert isinstance(err_call.args[1], copy_module.ListAccumulator), \
            "ListAccumulator is the AccumulatorParam"


class TestRunErrorHandling:
    """Errors from worker partitions are accumulated; if any error landed,
    run() raises the first one. Aggregator shutdown happens in finally."""

    def test_first_worker_error_is_raised(self, monkeypatch, shared_table_info_mocks,
                                           rate_limiter_mocks, spark_context, base_args):
        # Wire the error accumulator to come back with two errors
        accs = [
            MagicMock(value=42),  # total_matched
            MagicMock(value=['first error', 'second error']),  # error_accumulator
        ]
        spark_context.accumulator = MagicMock(side_effect=accs)
        spark_context.parallelize.return_value.foreach = MagicMock()

        with pytest.raises(Exception, match='first error'):
            copy_module.run(MagicMock(), spark_context, MagicMock(), base_args)

    def test_no_errors_means_no_raise(self, monkeypatch, shared_table_info_mocks,
                                       rate_limiter_mocks, spark_context, base_args, capsys):
        accs = [MagicMock(value=99), MagicMock(value=[])]
        spark_context.accumulator = MagicMock(side_effect=accs)
        spark_context.parallelize.return_value.foreach = MagicMock()

        copy_module.run(MagicMock(), spark_context, MagicMock(), base_args)
        out = capsys.readouterr().out
        assert '99' in out, "total record count printed when no errors"

    def test_aggregators_shutdown_even_on_foreach_failure(self, monkeypatch, shared_table_info_mocks,
                                                           rate_limiter_mocks, spark_context, base_args):
        # Track aggregator instances and confirm both got shutdown
        instances = []
        def aggregator_factory(**kw):
            inst = MagicMock()
            instances.append(inst)
            return inst
        monkeypatch.setattr(copy_module, 'RateLimiterAggregator', aggregator_factory)

        spark_context.parallelize.return_value.foreach = MagicMock(
            side_effect=RuntimeError('spark exploded')
        )

        with pytest.raises(Exception):
            copy_module.run(MagicMock(), spark_context, MagicMock(), base_args)

        assert len(instances) == 2, "one aggregator per side (source + target)"
        for inst in instances:
            assert inst.shutdown.called, "shutdown invoked in finally on every aggregator"

    def test_parallelize_failure_wraps_exception(self, monkeypatch, shared_table_info_mocks,
                                                  rate_limiter_mocks, spark_context, base_args):
        spark_context.parallelize.return_value.foreach = MagicMock(
            side_effect=RuntimeError('worker bug')
        )
        with pytest.raises(Exception, match='Error in parallel execution'):
            copy_module.run(MagicMock(), spark_context, MagicMock(), base_args)


class TestRunForeachDispatch:
    """The lambda passed to rdd.foreach must call _copy_data with the
    expected positional arguments per partition."""

    def test_foreach_lambda_invokes_copy_data_with_segment(self, monkeypatch, shared_table_info_mocks,
                                                            rate_limiter_mocks, spark_context, base_args):
        captured = {}

        def fake_copy_data(*args, **kwargs):
            captured.setdefault('calls', []).append(args)

        monkeypatch.setattr(copy_module, '_copy_data', fake_copy_data)

        # Have foreach actually invoke the lambda for a few worker IDs
        def fake_foreach(fn):
            for worker_id in (0, 1, 399):
                fn(worker_id)
        spark_context.parallelize.return_value.foreach = fake_foreach

        copy_module.run(MagicMock(), spark_context, MagicMock(), base_args)

        # Each call: (source_table, target_table, source_monitor, target_monitor,
        #            worker_id, total_segments, total_acc, error_acc, src_cfg, tgt_cfg)
        assert len(captured['calls']) == 3
        assert captured['calls'][0][0] == 'src-table'
        assert captured['calls'][0][1] == 'dst-table'
        assert captured['calls'][0][4] == 0, "worker_id passed as 5th arg"
        assert captured['calls'][1][4] == 1
        assert captured['calls'][2][4] == 399
        # total_segments is fixed at parallelize_count (400)
        for call_args in captured['calls']:
            assert call_args[5] == 400, "total_segments is the parallelize count"


# --- _copy_data ---------------------------------------------------------

def _stub_table_with_empty_scan(session):
    """Wire `session.resource(...).Table(...)` so scan returns one empty
    page (terminates pagination immediately) and batch_writer is a no-op
    context manager. Without this the `while True` in _copy_data spins
    forever on default MagicMock returns.
    """
    table = MagicMock()
    table.scan = MagicMock(return_value={'Items': [], 'LastEvaluatedKey': None})
    bw = MagicMock()
    bw.__enter__ = MagicMock(return_value=bw)
    bw.__exit__ = MagicMock(return_value=False)
    table.batch_writer.return_value = bw
    session.resource.return_value.Table.return_value = table
    return table


class TestCopyDataConfig:
    """The boto3 Config used inside the worker has specific timeout and
    retry settings — these are user-facing reliability knobs."""

    def test_boto3_config_has_4_second_timeouts_and_50_retries(self, monkeypatch):
        seen_configs = []

        rl_instance = MagicMock()
        session = MagicMock(region_name='us-east-1')

        def capture_resource(name, **kw):
            seen_configs.append(kw['config'])
            r = MagicMock()
            # Wire the chain so the scan loop terminates on first iteration
            r.Table.return_value.scan.return_value = {'Items': [], 'LastEvaluatedKey': None}
            bw = MagicMock()
            bw.__enter__ = MagicMock(return_value=bw)
            bw.__exit__ = MagicMock(return_value=False)
            r.Table.return_value.batch_writer.return_value = bw
            return r
        session.resource = capture_resource
        rl_instance.get_session.return_value = session

        monkeypatch.setattr(copy_module, 'RateLimiterWorker', MagicMock(return_value=rl_instance))
        monkeypatch.setattr(copy_module, '_region_from_table_ref', lambda ref: None)

        copy_module._copy_data('src', 'dst', {}, {}, 0, 1,
                                MagicMock(), MagicMock(), MagicMock(), MagicMock())

        assert len(seen_configs) == 2, "one Config per side (source + target)"
        for cfg in seen_configs:
            assert cfg.connect_timeout == 4.0
            assert cfg.read_timeout == 4.0
            assert cfg.retries['mode'] == 'standard'
            assert cfg.retries['total_max_attempts'] == 50, \
                "50 retry attempts handles transient throttles during high-volume copies"


class TestCopyDataRegionResolution:
    """Region routing matters for cross-region copies. ARNs override the
    session's default region; plain table names use session region."""

    def _make_rl_with_session(self, region_name='us-east-1'):
        rl_instance = MagicMock()
        session = MagicMock()
        session.region_name = region_name
        session.resource = MagicMock(return_value=MagicMock())
        rl_instance.get_session.return_value = session
        return rl_instance, session

    def test_plain_table_name_uses_session_region(self, monkeypatch):
        rl, session = self._make_rl_with_session(region_name='eu-west-1')
        monkeypatch.setattr(copy_module, 'RateLimiterWorker', MagicMock(return_value=rl))
        # Plain names — _region_from_table_ref returns None
        monkeypatch.setattr(copy_module, '_region_from_table_ref', lambda ref: None)

        # Mock pagination to exit immediately
        scan_resp = {'Items': [], 'LastEvaluatedKey': None}
        session.resource.return_value.Table.return_value.scan = MagicMock(return_value=scan_resp)
        session.resource.return_value.Table.return_value.batch_writer.return_value.__enter__ = \
            MagicMock(return_value=MagicMock())
        session.resource.return_value.Table.return_value.batch_writer.return_value.__exit__ = \
            MagicMock(return_value=False)

        copy_module._copy_data('plain-table', 'plain-target', {}, {}, 0, 1,
                                MagicMock(), MagicMock(), MagicMock(), MagicMock())

        # session.resource was called twice (once per side) — both with region_name=eu-west-1
        for c in session.resource.call_args_list:
            assert c.kwargs['region_name'] == 'eu-west-1'

    def test_arn_table_name_overrides_session_region(self, monkeypatch):
        rl, session = self._make_rl_with_session(region_name='us-east-1')
        monkeypatch.setattr(copy_module, 'RateLimiterWorker', MagicMock(return_value=rl))
        # _region_from_table_ref is imported into the copy module namespace
        monkeypatch.setattr(copy_module, '_region_from_table_ref',
                            lambda ref: 'ap-south-1' if ref.startswith('arn:') else None)

        scan_resp = {'Items': [], 'LastEvaluatedKey': None}
        session.resource.return_value.Table.return_value.scan = MagicMock(return_value=scan_resp)
        session.resource.return_value.Table.return_value.batch_writer.return_value.__enter__ = \
            MagicMock(return_value=MagicMock())
        session.resource.return_value.Table.return_value.batch_writer.return_value.__exit__ = \
            MagicMock(return_value=False)

        copy_module._copy_data('arn:aws:dynamodb:ap-south-1:1:table/X', 'plain', {}, {}, 0, 1,
                                MagicMock(), MagicMock(), MagicMock(), MagicMock())

        regions_used = [c.kwargs['region_name'] for c in session.resource.call_args_list]
        assert 'ap-south-1' in regions_used, "ARN region overrode session region for source"
        assert 'us-east-1' in regions_used, "plain target name still used session region"


class TestCopyDataPagination:
    """Scan results paginate via LastEvaluatedKey — the worker must thread
    that key into ExclusiveStartKey on the next scan and stop when absent."""

    def _build_paginated_scan(self, pages):
        """Return a side_effect function returning each page's response in turn."""
        responses = iter(pages)
        seen_kwargs = []

        def scan(**kwargs):
            seen_kwargs.append(dict(kwargs))
            return next(responses)
        return scan, seen_kwargs

    def test_single_page_no_pagination(self, monkeypatch):
        rl_instance = MagicMock()
        session = MagicMock(region_name='us-east-1')
        rl_instance.get_session.return_value = session
        monkeypatch.setattr(copy_module, 'RateLimiterWorker', MagicMock(return_value=rl_instance))
        monkeypatch.setattr(copy_module, '_region_from_table_ref', lambda ref: None)

        # First scan response: items present, no LEK -> one iteration
        scan_fn, seen = self._build_paginated_scan([
            {'Items': [{'id': 1}, {'id': 2}], 'LastEvaluatedKey': None},
        ])
        table = MagicMock()
        table.scan = scan_fn
        bw = MagicMock()
        bw.__enter__ = MagicMock(return_value=bw)
        bw.__exit__ = MagicMock(return_value=False)
        table.batch_writer.return_value = bw
        session.resource.return_value.Table.return_value = table

        total_acc = MagicMock()
        copy_module._copy_data('s', 't', {}, {}, 5, 10,
                                total_acc, MagicMock(), MagicMock(), MagicMock())

        assert len(seen) == 1, "one scan call when no LEK returned"
        assert seen[0]['Segment'] == 5, "worker passes its segment id"
        assert seen[0]['TotalSegments'] == 10
        assert 'ExclusiveStartKey' not in seen[0], "first scan has no ESK"
        total_acc.add.assert_called_once_with(2), "2 items copied this worker"

    def test_multi_page_pagination_threads_lek(self, monkeypatch):
        rl_instance = MagicMock()
        session = MagicMock(region_name='us-east-1')
        rl_instance.get_session.return_value = session
        monkeypatch.setattr(copy_module, 'RateLimiterWorker', MagicMock(return_value=rl_instance))
        monkeypatch.setattr(copy_module, '_region_from_table_ref', lambda ref: None)

        # Three pages: LEK, LEK, then no LEK -> three scans
        scan_fn, seen = self._build_paginated_scan([
            {'Items': [{'a': 1}], 'LastEvaluatedKey': {'pk': 'p1'}},
            {'Items': [{'a': 2}, {'a': 3}], 'LastEvaluatedKey': {'pk': 'p2'}},
            {'Items': [{'a': 4}], 'LastEvaluatedKey': None},
        ])
        table = MagicMock()
        table.scan = scan_fn
        bw = MagicMock()
        bw.__enter__ = MagicMock(return_value=bw)
        bw.__exit__ = MagicMock(return_value=False)
        table.batch_writer.return_value = bw
        session.resource.return_value.Table.return_value = table

        total_acc = MagicMock()
        copy_module._copy_data('s', 't', {}, {}, 0, 1,
                                total_acc, MagicMock(), MagicMock(), MagicMock())

        assert len(seen) == 3, "three scan calls until LEK absent"
        assert 'ExclusiveStartKey' not in seen[0]
        assert seen[1]['ExclusiveStartKey'] == {'pk': 'p1'}, "second scan uses page-1 LEK"
        assert seen[2]['ExclusiveStartKey'] == {'pk': 'p2'}, "third scan uses page-2 LEK"
        total_acc.add.assert_called_once_with(4), "1+2+1 items copied"

    def test_each_item_put_via_batch_writer(self, monkeypatch):
        rl_instance = MagicMock()
        session = MagicMock(region_name='us-east-1')
        rl_instance.get_session.return_value = session
        monkeypatch.setattr(copy_module, 'RateLimiterWorker', MagicMock(return_value=rl_instance))
        monkeypatch.setattr(copy_module, '_region_from_table_ref', lambda ref: None)

        scan_fn, _ = self._build_paginated_scan([
            {'Items': [{'a': 1}, {'b': 2}, {'c': 3}], 'LastEvaluatedKey': None},
        ])
        # Source and target tables are different MagicMocks; we want to inspect target.batch_writer
        target_bw = MagicMock()
        target_bw.__enter__ = MagicMock(return_value=target_bw)
        target_bw.__exit__ = MagicMock(return_value=False)

        def make_table(name):
            tbl = MagicMock()
            tbl.scan = scan_fn
            tbl.batch_writer.return_value = target_bw
            return tbl
        # Both ddb resources point at the same Table mock factory
        session.resource.return_value.Table.side_effect = lambda n: make_table(n)

        copy_module._copy_data('s', 't', {}, {}, 0, 1,
                                MagicMock(), MagicMock(), MagicMock(), MagicMock())

        # Three put_item calls expected
        put_calls = target_bw.put_item.call_args_list
        assert len(put_calls) == 3
        items_written = [c.kwargs['Item'] for c in put_calls]
        assert {'a': 1} in items_written
        assert {'b': 2} in items_written
        assert {'c': 3} in items_written


class TestCopyDataErrorPath:
    """When the scan/write loop raises, the error is captured into the
    accumulator (not bubbled), and rate-limiters still get shut down."""

    def test_scan_error_appended_to_error_accumulator(self, monkeypatch):
        rl_instance = MagicMock()
        session = MagicMock(region_name='us-east-1')
        rl_instance.get_session.return_value = session
        monkeypatch.setattr(copy_module, 'RateLimiterWorker', MagicMock(return_value=rl_instance))
        monkeypatch.setattr(copy_module, '_region_from_table_ref', lambda ref: None)
        monkeypatch.setattr(copy_module, 'get_error_message', lambda e: f"wrapped:{e}")

        table = MagicMock()
        table.scan = MagicMock(side_effect=RuntimeError('throttled'))
        bw = MagicMock()
        bw.__enter__ = MagicMock(return_value=bw)
        bw.__exit__ = MagicMock(return_value=False)
        table.batch_writer.return_value = bw
        session.resource.return_value.Table.return_value = table

        error_acc = MagicMock()
        # Must NOT raise — error path swallows into accumulator
        copy_module._copy_data('s', 't', {}, {}, 7, 10,
                                MagicMock(), error_acc, MagicMock(), MagicMock())

        error_acc.add.assert_called_once()
        appended = error_acc.add.call_args.args[0]
        assert isinstance(appended, list) and len(appended) == 1, \
            "errors are wrapped in a single-element list for ListAccumulator"
        assert 'worker 7' in appended[0], "error message identifies the worker segment"
        assert 'wrapped:' in appended[0], "get_error_message wraps the underlying exception"

    def test_rate_limiters_shutdown_in_finally_after_scan_error(self, monkeypatch):
        instances = []

        def rl_factory(**kw):
            inst = MagicMock()
            inst.get_session.return_value = MagicMock(region_name='us-east-1', resource=MagicMock())
            inst.get_session.return_value.resource.return_value.Table.return_value.scan = \
                MagicMock(side_effect=RuntimeError('boom'))
            bw = MagicMock()
            bw.__enter__ = MagicMock(return_value=bw)
            bw.__exit__ = MagicMock(return_value=False)
            inst.get_session.return_value.resource.return_value.Table.return_value.batch_writer.return_value = bw
            instances.append(inst)
            return inst

        monkeypatch.setattr(copy_module, 'RateLimiterWorker', rl_factory)
        monkeypatch.setattr(copy_module, '_region_from_table_ref', lambda ref: None)
        monkeypatch.setattr(copy_module, 'get_error_message', lambda e: str(e))

        copy_module._copy_data('s', 't', {}, {}, 0, 1,
                                MagicMock(), MagicMock(), MagicMock(), MagicMock())

        assert len(instances) == 2, "two RateLimiterWorker instances (source + target)"
        for inst in instances:
            inst.shutdown.assert_called_once(), \
                "shutdown is in the finally block — must fire even on scan error"


class TestCopyDataWorkerRates:
    """Per-worker rate limits — copy uses elevated rates compared to defaults."""

    def test_source_worker_max_read_rate_is_2500(self, monkeypatch):
        rl_class = MagicMock()
        rl_instance = MagicMock()
        rl_instance.get_session.return_value = MagicMock(
            region_name='us-east-1',
            resource=MagicMock(),
        )
        scan_resp = {'Items': [], 'LastEvaluatedKey': None}
        rl_instance.get_session.return_value.resource.return_value.Table.return_value.scan = \
            MagicMock(return_value=scan_resp)
        bw = MagicMock()
        bw.__enter__ = MagicMock(return_value=bw)
        bw.__exit__ = MagicMock(return_value=False)
        rl_instance.get_session.return_value.resource.return_value.Table.return_value.batch_writer.return_value = bw
        rl_class.return_value = rl_instance
        monkeypatch.setattr(copy_module, 'RateLimiterWorker', rl_class)
        monkeypatch.setattr(copy_module, '_region_from_table_ref', lambda ref: None)

        copy_module._copy_data('s', 't', {'mon': 1}, {'mon': 2}, 0, 1,
                                MagicMock(), MagicMock(), MagicMock(), MagicMock())

        first_kwargs = rl_class.call_args_list[0].kwargs
        assert first_kwargs.get('worker_max_read_rate') == 2500, \
            "source worker reads at 2500 RCU/s — copy's elevated rate"

    def test_target_worker_max_write_rate_is_800(self, monkeypatch):
        rl_class = MagicMock()
        rl_instance = MagicMock()
        rl_instance.get_session.return_value = MagicMock(
            region_name='us-east-1',
            resource=MagicMock(),
        )
        scan_resp = {'Items': [], 'LastEvaluatedKey': None}
        rl_instance.get_session.return_value.resource.return_value.Table.return_value.scan = \
            MagicMock(return_value=scan_resp)
        bw = MagicMock()
        bw.__enter__ = MagicMock(return_value=bw)
        bw.__exit__ = MagicMock(return_value=False)
        rl_instance.get_session.return_value.resource.return_value.Table.return_value.batch_writer.return_value = bw
        rl_class.return_value = rl_instance
        monkeypatch.setattr(copy_module, 'RateLimiterWorker', rl_class)
        monkeypatch.setattr(copy_module, '_region_from_table_ref', lambda ref: None)

        copy_module._copy_data('s', 't', {'mon': 1}, {'mon': 2}, 0, 1,
                                MagicMock(), MagicMock(), MagicMock(), MagicMock())

        second_kwargs = rl_class.call_args_list[1].kwargs
        assert second_kwargs.get('worker_max_write_rate') == 800, \
            "target worker writes at 800 WCU/s — copy's elevated rate"

    def test_monitor_options_passed_through_to_workers(self, monkeypatch):
        rl_class = MagicMock()
        rl_instance = MagicMock()
        rl_instance.get_session.return_value = MagicMock(
            region_name='us-east-1', resource=MagicMock())
        scan_resp = {'Items': [], 'LastEvaluatedKey': None}
        rl_instance.get_session.return_value.resource.return_value.Table.return_value.scan = \
            MagicMock(return_value=scan_resp)
        bw = MagicMock()
        bw.__enter__ = MagicMock(return_value=bw)
        bw.__exit__ = MagicMock(return_value=False)
        rl_instance.get_session.return_value.resource.return_value.Table.return_value.batch_writer.return_value = bw
        rl_class.return_value = rl_instance
        monkeypatch.setattr(copy_module, 'RateLimiterWorker', rl_class)
        monkeypatch.setattr(copy_module, '_region_from_table_ref', lambda ref: None)

        source_mon = {'monitor_table': 'src', 'extra': 'a'}
        target_mon = {'monitor_table': 'dst', 'extra': 'b'}
        copy_module._copy_data('s', 't', source_mon, target_mon, 0, 1,
                                MagicMock(), MagicMock(), MagicMock(), MagicMock())

        first_kwargs = rl_class.call_args_list[0].kwargs
        assert first_kwargs['monitor_table'] == 'src', "source monitor opts splatted into worker"
        assert first_kwargs['extra'] == 'a'
        second_kwargs = rl_class.call_args_list[1].kwargs
        assert second_kwargs['monitor_table'] == 'dst'
        assert second_kwargs['extra'] == 'b'


class TestCopyDataReturn:
    """The worker returns its local count for callers/tests that need it."""

    def test_returns_local_count_after_pagination(self, monkeypatch):
        rl_instance = MagicMock()
        session = MagicMock(region_name='us-east-1')
        rl_instance.get_session.return_value = session
        monkeypatch.setattr(copy_module, 'RateLimiterWorker', MagicMock(return_value=rl_instance))
        monkeypatch.setattr(copy_module, '_region_from_table_ref', lambda ref: None)

        scan_responses = iter([
            {'Items': [{'a': 1}, {'b': 2}], 'LastEvaluatedKey': {'pk': 'p1'}},
            {'Items': [{'c': 3}], 'LastEvaluatedKey': None},
        ])
        table = MagicMock()
        table.scan = MagicMock(side_effect=lambda **kw: next(scan_responses))
        bw = MagicMock()
        bw.__enter__ = MagicMock(return_value=bw)
        bw.__exit__ = MagicMock(return_value=False)
        table.batch_writer.return_value = bw
        session.resource.return_value.Table.return_value = table

        result = copy_module._copy_data('s', 't', {}, {}, 0, 1,
                                         MagicMock(), MagicMock(), MagicMock(), MagicMock())
        assert result == 3, "returns local_count (sum of items copied across pages)"

    def test_returns_zero_after_scan_error(self, monkeypatch):
        rl_instance = MagicMock()
        session = MagicMock(region_name='us-east-1')
        rl_instance.get_session.return_value = session
        monkeypatch.setattr(copy_module, 'RateLimiterWorker', MagicMock(return_value=rl_instance))
        monkeypatch.setattr(copy_module, '_region_from_table_ref', lambda ref: None)
        monkeypatch.setattr(copy_module, 'get_error_message', lambda e: str(e))

        table = MagicMock()
        table.scan = MagicMock(side_effect=RuntimeError('throttle'))
        bw = MagicMock()
        bw.__enter__ = MagicMock(return_value=bw)
        bw.__exit__ = MagicMock(return_value=False)
        table.batch_writer.return_value = bw
        session.resource.return_value.Table.return_value = table

        result = copy_module._copy_data('s', 't', {}, {}, 0, 1,
                                         MagicMock(), MagicMock(), MagicMock(), MagicMock())
        # local_count never incremented because scan blew up before any items
        assert result == 0, "no items copied means local_count stays 0 even on error"
