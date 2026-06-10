"""Unit tests for the `find` server-side verb.

Covers `python_modules/find.py`:
- print_dynamodb_table_info: generator that prints table info, optionally
  computes delete costs (PROVISIONED vs PAY_PER_REQUEST billing modes)
- run(): argument wiring, simple count (direct via count_dynamodb_table),
  DataFrame path (via read_dynamodb_dataframe) with WHERE/ORDERBY/LIMIT,
  parse_sort_order (inner fn): asc/desc/default/multi-column/empty-spec,
  DO_FIND branch (S3 write, top-N printing, count <= TOP_N vs > TOP_N),
  DO_DELETE branch (repartitioning, delete_partition inner fn, error
  handling, rate-limiter shutdown), unknown action ValueError

MIGRATION NOTE (Glue 4.0 DynamicFrame -> Glue 5.0 DataFrame connector):
The verb no longer calls glue_context.create_dynamic_frame.from_options(...)
.toDF(). The simple-count shortcut now calls
``count_dynamodb_table(glue_context, table, parsed_args, splits=...)`` and the
DataFrame path calls ``read_dynamodb_dataframe(glue_context, table,
parsed_args, splits=...)`` -- both imported into find's namespace from
python_modules.shared.glue_connector. The KEY MOCK SEAM is therefore those two
names on ``find_module``; we patch them with MagicMocks returning a DataFrame
mock whose chainable transforms (.filter/.orderBy/.limit/.cache/.select/
.repartition) return the same mock.

Connector READ-option details (table name, splits, consistentRead,
throughput) now live INSIDE the wrapper and are covered by
tests/server/test_glue_connector.py (TestReadDataFrame / TestCountDynamoDBTable).
The legacy tests that inspected from_options connection_options for those
details have been deleted here with a note, since asserting them at the verb
boundary would only re-test the wrapper.

The existing tests/server/conftest.py mocks awsglue, pyspark, and
shared modules at all resolution paths. These tests build on that.
"""

import json
import sys
from unittest.mock import MagicMock, call, patch

import pytest

# find.py imports these modules not covered by conftest
sys.modules.setdefault('awsglue.transforms', MagicMock())
_pyspark_sql = MagicMock()
sys.modules.setdefault('pyspark.sql', _pyspark_sql)
_pyspark_sql_functions = MagicMock()
sys.modules.setdefault('pyspark.sql.functions', _pyspark_sql_functions)

from python_modules import find as find_module

# Star-imported from shared.errors — Mock doesn't populate __all__ so we inject it
if not hasattr(find_module, 'get_error_message'):
    find_module.get_error_message = lambda e: str(e)


# --- Helpers ----------------------------------------------------------------

def _make_df_mock(count=5):
    """A DataFrame mock whose chainable transforms return itself.

    Mirrors the shape read_dynamodb_dataframe returns: .filter/.orderBy/
    .limit/.cache/.select/.repartition are chainable (return the same df),
    while .count/.toJSON produce terminal values.
    """
    df = MagicMock()
    df.filter = MagicMock(return_value=df)
    df.orderBy = MagicMock(return_value=df)
    df.limit = MagicMock(return_value=df)
    df.select = MagicMock(return_value=df)
    df.repartition = MagicMock(return_value=df)
    df.cache = MagicMock(return_value=df)
    df.count = MagicMock(return_value=count)
    df.toJSON = MagicMock(return_value=MagicMock())
    return df


# --- Fixtures ---------------------------------------------------------------

@pytest.fixture
def table_info_mocks(monkeypatch):
    """Replace shared.table_info helpers in find's namespace."""
    mocks = MagicMock()
    mocks.get_and_print_dynamodb_table_info = MagicMock(return_value={
        'item_count': 1000,
        'size_bytes': 50000,
        'region_name': 'us-east-1',
        'billing_mode': 'PAY_PER_REQUEST',
        'write_pricing_category': 'WriteRequestUnits',
    })
    mocks.get_and_print_table_scan_cost = MagicMock(return_value=0.50)
    mocks.get_dynamodb_throughput_configs = MagicMock(return_value={'throughput': 'val'})

    monkeypatch.setattr(find_module, 'get_and_print_dynamodb_table_info',
                        mocks.get_and_print_dynamodb_table_info)
    monkeypatch.setattr(find_module, 'get_and_print_table_scan_cost',
                        mocks.get_and_print_table_scan_cost)
    monkeypatch.setattr(find_module, 'get_dynamodb_throughput_configs',
                        mocks.get_dynamodb_throughput_configs)
    return mocks


@pytest.fixture
def pricing_mock(monkeypatch):
    """Mock PricingUtility used in delete cost estimation."""
    pricing_instance = MagicMock()
    pricing_instance.get_on_demand_capacity_pricing = MagicMock(
        return_value={'WriteRequestUnits': '0.000001'}
    )
    pricing_cls = MagicMock(return_value=pricing_instance)
    monkeypatch.setattr(find_module, 'PricingUtility', pricing_cls)
    return pricing_cls


@pytest.fixture
def boto3_session_mock(monkeypatch):
    """Mock boto3.Session() used for region_name in print_dynamodb_table_info."""
    session = MagicMock()
    session.region_name = 'us-west-2'
    session_cls = MagicMock(return_value=session)
    monkeypatch.setattr(find_module, 'boto3', MagicMock(Session=session_cls, client=MagicMock()))
    return session_cls


@pytest.fixture
def df_mock():
    """The DataFrame mock that read_dynamodb_dataframe returns."""
    return _make_df_mock(count=5)


@pytest.fixture
def connector_mocks(monkeypatch, df_mock):
    """Patch the wrapper functions in find's namespace (the migration seam).

    Returns a MagicMock holding ``read_dynamodb_dataframe`` (returns df_mock),
    ``count_dynamodb_table`` (returns 42), and the ``df`` itself for assertions.
    """
    mocks = MagicMock()
    mocks.df = df_mock
    mocks.read_dynamodb_dataframe = MagicMock(return_value=df_mock)
    mocks.count_dynamodb_table = MagicMock(return_value=42)
    monkeypatch.setattr(find_module, 'read_dynamodb_dataframe',
                        mocks.read_dynamodb_dataframe)
    monkeypatch.setattr(find_module, 'count_dynamodb_table',
                        mocks.count_dynamodb_table)
    return mocks


@pytest.fixture
def base_args():
    """Minimal parsed_args for a find action."""
    return {
        'splits': '100',
        'table': 'test-table',
        'where': None,
        'orderby': None,
        'limit': None,
        'XAction': 'find',
        's3-bucket-name': 'my-bucket',
        'JOB_RUN_ID': 'run-123',
    }


def _delete_env(monkeypatch, table_info_mocks, key_schema=None):
    """Wire boto3 (region + describe_table), PricingUtility, rate limiters,
    and PAY_PER_REQUEST table info for the DO_DELETE path."""
    if key_schema is None:
        key_schema = [{'AttributeName': 'pk', 'KeyType': 'HASH'}]
    monkeypatch.setattr(find_module, 'RateLimiterSharedConfig', MagicMock())
    agg_instance = MagicMock()
    monkeypatch.setattr(find_module, 'RateLimiterAggregator',
                        MagicMock(return_value=agg_instance))
    monkeypatch.setattr(find_module, 'RateLimiterWorker', MagicMock())

    client_mock = MagicMock()
    client_mock.describe_table.return_value = {
        'Table': {'KeySchema': key_schema}
    }
    boto3_mock = MagicMock(
        Session=MagicMock(return_value=MagicMock(region_name='us-east-1')),
        client=MagicMock(return_value=client_mock),
    )
    monkeypatch.setattr(find_module, 'boto3', boto3_mock)

    pricing_instance = MagicMock()
    pricing_instance.get_on_demand_capacity_pricing.return_value = {'WriteRequestUnits': '0.001'}
    monkeypatch.setattr(find_module, 'PricingUtility', MagicMock(return_value=pricing_instance))
    table_info_mocks.get_and_print_dynamodb_table_info.return_value = {
        'item_count': 100, 'size_bytes': 5000,
        'billing_mode': 'PAY_PER_REQUEST',
        'write_pricing_category': 'WriteRequestUnits',
    }
    return client_mock, agg_instance


# --- print_dynamodb_table_info -----------------------------------------------

# These tests exercise find.py's OWN generator/cost-math logic and never
# touched the DynamicFrame code path. The skip was over-broad; removed.
class TestPrintDynamodbTableInfo:
    """Generator that prints table info and optionally computes delete costs."""

    def test_non_delete_yields_table_info_and_completes(
        self, table_info_mocks, boto3_session_mock
    ):
        """Non-delete path calls info/cost helpers then yields."""
        gen = find_module.print_dynamodb_table_info('my-table', False)
        result = next(gen)

        table_info_mocks.get_and_print_dynamodb_table_info.assert_called_once_with('my-table')
        table_info_mocks.get_and_print_table_scan_cost.assert_called_once()
        assert result is None or result == table_info_mocks.get_and_print_dynamodb_table_info.return_value

    def test_non_delete_second_next_returns_stop_iteration(
        self, table_info_mocks, boto3_session_mock
    ):
        """Final yield prevents StopIteration on second next()."""
        gen = find_module.print_dynamodb_table_info('t', False)
        next(gen)
        # Second next should hit the final yield, not raise prematurely
        try:
            next(gen)
        except StopIteration:
            pass  # Expected after the final yield is consumed

    def test_kwargs_passed_through_to_scan_cost(
        self, table_info_mocks, boto3_session_mock
    ):
        """kwargs forwarded to get_and_print_table_scan_cost."""
        gen = find_module.print_dynamodb_table_info('t', False, fraction=0.5)
        next(gen)

        call_kwargs = table_info_mocks.get_and_print_table_scan_cost.call_args
        assert call_kwargs.kwargs.get('fraction') == 0.5, \
            "fraction kwarg passed through"

    def test_delete_provisioned_prints_provisioned_cost(
        self, table_info_mocks, boto3_session_mock, pricing_mock, capsys
    ):
        """is_delete=True with PROVISIONED billing prints provisioned cost."""
        table_info_mocks.get_and_print_dynamodb_table_info.return_value = {
            'item_count': 1000,
            'size_bytes': 50000,
            'billing_mode': 'PROVISIONED',
            'write_pricing_category': 'WriteRequestUnits',
        }
        gen = find_module.print_dynamodb_table_info('t', True)
        next(gen)
        gen.send(500)

        out = capsys.readouterr().out
        assert 'Provisioned' in out or 'provisioned' in out, \
            "PROVISIONED billing mode triggers provisioned cost line"
        assert '500' in out, "delete_count appears in output"

    def test_delete_pay_per_request_prints_ondemand_cost(
        self, table_info_mocks, boto3_session_mock, pricing_mock, capsys
    ):
        """is_delete=True with PAY_PER_REQUEST prints on-demand cost."""
        table_info_mocks.get_and_print_dynamodb_table_info.return_value = {
            'item_count': 1000,
            'size_bytes': 50000,
            'billing_mode': 'PAY_PER_REQUEST',
            'write_pricing_category': 'WriteRequestUnits',
        }
        gen = find_module.print_dynamodb_table_info('t', True)
        next(gen)
        gen.send(200)

        out = capsys.readouterr().out
        assert 'On-demand' in out or 'on-demand' in out or 'On-Demand' in out, \
            "PAY_PER_REQUEST billing triggers on-demand cost line"

    def test_delete_cost_math(
        self, table_info_mocks, boto3_session_mock, pricing_mock, capsys
    ):
        """avg_size, write_units, cost computation."""
        table_info_mocks.get_and_print_dynamodb_table_info.return_value = {
            'item_count': 99,  # +1 in denominator = 100
            'size_bytes': 10000,  # avg_size = ceil(10000/100) = 100
            'billing_mode': 'PAY_PER_REQUEST',
            'write_pricing_category': 'WriteRequestUnits',
        }
        gen = find_module.print_dynamodb_table_info('t', True)
        next(gen)
        gen.send(10)  # delete_count = 10

        out = capsys.readouterr().out
        # avg_size = ceil(10000/100) = 100, avg_write_units = ceil(100/1024) = 1
        # write_units = 10 * 1 = 10
        assert '10' in out, "write units or delete count in output"

    def test_delete_avoids_division_by_zero(
        self, table_info_mocks, boto3_session_mock, pricing_mock, capsys
    ):
        """item_count=0 uses +1 to avoid division by zero."""
        table_info_mocks.get_and_print_dynamodb_table_info.return_value = {
            'item_count': 0,
            'size_bytes': 1024,
            'billing_mode': 'PAY_PER_REQUEST',
            'write_pricing_category': 'WriteRequestUnits',
        }
        gen = find_module.print_dynamodb_table_info('t', True)
        next(gen)
        # Should not raise ZeroDivisionError
        gen.send(5)

    def test_delete_unknown_billing_mode_prints_neither_cost_line(
        self, table_info_mocks, boto3_session_mock, pricing_mock, capsys
    ):
        """billing_mode not PROVISIONED or PAY_PER_REQUEST skips both cost prints."""
        table_info_mocks.get_and_print_dynamodb_table_info.return_value = {
            'item_count': 100,
            'size_bytes': 5000,
            'billing_mode': 'UNKNOWN_MODE',
            'write_pricing_category': 'WriteRequestUnits',
        }
        gen = find_module.print_dynamodb_table_info('t', True)
        next(gen)
        gen.send(10)

        out = capsys.readouterr().out
        assert 'Approx DynamoDB cost for provisioned' not in out
        assert 'Approx DynamoDB cost for On-demand' not in out
        assert 'Write units required' in out, "common print still appears"


# --- run(): Simple count (no DataFrame conversion) ----------------------------

class TestRunSimpleCount:
    """The fast path: DO_COUNT with no WHERE/ORDERBY/LIMIT calls
    count_dynamodb_table directly, avoiding the DataFrame read."""

    def test_simple_count_prints_connector_count(
        self, monkeypatch, table_info_mocks, boto3_session_mock, connector_mocks, capsys
    ):
        """Simple count path prints count_dynamodb_table's result."""
        connector_mocks.count_dynamodb_table.return_value = 42
        args = {
            'splits': '200', 'table': 'my-table',
            'where': None, 'orderby': None, 'limit': None,
            'XAction': 'count',
        }
        find_module.run(MagicMock(), MagicMock(), MagicMock(), args)
        out = capsys.readouterr().out
        assert '42' in out, "count_dynamodb_table result (42) printed directly"
        # Fast path must NOT read the full DataFrame
        connector_mocks.count_dynamodb_table.assert_called_once()
        connector_mocks.read_dynamodb_dataframe.assert_not_called()

    def test_simple_count_passes_table_and_splits_to_connector(
        self, monkeypatch, table_info_mocks, boto3_session_mock, connector_mocks
    ):
        """count_dynamodb_table receives the table name and splits the verb resolved.

        (The connector-internal read options live in test_glue_connector.py;
        here we only verify the verb hands the right args to the wrapper.)"""
        args = {
            'splits': '150', 'table': 'my-table',
            'where': None, 'orderby': None, 'limit': None,
            'XAction': 'count',
        }
        find_module.run(MagicMock(), MagicMock(), MagicMock(), args)

        c = connector_mocks.count_dynamodb_table.call_args
        # positional: (glue_context, table, parsed_args); kw: splits
        assert c.args[1] == 'my-table'
        assert c.kwargs.get('splits') == '150'

    def test_simple_count_does_not_set_numberOfScans(
        self, monkeypatch, table_info_mocks, boto3_session_mock, connector_mocks
    ):
        """Simple count skips the numberOfScans=2 kwarg."""
        args = {
            'splits': '200', 'table': 'my-table',
            'where': None, 'orderby': None, 'limit': None,
            'XAction': 'count',
        }
        find_module.run(MagicMock(), MagicMock(), MagicMock(), args)

        scan_cost_call = table_info_mocks.get_and_print_table_scan_cost.call_args
        # Should NOT have numberOfScans in kwargs
        if scan_cost_call.kwargs:
            assert 'numberOfScans' not in scan_cost_call.kwargs

    def test_count_with_where_uses_dataframe(
        self, monkeypatch, table_info_mocks, boto3_session_mock, connector_mocks, capsys
    ):
        """count + WHERE forces the DataFrame path (read_dynamodb_dataframe)."""
        args = {
            'splits': '200', 'table': 'my-table',
            'where': 'attr > 5', 'orderby': None, 'limit': None,
            'XAction': 'count',
        }
        connector_mocks.df.count.return_value = 7

        find_module.run(MagicMock(), MagicMock(), MagicMock(), args)
        out = capsys.readouterr().out
        assert '7' in out, "DataFrame count used when WHERE present"
        connector_mocks.read_dynamodb_dataframe.assert_called_once()
        connector_mocks.count_dynamodb_table.assert_not_called()
        connector_mocks.df.filter.assert_called_once_with('attr > 5')

    def test_count_with_where_does_not_set_numberOfScans(
        self, monkeypatch, table_info_mocks, boto3_session_mock, connector_mocks
    ):
        """DataFrame connector reads once; no double-scan pricing multiplier."""
        args = {
            'splits': '200', 'table': 'my-table',
            'where': 'x = 1', 'orderby': None, 'limit': None,
            'XAction': 'count',
        }
        find_module.run(MagicMock(), MagicMock(), MagicMock(), args)

        scan_cost_call = table_info_mocks.get_and_print_table_scan_cost.call_args
        assert 'numberOfScans' not in (scan_cost_call.kwargs or {})


# --- run(): Connector argument wiring -----------------------------------------

class TestRunConnectorArgs:
    """How the verb hands table/splits to the connector wrapper.

    DELETED-WITH-WHY: the legacy TestRunConnectionOptions asserted the connector
    READ options (dynamodb.input.tableName, dynamodb.splits,
    dynamodb.consistentRead='false', and the merged throughput configs) by
    inspecting glue_context.create_dynamic_frame.from_options(...).call_args.
    After the Glue 5.0 migration those option strings are built INSIDE
    read_dynamodb_dataframe / count_dynamodb_table and are covered by
    tests/server/test_glue_connector.py (TestReadDataFrame.test_options_set_on_reader,
    test_xmax_read_rate_passes_through_as_direct_int, TestCountDynamoDBTable).
    Re-asserting them at the verb boundary would only duplicate the wrapper's
    own tests, so those three test methods were removed. What remains here is
    the verb-owned contract: it passes the resolved table name and splits to
    the wrapper."""

    def test_read_dataframe_receives_table_and_splits(
        self, monkeypatch, table_info_mocks, boto3_session_mock, connector_mocks
    ):
        """DataFrame path passes table name + splits through to the wrapper."""
        args = {
            'splits': '150', 'table': 'conn-table',
            'where': 'x = 1', 'orderby': None, 'limit': None,
            'XAction': 'count',
        }
        find_module.run(MagicMock(), MagicMock(), MagicMock(), args)

        c = connector_mocks.read_dynamodb_dataframe.call_args
        assert c.args[1] == 'conn-table'
        assert c.kwargs.get('splits') == '150'

    def test_parsed_args_forwarded_to_wrapper(
        self, monkeypatch, table_info_mocks, boto3_session_mock, connector_mocks
    ):
        """The full parsed_args dict is forwarded so the wrapper can read
        XMaxReadRate etc. (throughput handling now lives in the wrapper)."""
        args = {
            'splits': '200', 'table': 't', 'XMaxReadRate': 9999,
            'where': 'x = 1', 'orderby': None, 'limit': None,
            'XAction': 'count',
        }
        find_module.run(MagicMock(), MagicMock(), MagicMock(), args)

        c = connector_mocks.read_dynamodb_dataframe.call_args
        assert c.args[2] is args, "parsed_args passed through unchanged"
        assert c.args[2].get('XMaxReadRate') == 9999


# --- run(): parse_sort_order (inner function) ---------------------------------

class TestParseSortOrder:
    """The inner parse_sort_order converts 'col asc, col2 desc' into pyspark
    sort directives. Tested via run() since it's not module-level. This is
    find.py's OWN logic and is preserved as passing tests."""

    def test_single_column_asc(
        self, monkeypatch, table_info_mocks, boto3_session_mock, connector_mocks
    ):
        """Explicit 'asc' produces one sort directive passed to orderBy."""
        args = {
            'splits': '200', 'table': 't',
            'where': None, 'orderby': 'name asc', 'limit': None,
            'XAction': 'count',
        }
        find_module.run(MagicMock(), MagicMock(), MagicMock(), args)

        connector_mocks.df.orderBy.assert_called_once()
        sort_list = connector_mocks.df.orderBy.call_args.args[0]
        assert len(sort_list) == 1

    def test_single_column_desc(
        self, monkeypatch, table_info_mocks, boto3_session_mock, connector_mocks
    ):
        """Explicit 'desc' produces one sort directive."""
        args = {
            'splits': '200', 'table': 't',
            'where': None, 'orderby': 'age desc', 'limit': None,
            'XAction': 'count',
        }
        find_module.run(MagicMock(), MagicMock(), MagicMock(), args)

        connector_mocks.df.orderBy.assert_called_once()

    def test_default_sort_is_asc(
        self, monkeypatch, table_info_mocks, boto3_session_mock, connector_mocks
    ):
        """No direction specified defaults to 'asc'."""
        args = {
            'splits': '200', 'table': 't',
            'where': None, 'orderby': 'score', 'limit': None,
            'XAction': 'count',
        }
        find_module.run(MagicMock(), MagicMock(), MagicMock(), args)

        connector_mocks.df.orderBy.assert_called_once()

    def test_multiple_columns(
        self, monkeypatch, table_info_mocks, boto3_session_mock, connector_mocks
    ):
        """Comma-separated specs produce multiple sort directives."""
        args = {
            'splits': '200', 'table': 't',
            'where': None, 'orderby': 'a asc, b desc, c', 'limit': None,
            'XAction': 'count',
        }
        find_module.run(MagicMock(), MagicMock(), MagicMock(), args)

        sort_list = connector_mocks.df.orderBy.call_args.args[0]
        assert len(sort_list) == 3, "three sort specs parsed"

    def test_empty_spec_in_orderby_raises_value_error(
        self, monkeypatch, table_info_mocks, boto3_session_mock, connector_mocks
    ):
        """Empty spec (e.g. trailing comma) fails regex → ValueError."""
        args = {
            'splits': '200', 'table': 't',
            'where': None, 'orderby': 'col asc,', 'limit': None,
            'XAction': 'count',
        }
        with pytest.raises(ValueError, match="Invalid sort specification"):
            find_module.run(MagicMock(), MagicMock(), MagicMock(), args)

    def test_orderby_sets_needsRepartitioning(
        self, monkeypatch, table_info_mocks, connector_mocks
    ):
        """orderBy sets needsRepartitioning=True (observable in delete path)."""
        args = {
            'splits': '200', 'table': 't',
            'where': None, 'orderby': 'x asc', 'limit': None,
            'XAction': 'delete',
            's3-bucket-name': 'b', 'JOB_RUN_ID': 'j',
        }
        _delete_env(monkeypatch, table_info_mocks)
        connector_mocks.df.count.return_value = 3
        connector_mocks.df.toJSON.return_value.foreachPartition = MagicMock()

        find_module.run(MagicMock(), MagicMock(), MagicMock(), args)

        connector_mocks.df.select.assert_called(), "repartitioning selects keys"
        connector_mocks.df.repartition.assert_called_with(200)


# --- run(): Error paths -------------------------------------------------------

class TestRunErrorPaths:
    """WHERE, ORDERBY, LIMIT each wrap exceptions with get_error_message.
    This is find.py's OWN logic — preserved as passing tests."""

    def test_invalid_where_raises_with_message(
        self, monkeypatch, table_info_mocks, boto3_session_mock, connector_mocks
    ):
        """filter exception wrapped in 'Invalid where'."""
        monkeypatch.setattr(find_module, 'get_error_message', lambda e: f"msg:{e}")
        connector_mocks.df.filter.side_effect = RuntimeError('bad filter')

        args = {
            'splits': '200', 'table': 't',
            'where': 'broken', 'orderby': None, 'limit': None,
            'XAction': 'count',
        }
        with pytest.raises(Exception, match="Invalid 'where'"):
            find_module.run(MagicMock(), MagicMock(), MagicMock(), args)

    def test_invalid_orderby_raises_with_message(
        self, monkeypatch, table_info_mocks, boto3_session_mock, connector_mocks
    ):
        """orderBy exception wrapped in 'Invalid orderby'."""
        monkeypatch.setattr(find_module, 'get_error_message', lambda e: f"msg:{e}")
        connector_mocks.df.orderBy.side_effect = RuntimeError('bad order')

        args = {
            'splits': '200', 'table': 't',
            'where': None, 'orderby': 'x asc', 'limit': None,
            'XAction': 'count',
        }
        with pytest.raises(Exception, match="Invalid 'orderby'"):
            find_module.run(MagicMock(), MagicMock(), MagicMock(), args)

    def test_invalid_limit_raises_with_message(
        self, monkeypatch, table_info_mocks, boto3_session_mock, connector_mocks
    ):
        """Non-integer limit wrapped in 'Invalid limit'."""
        monkeypatch.setattr(find_module, 'get_error_message', lambda e: f"msg:{e}")

        args = {
            'splits': '200', 'table': 't',
            'where': None, 'orderby': None, 'limit': 'not-a-number',
            'XAction': 'count',
        }
        with pytest.raises(Exception, match="Invalid 'limit'"):
            find_module.run(MagicMock(), MagicMock(), MagicMock(), args)

    def test_unknown_action_raises_value_error(
        self, monkeypatch, table_info_mocks, boto3_session_mock, connector_mocks
    ):
        """else branch raises ValueError for unknown action."""
        args = {
            'splits': '200', 'table': 't',
            'where': 'x = 1', 'orderby': None, 'limit': None,
            'XAction': 'unknown',
        }
        with pytest.raises(ValueError, match="Logic error"):
            find_module.run(MagicMock(), MagicMock(), MagicMock(), args)


# --- run(): LIMIT behavior ----------------------------------------------------

class TestRunLimit:
    """LIMIT converts to int and optionally sets needsRepartitioning.
    find.py's OWN logic — preserved as passing tests."""

    def test_limit_applied_to_dataframe(
        self, monkeypatch, table_info_mocks, boto3_session_mock, connector_mocks, capsys
    ):
        """int(LIMIT) passed to records.limit()."""
        args = {
            'splits': '200', 'table': 't',
            'where': None, 'orderby': None, 'limit': '50',
            'XAction': 'count',
        }
        find_module.run(MagicMock(), MagicMock(), MagicMock(), args)

        connector_mocks.df.limit.assert_called_once_with(50)

    def test_limit_over_1000_sets_repartitioning(
        self, monkeypatch, table_info_mocks, connector_mocks
    ):
        """limit > 1000 sets needsRepartitioning (observable in delete)."""
        args = {
            'splits': '200', 'table': 't',
            'where': None, 'orderby': None, 'limit': '2000',
            'XAction': 'delete',
            's3-bucket-name': 'b', 'JOB_RUN_ID': 'j',
        }
        _delete_env(monkeypatch, table_info_mocks)
        connector_mocks.df.count.return_value = 3
        connector_mocks.df.toJSON.return_value.foreachPartition = MagicMock()

        find_module.run(MagicMock(), MagicMock(), MagicMock(), args)

        connector_mocks.df.repartition.assert_called_with(200), \
            "limit > 1000 triggers repartition"

    def test_limit_under_1000_no_repartitioning(
        self, monkeypatch, table_info_mocks, connector_mocks
    ):
        """limit <= 1000 does NOT set needsRepartitioning."""
        args = {
            'splits': '200', 'table': 't',
            'where': None, 'orderby': None, 'limit': '500',
            'XAction': 'delete',
            's3-bucket-name': 'b', 'JOB_RUN_ID': 'j',
        }
        _delete_env(monkeypatch, table_info_mocks)
        connector_mocks.df.count.return_value = 3
        connector_mocks.df.toJSON.return_value.foreachPartition = MagicMock()

        find_module.run(MagicMock(), MagicMock(), MagicMock(), args)

        connector_mocks.df.repartition.assert_not_called(), \
            "limit <= 1000 skips repartition"


# --- run(): DO_FIND branch ----------------------------------------------------

class TestRunFindAction:
    """DO_FIND writes JSON to S3 and prints top-N records.
    find.py's OWN logic — preserved as passing tests."""

    def test_find_writes_json_to_s3_location(
        self, monkeypatch, table_info_mocks, boto3_session_mock, connector_mocks, base_args, capsys
    ):
        """S3 output location derived from bucket + job_run_id; spark reads the rdd."""
        spark_session = MagicMock()
        monkeypatch.setattr(find_module, 'SparkSession', MagicMock(return_value=spark_session))

        df = connector_mocks.df
        df.count.return_value = 3
        json_rdd = MagicMock()
        df.toJSON.return_value = json_rdd
        df.limit.return_value.toJSON.return_value.collect.return_value = ['{"a":1}', '{"b":2}', '{"c":3}']

        find_module.run(MagicMock(), MagicMock(), MagicMock(), base_args)

        out = capsys.readouterr().out
        assert 's3://my-bucket/output/run-123' in out, "S3 path printed"
        spark_session.read.json.assert_called_once_with(json_rdd)

    def test_find_count_le_top_n_prints_all(
        self, monkeypatch, table_info_mocks, boto3_session_mock, connector_mocks, base_args, capsys
    ):
        """count <= 10 prints 'N matching items:' header."""
        monkeypatch.setattr(find_module, 'SparkSession', MagicMock())
        df = connector_mocks.df
        df.count.return_value = 3
        df.limit.return_value.toJSON.return_value.collect.return_value = ['{"a":1}', '{"b":2}', '{"c":3}']

        find_module.run(MagicMock(), MagicMock(), MagicMock(), base_args)

        out = capsys.readouterr().out
        assert '3 matching items:' in out
        assert 'more not printed' not in out

    def test_find_count_gt_top_n_prints_truncated(
        self, monkeypatch, table_info_mocks, boto3_session_mock, connector_mocks, base_args, capsys
    ):
        """count > 10 prints first 10 and '...and N more'."""
        monkeypatch.setattr(find_module, 'SparkSession', MagicMock())
        df = connector_mocks.df
        df.count.return_value = 25
        records = [f'{{"id":{i}}}' for i in range(10)]
        df.limit.return_value.toJSON.return_value.collect.return_value = records

        find_module.run(MagicMock(), MagicMock(), MagicMock(), base_args)

        out = capsys.readouterr().out
        assert 'First 10 matching items:' in out
        assert '15 more not printed' in out

    def test_find_prints_each_record(
        self, monkeypatch, table_info_mocks, boto3_session_mock, connector_mocks, base_args, capsys
    ):
        """each top-N record printed."""
        monkeypatch.setattr(find_module, 'SparkSession', MagicMock())
        df = connector_mocks.df
        df.count.return_value = 2
        df.limit.return_value.toJSON.return_value.collect.return_value = ['{"x":1}', '{"y":2}']

        find_module.run(MagicMock(), MagicMock(), MagicMock(), base_args)

        out = capsys.readouterr().out
        assert '{"x":1}' in out
        assert '{"y":2}' in out

    def test_find_caches_dataframe(
        self, monkeypatch, table_info_mocks, boto3_session_mock, connector_mocks, base_args
    ):
        """records.cache() called before count."""
        monkeypatch.setattr(find_module, 'SparkSession', MagicMock())
        df = connector_mocks.df
        df.count.return_value = 1
        df.limit.return_value.toJSON.return_value.collect.return_value = ['{}']

        find_module.run(MagicMock(), MagicMock(), MagicMock(), base_args)

        df.cache.assert_called_once()

    def test_find_writes_count_items_message(
        self, monkeypatch, table_info_mocks, boto3_session_mock, connector_mocks, base_args, capsys
    ):
        """'Wrote N items in JSON format' message."""
        monkeypatch.setattr(find_module, 'SparkSession', MagicMock())
        df = connector_mocks.df
        df.count.return_value = 42
        df.limit.return_value.toJSON.return_value.collect.return_value = [f'{{"i":{i}}}' for i in range(10)]

        find_module.run(MagicMock(), MagicMock(), MagicMock(), base_args)

        out = capsys.readouterr().out
        assert 'Wrote 42 items in JSON format' in out


# --- run(): DO_DELETE branch --------------------------------------------------

class TestRunDeleteAction:
    """DO_DELETE gets table keys, optionally repartitions, then deletes via
    foreachPartition. find.py's OWN logic — preserved as passing tests."""

    def _delete_args(self):
        return {
            'splits': '200', 'table': 'del-table',
            'where': None, 'orderby': None, 'limit': None,
            'XAction': 'delete',
            's3-bucket-name': 'bucket', 'JOB_RUN_ID': 'run-1',
        }

    def test_delete_calls_get_table_keys(
        self, monkeypatch, table_info_mocks, connector_mocks, capsys
    ):
        """boto3.client('dynamodb').describe_table called with the table name."""
        args = self._delete_args()
        client_mock, _ = _delete_env(monkeypatch, table_info_mocks)
        connector_mocks.df.count.return_value = 0
        connector_mocks.df.toJSON.return_value.foreachPartition = MagicMock()

        find_module.run(MagicMock(), MagicMock(), MagicMock(), args)

        client_mock.describe_table.assert_called_once_with(TableName='del-table')

    def test_delete_sends_count_to_pricing_generator(
        self, monkeypatch, table_info_mocks, connector_mocks, capsys
    ):
        """print_pricing_generator.send(count) passes the item count (shown in cost output)."""
        args = self._delete_args()
        _delete_env(monkeypatch, table_info_mocks)
        connector_mocks.df.count.return_value = 77
        connector_mocks.df.toJSON.return_value.foreachPartition = MagicMock()

        find_module.run(MagicMock(), MagicMock(), MagicMock(), args)

        out = capsys.readouterr().out
        assert '77' in out, "delete count sent to pricing generator"

    def test_delete_prints_deleted_count(
        self, monkeypatch, table_info_mocks, connector_mocks, capsys
    ):
        """'Deleted N items' printed after foreachPartition."""
        args = self._delete_args()
        _delete_env(monkeypatch, table_info_mocks)
        connector_mocks.df.count.return_value = 15
        connector_mocks.df.toJSON.return_value.foreachPartition = MagicMock()

        find_module.run(MagicMock(), MagicMock(), MagicMock(), args)

        out = capsys.readouterr().out
        assert 'Deleted 15 items' in out

    def test_delete_no_repartition_without_orderby_or_large_limit(
        self, monkeypatch, table_info_mocks, connector_mocks, capsys
    ):
        """needsRepartitioning=False skips select/repartition."""
        args = self._delete_args()
        _delete_env(monkeypatch, table_info_mocks)
        connector_mocks.df.count.return_value = 2
        connector_mocks.df.toJSON.return_value.foreachPartition = MagicMock()

        find_module.run(MagicMock(), MagicMock(), MagicMock(), args)

        connector_mocks.df.select.assert_not_called()
        connector_mocks.df.repartition.assert_not_called()

    def test_delete_aggregator_shutdown_in_finally(
        self, monkeypatch, table_info_mocks, connector_mocks, capsys
    ):
        """rate_limiter_aggregator.shutdown() in finally block (even on error)."""
        args = self._delete_args()
        _, agg_instance = _delete_env(monkeypatch, table_info_mocks)
        connector_mocks.df.count.return_value = 1
        connector_mocks.df.toJSON.return_value.foreachPartition = MagicMock(
            side_effect=RuntimeError('partition error')
        )

        with pytest.raises(RuntimeError, match='partition error'):
            find_module.run(MagicMock(), MagicMock(), MagicMock(), args)

        agg_instance.shutdown.assert_called_once()

    def test_delete_throughput_configs_write_mode_monitor_format(
        self, monkeypatch, table_info_mocks, connector_mocks, capsys
    ):
        """get_dynamodb_throughput_configs called with write mode and monitor format."""
        args = self._delete_args()
        _delete_env(monkeypatch, table_info_mocks)
        connector_mocks.df.count.return_value = 2
        connector_mocks.df.toJSON.return_value.foreachPartition = MagicMock()

        find_module.run(MagicMock(), MagicMock(), MagicMock(), args)

        # In the DataFrame-connector path the verb only calls
        # get_dynamodb_throughput_configs once (for monitor_options); the read
        # throughput config now lives inside read_dynamodb_dataframe.
        calls = table_info_mocks.get_dynamodb_throughput_configs.call_args_list
        assert len(calls) >= 1
        monitor_call = calls[-1]
        assert monitor_call.kwargs.get('modes') == ['write']
        assert monitor_call.kwargs.get('format') == 'monitor'


# --- delete_partition (inner function) ----------------------------------------

class TestDeletePartition:
    """The inner delete_partition function is invoked by foreachPartition.
    We capture the lambda and invoke it to test delete behavior.
    find.py's OWN logic — preserved as passing tests."""

    def _capture_and_run_delete(self, monkeypatch, table_info_mocks, connector_mocks,
                                 partition_data, rl_worker_mock=None,
                                 key_schema=None):
        """Set up delete path and capture + invoke the foreachPartition lambda."""
        if key_schema is None:
            key_schema = [
                {'AttributeName': 'pk', 'KeyType': 'HASH'},
                {'AttributeName': 'sk', 'KeyType': 'RANGE'},
            ]
        args = {
            'splits': '200', 'table': 'del-table',
            'where': None, 'orderby': None, 'limit': None,
            'XAction': 'delete',
            's3-bucket-name': 'bucket', 'JOB_RUN_ID': 'run-1',
        }
        _delete_env(monkeypatch, table_info_mocks, key_schema=key_schema)

        if rl_worker_mock is None:
            rl_worker_mock = MagicMock()
            session = MagicMock()
            table = MagicMock()
            bw = MagicMock()
            bw.__enter__ = MagicMock(return_value=bw)
            bw.__exit__ = MagicMock(return_value=False)
            table.batch_writer.return_value = bw
            session.resource.return_value.Table.return_value = table
            rl_worker_mock.get_session.return_value = session
            rl_worker_mock.table = table
            rl_worker_mock.bw = bw

        monkeypatch.setattr(find_module, 'RateLimiterWorker',
                            MagicMock(return_value=rl_worker_mock))

        df = connector_mocks.df
        df.count.return_value = len(partition_data)

        captured_fn = []

        def capture_foreach_partition(fn):
            captured_fn.append(fn)

        df.toJSON.return_value.foreachPartition = capture_foreach_partition

        find_module.run(MagicMock(), MagicMock(), MagicMock(), args)

        # Now invoke the captured lambda with our partition data
        assert len(captured_fn) == 1
        captured_fn[0](iter(partition_data))

        return rl_worker_mock

    def test_deletes_items_by_key(
        self, monkeypatch, table_info_mocks, connector_mocks, capsys
    ):
        """each record parsed, keys extracted, delete_item called."""
        partition = [
            json.dumps({'pk': 'a', 'sk': '1', 'data': 'x'}),
            json.dumps({'pk': 'b', 'sk': '2', 'data': 'y'}),
        ]
        rl_mock = self._capture_and_run_delete(
            monkeypatch, table_info_mocks, connector_mocks, partition)

        bw = rl_mock.bw
        assert bw.delete_item.call_count == 2
        keys_deleted = [c.kwargs['Key'] for c in bw.delete_item.call_args_list]
        assert {'pk': 'a', 'sk': '1'} in keys_deleted
        assert {'pk': 'b', 'sk': '2'} in keys_deleted

    def test_delete_item_error_prints_but_continues(
        self, monkeypatch, table_info_mocks, connector_mocks, capsys
    ):
        """exception in delete_item prints error, continues loop."""
        rl_worker_mock = MagicMock()
        session = MagicMock()
        table = MagicMock()
        bw = MagicMock()
        bw.__enter__ = MagicMock(return_value=bw)
        bw.__exit__ = MagicMock(return_value=False)
        bw.delete_item = MagicMock(side_effect=[RuntimeError('throttled'), None])
        table.batch_writer.return_value = bw
        session.resource.return_value.Table.return_value = table
        rl_worker_mock.get_session.return_value = session
        rl_worker_mock.table = table
        rl_worker_mock.bw = bw

        partition = [
            json.dumps({'pk': 'a', 'sk': '1'}),
            json.dumps({'pk': 'b', 'sk': '2'}),
        ]
        self._capture_and_run_delete(
            monkeypatch, table_info_mocks, connector_mocks, partition,
            rl_worker_mock=rl_worker_mock
        )

        out = capsys.readouterr().out
        assert 'Error deleting item' in out
        assert bw.delete_item.call_count == 2, "continues after first error"

    def test_rate_limiter_worker_shutdown_in_finally(
        self, monkeypatch, table_info_mocks, connector_mocks
    ):
        """rate_limiter_worker.shutdown() called in finally."""
        partition = [json.dumps({'pk': 'x', 'sk': 'y'})]
        rl_mock = self._capture_and_run_delete(
            monkeypatch, table_info_mocks, connector_mocks, partition)

        rl_mock.shutdown.assert_called_once()

    def test_rate_limiter_worker_shutdown_even_on_error(
        self, monkeypatch, table_info_mocks, connector_mocks
    ):
        """shutdown called even when batch_writer raises."""
        rl_worker_mock = MagicMock()
        session = MagicMock()
        table = MagicMock()
        # batch_writer context manager raises on __enter__
        bw = MagicMock()
        bw.__enter__ = MagicMock(side_effect=RuntimeError('connection failed'))
        bw.__exit__ = MagicMock(return_value=False)
        table.batch_writer.return_value = bw
        session.resource.return_value.Table.return_value = table
        rl_worker_mock.get_session.return_value = session
        rl_worker_mock.table = table
        rl_worker_mock.bw = bw

        partition = [json.dumps({'pk': 'x', 'sk': 'y'})]

        # The error will propagate from the partition function but shutdown should still be called
        with pytest.raises(RuntimeError, match='connection failed'):
            self._capture_and_run_delete(
                monkeypatch, table_info_mocks, connector_mocks, partition,
                rl_worker_mock=rl_worker_mock
            )

        rl_worker_mock.shutdown.assert_called_once()

    def test_delete_partition_uses_config_with_timeouts(
        self, monkeypatch, table_info_mocks, connector_mocks
    ):
        """Config with connect_timeout=4, read_timeout=4, 50 retries."""
        seen_configs = []

        rl_worker_mock = MagicMock()
        session = MagicMock()

        def capture_resource(svc, **kwargs):
            if 'config' in kwargs:
                seen_configs.append(kwargs['config'])
            resource = MagicMock()
            table = MagicMock()
            bw = MagicMock()
            bw.__enter__ = MagicMock(return_value=bw)
            bw.__exit__ = MagicMock(return_value=False)
            table.batch_writer.return_value = bw
            resource.Table.return_value = table
            return resource

        session.resource = capture_resource
        rl_worker_mock.get_session.return_value = session
        # Provide dummy attributes for the assertion helper
        rl_worker_mock.table = MagicMock()
        rl_worker_mock.bw = MagicMock()
        rl_worker_mock.bw.__enter__ = MagicMock(return_value=rl_worker_mock.bw)
        rl_worker_mock.bw.__exit__ = MagicMock(return_value=False)
        rl_worker_mock.table.batch_writer.return_value = rl_worker_mock.bw

        partition = [json.dumps({'pk': 'a', 'sk': 'b'})]
        self._capture_and_run_delete(
            monkeypatch, table_info_mocks, connector_mocks, partition,
            rl_worker_mock=rl_worker_mock
        )

        assert len(seen_configs) == 1
        cfg = seen_configs[0]
        assert cfg.connect_timeout == 4.0
        assert cfg.read_timeout == 4.0
        assert cfg.retries['mode'] == 'standard'
        assert cfg.retries['total_max_attempts'] == 50


# --- run(): warnings suppression and defaults ---------------------------------

class TestRunMiscBehavior:
    """Miscellaneous behavior: default splits, warnings suppression.
    find.py's OWN logic — preserved as passing tests."""

    def test_default_splits_is_200(
        self, monkeypatch, table_info_mocks, boto3_session_mock, connector_mocks
    ):
        """splits defaults to '200' when not in parsed_args (forwarded to wrapper)."""
        args = {
            'table': 't', 'where': None, 'orderby': None, 'limit': None,
            'XAction': 'count',
        }
        find_module.run(MagicMock(), MagicMock(), MagicMock(), args)

        # Simple full count → count_dynamodb_table gets the default splits
        c = connector_mocks.count_dynamodb_table.call_args
        assert c.kwargs.get('splits') == '200'

    def test_warnings_suppressed_in_dataframe_path(
        self, monkeypatch, table_info_mocks, boto3_session_mock, connector_mocks
    ):
        """warnings.filterwarnings called for DataFrame constructor."""
        with patch.object(find_module.warnings, 'filterwarnings') as mock_fw:
            args = {
                'splits': '200', 'table': 't',
                'where': 'x = 1', 'orderby': None, 'limit': None,
                'XAction': 'count',
            }
            find_module.run(MagicMock(), MagicMock(), MagicMock(), args)
            mock_fw.assert_called_once_with(
                "ignore",
                message="DataFrame constructor is internal. Do not directly use it."
            )

    def test_count_with_orderby_uses_dataframe(
        self, monkeypatch, table_info_mocks, boto3_session_mock, connector_mocks, capsys
    ):
        """count + ORDERBY forces DataFrame path (not simple count)."""
        args = {
            'splits': '200', 'table': 't',
            'where': None, 'orderby': 'col asc', 'limit': None,
            'XAction': 'count',
        }
        connector_mocks.df.count.return_value = 99

        find_module.run(MagicMock(), MagicMock(), MagicMock(), args)

        out = capsys.readouterr().out
        assert '99' in out
        connector_mocks.read_dynamodb_dataframe.assert_called_once()
        connector_mocks.count_dynamodb_table.assert_not_called()

    def test_count_with_limit_uses_dataframe(
        self, monkeypatch, table_info_mocks, boto3_session_mock, connector_mocks, capsys
    ):
        """count + LIMIT forces DataFrame path."""
        args = {
            'splits': '200', 'table': 't',
            'where': None, 'orderby': None, 'limit': '10',
            'XAction': 'count',
        }
        connector_mocks.df.count.return_value = 8

        find_module.run(MagicMock(), MagicMock(), MagicMock(), args)

        out = capsys.readouterr().out
        assert '8' in out
        connector_mocks.read_dynamodb_dataframe.assert_called_once()
        connector_mocks.count_dynamodb_table.assert_not_called()
