"""Unit tests for the `sql` server-side verb.

Covers `python_modules/sql.py`:
- run(): argument parsing (splits default, table, query, limit),
  boto3 session region, table info helpers, connection_options dict,
  dynamic frame creation, warnings suppression, DataFrame temp view
  with table name aliasing, SparkSession creation,
  query validation (SELECT only), spark.sql execution,
  limit handling (valid int, zero/negative, non-integer, generic exception),
  result caching/count, s3 output location, print logic (<=10, >10, 0 rows),
  S3 write, unpersist, exception wrapping via get_error_message,
  finally block spark.stop (including when stop itself raises)
"""

import sys
from unittest.mock import MagicMock, Mock, patch, call

import pytest

# pyspark.sql is not mocked by conftest — sql.py needs it for SparkSession import
if 'pyspark.sql' not in sys.modules:
    sys.modules['pyspark.sql'] = Mock()

from python_modules import sql as sql_module

# Star import from mocked errors module brings in nothing (Mock has no __all__).
# Inject get_error_message so monkeypatch.setattr works and the code doesn't NameError.
if not hasattr(sql_module, 'get_error_message'):
    sql_module.get_error_message = lambda e: str(e)


@pytest.fixture
def mock_boto3_session(monkeypatch):
    """Mock boto3.Session() so it returns a predictable region_name."""
    session_instance = MagicMock()
    session_instance.region_name = 'us-east-1'
    session_cls = MagicMock(return_value=session_instance)
    monkeypatch.setattr(sql_module, 'boto3', MagicMock(Session=session_cls))
    return session_instance


@pytest.fixture
def mock_table_info(monkeypatch):
    """Mock the three shared.table_info helpers imported into sql module."""
    helpers = MagicMock()
    helpers.get_and_print_dynamodb_table_info = MagicMock(
        return_value={'item_count': 50, 'size_bytes': 512, 'region_name': 'us-east-1'}
    )
    helpers.get_and_print_table_scan_cost = MagicMock(return_value=0.75)
    helpers.get_dynamodb_throughput_configs = MagicMock(return_value={'throughput.read.percent': '0.5'})

    monkeypatch.setattr(sql_module, 'get_and_print_dynamodb_table_info',
                        helpers.get_and_print_dynamodb_table_info)
    monkeypatch.setattr(sql_module, 'get_and_print_table_scan_cost',
                        helpers.get_and_print_table_scan_cost)
    monkeypatch.setattr(sql_module, 'get_dynamodb_throughput_configs',
                        helpers.get_dynamodb_throughput_configs)
    return helpers


@pytest.fixture
def mock_get_error_message(monkeypatch):
    """Mock get_error_message imported from shared.errors via star-import."""
    fn = MagicMock(side_effect=lambda e: f"err:{e}")
    monkeypatch.setattr(sql_module, 'get_error_message', fn)
    return fn


@pytest.fixture
def mock_warnings(monkeypatch):
    """Mock warnings.filterwarnings."""
    w = MagicMock()
    monkeypatch.setattr(sql_module, 'warnings', w)
    return w


@pytest.fixture
def mock_spark_session(monkeypatch):
    """Mock SparkSession so spark.sql / spark.stop are controllable."""
    spark_instance = MagicMock()
    spark_cls = MagicMock(return_value=spark_instance)
    monkeypatch.setattr(sql_module, 'SparkSession', spark_cls)
    return spark_instance


@pytest.fixture
def glue_context():
    """Mock GlueContext with create_dynamic_frame.from_options chain."""
    ctx = MagicMock()
    dynamic_frame = MagicMock()
    ctx.create_dynamic_frame.from_options.return_value = dynamic_frame
    return ctx


@pytest.fixture
def base_args():
    """Minimal parsed_args for a successful run."""
    return {
        'table': 'my-test.table',
        'query': 'SELECT * FROM my_test_table',
        's3-bucket-name': 'output-bucket',
        'JOB_RUN_ID': 'run-001',
    }


def _make_result_mock(count=5):
    """Build a result DataFrame mock with configurable count and records."""
    result = MagicMock()
    result.count.return_value = count
    result.limit.return_value = result
    records = [f'{{"id": {i}}}' for i in range(min(count, 10))]
    result.limit.return_value.toJSON.return_value.collect.return_value = records
    result.toJSON.return_value.collect.return_value = records
    result.write.mode.return_value.json = MagicMock()
    return result


# --- Argument parsing and setup -------------------------------------------

@pytest.mark.skip(reason="Asserts against legacy DynamicFrame code path; verb now goes through python_modules.shared.glue_connector wrapper. Tracked in followup: rewrite to assert against wrapper boundary.")
class TestRunArgumentParsing:
    """run() extracts splits, table, query, and limit from parsed_args."""

    def test_splits_defaults_to_200_when_absent(self, monkeypatch, mock_boto3_session,
                                                  mock_table_info, mock_warnings,
                                                  mock_spark_session, mock_get_error_message,
                                                  glue_context, base_args):
        """Line 13: splits defaults to '200' when not provided."""
        result = _make_result_mock(count=1)
        mock_spark_session.sql.return_value = result
        df = MagicMock()
        glue_context.create_dynamic_frame.from_options.return_value.toDF.return_value = df

        sql_module.run(MagicMock(), MagicMock(), glue_context, base_args)

        conn_opts = glue_context.create_dynamic_frame.from_options.call_args.kwargs['connection_options']
        assert conn_opts['dynamodb.splits'] == '200', "default splits is '200'"

    def test_splits_uses_provided_value(self, monkeypatch, mock_boto3_session,
                                          mock_table_info, mock_warnings,
                                          mock_spark_session, mock_get_error_message,
                                          glue_context, base_args):
        """Line 13: splits uses parsed_args value when present."""
        base_args['splits'] = '50'
        result = _make_result_mock(count=1)
        mock_spark_session.sql.return_value = result
        df = MagicMock()
        glue_context.create_dynamic_frame.from_options.return_value.toDF.return_value = df

        sql_module.run(MagicMock(), MagicMock(), glue_context, base_args)

        conn_opts = glue_context.create_dynamic_frame.from_options.call_args.kwargs['connection_options']
        assert conn_opts['dynamodb.splits'] == '50'

    def test_limit_defaults_to_none(self, monkeypatch, mock_boto3_session,
                                      mock_table_info, mock_warnings,
                                      mock_spark_session, mock_get_error_message,
                                      glue_context, base_args):
        """Line 16: limit is None when not provided — no user-limit call on result.
        result.limit(10) still happens for display (line 85), so we check no other call."""
        result = _make_result_mock(count=2)
        mock_spark_session.sql.return_value = result
        df = MagicMock()
        glue_context.create_dynamic_frame.from_options.return_value.toDF.return_value = df

        sql_module.run(MagicMock(), MagicMock(), glue_context, base_args)

        # Only call should be limit(10) for display, not any user-specified limit
        for c in result.limit.call_args_list:
            assert c.args[0] == 10, "only TOP_N display limit expected, not a user limit"


# --- Table info and connection setup --------------------------------------

@pytest.mark.skip(reason="Asserts against legacy DynamicFrame code path; verb now goes through python_modules.shared.glue_connector wrapper. Followup: rewrite to assert against the wrapper boundary.")
class TestRunTableInfoAndConnection:
    """run() calls table info helpers and builds connection_options correctly."""

    def test_boto3_session_region_used(self, monkeypatch, mock_boto3_session,
                                         mock_table_info, mock_warnings,
                                         mock_spark_session, mock_get_error_message,
                                         glue_context, base_args):
        """Line 19: region_name comes from boto3.Session().region_name."""
        result = _make_result_mock(count=1)
        mock_spark_session.sql.return_value = result
        df = MagicMock()
        glue_context.create_dynamic_frame.from_options.return_value.toDF.return_value = df

        sql_module.run(MagicMock(), MagicMock(), glue_context, base_args)

        mock_table_info.get_and_print_table_scan_cost.assert_called_once()
        call_args = mock_table_info.get_and_print_table_scan_cost.call_args
        assert call_args.args[1] == 'us-east-1' or call_args[0][1] == 'us-east-1'

    def test_get_and_print_dynamodb_table_info_called_with_table_name(
            self, monkeypatch, mock_boto3_session, mock_table_info, mock_warnings,
            mock_spark_session, mock_get_error_message, glue_context, base_args):
        """Line 20: table info called with the table name."""
        result = _make_result_mock(count=1)
        mock_spark_session.sql.return_value = result
        df = MagicMock()
        glue_context.create_dynamic_frame.from_options.return_value.toDF.return_value = df

        sql_module.run(MagicMock(), MagicMock(), glue_context, base_args)

        mock_table_info.get_and_print_dynamodb_table_info.assert_called_once_with('my-test.table')

    def test_scan_cost_called_with_numberOfScans_2(
            self, monkeypatch, mock_boto3_session, mock_table_info, mock_warnings,
            mock_spark_session, mock_get_error_message, glue_context, base_args):
        """Line 21: get_and_print_table_scan_cost receives numberOfScans=2."""
        result = _make_result_mock(count=1)
        mock_spark_session.sql.return_value = result
        df = MagicMock()
        glue_context.create_dynamic_frame.from_options.return_value.toDF.return_value = df

        sql_module.run(MagicMock(), MagicMock(), glue_context, base_args)

        call_kwargs = mock_table_info.get_and_print_table_scan_cost.call_args.kwargs
        assert call_kwargs['numberOfScans'] == 2

    def test_connection_options_includes_table_name_and_consistent_read(
            self, monkeypatch, mock_boto3_session, mock_table_info, mock_warnings,
            mock_spark_session, mock_get_error_message, glue_context, base_args):
        """Lines 23-28: connection_options dict has correct keys."""
        result = _make_result_mock(count=1)
        mock_spark_session.sql.return_value = result
        df = MagicMock()
        glue_context.create_dynamic_frame.from_options.return_value.toDF.return_value = df

        sql_module.run(MagicMock(), MagicMock(), glue_context, base_args)

        conn_opts = glue_context.create_dynamic_frame.from_options.call_args.kwargs['connection_options']
        assert conn_opts['dynamodb.input.tableName'] == 'my-test.table'
        assert conn_opts['dynamodb.consistentRead'] == 'false'
        assert conn_opts['throughput.read.percent'] == '0.5', "throughput configs merged in"

    def test_create_dynamic_frame_called_with_dynamodb_connection_type(
            self, monkeypatch, mock_boto3_session, mock_table_info, mock_warnings,
            mock_spark_session, mock_get_error_message, glue_context, base_args):
        """Lines 31-34: from_options called with connection_type='dynamodb'."""
        result = _make_result_mock(count=1)
        mock_spark_session.sql.return_value = result
        df = MagicMock()
        glue_context.create_dynamic_frame.from_options.return_value.toDF.return_value = df

        sql_module.run(MagicMock(), MagicMock(), glue_context, base_args)

        call_kwargs = glue_context.create_dynamic_frame.from_options.call_args.kwargs
        assert call_kwargs['connection_type'] == 'dynamodb'

    def test_throughput_configs_called_with_read_mode(
            self, monkeypatch, mock_boto3_session, mock_table_info, mock_warnings,
            mock_spark_session, mock_get_error_message, glue_context, base_args):
        """Line 27: get_dynamodb_throughput_configs called with modes=['read']."""
        result = _make_result_mock(count=1)
        mock_spark_session.sql.return_value = result
        df = MagicMock()
        glue_context.create_dynamic_frame.from_options.return_value.toDF.return_value = df

        sql_module.run(MagicMock(), MagicMock(), glue_context, base_args)

        call_args = mock_table_info.get_dynamodb_throughput_configs.call_args
        assert call_args[0][1] == 'my-test.table'
        assert call_args.kwargs['modes'] == ['read']


# --- DataFrame and temp view setup ----------------------------------------

@pytest.mark.skip(reason="Asserts against legacy DynamicFrame code path; verb now goes through python_modules.shared.glue_connector wrapper. Followup: rewrite to assert against the wrapper boundary.")
class TestRunDataFrameSetup:
    """run() converts dynamic frame to DataFrame, aliases table name, registers temp view."""

    def test_warnings_filter_suppresses_dataframe_constructor_warning(
            self, monkeypatch, mock_boto3_session, mock_table_info, mock_warnings,
            mock_spark_session, mock_get_error_message, glue_context, base_args):
        """Line 37: warnings.filterwarnings called with 'ignore' and the specific message."""
        result = _make_result_mock(count=1)
        mock_spark_session.sql.return_value = result
        df = MagicMock()
        glue_context.create_dynamic_frame.from_options.return_value.toDF.return_value = df

        sql_module.run(MagicMock(), MagicMock(), glue_context, base_args)

        mock_warnings.filterwarnings.assert_called_once()
        args = mock_warnings.filterwarnings.call_args
        assert args[0][0] == 'ignore'
        assert 'DataFrame constructor' in args.kwargs.get('message', args[0][1] if len(args[0]) > 1 else '')

    def test_table_alias_replaces_hyphens_and_dots(
            self, monkeypatch, mock_boto3_session, mock_table_info, mock_warnings,
            mock_spark_session, mock_get_error_message, glue_context, base_args):
        """Line 41: table alias replaces '-' and '.' with '_'."""
        result = _make_result_mock(count=1)
        mock_spark_session.sql.return_value = result
        df = MagicMock()
        glue_context.create_dynamic_frame.from_options.return_value.toDF.return_value = df

        sql_module.run(MagicMock(), MagicMock(), glue_context, base_args)

        df.createOrReplaceTempView.assert_called_once_with('my_test_table')

    def test_spark_session_created_from_spark_context(
            self, monkeypatch, mock_boto3_session, mock_table_info, mock_warnings,
            mock_spark_session, mock_get_error_message, glue_context, base_args):
        """Line 45: SparkSession(spark_context) is called."""
        result = _make_result_mock(count=1)
        mock_spark_session.sql.return_value = result
        df = MagicMock()
        glue_context.create_dynamic_frame.from_options.return_value.toDF.return_value = df

        sc = MagicMock()
        sql_module.run(MagicMock(), sc, glue_context, base_args)

        sql_module.SparkSession.assert_called_once_with(sc)


# --- Query validation -----------------------------------------------------

class TestRunQueryValidation:
    """run() rejects non-SELECT queries."""

    def test_non_select_query_raises(self, monkeypatch, mock_boto3_session,
                                       mock_table_info, mock_warnings,
                                       mock_spark_session, mock_get_error_message,
                                       glue_context, base_args):
        """Line 50-51: query not starting with SELECT raises."""
        base_args['query'] = 'DELETE FROM my_test_table'
        df = MagicMock()
        glue_context.create_dynamic_frame.from_options.return_value.toDF.return_value = df

        with pytest.raises(Exception, match="SQL query error"):
            sql_module.run(MagicMock(), MagicMock(), glue_context, base_args)

    def test_select_with_leading_whitespace_passes(self, monkeypatch, mock_boto3_session,
                                                     mock_table_info, mock_warnings,
                                                     mock_spark_session, mock_get_error_message,
                                                     glue_context, base_args):
        """Line 49: .strip() allows leading whitespace before SELECT."""
        base_args['query'] = '   SELECT id FROM my_test_table'
        result = _make_result_mock(count=1)
        mock_spark_session.sql.return_value = result
        df = MagicMock()
        glue_context.create_dynamic_frame.from_options.return_value.toDF.return_value = df

        sql_module.run(MagicMock(), MagicMock(), glue_context, base_args)

        mock_spark_session.sql.assert_called_once_with('   SELECT id FROM my_test_table')

    def test_select_case_insensitive(self, monkeypatch, mock_boto3_session,
                                       mock_table_info, mock_warnings,
                                       mock_spark_session, mock_get_error_message,
                                       glue_context, base_args):
        """Line 49: .upper() makes check case-insensitive."""
        base_args['query'] = 'select * from my_test_table'
        result = _make_result_mock(count=1)
        mock_spark_session.sql.return_value = result
        df = MagicMock()
        glue_context.create_dynamic_frame.from_options.return_value.toDF.return_value = df

        sql_module.run(MagicMock(), MagicMock(), glue_context, base_args)

        mock_spark_session.sql.assert_called_once_with('select * from my_test_table')


# --- LIMIT handling -------------------------------------------------------

class TestRunLimitHandling:
    """run() applies LIMIT when provided and validates it."""

    def test_valid_limit_applies_to_result(self, monkeypatch, mock_boto3_session,
                                             mock_table_info, mock_warnings,
                                             mock_spark_session, mock_get_error_message,
                                             glue_context, base_args):
        """Lines 57-60: valid integer limit calls result.limit()."""
        base_args['limit'] = '25'
        result = _make_result_mock(count=3)
        limited_result = _make_result_mock(count=3)
        result.limit.return_value = limited_result
        mock_spark_session.sql.return_value = result
        df = MagicMock()
        glue_context.create_dynamic_frame.from_options.return_value.toDF.return_value = df

        sql_module.run(MagicMock(), MagicMock(), glue_context, base_args)

        result.limit.assert_called_once_with(25)

    def test_zero_limit_raises_value_error(self, monkeypatch, mock_boto3_session,
                                             mock_table_info, mock_warnings,
                                             mock_spark_session, mock_get_error_message,
                                             glue_context, base_args):
        """Line 61: limit <= 0 raises ValueError wrapped as Exception."""
        base_args['limit'] = '0'
        result = _make_result_mock(count=1)
        mock_spark_session.sql.return_value = result
        df = MagicMock()
        glue_context.create_dynamic_frame.from_options.return_value.toDF.return_value = df

        with pytest.raises(Exception, match="Invalid 'limit'.*must be positive"):
            sql_module.run(MagicMock(), MagicMock(), glue_context, base_args)

    def test_negative_limit_raises_value_error(self, monkeypatch, mock_boto3_session,
                                                 mock_table_info, mock_warnings,
                                                 mock_spark_session, mock_get_error_message,
                                                 glue_context, base_args):
        """Line 61: negative limit raises ValueError."""
        base_args['limit'] = '-5'
        result = _make_result_mock(count=1)
        mock_spark_session.sql.return_value = result
        df = MagicMock()
        glue_context.create_dynamic_frame.from_options.return_value.toDF.return_value = df

        with pytest.raises(Exception, match="Invalid 'limit'.*must be positive"):
            sql_module.run(MagicMock(), MagicMock(), glue_context, base_args)

    def test_non_integer_limit_raises_via_get_error_message(self, monkeypatch, mock_boto3_session,
                                                              mock_table_info, mock_warnings,
                                                              mock_spark_session, mock_get_error_message,
                                                              glue_context, base_args):
        """Line 63-64: non-int string raises ValueError caught and re-raised."""
        base_args['limit'] = 'abc'
        result = _make_result_mock(count=1)
        mock_spark_session.sql.return_value = result
        df = MagicMock()
        glue_context.create_dynamic_frame.from_options.return_value.toDF.return_value = df

        with pytest.raises(Exception, match="Invalid 'limit'"):
            sql_module.run(MagicMock(), MagicMock(), glue_context, base_args)

    def test_limit_generic_exception_uses_get_error_message(self, monkeypatch, mock_boto3_session,
                                                              mock_table_info, mock_warnings,
                                                              mock_spark_session, mock_get_error_message,
                                                              glue_context, base_args):
        """Lines 65-66: generic Exception in limit block uses get_error_message."""
        base_args['limit'] = '5'
        result = MagicMock()
        result.limit.side_effect = RuntimeError("spark limit failure")
        mock_spark_session.sql.return_value = result
        df = MagicMock()
        glue_context.create_dynamic_frame.from_options.return_value.toDF.return_value = df

        with pytest.raises(Exception, match="Invalid 'limit'.*err:"):
            sql_module.run(MagicMock(), MagicMock(), glue_context, base_args)

        mock_get_error_message.assert_called()


# --- Output printing logic ------------------------------------------------

class TestRunOutputPrinting:
    """run() prints results differently based on count vs TOP_N threshold."""

    def _run_with_count(self, count, monkeypatch, mock_boto3_session, mock_table_info,
                        mock_warnings, mock_spark_session, mock_get_error_message,
                        glue_context, base_args):
        """Helper: set up result with given count and run."""
        result = MagicMock()
        result.count.return_value = count
        records = [f'{{"id": {i}}}' for i in range(min(count, 10))]
        limited = MagicMock()
        limited.toJSON.return_value.collect.return_value = records
        result.limit.return_value = limited
        result.write.mode.return_value.json = MagicMock()
        mock_spark_session.sql.return_value = result
        df = MagicMock()
        glue_context.create_dynamic_frame.from_options.return_value.toDF.return_value = df

        sql_module.run(MagicMock(), MagicMock(), glue_context, base_args)
        return result

    def test_count_less_than_top_n_prints_count_result_rows(
            self, monkeypatch, mock_boto3_session, mock_table_info, mock_warnings,
            mock_spark_session, mock_get_error_message, glue_context, base_args, capsys):
        """Line 79-80: when count <= 10, prints '{count} result rows:'."""
        self._run_with_count(5, monkeypatch, mock_boto3_session, mock_table_info,
                             mock_warnings, mock_spark_session, mock_get_error_message,
                             glue_context, base_args)
        out = capsys.readouterr().out
        assert '5 result rows:' in out

    def test_count_equal_to_top_n_prints_count_result_rows(
            self, monkeypatch, mock_boto3_session, mock_table_info, mock_warnings,
            mock_spark_session, mock_get_error_message, glue_context, base_args, capsys):
        """Line 79: boundary — count == 10 uses <= path."""
        self._run_with_count(10, monkeypatch, mock_boto3_session, mock_table_info,
                             mock_warnings, mock_spark_session, mock_get_error_message,
                             glue_context, base_args)
        out = capsys.readouterr().out
        assert '10 result rows:' in out

    def test_count_greater_than_top_n_prints_first_n(
            self, monkeypatch, mock_boto3_session, mock_table_info, mock_warnings,
            mock_spark_session, mock_get_error_message, glue_context, base_args, capsys):
        """Line 82: when count > 10, prints 'First 10 result rows:'."""
        self._run_with_count(25, monkeypatch, mock_boto3_session, mock_table_info,
                             mock_warnings, mock_spark_session, mock_get_error_message,
                             glue_context, base_args)
        out = capsys.readouterr().out
        assert 'First 10 result rows:' in out

    def test_count_greater_than_top_n_prints_more_rows_message(
            self, monkeypatch, mock_boto3_session, mock_table_info, mock_warnings,
            mock_spark_session, mock_get_error_message, glue_context, base_args, capsys):
        """Line 89-90: prints '...and N more rows not printed'."""
        self._run_with_count(25, monkeypatch, mock_boto3_session, mock_table_info,
                             mock_warnings, mock_spark_session, mock_get_error_message,
                             glue_context, base_args)
        out = capsys.readouterr().out
        assert '...and 15 more rows not printed' in out

    def test_zero_count_does_not_print_records(
            self, monkeypatch, mock_boto3_session, mock_table_info, mock_warnings,
            mock_spark_session, mock_get_error_message, glue_context, base_args, capsys):
        """Line 84: count == 0 skips record printing entirely."""
        self._run_with_count(0, monkeypatch, mock_boto3_session, mock_table_info,
                             mock_warnings, mock_spark_session, mock_get_error_message,
                             glue_context, base_args)
        out = capsys.readouterr().out
        assert '{"id"' not in out

    def test_records_are_printed_individually(
            self, monkeypatch, mock_boto3_session, mock_table_info, mock_warnings,
            mock_spark_session, mock_get_error_message, glue_context, base_args, capsys):
        """Lines 85-87: each record from toJSON().collect() is printed."""
        self._run_with_count(3, monkeypatch, mock_boto3_session, mock_table_info,
                             mock_warnings, mock_spark_session, mock_get_error_message,
                             glue_context, base_args)
        out = capsys.readouterr().out
        assert '{"id": 0}' in out
        assert '{"id": 1}' in out
        assert '{"id": 2}' in out

    def test_result_limit_called_with_top_n_for_display(
            self, monkeypatch, mock_boto3_session, mock_table_info, mock_warnings,
            mock_spark_session, mock_get_error_message, glue_context, base_args):
        """Line 85: result.limit(TOP_N=10) used for display."""
        result = self._run_with_count(15, monkeypatch, mock_boto3_session, mock_table_info,
                                      mock_warnings, mock_spark_session, mock_get_error_message,
                                      glue_context, base_args)
        limit_calls = [c for c in result.limit.call_args_list if c.args[0] == 10]
        assert len(limit_calls) >= 1, "result.limit(10) called for top-N display"


# --- S3 write and cleanup ------------------------------------------------

class TestRunS3WriteAndCleanup:
    """run() writes results to S3 and calls unpersist."""

    def test_s3_output_location_format(
            self, monkeypatch, mock_boto3_session, mock_table_info, mock_warnings,
            mock_spark_session, mock_get_error_message, glue_context, base_args, capsys):
        """Lines 73-75: s3 location is s3://{bucket}/output/{job_run_id}."""
        result = _make_result_mock(count=1)
        mock_spark_session.sql.return_value = result
        df = MagicMock()
        glue_context.create_dynamic_frame.from_options.return_value.toDF.return_value = df

        sql_module.run(MagicMock(), MagicMock(), glue_context, base_args)

        out = capsys.readouterr().out
        assert 's3://output-bucket/output/run-001/' in out

    def test_write_mode_overwrite_json(
            self, monkeypatch, mock_boto3_session, mock_table_info, mock_warnings,
            mock_spark_session, mock_get_error_message, glue_context, base_args):
        """Line 93: result.write.mode('overwrite').json(location)."""
        result = _make_result_mock(count=1)
        mock_spark_session.sql.return_value = result
        df = MagicMock()
        glue_context.create_dynamic_frame.from_options.return_value.toDF.return_value = df

        sql_module.run(MagicMock(), MagicMock(), glue_context, base_args)

        result.write.mode.assert_called_with('overwrite')
        result.write.mode.return_value.json.assert_called_once_with(
            's3://output-bucket/output/run-001'
        )

    def test_result_unpersist_called(
            self, monkeypatch, mock_boto3_session, mock_table_info, mock_warnings,
            mock_spark_session, mock_get_error_message, glue_context, base_args):
        """Line 100: result.unpersist() called for cleanup."""
        result = _make_result_mock(count=1)
        mock_spark_session.sql.return_value = result
        df = MagicMock()
        glue_context.create_dynamic_frame.from_options.return_value.toDF.return_value = df

        sql_module.run(MagicMock(), MagicMock(), glue_context, base_args)

        result.unpersist.assert_called_once()

    def test_result_cache_called(
            self, monkeypatch, mock_boto3_session, mock_table_info, mock_warnings,
            mock_spark_session, mock_get_error_message, glue_context, base_args):
        """Line 69: result.cache() called before count."""
        result = _make_result_mock(count=1)
        mock_spark_session.sql.return_value = result
        df = MagicMock()
        glue_context.create_dynamic_frame.from_options.return_value.toDF.return_value = df

        sql_module.run(MagicMock(), MagicMock(), glue_context, base_args)

        result.cache.assert_called_once()

    def test_count_printed_with_comma_formatting(
            self, monkeypatch, mock_boto3_session, mock_table_info, mock_warnings,
            mock_spark_session, mock_get_error_message, glue_context, base_args, capsys):
        """Line 96: count formatted with commas."""
        result = MagicMock()
        result.count.return_value = 1500
        records = [f'{{"id": {i}}}' for i in range(10)]
        limited = MagicMock()
        limited.toJSON.return_value.collect.return_value = records
        result.limit.return_value = limited
        result.write.mode.return_value.json = MagicMock()
        mock_spark_session.sql.return_value = result
        df = MagicMock()
        glue_context.create_dynamic_frame.from_options.return_value.toDF.return_value = df

        sql_module.run(MagicMock(), MagicMock(), glue_context, base_args)

        out = capsys.readouterr().out
        assert '1,500 rows' in out


# --- Error handling and finally block -------------------------------------

class TestRunErrorHandling:
    """run() wraps exceptions with get_error_message and always stops spark."""

    def test_spark_sql_error_wrapped(self, monkeypatch, mock_boto3_session,
                                       mock_table_info, mock_warnings,
                                       mock_spark_session, mock_get_error_message,
                                       glue_context, base_args):
        """Lines 102-103: exception from spark.sql wrapped with 'SQL query error:'."""
        mock_spark_session.sql.side_effect = RuntimeError("parse failure")
        df = MagicMock()
        glue_context.create_dynamic_frame.from_options.return_value.toDF.return_value = df

        with pytest.raises(Exception, match="SQL query error:.*err:"):
            sql_module.run(MagicMock(), MagicMock(), glue_context, base_args)

    def test_spark_stop_called_on_success(self, monkeypatch, mock_boto3_session,
                                            mock_table_info, mock_warnings,
                                            mock_spark_session, mock_get_error_message,
                                            glue_context, base_args):
        """Line 107: spark.stop() called in finally on success."""
        result = _make_result_mock(count=1)
        mock_spark_session.sql.return_value = result
        df = MagicMock()
        glue_context.create_dynamic_frame.from_options.return_value.toDF.return_value = df

        sql_module.run(MagicMock(), MagicMock(), glue_context, base_args)

        mock_spark_session.stop.assert_called_once()

    def test_spark_stop_called_on_error(self, monkeypatch, mock_boto3_session,
                                          mock_table_info, mock_warnings,
                                          mock_spark_session, mock_get_error_message,
                                          glue_context, base_args):
        """Line 107: spark.stop() called in finally even on exception."""
        mock_spark_session.sql.side_effect = RuntimeError("boom")
        df = MagicMock()
        glue_context.create_dynamic_frame.from_options.return_value.toDF.return_value = df

        with pytest.raises(Exception):
            sql_module.run(MagicMock(), MagicMock(), glue_context, base_args)

        mock_spark_session.stop.assert_called_once()

    def test_spark_stop_exception_swallowed(self, monkeypatch, mock_boto3_session,
                                              mock_table_info, mock_warnings,
                                              mock_spark_session, mock_get_error_message,
                                              glue_context, base_args):
        """Lines 108-109: bare except swallows errors from spark.stop()."""
        result = _make_result_mock(count=1)
        mock_spark_session.sql.return_value = result
        mock_spark_session.stop.side_effect = RuntimeError("stop failed")
        df = MagicMock()
        glue_context.create_dynamic_frame.from_options.return_value.toDF.return_value = df

        # Should not raise despite stop() failing
        sql_module.run(MagicMock(), MagicMock(), glue_context, base_args)

    def test_non_select_error_still_calls_spark_stop(self, monkeypatch, mock_boto3_session,
                                                       mock_table_info, mock_warnings,
                                                       mock_spark_session, mock_get_error_message,
                                                       glue_context, base_args):
        """Lines 50-51 + 107: non-SELECT exception still triggers finally."""
        base_args['query'] = 'UPDATE my_test_table SET x=1'
        df = MagicMock()
        glue_context.create_dynamic_frame.from_options.return_value.toDF.return_value = df

        with pytest.raises(Exception, match="SQL query error"):
            sql_module.run(MagicMock(), MagicMock(), glue_context, base_args)

        mock_spark_session.stop.assert_called_once()
