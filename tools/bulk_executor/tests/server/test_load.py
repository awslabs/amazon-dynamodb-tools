"""Unit tests for the `load` server-side verb.

Covers `python_modules/load/__init__.py`:
- read_data: format parsing (csv with bool/str options, json, parquet with
  int options, unknown format ValueError), set_bool_option / set_str_option /
  set_int_option closures, mappings application from S3
- run: S3 existence check early-return, throughput config wiring, DynamicFrame
  counting (zero items early-return), removeEmptyStringAttributes Map.apply,
  boto3.Session creation, print_dynamodb_table_info call, repartition + write,
  error wrapping via get_error_message
- check_s3_file_exists: URI regex parsing, head_object success, 404 prefix
  fallback (KeyCount > 0 and == 0), non-404 ClientError re-raise, invalid URI
- get_mappings_from_s3: valid URI fetch + JSON decode, invalid URI prefix,
  short path (no key), exception logging returns None
- remove_empty_fields: filters empty-string values, keeps non-empty
- check_dynamic_frame_avg_size: average calculation, empty items raises
- print_dynamodb_table_info: PROVISIONED billing cost path, PAY_PER_REQUEST
  billing cost path, WRU math (ceil(avg/1024) * num_items)
"""

import json
import math
import sys
from io import BytesIO
from unittest.mock import MagicMock, patch, call

import pytest

# awsglue.transforms is imported by load/__init__.py at collection time.
# The conftest mocks 'awsglue' as a Mock() but doesn't register the
# submodule path 'awsglue.transforms'. We must do so before importing load.
if 'awsglue.transforms' not in sys.modules:
    sys.modules['awsglue.transforms'] = MagicMock()

from python_modules import load as load_module

# The source does `from python_modules.shared.errors import *`. Since the
# conftest mocks shared.errors as Mock(), star-import resolves nothing useful
# into load's namespace. We inject get_error_message so line 118 doesn't
# NameError during tests.
if not hasattr(load_module, 'get_error_message'):
    load_module.get_error_message = lambda e: str(e)


# --- Fixtures ---------------------------------------------------------------

@pytest.fixture
def glue_context():
    """Mock GlueContext with create_dynamic_frame.from_options chain."""
    ctx = MagicMock()
    dynamic_frame = MagicMock()
    ctx.create_dynamic_frame.from_options.return_value = dynamic_frame
    return ctx


@pytest.fixture
def base_parsed_args():
    """Minimal args for run()."""
    return {
        'table': 'my-table',
        's3_path': 's3://bucket/data.csv',
        'format': 'csv',
    }


# --- read_data --------------------------------------------------------------

class TestReadDataCsvFormat:
    """read_data with format='csv' wires bool and str options correctly.

    The CSV/JSON/parquet READ path was NOT migrated to the glue_connector
    wrapper — read_data() still builds a DynamicFrame via
    glueContext.create_dynamic_frame.from_options. These tests assert against
    that current read path and pass unchanged.
    """

    def test_csv_defaults_withHeader_true(self, glue_context):
        """Line 45: csv sets withHeader default to True."""
        args = {'format': 'csv'}
        load_module.read_data(glue_context, 's3://b/k', args)
        call_kwargs = glue_context.create_dynamic_frame.from_options.call_args.kwargs
        assert call_kwargs['format_options']['withHeader'] is True

    def test_csv_withHeader_override_false(self, glue_context):
        """Line 25: explicit 'false' string overrides the bool default."""
        args = {'format': 'csv', 'withHeader': 'false'}
        load_module.read_data(glue_context, 's3://b/k', args)
        opts = glue_context.create_dynamic_frame.from_options.call_args.kwargs['format_options']
        assert opts['withHeader'] is False

    def test_csv_withHeader_override_true(self, glue_context):
        """Line 25: explicit 'true' string sets bool True."""
        args = {'format': 'csv', 'withHeader': 'true'}
        load_module.read_data(glue_context, 's3://b/k', args)
        opts = glue_context.create_dynamic_frame.from_options.call_args.kwargs['format_options']
        assert opts['withHeader'] is True

    def test_csv_multiLine_default_false(self, glue_context):
        """Line 46: multiLine has default=False (no arg -> False in options)."""
        args = {'format': 'csv'}
        load_module.read_data(glue_context, 's3://b/k', args)
        opts = glue_context.create_dynamic_frame.from_options.call_args.kwargs['format_options']
        assert opts['multiLine'] is False

    def test_csv_skipFirst_default_false(self, glue_context):
        """Line 47: skipFirst defaults to False."""
        args = {'format': 'csv'}
        load_module.read_data(glue_context, 's3://b/k', args)
        opts = glue_context.create_dynamic_frame.from_options.call_args.kwargs['format_options']
        assert opts['skipFirst'] is False

    def test_csv_separator_passed_through(self, glue_context):
        """Line 48: separator is a str option passed directly."""
        args = {'format': 'csv', 'separator': '|'}
        load_module.read_data(glue_context, 's3://b/k', args)
        opts = glue_context.create_dynamic_frame.from_options.call_args.kwargs['format_options']
        assert opts['separator'] == '|'

    def test_csv_escaper_passed_through(self, glue_context):
        """Line 49: escaper is a str option."""
        args = {'format': 'csv', 'escaper': '\\\\'}
        load_module.read_data(glue_context, 's3://b/k', args)
        opts = glue_context.create_dynamic_frame.from_options.call_args.kwargs['format_options']
        assert opts['escaper'] == '\\\\'

    def test_csv_quoteChar_passed_through(self, glue_context):
        """Line 50: quoteChar is a str option."""
        args = {'format': 'csv', 'quoteChar': "'"}
        load_module.read_data(glue_context, 's3://b/k', args)
        opts = glue_context.create_dynamic_frame.from_options.call_args.kwargs['format_options']
        assert opts['quoteChar'] == "'"

    def test_csv_str_option_not_set_when_absent(self, glue_context):
        """Line 29: str options only set when present in parsed_args."""
        args = {'format': 'csv'}
        load_module.read_data(glue_context, 's3://b/k', args)
        opts = glue_context.create_dynamic_frame.from_options.call_args.kwargs['format_options']
        assert 'separator' not in opts
        assert 'escaper' not in opts
        assert 'quoteChar' not in opts


class TestReadDataJsonFormat:
    """read_data with format='json'."""

    def test_json_multiline_default_false(self, glue_context):
        """Line 52: json has multiline (lowercase) bool option default False."""
        args = {'format': 'json'}
        load_module.read_data(glue_context, 's3://b/k', args)
        opts = glue_context.create_dynamic_frame.from_options.call_args.kwargs['format_options']
        assert opts['multiline'] is False

    def test_json_multiline_override_true(self, glue_context):
        """Line 52: explicit 'true' override."""
        args = {'format': 'json', 'multiline': 'true'}
        load_module.read_data(glue_context, 's3://b/k', args)
        opts = glue_context.create_dynamic_frame.from_options.call_args.kwargs['format_options']
        assert opts['multiline'] is True


class TestReadDataParquetFormat:
    """read_data with format='parquet' exercises str and int options."""

    def test_parquet_compression_str_option(self, glue_context):
        """Line 54: compression is a str option."""
        args = {'format': 'parquet', 'compression': 'snappy'}
        load_module.read_data(glue_context, 's3://b/k', args)
        opts = glue_context.create_dynamic_frame.from_options.call_args.kwargs['format_options']
        assert opts['compression'] == 'snappy'

    def test_parquet_blockSize_int_option(self, glue_context):
        """Line 55: blockSize parsed as integer."""
        args = {'format': 'parquet', 'blockSize': '1024'}
        load_module.read_data(glue_context, 's3://b/k', args)
        opts = glue_context.create_dynamic_frame.from_options.call_args.kwargs['format_options']
        assert opts['blockSize'] == 1024

    def test_parquet_pageSize_int_option(self, glue_context):
        """Line 56: pageSize parsed as integer."""
        args = {'format': 'parquet', 'pageSize': '512'}
        load_module.read_data(glue_context, 's3://b/k', args)
        opts = glue_context.create_dynamic_frame.from_options.call_args.kwargs['format_options']
        assert opts['pageSize'] == 512

    def test_parquet_invalid_int_raises_valueerror(self, glue_context):
        """Line 40: non-integer string for int option raises ValueError."""
        args = {'format': 'parquet', 'blockSize': 'abc'}
        with pytest.raises(ValueError, match="Invalid integer for blockSize"):
            load_module.read_data(glue_context, 's3://b/k', args)

    def test_parquet_no_options_when_absent(self, glue_context):
        """Lines 54-56: options only set when present."""
        args = {'format': 'parquet'}
        load_module.read_data(glue_context, 's3://b/k', args)
        opts = glue_context.create_dynamic_frame.from_options.call_args.kwargs['format_options']
        assert opts == {}


class TestReadDataUnknownFormat:
    """read_data raises ValueError for unrecognized format."""

    def test_unknown_format_raises(self, glue_context):
        """Line 58: format not in csv/json/parquet raises."""
        args = {'format': 'avro'}
        with pytest.raises(ValueError, match="Unexpected format"):
            load_module.read_data(glue_context, 's3://b/k', args)


class TestReadDataMappings:
    """read_data applies mappings from S3 when 'mappings' arg is present."""

    def test_mappings_applied_when_arg_present(self, monkeypatch, glue_context):
        """Line 70-71: when mappings arg present, apply_mapping is called."""
        fake_mappings = [('col1', 'string', 'col2', 'int')]
        monkeypatch.setattr(load_module, 'get_mappings_from_s3',
                            lambda uri: fake_mappings)
        args = {'format': 'json', 'mappings': 's3://b/mappings.json'}
        df = glue_context.create_dynamic_frame.from_options.return_value
        df.apply_mapping.return_value = df

        result = load_module.read_data(glue_context, 's3://b/k', args)
        df.apply_mapping.assert_called_once_with(fake_mappings)

    def test_no_mappings_when_arg_absent(self, glue_context):
        """Line 69-70: no apply_mapping call when mappings not in args."""
        args = {'format': 'json'}
        df = glue_context.create_dynamic_frame.from_options.return_value

        load_module.read_data(glue_context, 's3://b/k', args)
        df.apply_mapping.assert_not_called()


class TestReadDataGlueFrameCreation:
    """read_data passes the correct connection_type, paths, and format."""

    def test_creates_frame_with_correct_connection_options(self, glue_context):
        """Lines 62-67: verify from_options call arguments."""
        args = {'format': 'json'}
        load_module.read_data(glue_context, 's3://bucket/path', args)
        call_kwargs = glue_context.create_dynamic_frame.from_options.call_args.kwargs
        assert call_kwargs['connection_type'] == 's3'
        assert call_kwargs['connection_options'] == {'paths': ['s3://bucket/path']}
        assert call_kwargs['format'] == 'json'


# --- run() ------------------------------------------------------------------

class TestRunS3Check:
    """run() exits early if S3 file doesn't exist."""

    def test_returns_early_when_s3_file_missing(self, monkeypatch):
        """Line 79-81: check_s3_file_exists returns False -> early return."""
        monkeypatch.setattr(load_module, 'check_s3_file_exists', lambda uri: False)
        args = {'table': 't', 's3_path': 's3://b/missing'}
        result = load_module.run(MagicMock(), MagicMock(), MagicMock(), args)
        assert result is None


class TestRunDynamicFrameCount:
    """run() handles zero-count and count exception cases."""

    def test_returns_early_when_count_is_zero(self, monkeypatch):
        """Line 92-94: zero items -> early return with error log."""
        monkeypatch.setattr(load_module, 'check_s3_file_exists', lambda uri: True)
        monkeypatch.setattr(load_module, 'get_dynamodb_throughput_configs',
                            lambda *a, **kw: {})
        glue_ctx = MagicMock()
        df = MagicMock()
        df.count.return_value = 0
        monkeypatch.setattr(load_module, 'read_data', lambda *a: df)

        result = load_module.run(MagicMock(), MagicMock(), glue_ctx,
                                 {'table': 't', 's3_path': 's3://b/k', 'format': 'csv'})
        assert result is None

    def test_raises_on_count_exception(self, monkeypatch):
        """Line 99-100: exception during count() re-raises wrapped."""
        monkeypatch.setattr(load_module, 'check_s3_file_exists', lambda uri: True)
        monkeypatch.setattr(load_module, 'get_dynamodb_throughput_configs',
                            lambda *a, **kw: {})
        df = MagicMock()
        df.count.side_effect = RuntimeError('spark error')
        monkeypatch.setattr(load_module, 'read_data', lambda *a: df)

        with pytest.raises(Exception, match="Failed to create DynamicFrame"):
            load_module.run(MagicMock(), MagicMock(), MagicMock(),
                            {'table': 't', 's3_path': 's3://b/k', 'format': 'csv'})


class TestRunRemoveEmptyStrings:
    """run() applies Map.apply when removeEmptyStringAttributes is set."""

    def test_map_apply_called_when_flag_present(self, monkeypatch):
        """Line 102-104: Map.apply called with remove_empty_fields."""
        monkeypatch.setattr(load_module, 'check_s3_file_exists', lambda uri: True)
        monkeypatch.setattr(load_module, 'get_dynamodb_throughput_configs',
                            lambda *a, **kw: {})
        df = MagicMock()
        df.count.return_value = 5
        mapped_df = MagicMock()
        mapped_df.repartition.return_value = mapped_df

        map_mock = MagicMock(return_value=mapped_df)
        monkeypatch.setattr(load_module, 'Map', MagicMock(apply=map_mock))
        monkeypatch.setattr(load_module, 'read_data', lambda *a: df)
        monkeypatch.setattr(load_module, 'print_dynamodb_table_info', lambda *a: None)
        monkeypatch.setattr(load_module, 'check_dynamic_frame_avg_size', lambda df: 100.0)
        monkeypatch.setattr(load_module, 'boto3', MagicMock())

        glue_ctx = MagicMock()
        args = {'table': 't', 's3_path': 's3://b/k', 'format': 'csv',
                'removeEmptyStringAttributes': 'true'}
        load_module.run(MagicMock(), MagicMock(), glue_ctx, args)
        map_mock.assert_called_once()
        call_kwargs = map_mock.call_args.kwargs
        assert call_kwargs['f'] is load_module.remove_empty_fields

    def test_map_not_called_when_flag_absent(self, monkeypatch):
        """Line 102: no Map.apply when removeEmptyStringAttributes not in args."""
        monkeypatch.setattr(load_module, 'check_s3_file_exists', lambda uri: True)
        monkeypatch.setattr(load_module, 'get_dynamodb_throughput_configs',
                            lambda *a, **kw: {})
        df = MagicMock()
        df.count.return_value = 5
        df.repartition.return_value = df
        monkeypatch.setattr(load_module, 'read_data', lambda *a: df)
        monkeypatch.setattr(load_module, 'print_dynamodb_table_info', lambda *a: None)
        monkeypatch.setattr(load_module, 'check_dynamic_frame_avg_size', lambda df: 100.0)
        monkeypatch.setattr(load_module, 'boto3', MagicMock())

        map_mock = MagicMock()
        monkeypatch.setattr(load_module, 'Map', MagicMock(apply=map_mock))

        glue_ctx = MagicMock()
        args = {'table': 't', 's3_path': 's3://b/k', 'format': 'csv'}
        load_module.run(MagicMock(), MagicMock(), glue_ctx, args)
        map_mock.assert_not_called()


class TestRunWritePath:
    """run() repartitions and writes to DynamoDB via the glue_connector wrapper.

    The write path was migrated from a direct
    glueContext.write_dynamic_frame_from_options(connection_type='dynamodb', ...)
    call to write_dynamodb_dataframe(glue_context, dynamicFrame, table_name,
    parsed_args) from python_modules.shared.glue_connector. These tests assert
    against that wrapper boundary: load is responsible for repartition(30),
    calling the wrapper with the right arguments, and wrapping any failure with
    get_error_message. The wrapper's own behavior (DynamicFrame->DataFrame
    conversion, dynamodb.output.tableName, XMaxWriteRate throughput) is covered
    in tests/server/test_glue_connector.py.
    """

    def test_repartition_30_and_write(self, monkeypatch):
        """Lines 107-110: repartitions to 30 and calls the wrapper with the
        repartitioned frame, glue_context, table name, and parsed_args."""
        monkeypatch.setattr(load_module, 'check_s3_file_exists', lambda uri: True)
        df = MagicMock()
        df.count.return_value = 10
        repartitioned = MagicMock()
        df.repartition.return_value = repartitioned
        monkeypatch.setattr(load_module, 'read_data', lambda *a: df)
        monkeypatch.setattr(load_module, 'print_dynamodb_table_info', lambda *a: None)
        monkeypatch.setattr(load_module, 'check_dynamic_frame_avg_size', lambda df: 50.0)
        monkeypatch.setattr(load_module, 'boto3', MagicMock())
        write_mock = MagicMock()
        monkeypatch.setattr(load_module, 'write_dynamodb_dataframe', write_mock)

        glue_ctx = MagicMock()
        args = {'table': 'my-tbl', 's3_path': 's3://b/k', 'format': 'json'}
        load_module.run(MagicMock(), MagicMock(), glue_ctx, args)

        df.repartition.assert_called_once_with(30)
        repartitioned.toDF.assert_called_once()
        write_mock.assert_called_once_with(glue_ctx, repartitioned.toDF(), 'my-tbl', args)

    def test_write_error_wraps_with_get_error_message(self, monkeypatch):
        """Lines 111-112: wrapper exception is wrapped via get_error_message."""
        monkeypatch.setattr(load_module, 'check_s3_file_exists', lambda uri: True)
        df = MagicMock()
        df.count.return_value = 5
        df.repartition.return_value = df
        monkeypatch.setattr(load_module, 'read_data', lambda *a: df)
        monkeypatch.setattr(load_module, 'print_dynamodb_table_info', lambda *a: None)
        monkeypatch.setattr(load_module, 'check_dynamic_frame_avg_size', lambda df: 100.0)
        monkeypatch.setattr(load_module, 'boto3', MagicMock())
        monkeypatch.setattr(load_module, 'get_error_message',
                            lambda e: f"wrapped:{e}")
        write_mock = MagicMock(side_effect=RuntimeError('write fail'))
        monkeypatch.setattr(load_module, 'write_dynamodb_dataframe', write_mock)

        glue_ctx = MagicMock()
        args = {'table': 't', 's3_path': 's3://b/k', 'format': 'json'}

        with pytest.raises(Exception, match="Error in writing to table: wrapped:write fail"):
            load_module.run(MagicMock(), MagicMock(), glue_ctx, args)

    # DELETED: test_connection_options_include_table_name.
    # It asserted that load directly built a connection_options dict with
    # dynamodb.output.tableName for glueContext.write_dynamic_frame_from_options.
    # That legacy connection-options construction no longer exists in load — the
    # Glue 5.0 wrapper write_dynamodb_dataframe now owns setting
    # dynamodb.output.tableName internally. Coverage for that behavior lives in
    # tests/server/test_glue_connector.py::TestWriteDataFrame
    # ::test_uses_dataframe_write_format_dynamodb (asserts
    # opts['dynamodb.output.tableName'] == 'out-tbl'). The remaining contract
    # load still owns — passing the table name through to the wrapper — is
    # verified by test_repartition_30_and_write above.


# DELETED: TestRunThroughputConfigs::test_throughput_configs_called_with_write_modes.
# It asserted that run() called get_dynamodb_throughput_configs(args, table,
# modes=['write']) to compute write throughput options before handing them to
# the legacy DynamicFrame writer. After the Glue 5.0 migration, run() no longer
# computes or passes throughput configs — write_dynamodb_dataframe resolves
# write throughput internally from XMaxWriteRate (parsed_args), setting
# dynamodb.throughput.write on the DataFrame writer. run() does not call
# get_dynamodb_throughput_configs anymore (the import is now vestigial), so this
# behavior is genuinely gone from load. Coverage for write-throughput resolution
# now lives in tests/server/test_glue_connector.py::TestWriteDataFrame
# ::test_xmax_write_rate_passes_through_as_direct_int (asserts
# opts['dynamodb.throughput.write'] == '75000' from XMaxWriteRate).


class TestRunReportsWriteRate:
    """run() logs the configured write rate at startup when XMaxWriteRate is set."""

    def test_logs_write_rate_when_xmax_set(self, monkeypatch, caplog):
        """When XMaxWriteRate is in parsed_args, run() logs it before writing."""
        monkeypatch.setattr(load_module, 'check_s3_file_exists', lambda uri: True)
        df = MagicMock()
        df.count.return_value = 5
        df.repartition.return_value = df
        monkeypatch.setattr(load_module, 'read_data', lambda *a: df)
        monkeypatch.setattr(load_module, 'print_dynamodb_table_info', lambda *a: None)
        monkeypatch.setattr(load_module, 'check_dynamic_frame_avg_size', lambda df: 100.0)
        monkeypatch.setattr(load_module, 'boto3', MagicMock())
        monkeypatch.setattr(load_module, 'write_dynamodb_dataframe', MagicMock())

        args = {'table': 't', 's3_path': 's3://b/k', 'format': 'json',
                'XMaxWriteRate': '5000'}

        import logging
        with caplog.at_level(logging.INFO):
            load_module.run(MagicMock(), MagicMock(), MagicMock(), args)
        assert '5000' in caplog.text
        assert 'write' in caplog.text.lower()

    def test_no_write_rate_log_when_xmax_absent(self, monkeypatch, caplog):
        """When XMaxWriteRate is not in parsed_args, no write rate line is logged."""
        monkeypatch.setattr(load_module, 'check_s3_file_exists', lambda uri: True)
        df = MagicMock()
        df.count.return_value = 5
        df.repartition.return_value = df
        monkeypatch.setattr(load_module, 'read_data', lambda *a: df)
        monkeypatch.setattr(load_module, 'print_dynamodb_table_info', lambda *a: None)
        monkeypatch.setattr(load_module, 'check_dynamic_frame_avg_size', lambda df: 100.0)
        monkeypatch.setattr(load_module, 'boto3', MagicMock())
        monkeypatch.setattr(load_module, 'write_dynamodb_dataframe', MagicMock())

        args = {'table': 't', 's3_path': 's3://b/k', 'format': 'json'}

        import logging
        with caplog.at_level(logging.INFO):
            load_module.run(MagicMock(), MagicMock(), MagicMock(), args)
        assert 'write rate' not in caplog.text.lower()


# --- check_s3_file_exists ---------------------------------------------------

class TestCheckS3FileExists:
    """S3 file existence check with head_object and prefix fallback."""

    def test_valid_uri_file_exists(self, monkeypatch):
        """Line 144: head_object succeeds -> True."""
        s3_client = MagicMock()
        s3_client.head_object.return_value = {}
        monkeypatch.setattr(load_module.boto3, 'client', lambda svc: s3_client)
        assert load_module.check_s3_file_exists('s3://my-bucket/path/file.csv') is True

    def test_valid_uri_file_not_found_but_prefix_has_objects(self, monkeypatch):
        """Line 149-150: 404 then list_objects_v2 with KeyCount>0 -> True."""
        s3_client = MagicMock()
        error_response = {'Error': {'Code': '404'}}
        s3_client.head_object.side_effect = load_module.ClientError(error_response, 'HeadObject')
        s3_client.list_objects_v2.return_value = {'KeyCount': 3}
        monkeypatch.setattr(load_module.boto3, 'client', lambda svc: s3_client)
        assert load_module.check_s3_file_exists('s3://bucket/prefix/') is True
        s3_client.list_objects_v2.assert_called_once_with(
            Bucket='bucket', Prefix='prefix/', MaxKeys=1)

    def test_valid_uri_file_not_found_and_no_prefix_objects(self, monkeypatch):
        """Line 149-150: 404 then KeyCount==0 -> False."""
        s3_client = MagicMock()
        error_response = {'Error': {'Code': '404'}}
        s3_client.head_object.side_effect = load_module.ClientError(error_response, 'HeadObject')
        s3_client.list_objects_v2.return_value = {'KeyCount': 0}
        monkeypatch.setattr(load_module.boto3, 'client', lambda svc: s3_client)
        assert load_module.check_s3_file_exists('s3://bucket/key') is False

    def test_non_404_error_re_raised(self, monkeypatch):
        """Line 152-153: non-404 ClientError is re-raised."""
        s3_client = MagicMock()
        error_response = {'Error': {'Code': '403'}}
        s3_client.head_object.side_effect = load_module.ClientError(error_response, 'HeadObject')
        monkeypatch.setattr(load_module.boto3, 'client', lambda svc: s3_client)
        with pytest.raises(load_module.ClientError):
            load_module.check_s3_file_exists('s3://bucket/key')

    def test_invalid_uri_raises_valueerror(self, monkeypatch):
        """Line 134-135: URI not matching s3://bucket/key raises."""
        with pytest.raises(ValueError, match="Invalid S3 URI format"):
            load_module.check_s3_file_exists('not-an-s3-uri')

    def test_invalid_uri_no_key(self, monkeypatch):
        """Line 134: s3://bucket with no trailing path doesn't match regex."""
        with pytest.raises(ValueError, match="Invalid S3 URI format"):
            load_module.check_s3_file_exists('s3://bucket-only')


# --- get_mappings_from_s3 ---------------------------------------------------

class TestGetMappingsFromS3:
    """Fetches JSON mappings from S3 and converts arrays to tuples."""

    def test_valid_uri_returns_tuples(self, monkeypatch):
        """Lines 169-177: fetches JSON, converts each mapping array to tuple."""
        body_content = json.dumps({
            'mappings': [['col1', 'string', 'col2', 'int'], ['a', 'b', 'c', 'd']]
        }).encode('utf-8')
        body_mock = MagicMock()
        body_mock.read.return_value = body_content

        s3_client = MagicMock()
        s3_client.get_object.return_value = {'Body': body_mock}
        monkeypatch.setattr(load_module.boto3, 'client', lambda svc: s3_client)

        result = load_module.get_mappings_from_s3('s3://my-bucket/mappings.json')
        assert result == [('col1', 'string', 'col2', 'int'), ('a', 'b', 'c', 'd')]
        s3_client.get_object.assert_called_once_with(Bucket='my-bucket', Key='mappings.json')

    def test_invalid_uri_prefix_returns_none(self, monkeypatch):
        """Line 161-162: URI not starting with s3:// raises internally, caught -> None."""
        s3_client = MagicMock()
        monkeypatch.setattr(load_module.boto3, 'client', lambda svc: s3_client)
        result = load_module.get_mappings_from_s3('http://wrong/path')
        assert result is None

    def test_short_uri_no_key(self, monkeypatch):
        """Line 166: path_parts has no second element -> key_name is empty string."""
        body_content = json.dumps({'mappings': [['x', 'y']]}).encode('utf-8')
        body_mock = MagicMock()
        body_mock.read.return_value = body_content

        s3_client = MagicMock()
        s3_client.get_object.return_value = {'Body': body_mock}
        monkeypatch.setattr(load_module.boto3, 'client', lambda svc: s3_client)

        result = load_module.get_mappings_from_s3('s3://bucket')
        assert result == [('x', 'y')]
        s3_client.get_object.assert_called_once_with(Bucket='bucket', Key='')

    def test_s3_exception_returns_none(self, monkeypatch):
        """Line 178-180: any exception during fetch logs and returns None."""
        s3_client = MagicMock()
        s3_client.get_object.side_effect = RuntimeError('network error')
        monkeypatch.setattr(load_module.boto3, 'client', lambda svc: s3_client)
        result = load_module.get_mappings_from_s3('s3://bucket/key.json')
        assert result is None


# --- remove_empty_fields ----------------------------------------------------

class TestRemoveEmptyFields:
    """Filters out empty-string values from a record dict."""

    def test_removes_empty_strings(self):
        """Line 183: empty string values are removed."""
        rec = {'a': 'hello', 'b': '', 'c': 'world', 'd': ''}
        result = load_module.remove_empty_fields(rec)
        assert result == {'a': 'hello', 'c': 'world'}

    def test_keeps_all_non_empty(self):
        """Line 183: non-empty values retained including zero and None."""
        rec = {'a': 0, 'b': None, 'c': 'x', 'd': False}
        result = load_module.remove_empty_fields(rec)
        assert result == {'a': 0, 'b': None, 'c': 'x', 'd': False}

    def test_empty_dict_stays_empty(self):
        """Line 183: empty input returns empty dict."""
        assert load_module.remove_empty_fields({}) == {}


# --- check_dynamic_frame_avg_size -------------------------------------------

class TestCheckDynamicFrameAvgSize:
    """Calculates average item size from a sampled DynamicFrame."""

    def test_returns_average_size(self):
        """Lines 196-206: sums json size of items, divides by count."""
        item1 = MagicMock()
        item1.asDict.return_value = {'key': 'value'}
        item2 = MagicMock()
        item2.asDict.return_value = {'key': 'longer_value_here'}

        sample_df = MagicMock()
        sample_df.collect.return_value = [item1, item2]

        dynamic_frame = MagicMock()
        dynamic_frame.toDF.return_value.limit.return_value = sample_df

        result = load_module.check_dynamic_frame_avg_size(dynamic_frame)

        import sys as _sys
        size1 = _sys.getsizeof(json.dumps({'key': 'value'}))
        size2 = _sys.getsizeof(json.dumps({'key': 'longer_value_here'}))
        expected = (size1 + size2) / 2
        assert result == expected

    def test_raises_when_no_items(self):
        """Line 210: empty items list raises Exception."""
        sample_df = MagicMock()
        sample_df.collect.return_value = []
        dynamic_frame = MagicMock()
        dynamic_frame.toDF.return_value.limit.return_value = sample_df

        with pytest.raises(Exception, match="can't determine an average size"):
            load_module.check_dynamic_frame_avg_size(dynamic_frame)

    def test_limit_is_100(self):
        """Line 189: sample_frame limits to 100 items."""
        sample_df = MagicMock()
        sample_df.collect.return_value = []
        dynamic_frame = MagicMock()
        to_df_mock = MagicMock()
        to_df_mock.limit.return_value = sample_df
        dynamic_frame.toDF.return_value = to_df_mock

        with pytest.raises(Exception):
            load_module.check_dynamic_frame_avg_size(dynamic_frame)
        to_df_mock.limit.assert_called_once_with(100)


# --- print_dynamodb_table_info ----------------------------------------------

class TestPrintDynamodbTableInfoProvisioned:
    """Cost calculation for PROVISIONED billing mode."""

    def test_provisioned_cost_output(self, monkeypatch):
        """Lines 216-231: PROVISIONED path prints prov_cost = od_cost / 1.5."""
        session = MagicMock()
        session.region_name = 'us-east-1'
        monkeypatch.setattr(load_module, 'get_and_print_dynamodb_table_info',
                            lambda t: {'billing_mode': 'PROVISIONED',
                                       'write_pricing_category': 'wru'})
        pricing_inst = MagicMock()
        pricing_inst.get_on_demand_capacity_pricing.return_value = {'wru': '0.00125'}
        monkeypatch.setattr(load_module, 'PricingUtility', lambda: pricing_inst)

        # avg_size=2048 -> ceil(2048/1024) = 2 WRUs per item
        # num_items=100 -> write_units = 200
        # od_cost = 200 * 0.00125 = 0.25
        # prov_cost = 0.25 / 1.5 = 0.1666...
        load_module.print_dynamodb_table_info(session, 'tbl', 100, 2048.0)

        pricing_inst.get_on_demand_capacity_pricing.assert_called_once_with('us-east-1')

    def test_provisioned_wru_math(self, monkeypatch, caplog):
        """Line 216: avg_write_units_per_item = ceil(avg_size / 1024)."""
        session = MagicMock()
        session.region_name = 'us-west-2'
        monkeypatch.setattr(load_module, 'get_and_print_dynamodb_table_info',
                            lambda t: {'billing_mode': 'PROVISIONED',
                                       'write_pricing_category': 'cat'})
        pricing_inst = MagicMock()
        pricing_inst.get_on_demand_capacity_pricing.return_value = {'cat': '0.001'}
        monkeypatch.setattr(load_module, 'PricingUtility', lambda: pricing_inst)

        # avg_size=1025 -> ceil(1025/1024) = 2
        # num_items=50 -> write_units = 100
        import logging
        with caplog.at_level(logging.INFO, logger='load_export'):
            load_module.print_dynamodb_table_info(session, 'tbl', 50, 1025.0)
        assert '100' in caplog.text, "write_units = 50 * 2 = 100"


class TestPrintDynamodbTableInfoOnDemand:
    """Cost calculation for PAY_PER_REQUEST billing mode."""

    def test_ondemand_cost_output(self, monkeypatch, caplog):
        """Lines 232-233: PAY_PER_REQUEST path prints od_cost."""
        session = MagicMock()
        session.region_name = 'eu-west-1'
        monkeypatch.setattr(load_module, 'get_and_print_dynamodb_table_info',
                            lambda t: {'billing_mode': 'PAY_PER_REQUEST',
                                       'write_pricing_category': 'wru'})
        pricing_inst = MagicMock()
        pricing_inst.get_on_demand_capacity_pricing.return_value = {'wru': '0.002'}
        monkeypatch.setattr(load_module, 'PricingUtility', lambda: pricing_inst)

        # avg_size=512 -> ceil(512/1024)=1 WRU per item
        # num_items=1000 -> write_units=1000
        # od_cost = 1000 * 0.002 = 2.0
        import logging
        with caplog.at_level(logging.INFO, logger='load_export'):
            load_module.print_dynamodb_table_info(session, 'tbl', 1000, 512.0)
        assert '$2.00' in caplog.text

    def test_ondemand_uses_correct_pricing_key(self, monkeypatch):
        """Line 221: uses table_info['write_pricing_category'] as dict key."""
        session = MagicMock()
        session.region_name = 'us-east-1'
        monkeypatch.setattr(load_module, 'get_and_print_dynamodb_table_info',
                            lambda t: {'billing_mode': 'PAY_PER_REQUEST',
                                       'write_pricing_category': 'custom_key'})
        pricing_inst = MagicMock()
        pricing_inst.get_on_demand_capacity_pricing.return_value = {'custom_key': '0.001'}
        monkeypatch.setattr(load_module, 'PricingUtility', lambda: pricing_inst)

        load_module.print_dynamodb_table_info(session, 'tbl', 10, 100.0)
        pricing_inst.get_on_demand_capacity_pricing.assert_called_once_with('us-east-1')


class TestPrintDynamodbTableInfoEdgeCases:
    """Edge cases in the cost calculation."""

    def test_avg_size_less_than_1024_rounds_to_1_wru(self, monkeypatch, caplog):
        """Line 216: ceil(500/1024) = 1."""
        session = MagicMock()
        session.region_name = 'us-east-1'
        monkeypatch.setattr(load_module, 'get_and_print_dynamodb_table_info',
                            lambda t: {'billing_mode': 'PAY_PER_REQUEST',
                                       'write_pricing_category': 'w'})
        pricing_inst = MagicMock()
        pricing_inst.get_on_demand_capacity_pricing.return_value = {'w': '0.001'}
        monkeypatch.setattr(load_module, 'PricingUtility', lambda: pricing_inst)

        # avg_size=500 -> ceil(500/1024) = 1
        # num_items=10 -> write_units=10
        # od_cost = 10 * 0.001 = 0.01
        import logging
        with caplog.at_level(logging.INFO, logger='load_export'):
            load_module.print_dynamodb_table_info(session, 't', 10, 500.0)
        assert 'Write units required' in caplog.text
        assert '10' in caplog.text, "write_units = 10 * 1 = 10"

    def test_exact_1024_is_1_wru(self, monkeypatch, caplog):
        """Line 216: ceil(1024/1024) = 1 exactly."""
        session = MagicMock()
        session.region_name = 'us-east-1'
        monkeypatch.setattr(load_module, 'get_and_print_dynamodb_table_info',
                            lambda t: {'billing_mode': 'PAY_PER_REQUEST',
                                       'write_pricing_category': 'w'})
        pricing_inst = MagicMock()
        pricing_inst.get_on_demand_capacity_pricing.return_value = {'w': '0.01'}
        monkeypatch.setattr(load_module, 'PricingUtility', lambda: pricing_inst)

        # avg_size=1024 -> ceil(1024/1024) = 1
        # num_items=5 -> write_units=5
        # od_cost = 5 * 0.01 = 0.05
        import logging
        with caplog.at_level(logging.INFO, logger='load_export'):
            load_module.print_dynamodb_table_info(session, 't', 5, 1024.0)
        assert '$0.05' in caplog.text

    def test_unknown_billing_mode_skips_cost_line(self, monkeypatch, caplog):
        """Line 232->234: billing_mode not PROVISIONED or PAY_PER_REQUEST skips both cost logs."""
        session = MagicMock()
        session.region_name = 'us-east-1'
        monkeypatch.setattr(load_module, 'get_and_print_dynamodb_table_info',
                            lambda t: {'billing_mode': 'UNKNOWN_MODE',
                                       'write_pricing_category': 'w'})
        pricing_inst = MagicMock()
        pricing_inst.get_on_demand_capacity_pricing.return_value = {'w': '0.001'}
        monkeypatch.setattr(load_module, 'PricingUtility', lambda: pricing_inst)

        import logging
        with caplog.at_level(logging.INFO, logger='load_export'):
            load_module.print_dynamodb_table_info(session, 't', 10, 500.0)
        assert 'Approx DynamoDB cost' not in caplog.text, \
            "neither PROVISIONED nor PAY_PER_REQUEST cost line is printed"
