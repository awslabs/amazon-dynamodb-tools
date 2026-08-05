"""Unit tests for shared/glue_connector.py.

Exercises the DataFrame-based connector wrapper. The legacy DynamicFrame
path was removed when we standardized on Glue 5.x + the new DynamoDB
DataFrame source -- see PR #162.
"""

import importlib.util
import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest


_REPO_ROOT = Path(__file__).resolve().parents[2] / "server/src"


def _load_real(module_path: str, file_path: Path):
    """Load the real module for this test file only.

    conftest installs a Mock at ``python_modules.shared.glue_connector`` so
    verb-side tests can import the wrapper as a black box. We need the real
    implementation here, but must NOT leave it in sys.modules — doing so
    swaps the real (describe_table-calling) get_dynamodb_throughput_configs
    into every downstream test that imports a verb (test_sql, test_load, ...).
    So we set it only for the duration of exec_module, then restore whatever
    conftest had there.
    """
    saved = {name: sys.modules.get(name)
             for name in (module_path, 'shared.glue_connector')}
    spec = importlib.util.spec_from_file_location(module_path, str(file_path))
    mod = importlib.util.module_from_spec(spec)
    sys.modules[module_path] = mod
    try:
        spec.loader.exec_module(mod)
    finally:
        for name, original in saved.items():
            if original is None:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = original
    return mod


glue_connector = _load_real(
    "python_modules.shared.glue_connector",
    _REPO_ROOT / "python_modules/shared/glue_connector.py",
)


@pytest.fixture(autouse=True)
def throughput_configs(monkeypatch):
    """Replace get_dynamodb_throughput_configs with a controllable MagicMock.

    Rate resolution + logging is the responsibility of
    get_dynamodb_throughput_configs (covered by test_table_info.py); the
    wrapper's only job is to apply whatever connector options it returns.
    Tests set `.return_value` to the connector-format dict they want applied.
    Defaults to {} (no throughput override). Autouse so no test hits the real
    describe_table.
    """
    mock = MagicMock(return_value={})
    monkeypatch.setattr(glue_connector, 'get_dynamodb_throughput_configs', mock)
    return mock


# --- Fixtures ---------------------------------------------------------------


@pytest.fixture
def glue_context():
    """GlueContext exposing spark_session.read.format('dynamodb') chain."""
    ctx = MagicMock()
    df = MagicMock()
    df.write = MagicMock()
    df.schema = MagicMock()
    reader = MagicMock()
    reader.option.return_value = reader
    reader.load.return_value = df
    ctx.spark_session.read.format.return_value = reader
    return ctx


# --- read_dynamodb_dataframe ----------------------------------------------


class TestReadDataFrame:
    def test_uses_format_dynamodb(self, glue_context):
        glue_connector.read_dynamodb_dataframe(
            glue_context, 't', {}, splits=200
        )
        glue_context.spark_session.read.format.assert_called_with('dynamodb')

    def test_options_set_on_reader(self, glue_context):
        glue_connector.read_dynamodb_dataframe(
            glue_context, 'my-table', {}, splits=300
        )
        reader = glue_context.spark_session.read.format.return_value
        opts = {args[0]: args[1] for args, _ in reader.option.call_args_list}
        assert opts['dynamodb.input.tableName'] == 'my-table'
        assert opts['dynamodb.splits'] == '300'
        assert opts['dynamodb.consistentRead'] == 'false'

    def test_read_rate_resolved_via_throughput_configs(self, glue_context, throughput_configs):
        # Issue #236: even without --XMaxReadRate, the wrapper delegates to
        # get_dynamodb_throughput_configs, which resolves the rate from the
        # table (quota/provisioned/on-demand) AND logs it. Whatever connector
        # options it returns must be applied to the reader.
        throughput_configs.return_value = {'dynamodb.throughput.read': '120000'}
        glue_connector.read_dynamodb_dataframe(
            glue_context, 't', {}, splits=200
        )
        throughput_configs.assert_called_once_with(
            {}, 't', modes=['read'], format='connector'
        )
        reader = glue_context.spark_session.read.format.return_value
        opts = {args[0]: args[1] for args, _ in reader.option.call_args_list}
        assert opts['dynamodb.throughput.read'] == '120000'

    def test_no_throughput_option_when_configs_empty(self, glue_context, throughput_configs):
        # When the resolver returns nothing (e.g. describe/quota lookups all
        # failed), the wrapper sets no throughput option and lets the connector
        # fall back to its own 0.5 ratio default.
        throughput_configs.return_value = {}
        glue_connector.read_dynamodb_dataframe(
            glue_context, 't', {}, splits=200
        )
        reader = glue_context.spark_session.read.format.return_value
        opts = {args[0]: args[1] for args, _ in reader.option.call_args_list}
        assert 'dynamodb.throughput.read' not in opts

    def test_load_is_called_and_dataframe_returned(self, glue_context):
        result = glue_connector.read_dynamodb_dataframe(
            glue_context, 't', {}, splits=200
        )
        reader = glue_context.spark_session.read.format.return_value
        reader.load.assert_called_once()
        assert result == reader.load.return_value


# --- write_dynamodb_dataframe ---------------------------------------------


class TestWriteDataFrame:
    def test_uses_dataframe_write_format_dynamodb(self):
        # Realistic Spark DataFrame: has write/schema but NO toDF() (that's
        # a DynamicFrame method). spec= enforces the absence so the wrapper
        # uses the frame directly without attempting a conversion.
        df = MagicMock(spec=['write', 'schema'])
        df.schema = MagicMock()
        writer = MagicMock()
        writer.option.return_value = writer
        writer.mode.return_value = writer
        df.write.format.return_value = writer

        glue_connector.write_dynamodb_dataframe(
            glue_context=MagicMock(),
            df=df,
            table_name='out-tbl',
            parsed_args={},
        )
        df.write.format.assert_called_with('dynamodb')
        # Glue 5.x connector rejects the default ErrorIfExists save mode;
        # DynamoDB upserts require Append. Regression guard for the
        # "cannot be written with ErrorIfExists mode" failure.
        writer.mode.assert_called_with('append')
        writer.save.assert_called_once()
        opts = {args[0]: args[1] for args, _ in writer.option.call_args_list}
        assert opts['dynamodb.output.tableName'] == 'out-tbl'
        # No XMaxWriteRate set → no direct throughput option, let connector
        # use its 0.5 ratio default.
        assert 'dynamodb.throughput.write' not in opts

    def test_xmax_write_rate_passes_through_as_direct_int(self):
        df = MagicMock(spec=['write', 'schema'])
        df.schema = MagicMock()
        writer = MagicMock()
        writer.option.return_value = writer
        writer.mode.return_value = writer
        df.write.format.return_value = writer

        glue_connector.write_dynamodb_dataframe(
            glue_context=MagicMock(),
            df=df,
            table_name='out-tbl',
            parsed_args={'XMaxWriteRate': 75000},
        )
        opts = {args[0]: args[1] for args, _ in writer.option.call_args_list}
        # Critical for issue #145: direct WCU passthrough kills the 60k
        # WRU ceiling that the percent-based legacy connector imposed on
        # on-demand tables.
        assert opts['dynamodb.throughput.write'] == '75000'

    def test_write_rate_above_legacy_60k_ceiling_passes_through(self):
        # The legacy connector hard-capped on-demand writes at 60k WCU/s
        # (40k assumption x the 1.5x percent cap). The DataFrame connector
        # takes dynamodb.throughput.write as an absolute integer, so a rate
        # above 60k must pass straight through unclamped. This is the cheap,
        # offline mirror of tests/e2e/whole_system/test_load_exceeds_legacy_
        # ceiling.py, which proves the same 120k rate is *sustained* on real
        # Glue; keep the two values in sync.
        df = MagicMock(spec=['write', 'schema'])
        df.schema = MagicMock()
        writer = MagicMock()
        writer.option.return_value = writer
        writer.mode.return_value = writer
        df.write.format.return_value = writer

        glue_connector.write_dynamodb_dataframe(
            glue_context=MagicMock(),
            df=df,
            table_name='out-tbl',
            parsed_args={'XMaxWriteRate': 120000},
        )
        opts = {args[0]: args[1] for args, _ in writer.option.call_args_list}
        assert opts['dynamodb.throughput.write'] == '120000'

    def test_explicit_write_rate_overrides_parsed_args(self):
        df = MagicMock(spec=['write', 'schema'])
        df.schema = MagicMock()
        writer = MagicMock()
        writer.option.return_value = writer
        writer.mode.return_value = writer
        df.write.format.return_value = writer

        glue_connector.write_dynamodb_dataframe(
            glue_context=MagicMock(),
            df=df,
            table_name='out-tbl',
            parsed_args={'XMaxWriteRate': 75000},
            write_rate=40000,
        )
        opts = {args[0]: args[1] for args, _ in writer.option.call_args_list}
        assert opts['dynamodb.throughput.write'] == '40000'

    def test_write_rate_none_falls_back_to_parsed_args(self):
        df = MagicMock(spec=['write', 'schema'])
        df.schema = MagicMock()
        writer = MagicMock()
        writer.option.return_value = writer
        writer.mode.return_value = writer
        df.write.format.return_value = writer

        glue_connector.write_dynamodb_dataframe(
            glue_context=MagicMock(),
            df=df,
            table_name='out-tbl',
            parsed_args={'XMaxWriteRate': 75000},
            write_rate=None,
        )
        opts = {args[0]: args[1] for args, _ in writer.option.call_args_list}
        assert opts['dynamodb.throughput.write'] == '75000'

    def test_dataframe_written_with_save(self):
        df = MagicMock(spec=['write', 'schema'])
        df.schema = MagicMock()
        writer = MagicMock()
        writer.option.return_value = writer
        writer.mode.return_value = writer
        df.write.format.return_value = writer

        glue_connector.write_dynamodb_dataframe(
            glue_context=MagicMock(),
            df=df,
            table_name='tbl',
            parsed_args={},
        )
        df.write.format.assert_called_with('dynamodb')
        writer.save.assert_called_once()


