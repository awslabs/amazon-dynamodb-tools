"""Unit tests for shared/glue_connector.py.

Exercises the DataFrame-based connector wrapper. The legacy DynamicFrame
path was removed when we standardized on Glue 5.0 + the new DynamoDB
DataFrame source -- see PR #162.
"""

import importlib.util
import logging
import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest


# Real modules; conftest mocks shared, so substitute the real ones.
sys.modules.pop('python_modules.shared.glue_connector', None)
sys.modules.pop('shared.glue_connector', None)

_REPO_ROOT = Path(__file__).resolve().parents[2] / "server/src"


def _load_real(module_path: str, file_path: Path):
    spec = importlib.util.spec_from_file_location(module_path, str(file_path))
    mod = importlib.util.module_from_spec(spec)
    sys.modules[module_path] = mod
    spec.loader.exec_module(mod)
    return mod


glue_connector = _load_real(
    "python_modules.shared.glue_connector",
    _REPO_ROOT / "python_modules/shared/glue_connector.py",
)


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
        # Without XMaxReadRate, no direct throughput option should be set --
        # let the connector use its 0.5 ratio default.
        assert 'dynamodb.throughput.read' not in opts

    def test_xmax_read_rate_passes_through_as_direct_int(self, glue_context):
        glue_connector.read_dynamodb_dataframe(
            glue_context, 't', {'XMaxReadRate': 12345}, splits=200
        )
        reader = glue_context.spark_session.read.format.return_value
        opts = {args[0]: args[1] for args, _ in reader.option.call_args_list}
        assert opts['dynamodb.throughput.read'] == '12345'

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
            frame=df,
            table_name='out-tbl',
            parsed_args={},
        )
        df.write.format.assert_called_with('dynamodb')
        # Glue 5.0 connector rejects the default ErrorIfExists save mode;
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
            frame=df,
            table_name='out-tbl',
            parsed_args={'XMaxWriteRate': 75000},
        )
        opts = {args[0]: args[1] for args, _ in writer.option.call_args_list}
        # Critical for issue #145: direct WCU passthrough kills the 60k
        # WRU ceiling that the percent-based legacy connector imposed on
        # on-demand tables.
        assert opts['dynamodb.throughput.write'] == '75000'

    def test_dynamic_frame_input_is_converted_to_dataframe(self):
        # A *realistic* Glue DynamicFrame: it exposes toDF() AND carries
        # write/schema/write_dynamic_frame attributes (its .write is a
        # method, not a DataFrameWriter property). The wrapper must detect
        # it by toDF() and convert before writing — regression guard for
        # the Glue 5.0 load failure "'function' object has no attribute
        # 'format'", where a DynamicFrame was misclassified as a DataFrame
        # and df.write.format(...) blew up. We give .write a side effect
        # that raises if .format is accessed, so a missed conversion fails
        # loudly here instead of only at real Glue runtime.
        dynamic_frame = MagicMock()
        dynamic_frame.toDF = MagicMock()
        dynamic_frame.write = MagicMock()
        dynamic_frame.write.format.side_effect = AssertionError(
            "df.write.format called on the DynamicFrame — it was not "
            "converted via toDF() first"
        )
        dynamic_frame.write_dynamic_frame = MagicMock()
        dynamic_frame.schema = MagicMock()

        df = MagicMock()
        df.schema = MagicMock()
        writer = MagicMock()
        writer.option.return_value = writer
        writer.mode.return_value = writer
        df.write.format.return_value = writer
        dynamic_frame.toDF.return_value = df

        glue_connector.write_dynamodb_dataframe(
            glue_context=MagicMock(),
            frame=dynamic_frame,
            table_name='tbl',
            parsed_args={},
        )
        dynamic_frame.toDF.assert_called_once()
        df.write.format.assert_called_with('dynamodb')

    def test_real_dataframe_is_not_converted(self):
        # A Spark DataFrame has no toDF(); the wrapper must use it directly
        # and NOT attempt a conversion. spec= without 'toDF' makes hasattr
        # (frame, 'toDF') False, matching a real DataFrame.
        df = MagicMock(spec=['write', 'schema'])
        df.schema = MagicMock()
        writer = MagicMock()
        writer.option.return_value = writer
        writer.mode.return_value = writer
        df.write.format.return_value = writer

        glue_connector.write_dynamodb_dataframe(
            glue_context=MagicMock(),
            frame=df,
            table_name='tbl',
            parsed_args={},
        )
        df.write.format.assert_called_with('dynamodb')
        writer.save.assert_called_once()


# --- count_dynamodb_table -------------------------------------------------


class TestCountDynamoDBTable:
    def test_returns_dataframe_count(self, glue_context):
        # The reader.load() result is the DataFrame; .count() is what the
        # wrapper calls and the value should propagate back.
        df = glue_context.spark_session.read.format.return_value.load.return_value
        df.count.return_value = 42

        result = glue_connector.count_dynamodb_table(
            glue_context, 't', {}, splits=200
        )

        assert result == 42
        df.count.assert_called_once()

    def test_count_uses_same_read_path(self, glue_context):
        # Sanity: count piggybacks on read_dynamodb_dataframe so the same
        # options/format calls happen.
        glue_connector.count_dynamodb_table(
            glue_context, 'metric-table', {}, splits=200
        )
        glue_context.spark_session.read.format.assert_called_with('dynamodb')
        reader = glue_context.spark_session.read.format.return_value
        opts = {args[0]: args[1] for args, _ in reader.option.call_args_list}
        assert opts['dynamodb.input.tableName'] == 'metric-table'


# --- Logging --------------------------------------------------------------


class TestLogging:
    def test_read_logs_table_and_elapsed(self, glue_context, caplog):
        caplog.set_level(logging.INFO)
        glue_connector.read_dynamodb_dataframe(
            glue_context, 'metrics-table', {}, splits=200
        )
        msgs = ' '.join(r.getMessage() for r in caplog.records)
        assert '[connector]' in msgs
        assert 'metrics-table' in msgs
        # Elapsed time is logged with 3 decimals + 's' suffix.
        assert 's' in msgs

    def test_write_logs_table_and_elapsed(self, caplog):
        df = MagicMock(spec=['write', 'schema'])
        df.schema = MagicMock()
        writer = MagicMock()
        writer.option.return_value = writer
        writer.mode.return_value = writer
        df.write.format.return_value = writer

        caplog.set_level(logging.INFO)
        glue_connector.write_dynamodb_dataframe(
            glue_context=MagicMock(),
            frame=df,
            table_name='sink-table',
            parsed_args={},
        )
        msgs = ' '.join(r.getMessage() for r in caplog.records)
        assert '[connector]' in msgs
        assert 'sink-table' in msgs

    def test_count_logs_table_and_elapsed(self, glue_context, caplog):
        caplog.set_level(logging.INFO)
        glue_connector.count_dynamodb_table(
            glue_context, 'count-table', {}, splits=200
        )
        msgs = ' '.join(r.getMessage() for r in caplog.records)
        # count() emits both the read-setup log and the count-completed log.
        assert msgs.count('[connector]') >= 2
        assert 'count-table' in msgs
