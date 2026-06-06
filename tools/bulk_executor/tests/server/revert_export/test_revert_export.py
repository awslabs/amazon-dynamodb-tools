"""Unit tests for the revert_export verb."""

import pytest
from unittest.mock import Mock, patch

from python_modules.revert_export import run, _post_validate, _revert
from python_modules.shared.bulk_executor_error import BulkExecutorError
from python_modules.shared.export.pipeline import _apply_transform_and_resolve
from python_modules.shared.export.utils.enums import ExportLoadType, Operation
from python_modules.shared.export.parsers.records import IncrementalExportRecord


@pytest.fixture
def mock_spark_context():
    sc = Mock()
    sc.defaultParallelism = 4
    sc.accumulator = Mock(side_effect=lambda init, *args: Mock(value=init))
    return sc


@pytest.fixture
def mock_glue_context():
    return Mock()


@pytest.fixture
def mock_job():
    return Mock()


@pytest.fixture
def parsed_args():
    return {
        'table': 'my-table',
        's3_path': 's3://my-bucket/prefix/AWSDynamoDB/export-001',
        'transform': None,
        's3-bucket-name': 'config-bucket',
        'JOB_RUN_ID': 'jr_123',
        'XDebug': 'false',
    }


@pytest.fixture
def parsed_args_with_transform():
    return {
        'table': 'my-table',
        's3_path': 's3://my-bucket/prefix/AWSDynamoDB/export-001',
        'transform': 'my_filter',
        's3-bucket-name': 'config-bucket',
        'JOB_RUN_ID': 'jr_123',
        'XDebug': 'false',
    }


@pytest.fixture
def key_schema():
    return {'pk': {'name': 'pk_attr', 'type': 'S'}, 'sk': {'name': 'sk_attr', 'type': 'S'}}


@pytest.fixture
def manifest_data_incremental_new_and_old():
    return {
        'total_item_count': 100,
        'data_files': [
            {'dataFileS3Key': 'data/file1.json.gz', 'itemCount': 100},
        ],
        'export_type': 'INCREMENTAL_EXPORT',
        'output_view': 'NEW_AND_OLD_IMAGES',
        'output_format': 'DYNAMODB_JSON',
    }


@pytest.fixture
def manifest_data_incremental_new_image():
    return {
        'total_item_count': 100,
        'data_files': [
            {'dataFileS3Key': 'data/file1.json.gz', 'itemCount': 100},
        ],
        'export_type': 'INCREMENTAL_EXPORT',
        'output_view': 'NEW_IMAGE',
        'output_format': 'DYNAMODB_JSON',
    }


@pytest.fixture
def manifest_data_full():
    return {
        'total_item_count': 100,
        'data_files': [
            {'dataFileS3Key': 'data/file1.json.gz', 'itemCount': 100},
        ],
        'export_type': 'FULL_EXPORT',
        'output_format': 'DYNAMODB_JSON',
    }


@pytest.fixture
def table_info():
    return {
        'key_schema': {'pk': {'name': 'pk_attr', 'type': 'S'}, 'sk': {'name': 'sk_attr', 'type': 'S'}},
        'table_name': 'my-table',
        'billing_mode': 'PAY_PER_REQUEST',
    }


class TestFailFastValidation:
    @patch('python_modules.shared.export.pipeline.validate')
    def test_fails_on_full_export(self, mock_validate, mock_spark_context, mock_glue_context, mock_job, parsed_args, table_info, key_schema, manifest_data_full):
        mock_validate.return_value = {
            'table_info': table_info,
            'key_schema': key_schema,
            'manifest_data': manifest_data_full,
            'key_schema_result': {'avg_item_size': 200},
        }

        with pytest.raises(BulkExecutorError, match="revert-export requires an incremental export"):
            run(mock_job, mock_spark_context, mock_glue_context, parsed_args)

    @patch('python_modules.shared.export.pipeline.validate')
    def test_fails_on_new_image_only(self, mock_validate, mock_spark_context, mock_glue_context, mock_job, parsed_args, table_info, key_schema, manifest_data_incremental_new_image):
        mock_validate.return_value = {
            'table_info': table_info,
            'key_schema': key_schema,
            'manifest_data': manifest_data_incremental_new_image,
            'key_schema_result': {'avg_item_size': 200},
        }

        with pytest.raises(BulkExecutorError, match="revert-export requires output view NEW_AND_OLD_IMAGES"):
            run(mock_job, mock_spark_context, mock_glue_context, parsed_args)

    @patch('python_modules.shared.export.pipeline.validate')
    def test_zero_items_exits_early(self, mock_validate, mock_spark_context, mock_glue_context, mock_job, parsed_args):
        mock_validate.return_value = None

        run(mock_job, mock_spark_context, mock_glue_context, parsed_args)


class TestApplyRevertAndTransformStage:
    def test_revert_swaps_new_image_to_old_image(self, mock_spark_context, key_schema):
        old_item = {'pk_attr': 'pk1', 'sk_attr': 'sk1', 'data': 'old'}
        new_item = {'pk_attr': 'pk1', 'sk_attr': 'sk1', 'data': 'new'}
        record = IncrementalExportRecord(
            keys={'pk_attr': 'pk1', 'sk_attr': 'sk1'},
            new_image=new_item,
            old_image=old_item,
            table_key_schema=key_schema,
            write_timestamp_micros=123456
        )

        mock_rdd = Mock()
        mock_rdd.map = Mock(return_value=mock_rdd)
        mock_rdd.filter = Mock(return_value=mock_rdd)

        parser = Mock()
        error_accumulator = Mock(value=[])

        _apply_transform_and_resolve(
            mock_spark_context, mock_rdd, ExportLoadType.INCREMENTAL, parser, None, 'python_modules.revert_export.transform', key_schema, error_accumulator, post_transform=_revert
        )

        # Revert is applied via map; extract the lambda and test it
        revert_fn = mock_rdd.map.call_args_list[0][0][0]
        result = revert_fn(record)
        assert result.new_image == old_item

    def test_revert_addition_becomes_delete(self, mock_spark_context, key_schema):
        record = IncrementalExportRecord(
            keys={'pk_attr': 'pk1', 'sk_attr': 'sk1'},
            new_image={'pk_attr': 'pk1', 'sk_attr': 'sk1', 'data': 'val'},
            old_image=None,
            table_key_schema=key_schema,
            write_timestamp_micros=123456
        )

        mock_rdd = Mock()
        mock_rdd.map = Mock(return_value=mock_rdd)
        mock_rdd.filter = Mock(return_value=mock_rdd)

        parser = Mock()
        error_accumulator = Mock(value=[])

        _apply_transform_and_resolve(
            mock_spark_context, mock_rdd, ExportLoadType.INCREMENTAL, parser, None, 'python_modules.revert_export.transform', key_schema, error_accumulator, post_transform=_revert
        )

        revert_fn = mock_rdd.map.call_args_list[0][0][0]
        result = revert_fn(record)
        assert result.new_image is None

    def test_revert_deletion_becomes_put(self, mock_spark_context, key_schema):
        old_item = {'pk_attr': 'pk1', 'sk_attr': 'sk1', 'data': 'val'}
        record = IncrementalExportRecord(
            keys={'pk_attr': 'pk1', 'sk_attr': 'sk1'},
            new_image=None,
            old_image=old_item,
            table_key_schema=key_schema,
            write_timestamp_micros=123456
        )

        mock_rdd = Mock()
        mock_rdd.map = Mock(return_value=mock_rdd)
        mock_rdd.filter = Mock(return_value=mock_rdd)

        parser = Mock()
        error_accumulator = Mock(value=[])

        _apply_transform_and_resolve(
            mock_spark_context, mock_rdd, ExportLoadType.INCREMENTAL, parser, None, 'python_modules.revert_export.transform', key_schema, error_accumulator, post_transform=_revert
        )

        revert_fn = mock_rdd.map.call_args_list[0][0][0]
        result = revert_fn(record)
        assert result.new_image == old_item

    @patch('python_modules.shared.export.pipeline.load_transform_module')
    def test_user_filter_applied_before_revert(self, mock_load_transform, mock_spark_context, key_schema):
        mock_fn = Mock(return_value=[])
        mock_fn.__name__ = 'transform_incremental_record'
        mock_filter_module = Mock()
        mock_filter_module.transform_incremental_record = mock_fn
        mock_load_transform.return_value = mock_filter_module

        mock_rdd = Mock()
        mock_rdd.flatMap = Mock(return_value=mock_rdd)
        mock_rdd.map = Mock(return_value=mock_rdd)
        mock_rdd.filter = Mock(return_value=mock_rdd)

        parser = Mock()
        error_accumulator = Mock(value=[])

        _apply_transform_and_resolve(
            mock_spark_context, mock_rdd, ExportLoadType.INCREMENTAL, parser, 'my_filter', 'python_modules.revert_export.transform', key_schema, error_accumulator, post_transform=_revert
        )

        # Filter (flatMap) then revert (map) then resolve (map)
        mock_load_transform.assert_called_once_with('my_filter', 'python_modules.revert_export.transform')
        assert mock_rdd.flatMap.call_count == 1
        assert mock_rdd.map.call_count == 2  # revert + resolve

    @patch('python_modules.shared.export.pipeline.load_transform_module')
    def test_filter_exclusion_tracked_by_accumulator(self, mock_load_transform, mock_spark_context, key_schema):
        mock_fn = Mock(return_value=[])
        mock_fn.__name__ = 'transform_incremental_record'
        mock_filter_module = Mock()
        mock_filter_module.transform_incremental_record = mock_fn
        mock_load_transform.return_value = mock_filter_module

        mock_rdd = Mock()
        mock_rdd.flatMap = Mock(return_value=mock_rdd)
        mock_rdd.map = Mock(return_value=mock_rdd)
        mock_rdd.filter = Mock(return_value=mock_rdd)

        parser = Mock()
        error_accumulator = Mock(value=[])

        _, filter_active, excluded_acc, included_acc = _apply_transform_and_resolve(
            mock_spark_context, mock_rdd, ExportLoadType.INCREMENTAL, parser, 'my_filter', 'python_modules.revert_export.transform', key_schema, error_accumulator, post_transform=_revert
        )

        assert filter_active is True
        assert excluded_acc is not None
        assert included_acc is not None

    def test_no_filter_accumulators_are_none(self, mock_spark_context, key_schema):
        mock_rdd = Mock()
        mock_rdd.map = Mock(return_value=mock_rdd)
        mock_rdd.filter = Mock(return_value=mock_rdd)

        parser = Mock()
        error_accumulator = Mock(value=[])

        _, filter_active, excluded_acc, included_acc = _apply_transform_and_resolve(
            mock_spark_context, mock_rdd, ExportLoadType.INCREMENTAL, parser, None, 'python_modules.revert_export.transform', key_schema, error_accumulator, post_transform=_revert
        )

        assert filter_active is False
        assert excluded_acc is None
        assert included_acc is None


class TestRunHappyPath:
    @patch('python_modules.shared.export.pipeline.report')
    @patch('python_modules.shared.export.pipeline.write')
    @patch('python_modules.shared.export.pipeline._apply_transform_and_resolve')
    @patch('python_modules.shared.export.pipeline.read_and_parse')
    @patch('python_modules.shared.export.pipeline.estimate_cost')
    @patch('python_modules.shared.export.pipeline.validate')
    def test_full_pipeline_incremental_new_and_old(
        self, mock_validate, mock_cost, mock_read, mock_transform, mock_write, mock_report,
        mock_spark_context, mock_glue_context, mock_job, parsed_args, table_info, key_schema,
        manifest_data_incremental_new_and_old
    ):
        mock_validate.return_value = {
            'table_info': table_info,
            'key_schema': key_schema,
            'manifest_data': manifest_data_incremental_new_and_old,
            'key_schema_result': {'avg_item_size': 200},
        }
        mock_cost.return_value = None
        mock_read.return_value = (Mock(), ExportLoadType.INCREMENTAL, Mock(), 100)
        mock_transform.return_value = (Mock(), False, None, None)
        mock_write.return_value = Mock(value=100)

        run(mock_job, mock_spark_context, mock_glue_context, parsed_args)

        mock_validate.assert_called_once()
        mock_cost.assert_called_once()
        mock_read.assert_called_once()
        mock_transform.assert_called_once()
        mock_write.assert_called_once()
        mock_report.assert_called_once()
