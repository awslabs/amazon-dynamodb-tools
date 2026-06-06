"""Unit tests for the shared export pipeline stages."""

import pytest
from unittest.mock import Mock, MagicMock, patch, call

from python_modules.shared.export.pipeline import run_export_pipeline
from python_modules.shared.export.pipeline import _apply_transform_and_resolve as _apply_transform_stage
from python_modules.shared.export.pipeline.validator import validate as _validate
from python_modules.shared.export.pipeline.cost_estimator import estimate_cost as _estimate_cost
from python_modules.shared.export.pipeline.reader import read_and_parse as _read_and_parse
from python_modules.shared.export.pipeline.writer import write as _write
from python_modules.shared.export.pipeline.reporter import report as _report
from python_modules.shared.export.utils.enums import ExportLoadType, Operation


@pytest.fixture
def mock_spark_context():
    sc = Mock()
    sc.defaultParallelism = 4
    sc.accumulator = Mock(side_effect=lambda init, *args: Mock(value=init))
    return sc


@pytest.fixture
def key_schema():
    return {'pk': {'name': 'pk_attr', 'type': 'S'}, 'sk': {'name': 'sk_attr', 'type': 'S'}}


@pytest.fixture
def manifest_data_full():
    return {
        'total_item_count': 100,
        'data_files': [
            {'dataFileS3Key': 'data/file1.json.gz', 'itemCount': 50},
            {'dataFileS3Key': 'data/file2.json.gz', 'itemCount': 50},
        ],
        'export_type': 'FULL_EXPORT',
        'output_format': 'DYNAMODB_JSON',
    }


@pytest.fixture
def manifest_data_incremental():
    return {
        'total_item_count': 100,
        'data_files': [
            {'dataFileS3Key': 'data/file1.json.gz', 'itemCount': 100},
        ],
        'export_type': 'INCREMENTAL_EXPORT',
        'output_format': 'DYNAMODB_JSON',
    }


@pytest.fixture
def table_info():
    return {
        'key_schema': {'pk': {'name': 'pk_attr', 'type': 'S'}, 'sk': {'name': 'sk_attr', 'type': 'S'}},
        'table_name': 'my-table',
        'billing_mode': 'PAY_PER_REQUEST',
    }


class TestValidate:
    @patch('python_modules.shared.export.pipeline.validator.get_and_print_dynamodb_table_info')
    @patch('python_modules.shared.export.pipeline.validator.boto3')
    def test_happy_path_returns_validation_dict(self, mock_boto3, mock_table_info, key_schema, table_info, manifest_data_full):
        mock_table_info.return_value = table_info
        mock_s3_client = Mock()
        mock_boto3.client.return_value = mock_s3_client

        path_resolver = Mock()
        path_resolver.get_base_path.return_value = 's3://my-bucket'

        with patch('python_modules.shared.export.pipeline.validator.S3Validator') as MockS3Val, \
             patch('python_modules.shared.export.pipeline.validator.ManifestValidator') as MockManVal, \
             patch('python_modules.shared.export.pipeline.validator.DataFileValidator') as MockDFVal, \
             patch('python_modules.shared.export.pipeline.validator.KeySchemaValidator') as MockKSVal:

            MockManVal.return_value.validate_and_parse_manifests.return_value = manifest_data_full
            MockDFVal.return_value.validate.return_value = {'verified_files': ['file1.json.gz']}
            MockKSVal.return_value.validate.return_value = {'avg_item_size': 200}

            result = _validate(path_resolver, 'my-table')

        assert result is not None
        assert result['table_info'] == table_info
        assert result['key_schema'] == key_schema
        assert result['manifest_data'] == manifest_data_full
        assert result['key_schema_result'] == {'avg_item_size': 200}

    @patch('python_modules.shared.export.pipeline.validator.get_and_print_dynamodb_table_info')
    @patch('python_modules.shared.export.pipeline.validator.boto3')
    def test_zero_item_count_returns_none(self, mock_boto3, mock_table_info, table_info):
        mock_table_info.return_value = table_info
        mock_boto3.client.return_value = Mock()

        path_resolver = Mock()
        path_resolver.get_base_path.return_value = 's3://my-bucket'

        empty_manifest = {
            'total_item_count': 0,
            'data_files': [],
            'export_type': 'FULL_EXPORT',
            'output_format': 'DYNAMODB_JSON',
        }

        with patch('python_modules.shared.export.pipeline.validator.S3Validator'), \
             patch('python_modules.shared.export.pipeline.validator.ManifestValidator') as MockManVal, \
             patch('python_modules.shared.export.pipeline.validator.DataFileValidator'), \
             patch('python_modules.shared.export.pipeline.validator.KeySchemaValidator'):

            MockManVal.return_value.validate_and_parse_manifests.return_value = empty_manifest

            result = _validate(path_resolver, 'my-table')

        assert result is None

    @patch('python_modules.shared.export.pipeline.validator.get_and_print_dynamodb_table_info')
    @patch('python_modules.shared.export.pipeline.validator.boto3')
    def test_manifest_validation_raises_propagates(self, mock_boto3, mock_table_info, table_info):
        mock_table_info.return_value = table_info
        mock_boto3.client.return_value = Mock()

        path_resolver = Mock()

        with patch('python_modules.shared.export.pipeline.validator.S3Validator'), \
             patch('python_modules.shared.export.pipeline.validator.ManifestValidator') as MockManVal, \
             patch('python_modules.shared.export.pipeline.validator.DataFileValidator'), \
             patch('python_modules.shared.export.pipeline.validator.KeySchemaValidator'):

            MockManVal.return_value.validate_and_parse_manifests.side_effect = ValueError("Invalid manifest")

            with pytest.raises(ValueError, match="Invalid manifest"):
                _validate(path_resolver, 'my-table')

    @patch('python_modules.shared.export.pipeline.validator.get_and_print_dynamodb_table_info')
    @patch('python_modules.shared.export.pipeline.validator.boto3')
    def test_s3_path_validation_raises_propagates(self, mock_boto3, mock_table_info, table_info):
        mock_table_info.return_value = table_info
        mock_boto3.client.return_value = Mock()

        path_resolver = Mock()

        with patch('python_modules.shared.export.pipeline.validator.S3Validator') as MockS3Val, \
             patch('python_modules.shared.export.pipeline.validator.ManifestValidator'), \
             patch('python_modules.shared.export.pipeline.validator.DataFileValidator'), \
             patch('python_modules.shared.export.pipeline.validator.KeySchemaValidator'):

            MockS3Val.return_value.validate_path_exists.side_effect = ValueError("S3 path not found")

            with pytest.raises(ValueError, match="S3 path not found"):
                _validate(path_resolver, 'my-table')


class TestEstimateCost:
    @patch('python_modules.shared.export.pipeline.cost_estimator.get_and_print_table_write_cost')
    def test_calls_cost_estimator(self, mock_cost):
        table_info = {'table_name': 'tbl'}
        manifest_data = {'total_item_count': 1000}
        key_schema_result = {'avg_item_size': 500}

        _estimate_cost(table_info, manifest_data, key_schema_result)

        mock_cost.assert_called_once_with(table_info, 1000, 500000)

    @patch('python_modules.shared.export.pipeline.cost_estimator.get_and_print_table_write_cost')
    def test_zero_avg_item_size(self, mock_cost):
        table_info = {'table_name': 'tbl'}
        manifest_data = {'total_item_count': 50}
        key_schema_result = {'avg_item_size': 0}

        _estimate_cost(table_info, manifest_data, key_schema_result)

        mock_cost.assert_called_once_with(table_info, 50, 0)


class TestReadAndParse:
    @patch('python_modules.shared.export.pipeline.reader.ParserFactory')
    @patch('python_modules.shared.export.pipeline.reader.get_export_file_paths')
    def test_full_export_uses_full_parser(self, mock_get_paths, mock_parser_factory, mock_spark_context, manifest_data_full, key_schema):
        mock_get_paths.return_value = (['s3://bucket/data/file1.json.gz', 's3://bucket/data/file2.json.gz'], 100)
        mock_parser = Mock()
        mock_parser_factory.get_parser.return_value = mock_parser

        mock_rdd = Mock()
        mock_spark_context.textFile.return_value = mock_rdd
        mock_rdd.map.return_value = mock_rdd

        path_resolver = Mock()
        path_resolver.get_base_path.return_value = 's3://bucket'

        records_rdd, export_load_type, parser, total_expected = _read_and_parse(
            mock_spark_context, manifest_data_full, path_resolver, key_schema
        )

        assert export_load_type == ExportLoadType.FULL
        assert parser is mock_parser
        assert total_expected == 100
        mock_parser_factory.get_parser.assert_called_once_with(ExportLoadType.FULL, key_schema)
        mock_spark_context.textFile.assert_called_once()
        mock_rdd.map.assert_called_once_with(mock_parser.parse_to_record)

    @patch('python_modules.shared.export.pipeline.reader.ParserFactory')
    @patch('python_modules.shared.export.pipeline.reader.get_export_file_paths')
    def test_incremental_export_uses_incremental_parser(self, mock_get_paths, mock_parser_factory, mock_spark_context, manifest_data_incremental, key_schema):
        mock_get_paths.return_value = (['s3://bucket/data/file1.json.gz'], 100)
        mock_parser = Mock()
        mock_parser_factory.get_parser.return_value = mock_parser

        mock_rdd = Mock()
        mock_spark_context.textFile.return_value = mock_rdd
        mock_rdd.map.return_value = mock_rdd

        path_resolver = Mock()
        path_resolver.get_base_path.return_value = 's3://bucket'

        records_rdd, export_load_type, parser, total_expected = _read_and_parse(
            mock_spark_context, manifest_data_incremental, path_resolver, key_schema
        )

        assert export_load_type == ExportLoadType.INCREMENTAL
        mock_parser_factory.get_parser.assert_called_once_with(ExportLoadType.INCREMENTAL, key_schema)


class TestApplyTransformStage:
    def test_no_transform_passes_records_through(self, mock_spark_context, key_schema):
        mock_parser = Mock()
        mock_parser.resolve.side_effect = lambda r: {'operation': Operation.PUT, 'data': {'pk_attr': '1', 'sk_attr': 'a'}}

        records_rdd = Mock()
        records_rdd.flatMap = Mock(return_value=records_rdd)
        records_rdd.map = Mock(return_value=records_rdd)
        records_rdd.filter = Mock(return_value=records_rdd)

        error_acc = Mock()
        error_acc.value = []

        items_rdd, transform_active, excl_acc, mod_acc = _apply_transform_stage(
            mock_spark_context, records_rdd, ExportLoadType.FULL, mock_parser,
            None, 'python_modules.load_export.transform', key_schema, error_acc
        )

        assert transform_active is False
        assert excl_acc is None
        assert mod_acc is None
        records_rdd.flatMap.assert_not_called()
        records_rdd.map.assert_called_once()
        records_rdd.filter.assert_called_once()

    @patch('python_modules.shared.export.pipeline.load_transform_module')
    def test_full_export_with_transform_uses_transform_full_record(self, mock_load_transform, mock_spark_context, key_schema):
        mock_transform_module = Mock()
        mock_transform_module.transform_full_record = Mock(__name__='transform_full_record')
        mock_load_transform.return_value = mock_transform_module

        mock_parser = Mock()
        records_rdd = Mock()
        records_rdd.flatMap = Mock(return_value=records_rdd)
        records_rdd.map = Mock(return_value=records_rdd)
        records_rdd.filter = Mock(return_value=records_rdd)

        acc_mock = Mock(value=0)
        mock_spark_context.accumulator = Mock(return_value=acc_mock)

        error_acc = Mock()
        error_acc.value = []

        items_rdd, transform_active, excl_acc, mod_acc = _apply_transform_stage(
            mock_spark_context, records_rdd, ExportLoadType.FULL, mock_parser,
            'my_transform', 'python_modules.load_export.transform', key_schema, error_acc
        )

        assert transform_active is True
        assert excl_acc is acc_mock
        assert mod_acc is acc_mock
        mock_load_transform.assert_called_once_with('my_transform', 'python_modules.load_export.transform')
        records_rdd.flatMap.assert_called_once()

    @patch('python_modules.shared.export.pipeline.load_transform_module')
    def test_incremental_export_with_transform_uses_transform_incremental_record(self, mock_load_transform, mock_spark_context, key_schema):
        mock_transform_module = Mock()
        mock_transform_module.transform_incremental_record = Mock(__name__='transform_incremental_record')
        mock_load_transform.return_value = mock_transform_module

        mock_parser = Mock()
        records_rdd = Mock()
        records_rdd.flatMap = Mock(return_value=records_rdd)
        records_rdd.map = Mock(return_value=records_rdd)
        records_rdd.filter = Mock(return_value=records_rdd)

        acc_mock = Mock(value=0)
        mock_spark_context.accumulator = Mock(return_value=acc_mock)

        error_acc = Mock()
        error_acc.value = []

        _apply_transform_stage(
            mock_spark_context, records_rdd, ExportLoadType.INCREMENTAL, mock_parser,
            'my_transform', 'python_modules.load_export.transform', key_schema, error_acc
        )

        records_rdd.flatMap.assert_called_once()
        flatmap_fn = records_rdd.flatMap.call_args[0][0]
        record = {'some': 'record'}
        mock_transform_module.transform_incremental_record.return_value = [record]
        flatmap_fn(record)
        mock_transform_module.transform_incremental_record.assert_called_once_with(record)

    @patch('python_modules.shared.export.pipeline.load_transform_module')
    def test_transform_returning_empty_list_increments_excluded(self, mock_load_transform, mock_spark_context, key_schema):
        mock_transform_module = Mock()
        mock_transform_module.transform_full_record = Mock(return_value=[], __name__='transform_full_record')
        mock_load_transform.return_value = mock_transform_module

        mock_parser = Mock()
        records_rdd = Mock()
        records_rdd.flatMap = Mock(return_value=records_rdd)
        records_rdd.map = Mock(return_value=records_rdd)
        records_rdd.filter = Mock(return_value=records_rdd)

        excl_acc = Mock(value=0)
        mod_acc = Mock(value=0)
        call_count = [0]

        def make_acc(init):
            call_count[0] += 1
            return excl_acc if call_count[0] == 1 else mod_acc

        mock_spark_context.accumulator = Mock(side_effect=make_acc)

        error_acc = Mock()
        error_acc.value = []

        _apply_transform_stage(
            mock_spark_context, records_rdd, ExportLoadType.FULL, mock_parser,
            'my_transform', 'python_modules.load_export.transform', key_schema, error_acc
        )

        flatmap_fn = records_rdd.flatMap.call_args[0][0]
        result = flatmap_fn({'some': 'record'})
        assert result == []
        excl_acc.add.assert_called_once_with(1)

    @patch('python_modules.shared.export.pipeline.load_transform_module')
    def test_transform_raising_exception_adds_error(self, mock_load_transform, mock_spark_context, key_schema):
        mock_transform_module = Mock()
        mock_transform_module.transform_full_record = Mock(side_effect=RuntimeError("boom"), __name__='transform_full_record')
        mock_load_transform.return_value = mock_transform_module

        mock_parser = Mock()
        records_rdd = Mock()
        records_rdd.flatMap = Mock(return_value=records_rdd)
        records_rdd.map = Mock(return_value=records_rdd)
        records_rdd.filter = Mock(return_value=records_rdd)

        acc_mock = Mock(value=0)
        mock_spark_context.accumulator = Mock(return_value=acc_mock)

        error_acc = Mock()
        error_acc.value = []

        _apply_transform_stage(
            mock_spark_context, records_rdd, ExportLoadType.FULL, mock_parser,
            'my_transform', 'python_modules.load_export.transform', key_schema, error_acc
        )

        flatmap_fn = records_rdd.flatMap.call_args[0][0]
        result = flatmap_fn({'some': 'record'})
        assert result == []
        error_acc.add.assert_called_once()
        assert "Transform function raised an exception" in error_acc.add.call_args[0][0][0]

    @patch('python_modules.shared.export.pipeline.load_transform_module')
    def test_transform_returning_non_list_wraps_in_list(self, mock_load_transform, mock_spark_context, key_schema):
        single_item = {'wrapped': 'item'}
        mock_transform_module = Mock()
        mock_transform_module.transform_full_record = Mock(return_value=single_item, __name__='transform_full_record')
        mock_load_transform.return_value = mock_transform_module

        mock_parser = Mock()
        records_rdd = Mock()
        records_rdd.flatMap = Mock(return_value=records_rdd)
        records_rdd.map = Mock(return_value=records_rdd)
        records_rdd.filter = Mock(return_value=records_rdd)

        excl_acc = Mock(value=0)
        mod_acc = Mock(value=0)
        call_count = [0]

        def make_acc(init):
            call_count[0] += 1
            return excl_acc if call_count[0] == 1 else mod_acc

        mock_spark_context.accumulator = Mock(side_effect=make_acc)

        error_acc = Mock()
        error_acc.value = []

        _apply_transform_stage(
            mock_spark_context, records_rdd, ExportLoadType.FULL, mock_parser,
            'my_transform', 'python_modules.load_export.transform', key_schema, error_acc
        )

        flatmap_fn = records_rdd.flatMap.call_args[0][0]
        result = flatmap_fn({'input': 'record'})
        assert result == [single_item]
        mod_acc.add.assert_called_once_with(1)

    def test_resolve_and_validate_filters_missing_keys_on_put(self, mock_spark_context, key_schema):
        mock_parser = Mock()
        mock_parser.resolve.return_value = {'operation': Operation.PUT, 'data': {'pk_attr': '1'}}

        records_rdd = Mock()
        records_rdd.map = Mock(return_value=records_rdd)
        records_rdd.filter = Mock(return_value=records_rdd)

        error_acc = Mock()
        error_acc.value = []

        _apply_transform_stage(
            mock_spark_context, records_rdd, ExportLoadType.FULL, mock_parser,
            None, 'python_modules.load_export.transform', key_schema, error_acc
        )

        resolve_fn = records_rdd.map.call_args[0][0]
        result = resolve_fn({'some': 'record'})
        assert result is None
        error_acc.add.assert_called_once()
        assert "missing key attributes" in error_acc.add.call_args[0][0][0]

    def test_resolve_and_validate_passes_valid_put(self, mock_spark_context, key_schema):
        mock_parser = Mock()
        mock_parser.resolve.return_value = {'operation': Operation.PUT, 'data': {'pk_attr': '1', 'sk_attr': 'a', 'extra': 'val'}}

        records_rdd = Mock()
        records_rdd.map = Mock(return_value=records_rdd)
        records_rdd.filter = Mock(return_value=records_rdd)

        error_acc = Mock()
        error_acc.value = []

        _apply_transform_stage(
            mock_spark_context, records_rdd, ExportLoadType.FULL, mock_parser,
            None, 'python_modules.load_export.transform', key_schema, error_acc
        )

        resolve_fn = records_rdd.map.call_args[0][0]
        result = resolve_fn({'some': 'record'})
        assert result == {'operation': Operation.PUT, 'data': {'pk_attr': '1', 'sk_attr': 'a', 'extra': 'val'}}
        error_acc.add.assert_not_called()

    def test_resolve_and_validate_delete_with_extra_attributes_rejected(self, mock_spark_context, key_schema):
        mock_parser = Mock()
        mock_parser.resolve.return_value = {'operation': Operation.DELETE, 'data': {'pk_attr': '1', 'sk_attr': 'a', 'extra': 'val'}}

        records_rdd = Mock()
        records_rdd.map = Mock(return_value=records_rdd)
        records_rdd.filter = Mock(return_value=records_rdd)

        error_acc = Mock()
        error_acc.value = []

        _apply_transform_stage(
            mock_spark_context, records_rdd, ExportLoadType.FULL, mock_parser,
            None, 'python_modules.load_export.transform', key_schema, error_acc
        )

        resolve_fn = records_rdd.map.call_args[0][0]
        result = resolve_fn({'some': 'record'})
        assert result is None
        error_acc.add.assert_called_once()
        assert "non-key attributes" in error_acc.add.call_args[0][0][0]

    def test_resolve_and_validate_delete_missing_keys_rejected(self, mock_spark_context, key_schema):
        mock_parser = Mock()
        mock_parser.resolve.return_value = {'operation': Operation.DELETE, 'data': {'pk_attr': '1'}}

        records_rdd = Mock()
        records_rdd.map = Mock(return_value=records_rdd)
        records_rdd.filter = Mock(return_value=records_rdd)

        error_acc = Mock()
        error_acc.value = []

        _apply_transform_stage(
            mock_spark_context, records_rdd, ExportLoadType.FULL, mock_parser,
            None, 'python_modules.load_export.transform', key_schema, error_acc
        )

        resolve_fn = records_rdd.map.call_args[0][0]
        result = resolve_fn({'some': 'record'})
        assert result is None
        error_acc.add.assert_called_once()
        assert "DELETE item missing key attributes" in error_acc.add.call_args[0][0][0]

    def test_resolve_and_validate_valid_delete(self, mock_spark_context, key_schema):
        mock_parser = Mock()
        mock_parser.resolve.return_value = {'operation': Operation.DELETE, 'data': {'pk_attr': '1', 'sk_attr': 'a'}}

        records_rdd = Mock()
        records_rdd.map = Mock(return_value=records_rdd)
        records_rdd.filter = Mock(return_value=records_rdd)

        error_acc = Mock()
        error_acc.value = []

        _apply_transform_stage(
            mock_spark_context, records_rdd, ExportLoadType.FULL, mock_parser,
            None, 'python_modules.load_export.transform', key_schema, error_acc
        )

        resolve_fn = records_rdd.map.call_args[0][0]
        result = resolve_fn({'some': 'record'})
        assert result == {'operation': Operation.DELETE, 'data': {'pk_attr': '1', 'sk_attr': 'a'}}
        error_acc.add.assert_not_called()

    def test_resolve_and_validate_unknown_operation_passes_through(self, mock_spark_context, key_schema):
        mock_parser = Mock()
        mock_parser.resolve.return_value = {'operation': 'UNKNOWN', 'data': {'pk_attr': '1', 'sk_attr': 'a'}}

        records_rdd = Mock()
        records_rdd.map = Mock(return_value=records_rdd)
        records_rdd.filter = Mock(return_value=records_rdd)

        error_acc = Mock()
        error_acc.value = []

        _apply_transform_stage(
            mock_spark_context, records_rdd, ExportLoadType.FULL, mock_parser,
            None, 'python_modules.load_export.transform', key_schema, error_acc
        )

        resolve_fn = records_rdd.map.call_args[0][0]
        result = resolve_fn({'some': 'record'})
        assert result == {'operation': 'UNKNOWN', 'data': {'pk_attr': '1', 'sk_attr': 'a'}}
        error_acc.add.assert_not_called()


class TestWrite:
    @patch('python_modules.shared.export.pipeline.writer.WriterFactory')
    def test_happy_path_writes_and_returns_accumulator(self, mock_writer_factory, mock_spark_context):
        mock_writer = Mock()
        mock_writer_factory.create_writer.return_value = mock_writer

        items_rdd = Mock()
        items_rdd.getNumPartitions.return_value = 10
        items_rdd.repartition.return_value = items_rdd

        written_acc = Mock(value=50)
        error_acc = Mock(value=[])
        debug_acc = Mock(value=[])

        mock_spark_context.accumulator = Mock(return_value=written_acc)

        rate_config = Mock()
        monitor_options = {'write': {}}

        result_acc = _write(
            mock_spark_context, items_rdd, ExportLoadType.FULL, 'my-table',
            rate_config, monitor_options, error_acc, debug_acc
        )

        assert result_acc is written_acc
        items_rdd.repartition.assert_called_once()
        items_rdd.foreachPartition.assert_called_once()

    @patch('python_modules.shared.export.pipeline.writer.WriterFactory')
    def test_error_accumulator_nonempty_raises(self, mock_writer_factory, mock_spark_context):
        mock_writer = Mock()
        mock_writer_factory.create_writer.return_value = mock_writer

        items_rdd = Mock()
        items_rdd.getNumPartitions.return_value = 4
        items_rdd.repartition.return_value = items_rdd
        items_rdd.foreachPartition = Mock()

        error_acc = Mock(value=["Write failed: throttled"])
        debug_acc = Mock(value=[])
        written_acc = Mock(value=10)
        mock_spark_context.accumulator = Mock(return_value=written_acc)

        with pytest.raises(Exception, match="Write failed: throttled"):
            _write(
                mock_spark_context, items_rdd, ExportLoadType.FULL, 'my-table',
                Mock(), {}, error_acc, debug_acc
            )

    @patch('python_modules.shared.export.pipeline.writer.WriterFactory')
    def test_partitions_bounded_by_parallelism(self, mock_writer_factory, mock_spark_context):
        mock_writer_factory.create_writer.return_value = Mock()

        items_rdd = Mock()
        items_rdd.getNumPartitions.return_value = 100
        items_rdd.repartition.return_value = items_rdd

        error_acc = Mock(value=[])
        debug_acc = None
        written_acc = Mock(value=0)
        mock_spark_context.accumulator = Mock(return_value=written_acc)
        mock_spark_context.defaultParallelism = 4

        _write(
            mock_spark_context, items_rdd, ExportLoadType.FULL, 'my-table',
            Mock(), {}, error_acc, debug_acc
        )

        items_rdd.repartition.assert_called_once_with(8)


class TestReport:
    def test_report_without_transform(self):
        manifest_data = {'total_item_count': 200}
        written_acc = Mock(value=200)
        import time
        start_time = time.time() - 5.0

        _report(manifest_data, 200, written_acc, False, None, None, None, start_time)

    def test_report_with_transform(self):
        manifest_data = {'total_item_count': 200}
        written_acc = Mock(value=150)
        excl_acc = Mock(value=50)
        mod_acc = Mock(value=150)
        import time
        start_time = time.time() - 10.0

        _report(manifest_data, 200, written_acc, True, 'my_transform', excl_acc, mod_acc, start_time)


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
        'transform': 'my_transform',
        's3-bucket-name': 'config-bucket',
        'JOB_RUN_ID': 'jr_123',
        'XDebug': 'false',
    }


class TestRunExportPipeline:
    @patch('python_modules.shared.export.pipeline.RateLimiterAggregator')
    @patch('python_modules.shared.export.pipeline.RateLimiterSharedConfig')
    @patch('python_modules.shared.export.pipeline.get_dynamodb_throughput_configs')
    @patch('python_modules.shared.export.pipeline.report')
    @patch('python_modules.shared.export.pipeline.write')
    @patch('python_modules.shared.export.pipeline._apply_transform_and_resolve')
    @patch('python_modules.shared.export.pipeline.read_and_parse')
    @patch('python_modules.shared.export.pipeline.estimate_cost')
    @patch('python_modules.shared.export.pipeline.validate')
    def test_happy_path_full_pipeline(self, mock_validate, mock_estimate, mock_read, mock_transform, mock_write, mock_report,
                                      mock_throughput, mock_rl_config, mock_rl_agg,
                                      mock_spark_context, parsed_args, key_schema):
        mock_spark_context.accumulator = Mock(return_value=Mock(value=[]))

        mock_validate.return_value = {
            'table_info': {'key_schema': key_schema},
            'key_schema': key_schema,
            'manifest_data': {'total_item_count': 100, 'data_files': [], 'export_type': 'FULL_EXPORT'},
            'key_schema_result': {'avg_item_size': 200},
        }
        mock_estimate.return_value = None
        mock_read.return_value = (Mock(), ExportLoadType.FULL, Mock(), 100)
        mock_transform.return_value = (Mock(), False, None, None)
        mock_write.return_value = Mock(value=100)

        run_export_pipeline(mock_spark_context, parsed_args, transform_package='python_modules.load_export.transform')

        mock_validate.assert_called_once()
        mock_estimate.assert_called_once()
        mock_read.assert_called_once()
        mock_transform.assert_called_once()
        mock_write.assert_called_once()
        mock_report.assert_called_once()
        mock_rl_agg.return_value.shutdown.assert_called_once()

    @patch('python_modules.shared.export.pipeline.RateLimiterAggregator')
    @patch('python_modules.shared.export.pipeline.RateLimiterSharedConfig')
    @patch('python_modules.shared.export.pipeline.get_dynamodb_throughput_configs')
    @patch('python_modules.shared.export.pipeline.validate')
    def test_validation_returns_none_exits_early(self, mock_validate, mock_throughput, mock_rl_config, mock_rl_agg,
                                                  mock_spark_context, parsed_args):
        mock_spark_context.accumulator = Mock(return_value=Mock(value=[]))
        mock_validate.return_value = None

        run_export_pipeline(mock_spark_context, parsed_args, transform_package='python_modules.load_export.transform')

        mock_rl_agg.return_value.shutdown.assert_called_once()

    @patch('python_modules.shared.export.pipeline.RateLimiterAggregator')
    @patch('python_modules.shared.export.pipeline.RateLimiterSharedConfig')
    @patch('python_modules.shared.export.pipeline.get_dynamodb_throughput_configs')
    @patch('python_modules.shared.export.pipeline.validate')
    def test_validation_raises_value_error_propagates(self, mock_validate, mock_throughput, mock_rl_config, mock_rl_agg,
                                                       mock_spark_context, parsed_args):
        mock_spark_context.accumulator = Mock(return_value=Mock(value=[]))
        mock_validate.side_effect = ValueError("bad manifest")

        with pytest.raises(ValueError, match="bad manifest"):
            run_export_pipeline(mock_spark_context, parsed_args, transform_package='python_modules.load_export.transform')

        mock_rl_agg.return_value.shutdown.assert_called_once()

    @patch('python_modules.shared.export.pipeline.RateLimiterAggregator')
    @patch('python_modules.shared.export.pipeline.RateLimiterSharedConfig')
    @patch('python_modules.shared.export.pipeline.get_dynamodb_throughput_configs')
    @patch('python_modules.shared.export.pipeline.report')
    @patch('python_modules.shared.export.pipeline.write')
    @patch('python_modules.shared.export.pipeline._apply_transform_and_resolve')
    @patch('python_modules.shared.export.pipeline.read_and_parse')
    @patch('python_modules.shared.export.pipeline.estimate_cost')
    @patch('python_modules.shared.export.pipeline.validate')
    def test_write_raises_generic_exception_propagates(self, mock_validate, mock_estimate, mock_read, mock_transform, mock_write, mock_report,
                                                        mock_throughput, mock_rl_config, mock_rl_agg,
                                                        mock_spark_context, parsed_args, key_schema):
        mock_spark_context.accumulator = Mock(return_value=Mock(value=[]))

        mock_validate.return_value = {
            'table_info': {'key_schema': key_schema},
            'key_schema': key_schema,
            'manifest_data': {'total_item_count': 100, 'data_files': [], 'export_type': 'FULL_EXPORT'},
            'key_schema_result': {'avg_item_size': 200},
        }
        mock_estimate.return_value = None
        mock_read.return_value = (Mock(), ExportLoadType.FULL, Mock(), 100)
        mock_transform.return_value = (Mock(), False, None, None)
        mock_write.side_effect = Exception("DDB throttled")

        with pytest.raises(Exception, match="DDB throttled"):
            run_export_pipeline(mock_spark_context, parsed_args, transform_package='python_modules.load_export.transform')

        mock_rl_agg.return_value.shutdown.assert_called_once()
        mock_report.assert_not_called()

    @patch('python_modules.shared.export.pipeline.RateLimiterAggregator')
    @patch('python_modules.shared.export.pipeline.RateLimiterSharedConfig')
    @patch('python_modules.shared.export.pipeline.get_dynamodb_throughput_configs')
    @patch('python_modules.shared.export.pipeline.report')
    @patch('python_modules.shared.export.pipeline.write')
    @patch('python_modules.shared.export.pipeline._apply_transform_and_resolve')
    @patch('python_modules.shared.export.pipeline.read_and_parse')
    @patch('python_modules.shared.export.pipeline.estimate_cost')
    @patch('python_modules.shared.export.pipeline.validate')
    def test_debug_accumulator_enabled(self, mock_validate, mock_estimate, mock_read, mock_transform, mock_write, mock_report,
                                        mock_throughput, mock_rl_config, mock_rl_agg,
                                        mock_spark_context, key_schema):
        parsed_args = {
            'table': 'my-table',
            's3_path': 's3://my-bucket/prefix/AWSDynamoDB/export-001',
            'transform': None,
            's3-bucket-name': 'config-bucket',
            'JOB_RUN_ID': 'jr_123',
            'XDebug': 'true',
        }

        debug_acc = Mock(value=['debug msg 1'])
        error_acc = Mock(value=[])

        call_count = [0]
        def make_acc(init, *args):
            call_count[0] += 1
            if call_count[0] == 1:
                return error_acc
            return debug_acc

        mock_spark_context.accumulator = Mock(side_effect=make_acc)

        mock_validate.return_value = {
            'table_info': {'key_schema': key_schema},
            'key_schema': key_schema,
            'manifest_data': {'total_item_count': 100, 'data_files': [], 'export_type': 'FULL_EXPORT'},
            'key_schema_result': {'avg_item_size': 200},
        }
        mock_estimate.return_value = None
        mock_read.return_value = (Mock(), ExportLoadType.FULL, Mock(), 100)
        mock_transform.return_value = (Mock(), False, None, None)
        mock_write.return_value = Mock(value=100)

        run_export_pipeline(mock_spark_context, parsed_args, transform_package='python_modules.load_export.transform')

        assert mock_spark_context.accumulator.call_count == 2
        mock_rl_agg.return_value.shutdown.assert_called_once()

    @patch('python_modules.shared.export.pipeline.RateLimiterAggregator')
    @patch('python_modules.shared.export.pipeline.RateLimiterSharedConfig')
    @patch('python_modules.shared.export.pipeline.get_dynamodb_throughput_configs')
    @patch('python_modules.shared.export.pipeline.report')
    @patch('python_modules.shared.export.pipeline.write')
    @patch('python_modules.shared.export.pipeline._apply_transform_and_resolve')
    @patch('python_modules.shared.export.pipeline.read_and_parse')
    @patch('python_modules.shared.export.pipeline.estimate_cost')
    @patch('python_modules.shared.export.pipeline.validate')
    def test_transform_name_forwarded_to_transform_stage(self, mock_validate, mock_estimate, mock_read, mock_transform, mock_write, mock_report,
                                                          mock_throughput, mock_rl_config, mock_rl_agg,
                                                          mock_spark_context, parsed_args_with_transform, key_schema):
        mock_spark_context.accumulator = Mock(return_value=Mock(value=[]))

        mock_validate.return_value = {
            'table_info': {'key_schema': key_schema},
            'key_schema': key_schema,
            'manifest_data': {'total_item_count': 100, 'data_files': [], 'export_type': 'FULL_EXPORT'},
            'key_schema_result': {'avg_item_size': 200},
        }
        mock_estimate.return_value = None
        mock_read.return_value = (Mock(), ExportLoadType.FULL, Mock(), 100)

        excl_acc = Mock(value=10)
        mod_acc = Mock(value=90)
        mock_transform.return_value = (Mock(), True, excl_acc, mod_acc)
        mock_write.return_value = (Mock(value=90), 12)

        run_export_pipeline(mock_spark_context, parsed_args_with_transform, transform_package='python_modules.load_export.transform')

        transform_call_args = mock_transform.call_args
        assert transform_call_args[0][4] == 'my_transform'
        mock_report.assert_called_once()