"""Unit tests for the load_export verb wiring."""

from unittest.mock import Mock, patch

from python_modules.load_export import run, TRANSFORM_PACKAGE


class TestLoadExportVerb:
    @patch('python_modules.load_export.run_export_pipeline')
    def test_run_calls_pipeline_with_correct_transform_package(self, mock_pipeline):
        mock_job = Mock()
        mock_spark_context = Mock()
        mock_glue_context = Mock()
        parsed_args = {'table': 'tbl', 's3_path': 's3://b/p', 'transform': None, 's3-bucket-name': 'b', 'JOB_RUN_ID': 'jr', 'XDebug': 'false'}

        run(mock_job, mock_spark_context, mock_glue_context, parsed_args)

        mock_pipeline.assert_called_once_with(mock_spark_context, parsed_args, transform_package=TRANSFORM_PACKAGE)

    def test_transform_package_points_to_load_export_transforms(self):
        assert TRANSFORM_PACKAGE == 'python_modules.load_export.transform'