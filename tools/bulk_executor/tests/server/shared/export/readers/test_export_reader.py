"""Unit tests for export_reader.get_export_file_paths."""

import pytest
from python_modules.shared.export.readers.export_reader import get_export_file_paths


class TestGetExportFilePaths:

    def test_basic_file_paths(self):
        data_files = [
            {'dataFileS3Key': 'data/file1.json.gz', 'itemCount': 1000},
            {'dataFileS3Key': 'data/file2.json.gz', 'itemCount': 2000},
        ]
        paths, total = get_export_file_paths(data_files, 's3://bucket/export/')
        assert paths == [
            's3://bucket/export/data/file1.json.gz',
            's3://bucket/export/data/file2.json.gz',
        ]
        assert total == 3000

    def test_empty_data_files_list(self):
        paths, total = get_export_file_paths([], 's3://bucket/export/')
        assert paths == []
        assert total == 0

    def test_skips_zero_item_count_files(self):
        data_files = [
            {'dataFileS3Key': 'data/file1.json.gz', 'itemCount': 0},
            {'dataFileS3Key': 'data/file2.json.gz', 'itemCount': 500},
            {'dataFileS3Key': 'data/file3.json.gz', 'itemCount': 0},
        ]
        paths, total = get_export_file_paths(data_files, 's3://bucket/prefix')
        assert len(paths) == 1
        assert paths[0] == 's3://bucket/prefix/data/file2.json.gz'
        assert total == 500

    def test_all_files_empty(self):
        data_files = [
            {'dataFileS3Key': 'data/file1.json.gz', 'itemCount': 0},
            {'dataFileS3Key': 'data/file2.json.gz', 'itemCount': 0},
        ]
        paths, total = get_export_file_paths(data_files, 's3://bucket/export/')
        assert paths == []
        assert total == 0

    def test_base_path_trailing_slash_normalization(self):
        data_files = [{'dataFileS3Key': 'data/file.json.gz', 'itemCount': 10}]
        paths1, _ = get_export_file_paths(data_files, 's3://bucket/export/')
        paths2, _ = get_export_file_paths(data_files, 's3://bucket/export')
        assert paths1 == paths2

    def test_leading_slash_on_key_normalization(self):
        data_files = [{'dataFileS3Key': '/data/file.json.gz', 'itemCount': 10}]
        paths, total = get_export_file_paths(data_files, 's3://bucket/export')
        assert paths == ['s3://bucket/export/data/file.json.gz']
        assert total == 10

    def test_multiple_pages(self):
        data_files = [
            {'dataFileS3Key': f'data/file{i}.json.gz', 'itemCount': 100}
            for i in range(20)
        ]
        paths, total = get_export_file_paths(data_files, 's3://bucket/export')
        assert len(paths) == 20
        assert total == 2000
