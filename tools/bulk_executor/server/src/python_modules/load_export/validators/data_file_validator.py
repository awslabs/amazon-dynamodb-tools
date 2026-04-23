"""Data file MD5 checksum validation for DynamoDB export files."""
from typing import Dict, List

from ...shared.logger import log
from .md5_validator import MD5Validator


class DataFileValidator:
    def __init__(self, file_loader):
        self.file_loader = file_loader

    def validate(self, data_files: List[Dict], base_path: str,
                 validate_all: bool = False, sample_size: int = 5) -> Dict:
        """Validate MD5 checksums of data files listed in manifest-files.json.

        Returns dict with validated_count, total_count, validation_mode, failed_files, verified_files.
        The verified_files list contains the file entries that passed checksum validation,
        suitable for downstream validation (e.g. key schema checks).
        Raises ValueError if any validation fails.
        """
        total_count = len(data_files)

        if total_count == 0:
            log.warning("No data files to validate")
            return {'validated_count': 0, 'total_count': 0, 'validation_mode': 'none', 'failed_files': [], 'verified_files': []}

        if validate_all:
            files_to_validate = data_files
            validation_mode = 'full'
        else:
            actual_sample_size = min(sample_size, total_count)
            files_to_validate = data_files[:actual_sample_size]
            validation_mode = 'sample'

        log.info(f"Validating MD5 checksums for {len(files_to_validate):,} of {total_count:,} data files ({validation_mode} mode)...")

        validated_count = 0
        failed_files = []
        verified_files = []

        for idx, file_entry in enumerate(files_to_validate, 1):
            data_file_key = file_entry.get('dataFileS3Key')
            expected_md5 = file_entry.get('md5Checksum')

            if not data_file_key:
                failed_files.append({'file': 'unknown', 'error': f"Missing 'dataFileS3Key' in entry at index {idx}"})
                continue
            if not expected_md5:
                failed_files.append({'file': data_file_key, 'error': f"Missing 'md5Checksum' for {data_file_key}"})
                continue

            try:
                data_file_path = self.file_loader.join_path(base_path, data_file_key)
                data_file_content = self.file_loader.read_file(data_file_path)
                MD5Validator.validate_file_checksum(data_file_content, expected_md5)
                validated_count += 1
                verified_files.append(file_entry)
            except ValueError as e:
                failed_files.append({'file': data_file_key, 'error': str(e)})
            except Exception as e:
                failed_files.append({'file': data_file_key, 'error': str(e)})

        if failed_files:
            error_summary = f"Data file validation failed for {len(failed_files)} file(s)"
            log.error(error_summary)
            for failure in failed_files:
                log.error(f"  - {failure['file']}: {failure['error']}")
            raise ValueError(f"{error_summary}. See logs for details.")

        log.info(f"Data file MD5 validation completed: {validated_count}/{len(files_to_validate)} files ({validation_mode} mode)")
        return {'validated_count': validated_count, 'total_count': total_count, 'validation_mode': validation_mode, 'failed_files': [], 'verified_files': verified_files}
