"""Key schema validation for DynamoDB export data files."""
import gzip
import json
from typing import Dict, List

from ...shared.logger import log


class KeySchemaValidator:
    def __init__(self, file_loader):
        self.file_loader = file_loader

    def validate(self, verified_files: List[Dict], base_path: str,
                 key_schema: Dict, export_type: str, sample_size: int = 5) -> Dict:
        """Validate that sampled data rows contain the expected key attributes.

        Args:
            verified_files: File entries that passed checksum validation.
            base_path: S3 base path for resolving file keys.
            key_schema: e.g. {'pk': {'name': 'id', 'type': 'S'}, 'sk': ...}
            export_type: 'FULL_EXPORT' or 'INCREMENTAL_EXPORT'
            sample_size: Number of rows to sample from the first file.

        Returns dict with validated_count, sampled_rows, failed_rows.
        Raises ValueError if any validation fails.
        """
        if not verified_files:
            return {'validated_count': 0, 'sampled_rows': 0, 'failed_rows': []}

        expected_keys = [(key_schema[k]['name'], key_schema[k]['type']) for k in ('pk', 'sk') if k in key_schema]
        is_incremental = export_type == 'INCREMENTAL_EXPORT'

        data_file_key = verified_files[0].get('dataFileS3Key')
        if not data_file_key:
            return {'validated_count': 0, 'sampled_rows': 0, 'failed_rows': []}

        data_file_path = self.file_loader.join_path(base_path, data_file_key)

        try:
            raw = self.file_loader.read_file(data_file_path)
            content = gzip.decompress(raw) if data_file_path.endswith('.gz') else raw
            lines = content.decode('utf-8').strip().split('\n')
        except Exception as e:
            log.warning(f"Key validation: could not read data file: {e}")
            return {'validated_count': 0, 'sampled_rows': 0, 'failed_rows': []}

        rows_to_check = lines[:sample_size]
        validated_count = 0
        failed_rows = []
        total_item_size = 0

        for i, line in enumerate(rows_to_check):
            try:
                data = json.loads(line)
            except json.JSONDecodeError:
                failed_rows.append({'row': i, 'error': 'Malformed JSON'})
                continue

            error = self._check_incremental_keys(data, expected_keys, i) if is_incremental else self._check_full_keys(data, expected_keys, i)
            if error:
                failed_rows.append(error)
            else:
                validated_count += 1
                total_item_size += len(line.encode('utf-8'))

        if failed_rows:
            error_summary = f"Key validation failed for {len(failed_rows)} of {len(rows_to_check)} sampled row(s)"
            log.error(error_summary)
            for f in failed_rows:
                log.error(f"  - Row {f['row']}: {f['error']}")
            raise ValueError(f"{error_summary}. See logs for details.")

        avg_item_size = total_item_size // validated_count if validated_count > 0 else 0
        log.info(f"Key validation completed: {validated_count}/{len(rows_to_check)} sampled rows verified (avg item size: {avg_item_size:,} bytes)")
        return {'validated_count': validated_count, 'sampled_rows': len(rows_to_check), 'failed_rows': [], 'avg_item_size': avg_item_size}

    def _check_incremental_keys(self, data: Dict, expected_keys: List, row_idx: int) -> Dict | None:
        keys = data.get('Keys')
        if not keys:
            return {'row': row_idx, 'error': "Missing 'Keys' field"}
        for attr_name, ddb_type in expected_keys:
            if attr_name not in keys:
                return {'row': row_idx, 'error': f"Key attribute '{attr_name}' missing from Keys"}
            td = keys[attr_name]
            if not isinstance(td, dict) or ddb_type not in td:
                return {'row': row_idx, 'error': f"Key attribute '{attr_name}' expected type '{ddb_type}' but got {td}"}
        extra = set(keys.keys()) - {n for n, _ in expected_keys}
        if extra:
            return {'row': row_idx, 'error': f"Unexpected key attributes in Keys: {extra}"}
        return None

    def _check_full_keys(self, data: Dict, expected_keys: List, row_idx: int) -> Dict | None:
        item = data.get('Item')
        if not item:
            return {'row': row_idx, 'error': "Missing 'Item' field"}
        for attr_name, ddb_type in expected_keys:
            if attr_name not in item:
                return {'row': row_idx, 'error': f"Key attribute '{attr_name}' missing from Item"}
            td = item[attr_name]
            if not isinstance(td, dict) or ddb_type not in td:
                return {'row': row_idx, 'error': f"Key attribute '{attr_name}' expected type '{ddb_type}' but got {td}"}
        return None
