"""Manifest validator for DynamoDB export files."""

import json
from typing import Dict

from ...shared.logger import log
from ..utils.export_path_resolver import ExportPathResolver
from .md5_validator import MD5Validator


class ManifestValidator:
    """Validates and parses DynamoDB export manifest files."""
    
    def __init__(self, file_loader):
        """
        Initialize manifest validator.
        
        Args:
            file_loader: FileLoader instance for reading files
        """
        self.file_loader = file_loader
    
    def validate_and_parse_manifests(self, path_resolver: ExportPathResolver) -> Dict:
        """
        Validate and parse both manifest files.
        
        Args:
            path_resolver: PathResolver instance for reading files

        Returns:
            Dictionary containing:
            - total_item_count: Expected total items
            - output_format: Export format (should be DYNAMODB_JSON)
            - export_type: Export type (FULL_EXPORT or INCREMENTAL_EXPORT)
            - data_files: List of data file metadata
            
        Raises:
            ValueError: If validation fails
        """
        log.info(f"Starting manifest validation for export path: {path_resolver}")

        base_path = path_resolver.get_base_path()
        data_base_path = path_resolver.get_data_base_path()
        manifest_base_path = path_resolver.get_manifest_base_path()

        log.info(f"base_path {base_path}")
        log.info(f"data_base_path {data_base_path}")
        log.info(f"manifest_base_path {manifest_base_path}")
        
        # Step 1: Validate manifest-summary.json MD5 checksum
        log.info("Manifest validator-1: Validating manifest-summary.json MD5 checksum...")
        manifest_summary_path = self.file_loader.join_path(manifest_base_path, 'manifest-summary.json')
        manifest_summary_md5_path = self.file_loader.join_path(manifest_base_path, 'manifest-summary.md5')
        
        manifest_summary_content = self.file_loader.read_file(manifest_summary_path)
        log.info(f"manifest-summary read successfully")

        manifest_summary_expected_md5 = self.file_loader.read_file(manifest_summary_md5_path).decode('utf-8').strip()
        log.info(f"manifest-summary.md5 read successfully")
        
        try:
            if MD5Validator is not None:
                MD5Validator.validate_file_checksum(manifest_summary_content, manifest_summary_expected_md5)
            log.info("manifest-summary.json MD5 checksum validated successfully")
        except ValueError as e:
            log.error(f"manifest-summary.json MD5 validation failed: {e}")
            raise
        
        # Step 2: Parse manifest-summary.json
        log.info("Manifest validator-2: Parsing manifest-summary.json...")
        try:
            manifest_summary = json.loads(manifest_summary_content.decode('utf-8'))
        except json.JSONDecodeError as e:
            log.error(f"Failed to parse manifest-summary.json: {e}")
            raise ValueError(f"Invalid JSON in manifest-summary.json: {e}")
        
        total_item_count = manifest_summary.get('itemCount')
        output_format = manifest_summary.get('outputFormat')
        export_time = manifest_summary.get('exportTime')
        export_from_time = manifest_summary.get('exportFromTime')
        export_to_time = manifest_summary.get('exportToTime')
        export_type = manifest_summary.get('exportType', 'FULL_EXPORT')  # Default to FULL_EXPORT if not present
        manifest_files_key = manifest_summary.get('manifestFilesS3Key')
        
        log.info(f"Parsed manifest-summary.json: itemCount={total_item_count}, outputFormat={output_format}, exportType={export_type}, exportTime={export_time}, exportFromTime={export_from_time}, exportToTime={export_to_time}")
        
        # Step 3: Extract table name from ARN
        log.info("Manifest validator-3: Extracting table name from arn...")
        table_name = self._extract_table_name(manifest_summary)
        
        # Step 4: Validate output format
        # DynamoDB exports support DYNAMODB_JSON and ION formats; only DYNAMODB_JSON is currently supported.
        log.info("Manifest validator-4: Validating output format...")
        if output_format != 'DYNAMODB_JSON':
            error_msg = f"Unsupported output format: {output_format}. Only DYNAMODB_JSON is currently supported (ION is not supported)."
            log.error(error_msg)
            raise ValueError(error_msg)
        log.info("Output format validated successfully")
        
        # Step 5: Validate output view for incremental exports
        # Incremental exports support NEW_AND_OLD_IMAGES and NEW_IMAGE.
        log.info("Manifest validator-5: Validating export views...")
        output_view = None
        if export_type == 'INCREMENTAL_EXPORT':
            output_view = manifest_summary.get('outputView')
            if output_view not in ('NEW_AND_OLD_IMAGES', 'NEW_IMAGE'):
                raise ValueError(
                    f"Unsupported output view: {output_view}. "
                    f"Only NEW_AND_OLD_IMAGES and NEW_IMAGE are supported for incremental exports."
                )
            log.info("Output view validated successfully")
        
        # Step 6: Validate manifest-files.json MD5 checksum
        log.info("Manifest validator-6: Validating manifest-files.json MD5 checksum..." + manifest_files_key)
        manifest_files_path = self.file_loader.join_path(base_path, manifest_files_key)
        manifest_files_md5_path = self.file_loader.join_path(manifest_base_path, 'manifest-files.md5')

        manifest_files_content = self.file_loader.read_file(manifest_files_path)
        manifest_files_expected_md5 = self.file_loader.read_file(manifest_files_md5_path).decode('utf-8').strip()
        
        try:
            if MD5Validator is not None:
                MD5Validator.validate_file_checksum(manifest_files_content, manifest_files_expected_md5)
            log.info("manifest-files.json MD5 checksum validated successfully")
        except ValueError as e:
            log.error(f"manifest-files.json MD5 validation failed: {e}")
            raise
        
        # Step 7: Parse manifest-files.json (newline-delimited JSON)
        log.info("Manifest validator-7: Parsing manifest-files.json...")
        data_files = []
        try:
            lines = manifest_files_content.decode('utf-8').strip().split('\n')
            for line_num, line in enumerate(lines, 1):
                if line.strip():  # Skip empty lines
                    try:
                        file_entry = json.loads(line)
                        data_files.append(file_entry)
                    except json.JSONDecodeError as e:
                        log.error(f"Failed to parse line {line_num} in manifest-files.json: {e}")
                        raise ValueError(f"Invalid JSON at line {line_num} in manifest-files.json: {e}")
        except Exception as e:
            log.error(f"Failed to parse manifest-files.json: {e}")
            raise
        
        log.info(f"Parsed {len(data_files)} data file entries from manifest-files.json")

        # Step 8: Calculate and validate item count consistency
        log.info("Manifest validator-8: Validating item count consistency...")
        calculated_item_count = sum(entry.get('itemCount', 0) for entry in data_files)
        
        if calculated_item_count != total_item_count:
            error_msg = (
                f"Item count mismatch: manifest-summary.json reports {total_item_count} items, "
                f"but manifest-files.json entries sum to {calculated_item_count} items"
            )
            log.error(error_msg)
            raise ValueError(error_msg)
        
        log.info(f"Item count validated successfully: {total_item_count} items")
        
        # Step 9: Log success summary
        log.info(
            f"Manifest validator-9: Manifest validation completed successfully: "
            f"{total_item_count} total items across {len(data_files)} data files"
        )
        
        return {
            'total_item_count': total_item_count,
            'output_format': output_format,
            'export_type': export_type,
            'output_view': output_view,
            'export_time': export_time,
            'export_from_time': export_from_time,
            'export_to_time': export_to_time,
            'table_name': table_name,
            'data_files': data_files
        }

    def _extract_table_name(self, manifest_summary: dict) -> str:
        """
        Extract table name from the manifest's tableArn.
        
        Args:
            manifest_summary: Parsed manifest-summary.json content
            
        Returns:
            Table name extracted from the ARN
            
        Raises:
            ValueError: If table ARN is missing or invalid
        """
        table_arn = manifest_summary.get('tableArn')
        if not table_arn:
            error_msg = "Missing tableArn in manifest-summary.json"
            log.error(error_msg)
            raise ValueError(error_msg)
        
        # Extract table name from ARN: arn:aws:dynamodb:region:account:table/table-name
        try:
            if not table_arn.startswith('arn:aws:dynamodb:'):
                raise ValueError("Not a valid DynamoDB table ARN")
            table_name = table_arn.split('/')[-1]
            log.info(f"Extracted table name: {table_name}")
            return table_name
        except Exception as e:
            error_msg = f"Invalid tableArn format: {table_arn} - {e}"
            log.error(error_msg)
            raise ValueError(error_msg)
