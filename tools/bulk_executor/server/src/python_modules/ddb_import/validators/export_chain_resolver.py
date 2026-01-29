"""Export chain resolver for DynamoDB import operations."""

import os
from datetime import datetime
from typing import List, Tuple, Dict, Any
from ...shared.logger import log
from ..utils.enums import ImportType
from .manifest_validator import ManifestValidator
from ..utils.export_path_resolver import ExportPathResolver

class ExportChainResolver:
    """Resolves and validates export file chains based on import type."""
    
    def __init__(self, s3_client, file_loader, manifest_validator: ManifestValidator):
        """
        Initialize export chain resolver.
        
        Args:
            s3_client: boto3 S3 client
            file_loader: FileLoader instance
            manifest_validator: ManifestValidator instance
        """
        self.s3_client = s3_client
        self.file_loader = file_loader
        self.manifest_validator = manifest_validator
    
    def get_export_chain(self, bucket: str, prefix: str, export_id: str, import_type: ImportType, source_table_name: str, target_time: str = None) -> List[Tuple[str, str, Dict[str, Any]]]:
        """
        Get ordered list of exports for the specified import type.
        
        Args:
            bucket: S3 bucket name
            prefix: S3 prefix (optional)
            export_id: Primary export ID
            import_type: Type of import operation
            source_table_name: Source table name
            target_time: ISO timestamp for FULL_INCREMENTAL (required for that type)
            
        Returns:
            List of (export_type, export_id, manifest_data) tuples in chronological order
            
        Raises:
            ValueError: If chain validation fails or target_time missing for FULL_INCREMENTAL
        """
        log.info(f"Resolving export chain for import type: {import_type.value}")
        
        if import_type == ImportType.FULL_ONLY:
            return self._get_single_export(bucket, prefix, export_id, source_table_name, expected_export_type='FULL_EXPORT')
        
        elif import_type == ImportType.INCREMENTAL_ONLY:
            return self._get_single_export(bucket, prefix, export_id, source_table_name, expected_export_type='INCREMENTAL_EXPORT')
        
        elif import_type == ImportType.FULL_INCREMENTAL:
            if not target_time:
                raise ValueError("target_time is required for FULL_INCREMENTAL import type")
            return self._get_chained_exports(bucket, prefix, export_id, source_table_name, target_time)
        
        else:
            raise ValueError(f"Unsupported import type: {import_type}")
    
    def _get_single_export(self, bucket: str, prefix: str, export_id: str, source_table_name: str, expected_export_type: str) -> List[Tuple[str, str, Dict[str, Any]]]:
        """Get and validate a single export."""
        log.info(f"Validating single export: {export_id}")
        
        resolver = ExportPathResolver(bucket, export_id, prefix)
        manifest = self.manifest_validator.validate_and_parse_manifests(resolver)
        
        # Validate export type matches expectation
        if manifest['export_type'] != expected_export_type:
            raise ValueError(
                f"Export type mismatch: expected {expected_export_type}, got {manifest['export_type']}"
            )
        
        export_type = "FULL" if expected_export_type == 'FULL_EXPORT' else "INCREMENTAL"
        log.info(f"Single export validated successfully: {export_type}")
        
        return [(export_type, export_id, manifest)]
    
    def _get_chained_exports(self, bucket: str, prefix: str, full_export_id: str, source_table_name: str, target_time: str) -> List[Tuple[str, str, Dict[str, Any]]]:
        """Get and validate a chain of full + incremental exports up to target_time."""
        log.info(f"Building export chain from {full_export_id} to {target_time}")
        
        exports = []
        target_timestamp = self._parse_iso_to_timestamp(target_time)
        
        # 1. Get and validate the full export
        full_resolver = ExportPathResolver(bucket, full_export_id, prefix)
        full_manifest = self.manifest_validator.validate_and_parse_manifests(full_resolver)
        
        if full_manifest['export_type'] != 'FULL_EXPORT':
            raise ValueError(f"Expected FULL_EXPORT, got {full_manifest['export_type']}")
        
        exports.append(("FULL", full_export_id, full_manifest))
        
        # Get the full export's exportTime to start the chain
        current_export_time = full_manifest.get('export_time')
        if not current_export_time:
            log.warning("Full export missing exportTime, cannot chain incrementals")
            return exports
        
        log.info(f"Full export time: {current_export_time}")
        
        # 2. Build incremental chain in single S3 scan
        search_prefix = f"{prefix}/AWSDynamoDB/" if prefix else "AWSDynamoDB/"
        chain_count = 0
        seen_from_times = set()  # Track seen export_from_time values for duplicate detection
        
        try:
            paginator = self.s3_client.get_paginator('list_objects_v2')
            
            # Process incrementals in chronological order (S3 returns lexicographically sorted)
            for page in paginator.paginate(Bucket=bucket, Prefix=search_prefix, Delimiter='/'):
                for obj in page.get('CommonPrefixes', []):
                    export_id = obj['Prefix'].rstrip('/').split('/')[-1]
                    
                    if export_id == full_export_id:
                        continue
                    
                    try:
                        resolver = ExportPathResolver(bucket, export_id, prefix)
                        manifest = self.manifest_validator.validate_and_parse_manifests(resolver)

                        if manifest['table_name'] != source_table_name:
                            log.warning(f'Found export for table {manifest["table_name"]} in chain rather than {source_table_name}')
                            continue
                        
                        if manifest['export_type'] == 'INCREMENTAL_EXPORT':
                            from_time = manifest.get('export_from_time')
                            to_time = manifest.get('export_to_time')
                            
                            if not from_time or not to_time:
                                log.warning(f"Incremental export {export_id} missing time fields")
                                continue
                            
                            # Check for duplicate export_from_time
                            if from_time in seen_from_times:
                                raise ValueError(f"Duplicate export_from_time found: {from_time}")
                            seen_from_times.add(from_time)
                            
                            # Check if it exceeds target time first
                            to_timestamp = self._parse_iso_to_timestamp(to_time)
                            if to_timestamp > target_timestamp:
                                log.info(f"Stopping chain: incremental {export_id} exportToTime {to_time} exceeds target {target_time}")
                                return exports

                            # Check if this incremental fits in our chain (fromTime matches current toTime)
                            if from_time == current_export_time:
                                # Add to chain and continue
                                exports.append(("INCREMENTAL", export_id, manifest))
                                current_export_time = to_time
                                chain_count += 1
                                log.info(f"Added incremental {chain_count}: {export_id} ({from_time} -> {to_time})")
                            else:
                                # This incremental doesn't fit in the chain, skip it
                                log.debug(f"Skipping incremental {export_id}: fromTime {from_time} doesn't match current chain time {current_export_time}")
                                continue
                            
                    except ValueError as ve:
                        # Re-raise ValueError (like duplicate detection) to fail the operation
                        raise ve
                    except Exception as e:
                        log.warning(f"Skipping export {export_id}: {e}")
                        continue
                    
        except Exception as e:
            log.error(f"Failed to scan incremental exports: {e}")
            raise ValueError(f"Failed to scan incremental exports: {e}")
        
        # 3. Validate chain completeness
        if current_export_time:
            final_timestamp = self._parse_iso_to_timestamp(current_export_time)
            if final_timestamp < target_timestamp:
                log.warning(
                    f"Chain incomplete: ends at {current_export_time} but target is {target_time}. "
                    f"Missing incremental exports may exist."
                )
        
        log.info(f"Export chain built successfully: 1 full + {chain_count} incrementals")
        return exports
    
    def _parse_iso_to_timestamp(self, iso_time: str) -> float:
        """Parse ISO timestamp to Unix timestamp."""
        try:
            # Handle both with and without timezone
            if iso_time.endswith('Z'):
                dt = datetime.fromisoformat(iso_time.replace('Z', '+00:00'))
            elif '+' in iso_time or iso_time.count('-') > 2:
                dt = datetime.fromisoformat(iso_time)
            else:
                dt = datetime.fromisoformat(iso_time + '+00:00')
            
            return dt.timestamp()
        except ValueError as e:
            raise ValueError(f"Invalid ISO timestamp format: {iso_time}") from e
