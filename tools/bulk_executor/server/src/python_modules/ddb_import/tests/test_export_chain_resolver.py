"""Unit tests for ExportChainResolver."""

import pytest
from unittest.mock import Mock, MagicMock
from datetime import datetime
from ..validators.export_chain_resolver import ExportChainResolver
from ..utils.enums import ImportType

class TestExportChainResolver:
    """Test cases for ExportChainResolver."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.s3_client = Mock()
        self.file_loader = Mock()
        self.manifest_validator = Mock()
        self.resolver = ExportChainResolver(
            self.s3_client, self.file_loader, self.manifest_validator
        )
    
    def test_full_only_success(self):
        """Test successful FULL_ONLY import."""
        # Setup
        manifest_data = {
            'export_type': 'FULL_EXPORT',
            'total_item_count': 1000,
            'data_files': []
        }
        self.manifest_validator.validate_and_parse_manifests.return_value = manifest_data
        
        # Execute
        result = self.resolver.get_export_chain(
            'bucket', 'prefix', 'export123', ImportType.FULL_ONLY, 'table'
        )
        
        # Verify
        assert len(result) == 1
        assert result[0] == ('FULL', 'export123', manifest_data)
        self.manifest_validator.validate_and_parse_manifests.assert_called_once()
    
    def test_full_only_wrong_type_fails(self):
        """Test FULL_ONLY fails when export is incremental."""
        # Setup
        manifest_data = {'export_type': 'INCREMENTAL_EXPORT'}
        self.manifest_validator.validate_and_parse_manifests.return_value = manifest_data
        
        # Execute & Verify
        with pytest.raises(ValueError, match="Export type mismatch: expected FULL_EXPORT"):
            self.resolver.get_export_chain(
                'bucket', 'prefix', 'export123', ImportType.FULL_ONLY, 'table'
            )
    
    def test_incremental_only_success(self):
        """Test successful INCREMENTAL_ONLY import."""
        # Setup
        manifest_data = {
            'export_type': 'INCREMENTAL_EXPORT',
            'total_item_count': 500,
            'data_files': []
        }
        self.manifest_validator.validate_and_parse_manifests.return_value = manifest_data
        
        # Execute
        result = self.resolver.get_export_chain(
            'bucket', 'prefix', 'export456', ImportType.INCREMENTAL_ONLY, 'table'
        )
        
        # Verify
        assert len(result) == 1
        assert result[0] == ('INCREMENTAL', 'export456', manifest_data)
    
    def test_incremental_only_wrong_type_fails(self):
        """Test INCREMENTAL_ONLY fails when export is full."""
        # Setup
        manifest_data = {'export_type': 'FULL_EXPORT'}
        self.manifest_validator.validate_and_parse_manifests.return_value = manifest_data
        
        # Execute & Verify
        with pytest.raises(ValueError, match="Export type mismatch: expected INCREMENTAL_EXPORT"):
            self.resolver.get_export_chain(
                'bucket', 'prefix', 'export123', ImportType.INCREMENTAL_ONLY, 'table'
            )
    
    def test_full_incremental_missing_target_time_fails(self):
        """Test FULL_INCREMENTAL fails without target_time."""
        with pytest.raises(ValueError, match="target_time is required"):
            self.resolver.get_export_chain(
                'bucket', 'prefix', 'export123', ImportType.FULL_INCREMENTAL, 'table'
            )
    
    def test_full_incremental_single_export_success(self):
        """Test FULL_INCREMENTAL with only full export (no incrementals)."""
        # Setup
        full_manifest = {
            'export_type': 'FULL_EXPORT',
            'export_time': '2024-01-01T10:00:00Z',
            'total_item_count': 1000
        }
        self.manifest_validator.validate_and_parse_manifests.return_value = full_manifest
        
        # Mock S3 discovery (no incrementals found)
        self.s3_client.get_paginator.return_value.paginate.return_value = []
        
        # Execute
        result = self.resolver.get_export_chain(
            'bucket', 'prefix', 'full123', ImportType.FULL_INCREMENTAL, 'table', '2024-01-02T00:00:00Z'
        )
        
        # Verify
        assert len(result) == 1
        assert result[0] == ('FULL', 'full123', full_manifest)
    
    def test_full_incremental_chain_success(self):
        """Test successful FULL_INCREMENTAL chain formation."""
        # Setup full export
        full_manifest = {
            'export_type': 'FULL_EXPORT',
            'export_time': '2024-01-01T10:00:00Z',
            'total_item_count': 1000,
            'table_name': 'table'
        }
        
        # Setup incremental exports
        inc1_manifest = {
            'export_type': 'INCREMENTAL_EXPORT',
            'export_from_time': '2024-01-01T10:00:00Z',
            'export_to_time': '2024-01-01T12:00:00Z',
            'total_item_count': 100,
            'table_name': 'table'
        }
        
        inc2_manifest = {
            'export_type': 'INCREMENTAL_EXPORT',
            'export_from_time': '2024-01-01T12:00:00Z',
            'export_to_time': '2024-01-01T14:00:00Z',
            'total_item_count': 50,
            'table_name': 'table'
        }
        
        # Mock manifest validator calls
        self.manifest_validator.validate_and_parse_manifests.side_effect = [
            full_manifest,  # Full export validation
            inc1_manifest,  # First incremental
            inc2_manifest   # Second incremental
        ]
        
        # Mock S3 discovery
        mock_page = {
            'CommonPrefixes': [
                {'Prefix': 'AWSDynamoDB/inc1/'},
                {'Prefix': 'AWSDynamoDB/inc2/'}
            ]
        }
        self.s3_client.get_paginator.return_value.paginate.return_value = [mock_page]
        
        # Execute
        result = self.resolver.get_export_chain(
            'bucket', '', 'full123', ImportType.FULL_INCREMENTAL, 'table', '2024-01-01T15:00:00Z'
        )
        
        # Verify
        assert len(result) == 3
        assert result[0] == ('FULL', 'full123', full_manifest)
        assert result[1] == ('INCREMENTAL', 'inc1', inc1_manifest)
        assert result[2] == ('INCREMENTAL', 'inc2', inc2_manifest)
    
    def test_full_incremental_stops_at_target_time(self):
        """Test chain stops when incremental exceeds target time."""
        # Setup
        full_manifest = {
            'export_type': 'FULL_EXPORT',
            'export_time': '2024-01-01T10:00:00Z',
            'total_item_count': 1000,
            'table_name': 'table'
        }
        
        inc1_manifest = {
            'export_type': 'INCREMENTAL_EXPORT',
            'export_from_time': '2024-01-01T10:00:00Z',
            'export_to_time': '2024-01-01T12:00:00Z',
            'total_item_count': 100,
            'table_name': 'table'
        }
        
        inc2_manifest = {
            'export_type': 'INCREMENTAL_EXPORT',
            'export_from_time': '2024-01-01T12:00:00Z',
            'export_to_time': '2024-01-01T16:00:00Z',  # Exceeds target
            'total_item_count': 50,
            'table_name': 'table'
        }
        
        self.manifest_validator.validate_and_parse_manifests.side_effect = [
            full_manifest, inc1_manifest, inc2_manifest
        ]
        
        mock_page = {
            'CommonPrefixes': [
                {'Prefix': 'AWSDynamoDB/inc1/'},
                {'Prefix': 'AWSDynamoDB/inc2/'}
            ]
        }
        self.s3_client.get_paginator.return_value.paginate.return_value = [mock_page]
        
        # Execute with target time before inc2 ends
        result = self.resolver.get_export_chain(
            'bucket', '', 'full123', ImportType.FULL_INCREMENTAL, 'table', '2024-01-01T14:00:00Z'
        )
        
        # Verify - should stop at inc1, not include inc2
        assert len(result) == 2
        assert result[0] == ('FULL', 'full123', full_manifest)
        assert result[1] == ('INCREMENTAL', 'inc1', inc1_manifest)
    
    def test_broken_chain_fails(self):
        """Test chain fails when time sequence is broken."""
        # Setup
        full_manifest = {
            'export_type': 'FULL_EXPORT',
            'export_time': '2024-01-01T10:00:00Z',
            'total_item_count': 1000
        }
        
        # Incremental with wrong fromTime (gap in chain)
        inc_manifest = {
            'export_type': 'INCREMENTAL_EXPORT',
            'export_from_time': '2024-01-01T11:00:00Z',  # Should be 10:00:00Z
            'export_to_time': '2024-01-01T12:00:00Z',
            'total_item_count': 100,
            'table_name': 'table'
        }
        
        self.manifest_validator.validate_and_parse_manifests.side_effect = [
            full_manifest, inc_manifest
        ]
        
        mock_page = {
            'CommonPrefixes': [{'Prefix': 'AWSDynamoDB/inc1/'}]
        }
        self.s3_client.get_paginator.return_value.paginate.return_value = [mock_page]
        
        # Execute
        result = self.resolver.get_export_chain(
            'bucket', '', 'full123', ImportType.FULL_INCREMENTAL, 'table', '2024-01-01T15:00:00Z'
        )
        
        # Verify - should only contain full export (chain broken)
        assert len(result) == 1
        assert result[0] == ('FULL', 'full123', full_manifest)
    
    def test_duplicate_from_time_fails(self):
        """Test discovery fails when multiple exports have same export_from_time."""
        # Setup
        full_manifest = {
            'export_type': 'FULL_EXPORT',
            'export_time': '2024-01-01T10:00:00Z',
            'total_item_count': 1000,
            'table_name': 'table'
        }
        
        # Two incrementals with same fromTime
        inc1_manifest = {
            'export_type': 'INCREMENTAL_EXPORT',
            'export_from_time': '2024-01-01T10:00:00Z',
            'export_to_time': '2024-01-01T12:00:00Z',
            'total_item_count': 100,
            'table_name': 'table'
        }
        
        inc2_manifest = {
            'export_type': 'INCREMENTAL_EXPORT',
            'export_from_time': '2024-01-01T10:00:00Z',  # Duplicate!
            'export_to_time': '2024-01-01T13:00:00Z',
            'total_item_count': 50,
            'table_name': 'table'
        }
        
        self.manifest_validator.validate_and_parse_manifests.side_effect = [
            full_manifest, inc1_manifest, inc2_manifest
        ]
        
        mock_page = {
            'CommonPrefixes': [
                {'Prefix': 'AWSDynamoDB/inc1/'},
                {'Prefix': 'AWSDynamoDB/inc2/'}
            ]
        }
        self.s3_client.get_paginator.return_value.paginate.return_value = [mock_page]
        
        # Execute & Verify
        with pytest.raises(ValueError, match="Duplicate export_from_time"):
            self.resolver.get_export_chain(
                'bucket', '', 'full123', ImportType.FULL_INCREMENTAL, 'table', '2024-01-01T15:00:00Z'
            )
    
    def test_invalid_iso_timestamp_fails(self):
        """Test invalid ISO timestamp format fails."""
        with pytest.raises(ValueError, match="Invalid ISO timestamp format"):
            self.resolver._parse_iso_to_timestamp("not-a-timestamp")
    
    def test_iso_timestamp_parsing_variants(self):
        """Test various ISO timestamp formats are parsed correctly."""
        # Test different formats
        test_cases = [
            "2024-01-01T10:00:00Z",
            "2024-01-01T10:00:00+00:00",
            "2024-01-01T10:00:00-05:00",
            "2024-01-01T10:00:00"
        ]
        
        for timestamp in test_cases:
            result = self.resolver._parse_iso_to_timestamp(timestamp)
            assert isinstance(result, float)
            assert result > 0


if __name__ == '__main__':
    pytest.main([__file__])
    def test_target_timestamp_filtering(self):
        """Test that chain stops when incremental exceeds target timestamp."""
        # Setup
        full_manifest = {
            'export_type': 'FULL_EXPORT',
            'export_time': '2024-01-01T10:00:00Z',
            'table_name': 'test_table',
            'total_item_count': 1000
        }
        
        # This incremental exceeds target time
        inc_manifest = {
            'export_type': 'INCREMENTAL_EXPORT',
            'export_from_time': '2024-01-01T10:00:00Z',
            'export_to_time': '2024-01-01T16:00:00Z',  # Exceeds target
            'table_name': 'test_table',
            'total_item_count': 100
        }
        
        self.manifest_validator.validate_and_parse_manifests.side_effect = [
            full_manifest, inc_manifest
        ]
        
        # Mock S3 to return the incremental export
        mock_page = {
            'CommonPrefixes': [{'Prefix': 'AWSDynamoDB/01716791487000-6ddcf8cf/'}]
        }
        self.s3_client.get_paginator.return_value.paginate.return_value = [mock_page]
        
        # Execute with target time before incremental ends
        result = self.resolver.get_export_chain(
            'bucket', '', 'full123', ImportType.FULL_INCREMENTAL, 'table', '2024-01-01T14:00:00Z'
        )
        
        # Verify - should only contain full export (incremental exceeds target)
        assert len(result) == 1
        assert result[0] == ('FULL', 'full123', full_manifest)
    
    def test_chronological_ordering_with_epoch_prefixes(self):
        """Test that exports are processed in chronological order based on epoch prefixes."""
        # Setup
        full_manifest = {
            'export_type': 'FULL_EXPORT',
            'export_time': '2024-01-01T10:00:00Z',
            'table_name': 'test_table',
            'total_item_count': 1000
        }
        
        # Two incrementals with epoch prefixes (should be processed in order)
        inc1_manifest = {
            'export_type': 'INCREMENTAL_EXPORT',
            'export_from_time': '2024-01-01T10:00:00Z',
            'export_to_time': '2024-01-01T12:00:00Z',
            'table_name': 'test_table',
            'total_item_count': 100
        }
        
        inc2_manifest = {
            'export_type': 'INCREMENTAL_EXPORT',
            'export_from_time': '2024-01-01T12:00:00Z',
            'export_to_time': '2024-01-01T14:00:00Z',
            'table_name': 'test_table',
            'total_item_count': 50
        }
        
        self.manifest_validator.validate_and_parse_manifests.side_effect = [
            full_manifest, inc1_manifest, inc2_manifest
        ]
        
        # Mock S3 to return exports in lexicographical order (which equals chronological for epoch prefixes)
        mock_page = {
            'CommonPrefixes': [
                {'Prefix': 'AWSDynamoDB/01716791487000-6ddcf8cf/'},  # Earlier epoch
                {'Prefix': 'AWSDynamoDB/01716791500000-7eecf9da/'}   # Later epoch
            ]
        }
        self.s3_client.get_paginator.return_value.paginate.return_value = [mock_page]
        
        # Execute
        result = self.resolver.get_export_chain(
            'bucket', '', 'full123', ImportType.FULL_INCREMENTAL, 'table', '2024-01-01T15:00:00Z'
        )
        
        # Verify - should process in chronological order
        assert len(result) == 3
        assert result[0] == ('FULL', 'full123', full_manifest)
        assert result[1] == ('INCREMENTAL', '01716791487000-6ddcf8cf', inc1_manifest)
        assert result[2] == ('INCREMENTAL', '01716791500000-7eecf9da', inc2_manifest)
    
    def test_table_name_extraction_in_results(self):
        """Test that table names are extracted and returned in manifest data."""
        # Setup
        full_manifest = {
            'export_type': 'FULL_EXPORT',
            'export_time': '2024-01-01T10:00:00Z',
            'table_name': 'my_test_table',
            'total_item_count': 1000
        }
        
        self.manifest_validator.validate_and_parse_manifests.return_value = full_manifest
        
        # Execute
        result = self.resolver.get_export_chain(
            'bucket', 'prefix', 'export123', ImportType.FULL_ONLY, 'table'
        )
        
        # Verify table name is in result
        assert len(result) == 1
        assert result[0][2]['table_name'] == 'my_test_table'
    
    def test_single_s3_scan_optimization(self):
        """Test that only one S3 scan is performed for chain building."""
        # Setup
        full_manifest = {
            'export_type': 'FULL_EXPORT',
            'export_time': '2024-01-01T10:00:00Z',
            'table_name': 'test_table',
            'total_item_count': 1000
        }
        
        inc_manifest = {
            'export_type': 'INCREMENTAL_EXPORT',
            'export_from_time': '2024-01-01T10:00:00Z',
            'export_to_time': '2024-01-01T12:00:00Z',
            'table_name': 'test_table',
            'total_item_count': 100
        }
        
        self.manifest_validator.validate_and_parse_manifests.side_effect = [
            full_manifest, inc_manifest
        ]
        
        # Mock S3 paginator
        mock_paginator = Mock()
        mock_page = {
            'CommonPrefixes': [{'Prefix': 'AWSDynamoDB/inc1/'}]
        }
        mock_paginator.paginate.return_value = [mock_page]
        self.s3_client.get_paginator.return_value = mock_paginator
        
        # Execute
        result = self.resolver.get_export_chain(
            'bucket', '', 'full123', ImportType.FULL_INCREMENTAL, 'table', '2024-01-01T15:00:00Z'
        )
        
        # Verify only one S3 scan was performed
        self.s3_client.get_paginator.assert_called_once_with('list_objects_v2')
        mock_paginator.paginate.assert_called_once()
        
        # Verify result
        assert len(result) == 2
        assert result[0] == ('FULL', 'full123', full_manifest)
        assert result[1] == ('INCREMENTAL', 'inc1', inc_manifest)
    
    def test_gap_in_chain_skips_incremental(self):
        """Test that incrementals with gaps in time chain are skipped."""
        # Setup
        full_manifest = {
            'export_type': 'FULL_EXPORT',
            'export_time': '2024-01-01T10:00:00Z',
            'table_name': 'test_table',
            'total_item_count': 1000
        }
        
        # Incremental with gap (fromTime doesn't match full export's export_time)
        inc_manifest = {
            'export_type': 'INCREMENTAL_EXPORT',
            'export_from_time': '2024-01-01T11:00:00Z',  # Gap! Should be 10:00:00Z
            'export_to_time': '2024-01-01T12:00:00Z',
            'table_name': 'test_table',
            'total_item_count': 100
        }
        
        self.manifest_validator.validate_and_parse_manifests.side_effect = [
            full_manifest, inc_manifest
        ]
        
        mock_page = {
            'CommonPrefixes': [{'Prefix': 'AWSDynamoDB/inc1/'}]
        }
        self.s3_client.get_paginator.return_value.paginate.return_value = [mock_page]
        
        # Execute
        result = self.resolver.get_export_chain(
            'bucket', '', 'full123', ImportType.FULL_INCREMENTAL, 'table', '2024-01-01T15:00:00Z'
        )
        
        # Verify - should only contain full export (incremental skipped due to gap)
        assert len(result) == 1
        assert result[0] == ('FULL', 'full123', full_manifest)
