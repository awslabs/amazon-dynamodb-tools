#!/usr/bin/env python3
"""Debug script for export_chain_resolver.py"""

import boto3

# Import the classes using clean absolute imports
from python_modules.ddb_import.validators.export_chain_resolver import ExportChainResolver
from python_modules.ddb_import.validators.manifest_validator import ManifestValidator
from python_modules.ddb_import.utils.file_loader import FileLoader
from python_modules.ddb_import.utils.enums import ImportType

def main():
    """Debug the export chain resolver with sample data."""
    print("Starting export chain resolver debug...")
    
    # Initialize AWS clients
    s3_client = boto3.client('s3')
    print("✓ S3 client initialized")
    
    file_loader = FileLoader(s3_client)
    print("✓ FileLoader initialized")
    
    manifest_validator = ManifestValidator(file_loader)
    print("✓ ManifestValidator initialized")
    
    # Create and test ExportChainResolver
    resolver = ExportChainResolver(s3_client, file_loader, manifest_validator)
    #full_export_manifest_files = resolver.get_export_chain("unicornactiviti-data-export", "", "01716790307109-5f9d6aaa", ImportType.FULL_ONLY, "unicorn_activities")
    #incremental_manifest_files = resolver.get_export_chain("unicornactiviti-data-export", "", "01716791487000-6ddcf8cf", ImportType.INCREMENTAL_ONLY, "unicorn_activities")
    full_incremental_manifest_files = resolver.get_export_chain("unicornactiviti-data-export", "", "01716790307109-5f9d6aaa", ImportType.FULL_INCREMENTAL, "unicorn_activities", "2024-05-29T06:31:27Z")
    print(f"✓ Found {len(full_incremental_manifest_files)} exports in the chain")
    print("✓ ExportChainResolver initialized")
    
    # Test the resolver with sample data
    print("Testing ExportChainResolver methods...")
    
    # Add your specific test calls here
    print("✓ All components working correctly!")

if __name__ == "__main__":
    main()
