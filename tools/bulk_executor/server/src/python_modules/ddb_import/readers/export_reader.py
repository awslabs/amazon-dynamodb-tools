"""Export file reader for DynamoDB export data."""

from typing import List, Dict, Any, Tuple
from ...shared.logger import log

def get_export_file_paths(
    data_files: List[Dict[str, Any]],
    file_base_path: str
) -> Tuple[List[str], int]:
    """
    Get full S3 paths for DynamoDB export files.
    
    This function constructs full S3 paths from data file metadata and calculates
    the total expected item count.
    
    Args:
        data_files: List of data file metadata from manifest, each containing:
            - dataFileS3Key: S3 key for the data file
            - itemCount: Expected number of items in the file
        file_base_path: S3 base path for resolving data file paths (e.g., 's3://bucket/prefix/')
        logger: Logger instance for logging
    
    Returns:
        Tuple of (list of full S3 file paths, total expected item count)
        
    Example:
        >>> data_files = [
        ...     {'dataFileS3Key': 'data/file1.json.gz', 'itemCount': 1000},
        ...     {'dataFileS3Key': 'data/file2.json.gz', 'itemCount': 2000}
        ... ]
        >>> file_paths, total_count = get_export_file_paths(
        ...     data_files, 's3://bucket/export/', logger
        ... )
        >>> print(f"Total items: {total_count}")
        Total items: 3000
    """
    file_paths = []
    total_expected_items = 0
    
    skipped_empty = 0
    for data_file in data_files:
        file_key = data_file['dataFileS3Key']
        expected_count = data_file['itemCount']
        
        if expected_count == 0:
            skipped_empty += 1
            continue

        # Construct full S3 file path
        file_path = file_base_path.rstrip('/') + '/' + file_key.lstrip('/')
        
        file_paths.append(file_path)
        total_expected_items += expected_count
        log.info(f"Will process file: {file_key} (expected items: {expected_count})")
    
    if skipped_empty:
        log.info(f"Skipped {skipped_empty} empty data file(s)")

    log.info(f"Total files to process: {len(file_paths)}")
    log.info(f"Total expected items: {total_expected_items}")
    
    return file_paths, total_expected_items
