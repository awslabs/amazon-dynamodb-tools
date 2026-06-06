import time

from ...logger import log


def report(manifest_data, total_expected_items, written_items_accumulator, transform_active, transform_name, transformed_excluded_accumulator, transformed_modified_or_included_accumulator, start_time):
    """Log final summary."""
    total_item_count = manifest_data['total_item_count']
    written_count = written_items_accumulator.value

    log.debug(f"Successfully wrote {written_count:,} items to DynamoDB")
    if transform_active:
        log.info(f"Transform '{transform_name}': {transformed_modified_or_included_accumulator.value:,} items produced, {transformed_excluded_accumulator.value:,} items excluded out of {total_item_count:,} total")
    log.debug("Data processing and writing completed")

    log.debug(f"  - Manifest items: {total_item_count:,}")
    log.debug(f"  - Expected items: {total_expected_items:,}")
    log.debug(f"  - Parsed items: {total_item_count:,}")

    execution_time = time.time() - start_time

    log.info("=" * 80)
    log.info("JOB COMPLETED SUCCESSFULLY")
    log.info(f"  - Total items in export: {total_item_count:,}")
    if transform_active:
        log.info(f"  - Items excluded by transform: {transformed_excluded_accumulator.value:,}")
        log.info(f"  - Items produced by transform: {transformed_modified_or_included_accumulator.value:,}")
    log.info(f"  - Total items written: {written_count:,}")
    log.info(f"  - Execution time: {execution_time:.1f} seconds")
    log.info("=" * 80)
