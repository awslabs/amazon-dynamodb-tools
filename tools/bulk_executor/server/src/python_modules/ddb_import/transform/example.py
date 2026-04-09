from ..parsers.records import FullExportRecord, IncrementalExportRecord


def transform_full_record(record: FullExportRecord) -> list[FullExportRecord]:
    """
    Example: Only import items where 'status' attribute is 'active'.

    Args:
        record: record.item is the deserialized Item dict,
                record.table_key_schema has key info.

    Returns:
        list[FullExportRecord]: Single-element list to keep, empty list to skip
    """
    if record.item.get("status") == "active":
        return [record]
    return []


def transform_incremental_record(record: IncrementalExportRecord) -> list[IncrementalExportRecord]:
    """
    Example: Only import items where 'status' attribute is 'active'.

    Behavior:
        - If new_image exists and status is 'active': import the item (PUT)
        - If new_image exists but status is not 'active': skip the item
        - If new_image is None (a delete): return the record, i.e. respect the delete

    Args:
        record: record.keys, record.new_image, record.old_image,
                record.table_key_schema, record.write_timestamp_micros

    Returns:
        list[IncrementalExportRecord]: Single-element list to keep, empty list to skip
    """
    if record.new_image:
        if record.new_image.get("status") == "active":
            return [record]
        else:
            return []
    return [record]
