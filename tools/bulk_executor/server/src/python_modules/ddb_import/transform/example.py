from ..parsers.records import FullExportRecord, IncrementalExportRecord


def transform_full_record(record: FullExportRecord) -> list[FullExportRecord]:
    """
    Example: Only import items where 'status' attribute is 'active'.

    All item data is already deserialized from DDB-JSON to native Python types
    before reaching this function. You work with plain strings, numbers, lists,
    and dicts — not DDB type descriptors like {"S": "hello"}.

    Args:
        record.item: Deserialized Python dict.
            Example: {"pk": "user123", "status": "active", "age": 25, "tags": ["a", "b"]}
            NOT: {"pk": {"S": "user123"}, "status": {"S": "active"}, "age": {"N": "25"}}
        record.table_key_schema: Key schema dict.
            Example (pk only): {"pk": {"name": "Id", "type": "N"}}
            Example (pk + sk):  {"pk": {"name": "Id", "type": "N"}, "sk": {"name": "sort_key", "type": "S"}}

    Returns:
        list[FullExportRecord]: Single-element list to keep, empty list to skip
    """
    if record.item.get("status") == "active":
        return [record]
    return []


def transform_incremental_record(record: IncrementalExportRecord) -> list[IncrementalExportRecord]:
    """
    Example: Only import items where 'status' attribute is 'active'.

    All item data is already deserialized from DDB-JSON to native Python types.

    Behavior:
        - If new_image exists and status is 'active': import the item (PUT)
        - If new_image exists but status is not 'active': skip the item
        - If new_image is None (a delete): return the record, i.e. respect the delete

    Args:
        record.keys: Deserialized key dict. Example: {"pk": "user123", "sk": "profile"}
        record.new_image: Deserialized full item dict, or None for deletes.
            Example: {"pk": "user123", "status": "active", "age": 25}
        record.old_image: Deserialized full item dict, or None.
        record.table_key_schema: Key schema dict.
            Example (pk only): {"pk": {"name": "Id", "type": "N"}}
            Example (pk + sk):  {"pk": {"name": "Id", "type": "N"}, "sk": {"name": "sort_key", "type": "S"}}
        record.write_timestamp_micros: Integer timestamp of the write.

    Returns:
        list[IncrementalExportRecord]: Single-element list to keep, empty list to skip
    """
    if record.new_image:
        if record.new_image.get("status") == "active":
            return [record]
        else:
            return []
    return [record]
