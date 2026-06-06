from python_modules.shared.export.parsers.records import FullExportRecord, IncrementalExportRecord


def transform_full_record(record: FullExportRecord) -> list[FullExportRecord]:
    """
    Example: Only load items where 'location' is 'Manawatu, New Zealand'.

    All item data is already deserialized from DDB-JSON to native Python types
    before reaching this function. You work with plain strings, numbers, lists,
    and dicts — not DDB type descriptors like {"S": "hello"}.

    Args:
        record.item: Deserialized Python dict.
            Example: {"pk": "user123", "location": "Manawatu, New Zealand"}
            NOT: {"pk": {"S": "user123"}, "location": {"S": "Manawatu, New Zealand"}}
        record.table_key_schema: Key schema dict.
            Example (pk only): {"pk": {"name": "Id", "type": "N"}}
            Example (pk + sk):  {"pk": {"name": "Id", "type": "N"}, "sk": {"name": "sort_key", "type": "S"}}

    Returns:
        list[FullExportRecord]: Single-element list to keep, empty list to skip
    """
    if record.item.get("location") == "Manawatu, New Zealand":
        return [record]
    return []


def transform_incremental_record(record: IncrementalExportRecord) -> list[IncrementalExportRecord]:
    """
    Example: Only load items where 'location' is 'Manawatu, New Zealand'.

    All item data is already deserialized from DDB-JSON to native Python types.

    Behavior:
        - If new_image exists and location matches: load the item (PUT)
        - If new_image exists but location doesn't match: skip the item
        - If new_image is None (a delete): return the record, i.e. respect the delete

    Args:
        record.keys: Deserialized key dict. Example: {"pk": "user123", "sk": "profile"}
        record.new_image: Deserialized full item dict, or None for deletes.
        record.old_image: Deserialized full item dict, or None.
        record.table_key_schema: Key schema dict.
            Example (pk only): {"pk": {"name": "Id", "type": "N"}}
            Example (pk + sk):  {"pk": {"name": "Id", "type": "N"}, "sk": {"name": "sort_key", "type": "S"}}
        record.write_timestamp_micros: Integer timestamp of the write.

    Returns:
        list[IncrementalExportRecord]: Single-element list to keep, empty list to skip
    """
    if record.new_image:
        if record.new_image.get("location") == "Manawatu, New Zealand":
            return [record]
        return []
    return [record]
