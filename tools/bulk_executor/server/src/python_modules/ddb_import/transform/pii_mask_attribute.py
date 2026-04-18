from ..parsers.records import FullExportRecord, IncrementalExportRecord


# Attribute names to mask if they are not the partition key or sort key.
# Add more names here as needed — matching is case-sensitive.
PII_ATTRIBUTES = [
    "Name",
]


def _get_key_attribute_names(table_key_schema):
    """Return the set of key attribute names from the table key schema.

    The key schema is a dict like {"pk": {"name": "Id", "type": "N"}, "sk": {"name": "sort_key", "type": "S"}}.
    Returns e.g. {"Id", "sort_key"} or {"Id"} for pk-only tables.
    """
    return {table_key_schema[k]["name"] for k in ("pk", "sk") if k in table_key_schema}


def _mask_word(word):
    """Mask a single word, keeping the first and last character.

    Examples:
        "Alice"   -> "A***e"
        "Jo"      -> "J*o"
        "A"       -> "A"
        ""        -> ""
    """
    if len(word) <= 1:
        return word
    return word[0] + "*" * (len(word) - 2) + word[-1]


def _mask_value(value):
    """Mask each space-separated word in a string value.

    Non-string values are returned unchanged.

    Examples:
        "Alice Smith"  -> "A***e S***h"
        "Jo"           -> "J*o"
        42             -> 42
    """
    if not isinstance(value, str):
        return value
    return " ".join(_mask_word(word) for word in value.split(" "))


def _mask_pii(item, key_names):
    """Mask PII attributes in the item, skipping any that are key attributes."""
    for attr in PII_ATTRIBUTES:
        if attr not in key_names and attr in item:
            item[attr] = _mask_value(item[attr])


def transform_full_record(record: FullExportRecord) -> list[FullExportRecord]:
    """
    Example: Mask PII attributes in items during import.

    Masks attributes listed in PII_ATTRIBUTES by keeping the first and last
    character of each space-separated word and replacing the middle with '*'.
    Attributes that are the partition key or sort key are left intact.

    All item data is already deserialized from DDB-JSON to native Python types
    before reaching this function. You work with plain strings, numbers, lists,
    and dicts — not DDB type descriptors like {"S": "hello"}.

    Args:
        record.item: Deserialized Python dict.
            Example: {"Id": 42, "Name": "Alice Smith", "status": "active"}
            NOT: {"Id": {"N": "42"}, "Name": {"S": "Alice Smith"}}
        record.table_key_schema: Key schema dict.
            Example (pk only): {"pk": {"name": "Id", "type": "N"}}
            Example (pk + sk):  {"pk": {"name": "Id", "type": "N"}, "sk": {"name": "sort_key", "type": "S"}}

    Returns:
        list[FullExportRecord]: Single-element list with the modified record.

    Example result:
        Input:  {"Id": 42, "Name": "Alice Smith", "status": "active"}
        Output: {"Id": 42, "Name": "A***e S***h", "status": "active"}
    """
    key_names = _get_key_attribute_names(record.table_key_schema)
    _mask_pii(record.item, key_names)
    return [record]


def transform_incremental_record(record: IncrementalExportRecord) -> list[IncrementalExportRecord]:
    """
    Example: Mask PII attributes in items during import.

    Same logic as transform_full_record, adapted for incremental exports.

    All item data is already deserialized from DDB-JSON to native Python types.

    Behavior:
        - If new_image exists (PUT/update): mask PII attributes in new_image
        - If new_image is None (DELETE): return the record unchanged, i.e. respect the delete

    Args:
        record.keys: Deserialized key dict. Example: {"Id": 42, "sort_key": "profile"}
        record.new_image: Deserialized full item dict, or None for deletes.
            Example: {"Id": 42, "Name": "Alice Smith", "status": "active"}
        record.old_image: Deserialized full item dict, or None.
        record.table_key_schema: Key schema dict.
            Example (pk only): {"pk": {"name": "Id", "type": "N"}}
            Example (pk + sk):  {"pk": {"name": "Id", "type": "N"}, "sk": {"name": "sort_key", "type": "S"}}
        record.write_timestamp_micros: Integer timestamp of the write.

    Returns:
        list[IncrementalExportRecord]: Single-element list with the modified record.
    """
    if record.new_image:
        key_names = _get_key_attribute_names(record.table_key_schema)
        _mask_pii(record.new_image, key_names)
    return [record]
