from ..parsers.records import FullExportRecord, IncrementalExportRecord
import hashlib

"""
This example shows how to add a new attribute to an item based on values of other attributes. 
It stores a calculated md5 hash as a new attribute.
"""

def _get_pk_name(table_key_schema):
    """Extract the partition key attribute name from the table key schema.

    The key schema is a dict like {"pk": {"name": "Id", "type": "N"}}.
    This works for any table regardless of what the partition key is named.
    """
    return table_key_schema["pk"]["name"]


def transform_full_record(record: FullExportRecord) -> list[FullExportRecord]:
    """
    Example: Add an MD5 hash of the partition key as a new 'pk_md5' attribute.

    This dynamically discovers the partition key name from the table's key schema,
    reads its value from the item, computes an MD5 hash, and adds it as a new
    attribute. Works for any table regardless of key name or type.

    All item data is already deserialized from DDB-JSON to native Python types
    before reaching this function. You work with plain strings, numbers, lists,
    and dicts — not DDB type descriptors like {"S": "hello"}.

    Args:
        record.item: Deserialized Python dict.
            Example: {"Id": 42, "status": "active", "name": "Alice"}
            NOT: {"Id": {"N": "42"}, "status": {"S": "active"}}
        record.table_key_schema: Key schema dict.
            Example (pk only): {"pk": {"name": "Id", "type": "N"}}
            Example (pk + sk):  {"pk": {"name": "Id", "type": "N"}, "sk": {"name": "sort_key", "type": "S"}}

    Returns:
        list[FullExportRecord]: Single-element list with the modified record.

    Example result:
        Input:  {"Id": 42, "status": "active"}
        Output: {"Id": 42, "status": "active", "pk_md5": "a1d0c6e83f027327d8461063f4ac58a6"}
    """
    pk_name = _get_pk_name(record.table_key_schema)
    pk_value = str(record.item.get(pk_name, ""))
    record.item["pk_md5"] = hashlib.md5(pk_value.encode()).hexdigest()
    return [record]


def transform_incremental_record(record: IncrementalExportRecord) -> list[IncrementalExportRecord]:
    """
    Example: Add an MD5 hash of the partition key as a new 'pk_md5' attribute.

    Same logic as transform_full_record, adapted for incremental exports.

    All item data is already deserialized from DDB-JSON to native Python types.

    Behavior:
        - If new_image exists (PUT/update): add pk_md5 to the new_image
        - If new_image is None (DELETE): return the record unchanged, i.e. respect the delete

    Args:
        record.keys: Deserialized key dict. Example: {"Id": 42, "sort_key": "profile"}
        record.new_image: Deserialized full item dict, or None for deletes.
            Example: {"Id": 42, "status": "active", "name": "Alice"}
        record.old_image: Deserialized full item dict, or None.
        record.table_key_schema: Key schema dict.
            Example (pk only): {"pk": {"name": "Id", "type": "N"}}
            Example (pk + sk):  {"pk": {"name": "Id", "type": "N"}, "sk": {"name": "sort_key", "type": "S"}}
        record.write_timestamp_micros: Integer timestamp of the write.

    Returns:
        list[IncrementalExportRecord]: Single-element list with the modified record.
    """
    if record.new_image:
        pk_name = _get_pk_name(record.table_key_schema)
        pk_value = str(record.keys.get(pk_name, ""))
        record.new_image["pk_md5"] = hashlib.md5(pk_value.encode()).hexdigest()
    return [record]
