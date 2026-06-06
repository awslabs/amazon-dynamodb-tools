from python_modules.shared.export.parsers.records import FullExportRecord, IncrementalExportRecord
import hashlib

def _extract_pk_str(pk_value):
    if isinstance(pk_value, dict):
        # Raw DynamoDB JSON: {"N": "123"} or {"S": "abc"}
        return next(iter(pk_value.values()))
    return str(pk_value) if pk_value is not None else ""

def _get_pk_name(table_key_schema):
    return next(k["AttributeName"] for k in table_key_schema if k["KeyType"] == "HASH")

def transform_full_record(record) -> list:
    try:
        pk_name = _get_pk_name(record.table_key_schema)
        pk_value = record.item.get(pk_name)
        record.item["pk_md5"] = hashlib.md5(_extract_pk_str(pk_value).encode()).hexdigest()
        return [record]
    except Exception as e:
        raise RuntimeError(
            f"transform_full_record failed: {e}\n"
            f"  type(record.item)={type(record.item)}\n"
            f"  table_key_schema={record.table_key_schema}\n"
            f"  item_sample={repr(record.item)[:500]}"
        ) from e

def transform_incremental_record(record) -> list:
    try:
        if record.new_image:
            pk_name = _get_pk_name(record.table_key_schema)
            pk_value = record.keys.get(pk_name)
            record.new_image["pk_md5"] = hashlib.md5(_extract_pk_str(pk_value).encode()).hexdigest()
        return [record]
    except Exception as e:
        raise RuntimeError(
            f"transform_incremental_record failed: {e}\n"
            f"  type(record.keys)={type(record.keys)}\n"
            f"  type(record.new_image)={type(record.new_image)}\n"
            f"  table_key_schema={record.table_key_schema}\n"
            f"  keys_sample={repr(record.keys)[:500]}\n"
            f"  new_image_sample={repr(record.new_image)[:500]}"
        ) from e