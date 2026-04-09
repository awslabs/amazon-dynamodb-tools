# Bulk Import Capability

This utility allows you to do an import of DynamoDB table exported to S3 by leveraging Glue. The code is modularized to enable unit testing of individual components. It also leverages the `RateLimiter` classes to ensure that any bulk action executed on a table only consumes the capacity configured.

## Execution
Refer to the top level [README](../../../../../README.md) file

## Transform

The import supports an optional `--transform` parameter that specifies a Python module containing transform functions. These functions receive deserialized export records and can filter, mutate, or fan-out items before they are written to DynamoDB.

The transform module must contain one or both of these functions depending on the export type being imported:

### `transform_full_record(record: FullExportRecord) -> list[FullExportRecord]`

Called for each record in a full export. The record has:
- `record.item` - the deserialized Item (plain Python dict)
- `record.table_key_schema` - key schema from the destination table

**Return behavior:**
- `return [record]` - import the item as-is (PUT)
- `return []` - skip the item entirely
- `return [record1, record2, ...]` - fan-out: import multiple items from one source record
- Set `record.item = None` then `return [record]` - **this will error**, full export records must have an item

**Example - mutate an attribute during import:**
```python
def transform_full_record(record: FullExportRecord) -> list[FullExportRecord]:
    # Rename 'old_status' to 'status' during import
    if "old_status" in record.item:
        record.item["status"] = record.item.pop("old_status")
    return [record]
```

### `transform_incremental_record(record: IncrementalExportRecord) -> list[IncrementalExportRecord]`

Called for each record in an incremental export. The record has:
- `record.keys` - the deserialized key attributes (plain Python dict)
- `record.new_image` - the deserialized new item, or `None` for deletes
- `record.old_image` - the deserialized old item, or `None`
- `record.table_key_schema` - key schema from the destination table
- `record.write_timestamp_micros` - WriteTimestampMicros from the export metadata

**Return behavior:**
- `return [record]` - import as-is. If `new_image` is present it becomes a PUT, if `new_image` is `None` it becomes a DELETE
- `return []` - skip the record entirely (both PUTs and DELETEs)
- Set `record.new_image = None` then `return [record]` - **converts a PUT into a DELETE** using `record.keys`
- `return [record1, record2, ...]` - fan-out: import multiple items from one source record

**Example - add an attribute during import:**
```python
def transform_incremental_record(record: IncrementalExportRecord) -> list[IncrementalExportRecord]:
    # Add a 'migrated' flag to all items being inserted or updated
    if record.new_image:
        record.new_image["migrated"] = True
    return [record]
```

## Role requirements
The bulk import reads data from S3 and writes to an existing DynamoDB table, therefore it needs the following permissions:
1. Access to the S3 bucket in which the source DynamoDB export lives
2. Write access to the DynamoDB table to which the export needs to be restored to
3. If the DynamoDB table uses KMS keys, ensure the role has relevant access

## Unit testing
Refer to [README](../../../../tests/README.md)