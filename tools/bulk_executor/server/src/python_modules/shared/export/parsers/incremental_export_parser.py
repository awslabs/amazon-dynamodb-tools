"""DynamoDB incremental export parser."""

import json
from typing import Any, Dict
from .base_parser import BaseExportParser
from .records import IncrementalExportRecord
from ..utils.enums import Operation


class IncrementalExportParser(BaseExportParser):
    """Parser for DynamoDB incremental export format."""

    def __init__(self, table_key_schema):
        super().__init__()
        self.table_key_schema = table_key_schema

    def parse_to_record(self, line: str) -> IncrementalExportRecord:
        """Parse a JSON line, deserialize, and return an IncrementalExportRecord."""
        try:
            data = json.loads(line)
        except json.JSONDecodeError as e:
            raise ValueError(f"Malformed JSON: {e}")

        if not isinstance(data, dict):
            raise ValueError("Export line must be a JSON object")

        if "Keys" not in data:
            raise ValueError("Incremental export line missing 'Keys' field")

        if "Metadata" not in data:
            raise ValueError("Incremental export line missing 'Metadata' field")

        new_image = data.get("NewImage")
        old_image = data.get("OldImage")

        return IncrementalExportRecord(
            keys=self.deserialize_item(data["Keys"]),
            new_image=self.deserialize_item(new_image) if new_image else None,
            old_image=self.deserialize_item(old_image) if old_image else None,
            table_key_schema=self.table_key_schema,
            write_timestamp_micros=data["Metadata"].get("WriteTimestampMicros", {}).get("N")
        )

    def resolve(self, record: IncrementalExportRecord) -> Dict[str, Any]:
        """Resolve an IncrementalExportRecord into {"operation", "data"}."""
        if record.new_image:
            return {"operation": Operation.PUT, "data": record.new_image}
        else:
            return {"operation": Operation.DELETE, "data": record.keys}
