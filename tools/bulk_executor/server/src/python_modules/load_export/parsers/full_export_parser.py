"""DynamoDB full export parser."""

import json
from typing import Any, Dict
from .base_parser import BaseExportParser
from .records import FullExportRecord
from ..utils.enums import Operation


class FullExportParser(BaseExportParser):
    """Parser for DynamoDB full export format."""

    def __init__(self, table_key_schema):
        super().__init__()
        self.table_key_schema = table_key_schema

    def parse_to_record(self, line: str) -> FullExportRecord:
        """Parse a JSON line, deserialize, and return a FullExportRecord."""
        try:
            data = json.loads(line)
        except json.JSONDecodeError as e:
            raise ValueError(f"Malformed JSON: {e}")

        if not isinstance(data, dict):
            raise ValueError("Export line must be a JSON object")

        if "Item" not in data:
            raise ValueError("Export line missing 'Item' field")

        return FullExportRecord(
            item=self.deserialize_item(data["Item"]),
            table_key_schema=self.table_key_schema
        )

    def resolve(self, record: FullExportRecord) -> Dict[str, Any]:
        """Resolve a FullExportRecord into {"operation", "data"}."""
        return {"operation": Operation.PUT, "data": record.item}
