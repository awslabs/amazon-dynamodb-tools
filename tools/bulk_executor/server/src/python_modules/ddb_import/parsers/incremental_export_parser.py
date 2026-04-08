"""DynamoDB incremental export parser for extracting items from incremental export format."""

import json
from typing import Any, Dict, Tuple
from .base_parser import BaseExportParser

class IncrementalExportParser(BaseExportParser):
    """
    Parser for DynamoDB incremental export format.
    
    Handles both NEW_AND_OLD_IMAGES and NEW_IMAGE export view types.
    """

    def __init__(self):
        super().__init__()

    def parse_export_line(self, line: str) -> Tuple[str, Dict[str, Any]]:
        """
        Parse a single line from a DynamoDB incremental export file.

        Args:
            line: JSON string from incremental export file

        Returns:
            Tuple of (operation, item_data)
            - operation: "PUT" or "DELETE"
            - item_data: Item in plain Python format (for PUT) or Keys (for DELETE)

        Raises:
            ValueError: If JSON is malformed or doesn't contain expected fields
        """
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

        keys = data["Keys"]
        new_image = data.get("NewImage")

        if new_image:
            return ("PUT", self.deserialize_item(new_image))
        else:
            return ("DELETE", self.deserialize_item(keys))
