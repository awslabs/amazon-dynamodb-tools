"""DynamoDB incremental export parser for extracting items from incremental export format."""

import json
from typing import Any, Dict, Optional, Tuple
from .base_parser import BaseExportParser

class IncrementalExportParser(BaseExportParser):
    """
    Parser for DynamoDB incremental export format.
    
    Handles both NEW_AND_OLD_IMAGES and NEW_IMAGE export view types.
    """

    NEW_AND_OLD_IMAGES = "NEW_AND_OLD_IMAGES"
    NEW_IMAGE = "NEW_IMAGE"

    def __init__(self, output_view: str = NEW_AND_OLD_IMAGES):
        """
        Initialize parser with the export view type.

        Args:
            output_view: Either NEW_AND_OLD_IMAGES (default) or NEW_IMAGE
        """
        super().__init__()
        if output_view not in (self.NEW_AND_OLD_IMAGES, self.NEW_IMAGE):
            raise ValueError(f"Unsupported output_view: {output_view}")
        self.output_view = output_view

    def parse_export_line(self, line: str) -> Optional[Tuple[str, Dict[str, Any], Optional[str]]]:
        """
        Parse a single line from a DynamoDB incremental export file.

        Args:
            line: JSON string from incremental export file

        Returns:
            Tuple of (operation, item_data, condition_expression) or None for unsupported ops
            - operation: "PUT" or "DELETE"
            - item_data: Item in DynamoDB JSON format (for PUT) or Keys (for DELETE)
            - condition_expression: Conditional expression, or None for unconditional

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
        key_names = list(keys.keys())
        expr_names = {f"#k{i}": name for i, name in enumerate(key_names)}
        placeholders = list(expr_names.keys())
        has_old_image = "OldImage" in data
        has_new_image = "NewImage" in data

        exists_cond = " AND ".join(f"attribute_exists({p})" for p in placeholders)
        not_exists_cond = " AND ".join(f"attribute_not_exists({p})" for p in placeholders)

        if self.output_view == self.NEW_IMAGE:
            if has_new_image:
                new_image = self.deserialize_item(data["NewImage"])
                return ("PUT", new_image, None, None)
            else:
                keys_deserialized = self.deserialize_item(keys)
                return ("DELETE", keys_deserialized, exists_cond, expr_names)
        else:
            # NEW_AND_OLD_IMAGES
            if has_new_image and not has_old_image:
                # INSERT
                new_image = self.deserialize_item(data["NewImage"])
                return ("PUT", new_image, not_exists_cond, expr_names)
            elif has_new_image and has_old_image:
                # MODIFY
                new_image = self.deserialize_item(data["NewImage"])
                return ("PUT", new_image, exists_cond, expr_names)
            elif has_old_image and not has_new_image:
                # REMOVE
                keys_deserialized = self.deserialize_item(keys)
                return ("DELETE", keys_deserialized, exists_cond, expr_names)
            else:
                raise ValueError("Invalid incremental export record: no OldImage or NewImage")
