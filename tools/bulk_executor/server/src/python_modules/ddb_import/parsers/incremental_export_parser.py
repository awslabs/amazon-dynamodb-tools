"""DynamoDB incremental export parser for extracting items from incremental export format."""

import json
from typing import Any, Dict, Optional, Tuple
from .base_parser import BaseExportParser

class IncrementalExportParser(BaseExportParser):
    """
    Parser for DynamoDB incremental export format.
    
    Handles incremental export records that contain change events with
    OldImage, NewImage, Keys, and Metadata fields.
    """
    
    def parse_export_line(self, line: str) -> Optional[Tuple[str, Dict[str, Any], Optional[str]]]:
        """
        Parse a single line from a DynamoDB incremental export file.
        
        Returns the operation type, item data, and conditional expression needed
        for safe incremental imports.
        
        Args:
            line: JSON string from incremental export file
            
        Returns:
            Tuple of (operation, item_data, condition_expression) or None for unsupported ops
            - operation: "PUT" or "DELETE"
            - item_data: Item in DynamoDB JSON format (for PUT) or Keys (for DELETE)
            - condition_expression: Conditional expression for batch_writer
            
        Raises:
            ValueError: If JSON is malformed or doesn't contain expected fields
        """
        try:
            data = json.loads(line)
        except json.JSONDecodeError as e:
            raise ValueError(f"Malformed JSON: {e}")
        
        if not isinstance(data, dict):
            raise ValueError("Export line must be a JSON object")
        
        # Validate required fields
        if "Keys" not in data:
            raise ValueError("Incremental export line missing 'Keys' field")
        
        if "Metadata" not in data:
            raise ValueError("Incremental export line missing 'Metadata' field")
        
        has_old_image = "OldImage" in data
        has_new_image = "NewImage" in data
        
        # Get primary key for conditional expressions
        keys = data["Keys"]
        pk_name = list(keys.keys())[0]  # First key is partition key
        
        if has_new_image and not has_old_image:
            # INSERT: Only insert if key doesn't exist
            new_image = self.deserialize_item(data["NewImage"])
            return ("PUT", new_image, f"attribute_not_exists({pk_name})")
        elif has_new_image and has_old_image:
            # MODIFY: Only update if key exists
            new_image = self.deserialize_item(data["NewImage"])
            return ("PUT", new_image, f"attribute_exists({pk_name})")
        elif has_old_image and not has_new_image:
            # REMOVE: Only delete if key exists
            keys_deserialized = self.deserialize_item(keys)
            return ("DELETE", keys_deserialized, f"attribute_exists({pk_name})")
        else:
            raise ValueError("Invalid incremental export record: no OldImage or NewImage")
