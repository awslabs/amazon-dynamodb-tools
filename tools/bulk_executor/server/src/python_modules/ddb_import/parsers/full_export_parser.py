"""DynamoDB full export parser for extracting items from full export format."""

import json
from typing import Any, Dict, Optional, Tuple
from .base_parser import BaseExportParser

class FullExportParser(BaseExportParser):
    """
    Parser for DynamoDB full export format.
    
    Handles full export records that contain complete item snapshots
    in the format: {"Item": {...}}
    """
    
    def parse_export_line(self, line: str) -> Tuple[str, Dict[str, Any], Optional[str], None]:
        """
        Parse a single line from a DynamoDB full export file.
        
        Args:
            line: JSON string from full export file
            
        Returns:
            Tuple of (operation, item_data, condition_expression, expression_attribute_names)
            - operation: Always "PUT" for full exports
            - item_data: Item in plain Python format (converted from DDB-JSON)
            - condition_expression: None (no conditions needed for full imports)
            - expression_attribute_names: None (no expression names needed for full imports)
        """
        try:
            data = json.loads(line)
        except json.JSONDecodeError as e:
            raise ValueError(f"Malformed JSON: {e}")
        
        if not isinstance(data, dict):
            raise ValueError("Export line must be a JSON object")
        
        if "Item" not in data:
            raise ValueError("Export line missing 'Item' field")
        
        # Convert from DDB-JSON format to plain Python dict
        ddb_item = data["Item"]
        plain_item = self.deserialize_item(ddb_item)
        
        return ("PUT", plain_item, None, None)
