"""Abstract base parser for DynamoDB export formats."""

import base64
from abc import ABC, abstractmethod
from typing import Any, Dict, Optional, Tuple
from boto3.dynamodb.types import TypeDeserializer

class BaseExportParser(ABC):
    """Abstract base class for DynamoDB export parsers."""
    
    def __init__(self):
        """Initialize the parser with deserializer."""
        self.deserializer = TypeDeserializer()
    
    def deserialize_item(self, ddb_item):
        """
        Deserialize a DynamoDB JSON item to plain Python format.
        
        Args:
            ddb_item: Item in DynamoDB JSON format with type descriptors
            
        Returns:
            Plain Python dictionary with proper types
        """
        if not ddb_item:
            return ddb_item
            
        # Handle binary data by decoding base64 before deserializing
        processed_item = {}
        for k, v in ddb_item.items():
            if isinstance(v, dict):
                if 'B' in v:
                    # Decode base64 binary data
                    processed_item[k] = {'B': base64.b64decode(v['B'])}
                elif 'BS' in v:
                    # Decode base64 binary set data
                    processed_item[k] = {'BS': [base64.b64decode(b) for b in v['BS']]}
                else:
                    processed_item[k] = v
            else:
                processed_item[k] = v
        
        # Deserialize each attribute
        python_item = {}
        for key, value in processed_item.items():
            python_item[key] = self.deserializer.deserialize(value)
        
        return python_item
    
    @abstractmethod
    def parse_export_line(self, line: str) -> Optional[Tuple[str, Dict[str, Any], Optional[str], Optional[Dict[str, str]]]]:
        """
        Parse a single line from a DynamoDB export file.
        
        Args:
            line: JSON string from export file
            
        Returns:
            Tuple of (operation, item_data, condition_expression, expression_attribute_names)
            - operation: "PUT" or "DELETE"
            - item_data: Deserialized item dict (for PUT) or key dict (for DELETE)
            - condition_expression: ConditionExpression string, or None
            - expression_attribute_names: ExpressionAttributeNames dict, or None
        """
        pass
