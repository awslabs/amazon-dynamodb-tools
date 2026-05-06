"""Abstract base parser for DynamoDB export formats."""

import base64
from abc import ABC, abstractmethod
from typing import Any, Dict
from boto3.dynamodb.types import TypeDeserializer


class BaseExportParser(ABC):
    """Abstract base class for DynamoDB export parsers."""

    def __init__(self):
        self.deserializer = TypeDeserializer()

    @abstractmethod
    def parse_to_record(self, line: str):
        """Parse a JSON line into a record object."""
        pass

    @abstractmethod
    def resolve(self, record) -> Dict[str, Any]:
        """Resolve a record into {"operation": ..., "data": ...}."""
        pass

    def deserialize_item(self, ddb_item):
        """Deserialize a DynamoDB JSON item to plain Python format."""
        if not ddb_item:
            return ddb_item

        processed_item = self._decode_binary_values(ddb_item)

        python_item = {}
        for key, value in processed_item.items():
            python_item[key] = self.deserializer.deserialize(value)

        return python_item

    def _decode_binary_values(self, node):
        """Recursively decode base64 binary values throughout a DynamoDB JSON structure."""
        if not isinstance(node, dict):
            return node

        result = {}
        for k, v in node.items():
            if k == 'B' and isinstance(v, str):
                result[k] = base64.b64decode(v)
            elif k == 'BS' and isinstance(v, list):
                result[k] = [base64.b64decode(b) if isinstance(b, str) else b for b in v]
            elif k == 'L' and isinstance(v, list):
                result[k] = [self._decode_binary_values(item) for item in v]
            elif k == 'M' and isinstance(v, dict):
                result[k] = {mk: self._decode_binary_values(mv) for mk, mv in v.items()}
            elif isinstance(v, dict):
                result[k] = self._decode_binary_values(v)
            else:
                result[k] = v

        return result
