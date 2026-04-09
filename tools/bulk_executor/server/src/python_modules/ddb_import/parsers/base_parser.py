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

        processed_item = {}
        for k, v in ddb_item.items():
            if isinstance(v, dict):
                if 'B' in v:
                    processed_item[k] = {'B': base64.b64decode(v['B'])}
                elif 'BS' in v:
                    processed_item[k] = {'BS': [base64.b64decode(b) for b in v['BS']]}
                else:
                    processed_item[k] = v
            else:
                processed_item[k] = v

        python_item = {}
        for key, value in processed_item.items():
            python_item[key] = self.deserializer.deserialize(value)

        return python_item
