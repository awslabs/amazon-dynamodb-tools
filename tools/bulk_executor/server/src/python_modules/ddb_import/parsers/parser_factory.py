"""Parser factory for selecting the appropriate DynamoDB export parser."""

from typing import Any, Dict, Optional
from ..utils.enums import ImportType
from .full_export_parser import FullExportParser
from .incremental_export_parser import IncrementalExportParser

class ParserFactory:
    """Factory for creating the appropriate parser based on import type."""
    
    @staticmethod
    def get_parser(import_type: ImportType):
        """
        Get the appropriate parser for the given import type.
        
        Args:
            import_type: The type of import (FULL, INCREMENTAL, or FULL_INCREMENTAL)
            
        Returns:
            Parser instance with parse_export_line method
        """
        if import_type == ImportType.FULL_ONLY:
            return FullExportParser()
        elif import_type == ImportType.INCREMENTAL_ONLY:
            return IncrementalExportParser()
        elif import_type == ImportType.FULL_INCREMENTAL:
            # For full-incremental, we need to handle both formats
            # This would require detecting the format per line
            return MixedFormatParser()
        else:
            raise ValueError(f"Unsupported import type: {import_type}")


class MixedFormatParser:
    """
    Parser that can handle both full and incremental export formats.
    
    Used for FULL_INCREMENTAL imports where the data might contain
    both full export records and incremental export records.
    """
    
    def __init__(self):
        self.full_parser = FullExportParser()
        self.incremental_parser = IncrementalExportParser()
    
    def parse_export_line(self, line: str) -> Optional[Dict[str, Any]]:
        """
        Parse a line that could be either full or incremental format.
        
        Detects the format and delegates to the appropriate parser.
        
        Args:
            line: JSON string from export file
            
        Returns:
            Item in DynamoDB JSON format, or None for delete operations
        """
        import json
        
        try:
            data = json.loads(line)
        except json.JSONDecodeError as e:
            raise ValueError(f"Malformed JSON: {e}")
        
        if not isinstance(data, dict):
            raise ValueError("Export line must be a JSON object")
        
        # Detect format based on structure
        if "Item" in data:
            # Full export format
            return self.full_parser.parse_export_line(line)
        elif "Keys" in data and "Metadata" in data:
            # Incremental export format
            return self.incremental_parser.parse_export_line(line)
        else:
            raise ValueError("Unknown export format: line contains neither 'Item' nor 'Keys'/'Metadata'")
