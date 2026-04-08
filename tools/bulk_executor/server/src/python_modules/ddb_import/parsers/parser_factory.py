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
            import_type: The type of import (FULL or INCREMENTAL)

        Returns:
            Parser instance with parse_export_line method
        """
        if import_type == ImportType.FULL:
            return FullExportParser()
        elif import_type == ImportType.INCREMENTAL:
            return IncrementalExportParser()
        else:
            raise ValueError(f"Unsupported import type: {import_type}")
