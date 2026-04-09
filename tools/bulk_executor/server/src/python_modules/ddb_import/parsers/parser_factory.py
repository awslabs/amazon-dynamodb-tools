"""Parser factory for selecting the appropriate DynamoDB export parser."""

from ..utils.enums import ImportType
from .full_export_parser import FullExportParser
from .incremental_export_parser import IncrementalExportParser


class ParserFactory:
    """Factory for creating the appropriate parser based on import type."""

    @staticmethod
    def get_parser(import_type: ImportType, table_key_schema: dict):
        if import_type == ImportType.FULL:
            return FullExportParser(table_key_schema)
        elif import_type == ImportType.INCREMENTAL:
            return IncrementalExportParser(table_key_schema)
        else:
            raise ValueError(f"Unsupported import type: {import_type}")
