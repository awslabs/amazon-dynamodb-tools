"""Parser factory for selecting the appropriate DynamoDB export parser."""

from ..utils.enums import ExportLoadType
from .full_export_parser import FullExportParser
from .incremental_export_parser import IncrementalExportParser


class ParserFactory:
    """Factory for creating the appropriate parser based on export load type."""

    @staticmethod
    def get_parser(load_type: ExportLoadType, table_key_schema: dict):
        if load_type == ExportLoadType.FULL:
            return FullExportParser(table_key_schema)
        elif load_type == ExportLoadType.INCREMENTAL:
            return IncrementalExportParser(table_key_schema)
        else:
            raise ValueError(f"Unsupported load type: {load_type}")
