"""Factory for selecting the appropriate DynamoDB writer based on import type."""

from ..utils.enums import ImportType
from .batch_writer import BatchWriter
from .item_writer import ItemWriter

class WriterFactory:
    """Factory for creating appropriate DynamoDB writers."""
    
    @staticmethod
    def create_writer(import_type: ImportType):
        """Create the appropriate writer instance based on import type."""
        if import_type == ImportType.FULL:
            return BatchWriter()
        else:
            return ItemWriter()
