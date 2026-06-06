"""Factory for selecting the appropriate DynamoDB writer."""

from .batch_writer import BatchWriter

class WriterFactory:
    """Factory for creating DynamoDB writers."""
    
    @staticmethod
    def create_writer():
        return BatchWriter()
