"""Unit tests for writer factory."""

import pytest
from unittest.mock import Mock

from ..writers.writer_factory import WriterFactory
from ..writers.batch_writer import BatchWriter
from ..writers.item_writer import ItemWriter
from ..utils.enums import ImportType

class TestWriterFactory:
    """Unit tests for WriterFactory class."""
    
    def test_returns_batch_writer_for_full_only_import(self):
        """Test that FULL import returns BatchWriter."""
        writer = WriterFactory.create_writer(ImportType.FULL)
        assert isinstance(writer, BatchWriter)
    
    def test_returns_item_writer_for_incremental_only_import(self):
        """Test that INCREMENTAL_ONLY import returns ItemWriter."""
        writer = WriterFactory.create_writer(ImportType.INCREMENTAL)
        assert isinstance(writer, ItemWriter)
    
    def test_factory_creates_different_writer_instances(self):
        """Test that factory creates new instances each time."""
        writer1 = WriterFactory.create_writer(ImportType.FULL)
        writer2 = WriterFactory.create_writer(ImportType.FULL)
        assert writer1 is not writer2


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
