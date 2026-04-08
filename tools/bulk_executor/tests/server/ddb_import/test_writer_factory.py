"""Unit tests for writer factory."""

import pytest

from python_modules.ddb_import.writers.writer_factory import WriterFactory
from python_modules.ddb_import.writers.batch_writer import BatchWriter


class TestWriterFactory:
    """Unit tests for WriterFactory class."""

    def test_returns_batch_writer(self):
        """Test that factory returns BatchWriter."""
        writer = WriterFactory.create_writer()
        assert isinstance(writer, BatchWriter)

    def test_factory_creates_different_writer_instances(self):
        """Test that factory creates new instances each time."""
        writer1 = WriterFactory.create_writer()
        writer2 = WriterFactory.create_writer()
        assert writer1 is not writer2


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
