"""Unit tests for the abstract DynamoDBWriter base class.

Covers `python_modules/load_export/writers/base_writer.py`:
- DynamoDBWriter.__init__ via subclassing (cannot instantiate ABC directly)
- DynamoDBWriter.write_partition_to_dynamodb: the @abstractmethod has a `pass`
  body which is reached only when a subclass calls super().write_partition_to_dynamodb(...).
  This module subclasses DynamoDBWriter and exercises that path.
"""

import unittest
from unittest.mock import MagicMock

from python_modules.shared.export.writers.base_writer import DynamoDBWriter


class _SuperCallingWriter(DynamoDBWriter):
    """Concrete writer that delegates to super() before doing its own work.

    The standard pattern for covering an @abstractmethod `pass` body is to
    override in a subclass and call `super().the_method(...)` first. The super
    call runs the abstract body (which is just `pass`) and returns None.
    """

    def write_partition_to_dynamodb(
        self,
        partition_data,
        table_name,
        rate_limiter_shared_config,
        monitor_options,
        error_accumulator,
        debug_accumulator,
        written_items_accumulator,
    ):
        # Hit the abstract method body (line 32: `pass`).
        super_result = super().write_partition_to_dynamodb(
            partition_data,
            table_name,
            rate_limiter_shared_config,
            monitor_options,
            error_accumulator,
            debug_accumulator,
            written_items_accumulator,
        )
        return super_result  # None — caller asserts on it


class TestDynamoDBWriterAbstract(unittest.TestCase):
    """Verify the abstract base class refuses direct instantiation and that
    its abstract method body can be reached via a super() call from a
    concrete subclass."""

    def test_cannot_instantiate_abstract_class(self):
        """DynamoDBWriter requires write_partition_to_dynamodb to be overridden."""
        with self.assertRaises(TypeError):
            DynamoDBWriter()

    def test_super_write_partition_returns_none(self):
        """super().write_partition_to_dynamodb runs the abstract `pass` body."""
        writer = _SuperCallingWriter()
        result = writer.write_partition_to_dynamodb(
            partition_data=iter([]),
            table_name='my-table',
            rate_limiter_shared_config=MagicMock(),
            monitor_options={'aggregate_max_write_rate': 1000},
            error_accumulator=MagicMock(),
            debug_accumulator=MagicMock(),
            written_items_accumulator=MagicMock(),
        )
        self.assertIsNone(result)


if __name__ == '__main__':
    unittest.main()
