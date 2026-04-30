"""Abstract base writer interface for DynamoDB operations."""

from abc import ABC, abstractmethod
from typing import Iterator, Dict, Any

class DynamoDBWriter(ABC):
    """Abstract base class for DynamoDB writers."""
    
    @abstractmethod
    def write_partition_to_dynamodb(
        self,
        partition_data: Iterator[Dict[str, Any]],
        table_name: str,
        rate_limiter_shared_config,
        monitor_options,
        error_accumulator,
        debug_accumulator,
        written_items_accumulator
    ) -> None:
        """
        Write a partition of operations to DynamoDB.
        
        Args:
            partition_data: Iterator of operation dictionaries
            table_name: Name of the target DynamoDB table
            rate_limiter_shared_config: Rate limiter configuration
            monitor_options: Monitoring options for rate limiting
            error_accumulator: Spark accumulator for collecting errors
            debug_accumulator: Spark accumulator for collecting debug info
            written_items_accumulator: Spark accumulator for collecting written items
        """
        pass
