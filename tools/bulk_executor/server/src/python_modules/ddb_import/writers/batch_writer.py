"""Batch writer implementation using DynamoDB batch_writer."""

import boto3
import botocore.exceptions
from botocore.config import Config
from typing import Dict, Any, Iterator
from .base_writer import DynamoDBWriter
from ...shared.rate_limiter import RateLimiterWorker
from ...shared.logger import log
from ...shared.errors import get_error_code, get_error_message

from .constants import DYNAMO_DB_THROTTLE_EXCEPTION, DYNAMO_DB_VALIDATION_EXCEPTION


class BatchWriter(DynamoDBWriter):
    """DynamoDB writer using batch operations (no condition support)."""
    
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
        """Write partition using batch_writer for high performance."""
        local_count = 0
        rate_limiter_worker = None
        
        if debug_accumulator: debug_accumulator.add(["Batch writer function started"])
        
        try:
            # Initialize rate limiter
            log.info("Rate limiter worker started...")
            if debug_accumulator: debug_accumulator.add(["Rate limiter worker started"])
            rate_limiter_worker = RateLimiterWorker(
                shared_config=rate_limiter_shared_config,
                debug_accumulator=debug_accumulator,
                **monitor_options,
            )
            session = rate_limiter_worker.get_session()
            dynamodb = session.resource('dynamodb', config=Config(
                connect_timeout=4.0,
                read_timeout=4.0,
                retries={
                    'mode': 'standard',
                    'total_max_attempts': 50
                }
            ))
            
            table = dynamodb.Table(table_name)
            
            # Use batch_writer (no condition support)
            with table.batch_writer() as batch:
                for operation_data in partition_data:
                    operation, data = operation_data["operation"], operation_data["data"]

                    if debug_accumulator: debug_accumulator.add([f"Operation: {operation}, Data: {data}"])
                    
                    if operation == "PUT":
                        batch.put_item(Item=data)
                    elif operation == "DELETE":
                        batch.delete_item(Key=data)
                    
                    local_count += 1

                    if local_count % 1000 == 0:
                        log.info(f"Batch writer progress: {local_count:,} operations processed")
            
            log.info(f"Batch writer completed: {local_count:,} operations processed on '{table_name}'")
            if debug_accumulator: debug_accumulator.add([f"Batch writer completed: {local_count} operations processed"])
            written_items_accumulator.add(local_count)
        
        except botocore.exceptions.ClientError as e:
            if get_error_code(e) == DYNAMO_DB_THROTTLE_EXCEPTION:
                log.info('Persistent throttling on batch_writer exit, give up on last few operations...')
                error_accumulator.add([f"Persistent throttling, retries exhausted. {local_count} items written before failure."])
            elif get_error_code(e) == DYNAMO_DB_VALIDATION_EXCEPTION:
                error_accumulator.add([f"Schema validation error: Perhaps items don't match table schema?: {get_error_message(e)}"])
            else:
                error_accumulator.add([f"Error during writing: {get_error_message(e)}"])
        except Exception as e:
            error_accumulator.add([f"Unexpected error during write: {str(e)}"])
        finally:
            if rate_limiter_worker:
                rate_limiter_worker.shutdown()

