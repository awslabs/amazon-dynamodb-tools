"""Item writer implementation using individual DynamoDB operations with condition support."""

import boto3
import botocore.exceptions
from botocore.config import Config
from typing import Dict, Any, Iterator
from .base_writer import DynamoDBWriter
from ...shared.rate_limiter import RateLimiterWorker
from ...shared.logger import log
from ...shared.errors import get_error_code, get_error_message

# DynamoDB exception constants
DYNAMO_DB_THROTTLE_EXCEPTION = 'ProvisionedThroughputExceededException'
DYNAMO_DB_VALIDATION_EXCEPTION = 'ValidationException'
DYNAMO_DB_CONDITIONAL_CHECK_FAILED = 'ConditionalCheckFailedException'


class ItemWriter(DynamoDBWriter):
    """DynamoDB writer using individual operations with condition support."""
    
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
        """Write partition using individual operations with condition support."""
        local_count = 0
        condition_failed_count = 0
        rate_limiter_worker = None
        
        if debug_accumulator: debug_accumulator.add(["Item writer function started"])
        
        try:
            # Initialize rate limiter
            log.info("Rate limiter worker started...")
            if debug_accumulator: debug_accumulator.add(["Rate limiter worker started"])
            rate_limiter_worker = RateLimiterWorker(
                shared_config=rate_limiter_shared_config,
                debug_accumulator=debug_accumulator,
                **monitor_options,
                #worker_max_write_rate=3000  # TODO: Parameterize this
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
            
            # Process operations individually to support conditions
            for operation_data in partition_data:
                operation, data, condition = operation_data["operation"], operation_data["data"], operation_data["condition"]

                if debug_accumulator: debug_accumulator.add([f"Operation: {operation}, Data: {data}, Condition: {condition}"])
                
                try:
                    if operation == "PUT":
                        if condition:
                            table.put_item(Item=data, ConditionExpression=condition)
                        else:
                            table.put_item(Item=data)
                    elif operation == "DELETE":
                        if condition:
                            table.delete_item(Key=data, ConditionExpression=condition)
                        else:
                            table.delete_item(Key=data)
                    
                    local_count += 1

                    if local_count % 1000 == 0:
                        log.info(f"Item writer progress: {local_count:,} operations processed")
                        
                except botocore.exceptions.ClientError as e:
                    error_code = get_error_code(e)
                    
                    if error_code == DYNAMO_DB_CONDITIONAL_CHECK_FAILED:
                        condition_failed_count += 1
                        if debug_accumulator: debug_accumulator.add([f"Condition failed for operation: {operation}, continuing..."])
                    elif error_code == DYNAMO_DB_THROTTLE_EXCEPTION:
                        raise
                    elif error_code == DYNAMO_DB_VALIDATION_EXCEPTION:
                        error_accumulator.add([f"Schema validation error: Perhaps items don't match table schema?: {get_error_message(e)}"])
                        break
                    else:
                        error_accumulator.add([f"Error during operation: {get_error_message(e)}"])
                        break
            
            log.info(f"Item writer completed: {local_count:,} operations processed, {condition_failed_count:,} conditions failed on '{table_name}'")
            if debug_accumulator: debug_accumulator.add([f"Item writer completed: {local_count} operations processed, {condition_failed_count} conditions failed"])
            written_items_accumulator.add(local_count)
        
        except botocore.exceptions.ClientError as e:
            if get_error_code(e) == DYNAMO_DB_THROTTLE_EXCEPTION:
                log.info('Persistent throttling on individual operations, retries exhausted...')
            else:
                error_accumulator.add([f"Error during writing: {get_error_message(e)}"])
        except Exception as e:
            error_accumulator.add([f"Unexpected error during write: {str(e)}"])
        finally:
            if rate_limiter_worker:
                rate_limiter_worker.shutdown()


# Keep the original function for backward compatibility
def write_partition_to_dynamodb(*args, **kwargs):
    """Backward compatibility function."""
    writer = ItemWriter()
    return writer.write_partition_to_dynamodb(*args, **kwargs)
