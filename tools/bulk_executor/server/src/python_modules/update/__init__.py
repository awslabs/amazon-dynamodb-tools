import importlib
import json
import math
import sys

import boto3
import botocore
from awsglue.context import GlueContext
from awsglue.job import Job
from botocore.config import Config
from pyspark import AccumulatorParam
from pyspark.context import SparkContext

# Custom Library Imports
sys.path.append('/server/src')
from python_modules.shared.errors import *
from python_modules.shared.logger import log
from python_modules.shared.pricing import PricingUtility
from python_modules.shared.rate_limiter import (
    RateLimiterAggregator,
    RateLimiterSharedConfig,
    RateLimiterWorker
)
from python_modules.shared.table_info import (
    get_and_print_dynamodb_table_info, get_and_print_table_scan_cost,
    get_dynamodb_throughput_configs)

class ListAccumulator(AccumulatorParam):
    def zero(self, initialValue):
        return []

    def addInPlace(self, v1, v2):
        v1.extend(v2)
        return v1

DYNAMO_DB_THROTTLE_EXCEPTION = 'ProvisionedThroughputExceededException'
DYNAMO_DB_VALIDATION_EXCEPTION = 'ValidationException'
DYNAMO_DB_CONDITIONAL_CHECK_FAILED = 'ConditionalCheckFailedException'

def print_dynamodb_table_info(table_name):
    region_name = boto3.Session().region_name
    table_info = get_and_print_dynamodb_table_info(table_name)
    _ = get_and_print_table_scan_cost(table_info, region_name)
    print("Cost for writes depends on how many items will be updated!")

def run(job, spark_context, glue_context, parsed_args):
    table_name = parsed_args.get('table')

    generator_name = parsed_args.get('generator', 'default')
    generator_function_name = parsed_args.get('generatorfunctionname', 'generate') # Recommend _not_ passing unless there's a specific reason.

    # Rate limiter configuration
    bucket_name = parsed_args.get('s3-bucket-name')
    job_run_id = parsed_args.get("JOB_RUN_ID")

    module = importlib.import_module(f"python_modules.update.{generator_name}")
    generate = getattr(module, generator_function_name)

    print_dynamodb_table_info(table_name)

    rate_limiter_shared_config = RateLimiterSharedConfig(
        bucket=bucket_name,
        job_run_id=job_run_id
    )

    rate_limiter_aggregator = RateLimiterAggregator(shared_config=rate_limiter_shared_config)

    # Get monitor options for rate limiting
    monitor_options = get_dynamodb_throughput_configs(parsed_args, table_name, modes=["read", "write"], format="monitor")

    # Since each task might generate errors, let's accumulate them and report intelligently
    error_accumulator = spark_context.accumulator([], ListAccumulator())

    parallelize_count = 800

    # --- Phase 1: Scan all segments, collect update operations ---
    print(f"Phase 1: Scanning {parallelize_count} segments to identify items needing updates...")
    try:
        scan_rdd = spark_context.parallelize(range(parallelize_count), parallelize_count)
        pending_ops = scan_rdd.flatMap(
            lambda worker_id: _scan_segment(
                monitor_options, table_name, generate, worker_id, parallelize_count,
                error_accumulator, rate_limiter_shared_config
            )
        ).collect()
    except Exception as e:
        raise Exception(f"Error in scan phase: {get_error_message(e)}") from None
    finally:
        rate_limiter_aggregator.shutdown()

    if error_accumulator.value:
        first_error = error_accumulator.value[0]
        raise Exception(first_error) from None

    print(f"Phase 1 complete: {len(pending_ops):,} items identified for update")

    if not pending_ops:
        print("No items need updating. Done.")
        return

    # --- Phase 2: Execute updates spread evenly across workers ---
    print(f"Phase 2: Executing {len(pending_ops):,} updates spread across {parallelize_count} workers...")

    rate_limiter_aggregator_2 = RateLimiterAggregator(shared_config=rate_limiter_shared_config)

    updated_accumulator = spark_context.accumulator(0)
    failed_accumulator = spark_context.accumulator(0)
    error_accumulator_2 = spark_context.accumulator([], ListAccumulator())

    try:
        ops_rdd = spark_context.parallelize(pending_ops, parallelize_count)
        ops_rdd.foreachPartition(
            lambda partition: _execute_updates(
                partition, table_name, updated_accumulator, failed_accumulator,
                error_accumulator_2, rate_limiter_shared_config, monitor_options
            )
        )
    except Exception as e:
        raise Exception(f"Error in update phase: {get_error_message(e)}") from None
    finally:
        rate_limiter_aggregator_2.shutdown()

    if error_accumulator_2.value:
        first_error = error_accumulator_2.value[0]
        raise Exception(first_error) from None

    skipped_count = len(pending_ops) - updated_accumulator.value - failed_accumulator.value
    total = len(pending_ops)
    print(f"Phase 2 complete. Processed {total:,} operations: ({updated_accumulator.value:,} updates, {failed_accumulator.value:,} conditions failed)")


def _scan_segment(monitor_options, table_name, generate, segment, total_segments, error_accumulator, rate_limiter_shared_config):
    """Phase 1: Scan a single segment and return a list of update_kwargs for items needing updates."""
    rate_limiter_worker = RateLimiterWorker(
        shared_config=rate_limiter_shared_config,
        **monitor_options
    )

    session = rate_limiter_worker.get_session()
    dynamodb_resource = session.resource('dynamodb', config=Config(
        connect_timeout=4.0,
        read_timeout=4.0,
        retries={
            'mode': 'standard',
            'total_max_attempts': 50
        }
    ))

    table = dynamodb_resource.Table(table_name)

    pending_ops = []
    scanned_count = 0
    scan_kwargs = {
        "TableName": table_name,
        "Segment": segment,
        "TotalSegments": total_segments
    }

    try:
        while True:
            response = table.scan(**scan_kwargs)
            for item in response.get("Items", []):
                scanned_count += 1
                try:
                    update_kwargs = generate(item)
                    if update_kwargs:
                        pending_ops.append(update_kwargs)
                except botocore.exceptions.ClientError as e:
                    error_code = get_error_code(e)
                    if error_code == DYNAMO_DB_VALIDATION_EXCEPTION:
                        exit(f"Validation exception (usually caused by the generator producing items incompatible with the table schema): {get_error_message(e)}")
                    else:
                        raise
            if "LastEvaluatedKey" not in response:
                break
            scan_kwargs["ExclusiveStartKey"] = response["LastEvaluatedKey"]

    except Exception as e:
        error_accumulator.add([f"Error in scan worker {segment}: {get_error_message(e)}"])
    finally:
        rate_limiter_worker.shutdown()

    print(f"Scan worker {segment}/{total_segments}: scanned {scanned_count:,} items, {len(pending_ops):,} need updates")
    return pending_ops


def _execute_updates(partition, table_name, updated_accumulator, failed_accumulator, error_accumulator, rate_limiter_shared_config, monitor_options):
    """Phase 2: Execute a partition of update operations against DynamoDB."""
    rate_limiter_worker = RateLimiterWorker(
        shared_config=rate_limiter_shared_config,
        **monitor_options
    )

    session = rate_limiter_worker.get_session()
    dynamodb_resource = session.resource('dynamodb', config=Config(
        connect_timeout=4.0,
        read_timeout=4.0,
        retries={
            'mode': 'standard',
            'total_max_attempts': 50
        }
    ))

    table = dynamodb_resource.Table(table_name)

    updated_count = 0
    failed_count = 0

    try:
        for update_kwargs in partition:
            try:
                table.update_item(**update_kwargs)
                updated_count += 1
            except botocore.exceptions.ClientError as e:
                error_code = get_error_code(e)
                if error_code == DYNAMO_DB_THROTTLE_EXCEPTION:
                    exit("Throttling observed despite massive retries")
                elif error_code == DYNAMO_DB_VALIDATION_EXCEPTION:
                    exit(f"Validation exception: {get_error_message(e)}")
                elif error_code == DYNAMO_DB_CONDITIONAL_CHECK_FAILED:
                    print(f"UpdateItem condition expression failed, skipping... with kwargs: {update_kwargs}")
                    failed_count += 1
                else:
                    print('Unhandled ClientError thrown!', e, file=sys.stderr)
                    raise e
    except Exception as e:
        error_accumulator.add([f"Error in update worker: {get_error_message(e)}"])
    finally:
        rate_limiter_worker.shutdown()

    updated_accumulator.add(updated_count)
    failed_accumulator.add(failed_count)
    return 0
