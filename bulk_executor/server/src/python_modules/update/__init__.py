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

    updated_accumulator = spark_context.accumulator(0)
    skipped_accumulator = spark_context.accumulator(0)
    failed_accumulator = spark_context.accumulator(0)

    # Since each task might generate errors, let's accumulate them and report intelligently
    error_accumulator = spark_context.accumulator([], ListAccumulator())

    # Distribute work among partitions, each knowing what segment it's to handle
    try:
        parallelize_count = 800
        rdd = spark_context.parallelize(range(parallelize_count), parallelize_count)
        rdd.map(lambda worker_id: _update_data(monitor_options, table_name, generate, worker_id, parallelize_count, updated_accumulator, skipped_accumulator, failed_accumulator, error_accumulator, rate_limiter_shared_config)).collect()
    except Exception as e:
        raise Exception(f"Error in parallel execution: {get_error_message(e)}") from None
    finally:
        rate_limiter_aggregator.shutdown()
    if error_accumulator.value:
        first_error = error_accumulator.value[0]
        raise Exception(first_error) from None

    # Print the total records inserted using the accumulator after all tasks complete
    #print(f"Total records scanned and possibly updated: {updated_accumulator.value:,}")
    total = updated_accumulator.value + skipped_accumulator.value + failed_accumulator.value
    print(f"Processed {total:,} records: ({updated_accumulator.value:,} updates, {skipped_accumulator.value:,} non-updates, {failed_accumulator.value:,} conditions failed)")

def _update_data(monitor_options, table_name, generate, segment, total_segments, updated_accumulator, skipped_accumulator, failed_accumulator, error_accumulator, rate_limiter_shared_config):
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
    skipped_count = 0
    failed_count = 0
    scan_kwargs = {
        "TableName": table_name,
        "Segment": segment,
        "TotalSegments": total_segments
    }

    try:
        while True:
            response = table.scan(**scan_kwargs)
            for item in response.get("Items", []):
                try:
                    update_kwargs = generate(item)          # Generate update parameters
                    if update_kwargs:                       # Empty return allowed if no update needed
                        table.update_item(**update_kwargs)  # Perform update, no API for batch update
                        updated_count += 1
                    else:
                        skipped_count += 1

                except botocore.exceptions.ClientError as e:
                    error_code = get_error_code(e)
                    if error_code == DYNAMO_DB_THROTTLE_EXCEPTION:
                        exit("Throttling observed despite massive retries")
                    elif error_code == DYNAMO_DB_VALIDATION_EXCEPTION:
                        exit(f"Validation exception (usually caused by the generator producing items incompatible with the table schema): {get_error_message(e)}")
                    elif error_code == DYNAMO_DB_CONDITIONAL_CHECK_FAILED:
                        print(f"UpdateItem condition expression failed, skipping... with kwargs: {update_kwargs}")
                        failed_count += 1
                    else:
                        print('Unhandled ClientError thrown!', e, file=sys.stderr)
                        raise e
            # If there are more items, continue scanning
            if "LastEvaluatedKey" not in response:
                break
            scan_kwargs["ExclusiveStartKey"] = response["LastEvaluatedKey"]

    except Exception as e:
        error_accumulator.add([f"Error in worker {segment}: {get_error_message(e)}"])
        # Let control drop down to exit
    finally:
        rate_limiter_worker.shutdown()

    total_count = updated_count + skipped_count + failed_count
    print(f"Worker {segment}/{total_segments} processed {total_count:,} records: ({updated_count:,} updates, {skipped_count:,} non-updates, {failed_count:,} conditions failed)")
    updated_accumulator.add(updated_count)
    skipped_accumulator.add(skipped_count)
    failed_accumulator.add(failed_count)
    return 0
