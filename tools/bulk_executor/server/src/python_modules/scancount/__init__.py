import importlib
import json
import math
import sys
from decimal import Decimal

import boto3
import botocore
from awsglue.context import GlueContext
from awsglue.job import Job
from botocore.config import Config
from pyspark import AccumulatorParam
from pyspark.context import SparkContext


class DecimalEncoder(json.JSONDecoder):
    def decode(self, s):
        result = super().decode(s)
        return {k: Decimal(str(v)) if isinstance(v, float) else v
                for k, v in result.items()}

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

def print_dynamodb_table_info(table_name, index_name=None):
    region_name = boto3.Session().region_name
    table_info = get_and_print_dynamodb_table_info(table_name, index_name)
    _ = get_and_print_table_scan_cost(table_info, region_name)

def run(job, spark_context, glue_context, parsed_args):
    table_name = parsed_args.get('table')
    index_name = parsed_args.get('index')
    filter_expression = parsed_args.get('filter_expression')
    expression_values = parsed_args.get('expression_values')
    expression_names = parsed_args.get('expression_names')

    # Rate limiter configuration
    bucket_name = parsed_args.get('s3-bucket-name')
    job_run_id = parsed_args.get("JOB_RUN_ID")

    print_dynamodb_table_info(table_name, index_name)

    rate_limiter_shared_config = RateLimiterSharedConfig(
        bucket=bucket_name,
        job_run_id=job_run_id
    )

    rate_limiter_aggregator = RateLimiterAggregator(shared_config=rate_limiter_shared_config)

    # Get monitor options for rate limiting
    monitor_options = get_dynamodb_throughput_configs(parsed_args, table_name, modes=["read"], format="monitor")

    total_matched_accumulator = spark_context.accumulator(0)

    # Since each task might generate errors, let's accumulate them and report intelligently
    error_accumulator = spark_context.accumulator([], ListAccumulator())

    # Distribute work among partitions, each knowing what segment it's to handle
    try:
        parallelize_count = 200
        rdd = spark_context.parallelize(range(parallelize_count), parallelize_count)
        rdd.foreach(lambda worker_id: _count_data(monitor_options, table_name, index_name, filter_expression, expression_values, expression_names, worker_id, parallelize_count, total_matched_accumulator, error_accumulator, rate_limiter_shared_config))
        rdd.count()
    except Exception as e:
        raise Exception(f"Error in parallel execution: {get_error_message(e)}") from None
    finally:
        rate_limiter_aggregator.shutdown()
    if error_accumulator.value:
        first_error = error_accumulator.value[0]
        raise Exception(first_error) from None

    # Print the total records inserted using the accumulator after all tasks complete
    print(f"Total records counted: {total_matched_accumulator.value:,}")

def _count_data(monitor_options, table_name, index_name, filter_expression, expression_values, expression_names, segment, total_segments, total_matched_accumulator, error_accumulator, rate_limiter_shared_config):

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

    local_count = 0

    try:
        table = dynamodb_resource.Table(table_name)

        scan_kwargs = {
            "TableName": table_name,
            "Select": "COUNT",
            "Segment": segment,
            "TotalSegments": total_segments
        }
        if index_name:
            scan_kwargs["IndexName"] = index_name
        if filter_expression:
            scan_kwargs["FilterExpression"] = filter_expression
        if expression_names:
            scan_kwargs["ExpressionAttributeNames"] = json.loads(expression_names, cls=DecimalEncoder)
        if expression_values:
            scan_kwargs["ExpressionAttributeValues"] = json.loads(expression_values, cls=DecimalEncoder)

        while True:
            response = table.scan(**scan_kwargs) # We do 50 retries within the SDK so shouldn't see a throttle response
            local_count += response.get("Count", 0)
            if "LastEvaluatedKey" not in response:
                break
            scan_kwargs["ExclusiveStartKey"] = response["LastEvaluatedKey"]
    except Exception as e:
        error_accumulator.add([f"Error in worker {segment}: {get_error_message(e)}"])
        # Let control drop down to exit
    finally:
        rate_limiter_worker.shutdown()

    print(f"Worker {segment}/{total_segments} counted {local_count} records.")
    total_matched_accumulator.add(local_count)
    return local_count
