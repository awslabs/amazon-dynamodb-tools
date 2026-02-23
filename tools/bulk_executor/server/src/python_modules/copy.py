import sys

import boto3
from botocore.config import Config
from pyspark import AccumulatorParam


sys.path.append('/server/src')
from python_modules.shared.errors import get_error_message
from python_modules.shared.table_info import (
    get_and_print_dynamodb_table_info,
    get_and_print_table_scan_cost,
    get_and_print_table_copy_write_cost,
    get_dynamodb_throughput_configs,
    _region_from_table_ref
)

from python_modules.shared.rate_limiter import (
    RateLimiterAggregator,  
    RateLimiterSharedConfig,
    RateLimiterWorker
)

class ListAccumulator(AccumulatorParam):
    def zero(self, initialValue):
        return []
            
    def addInPlace(self, v1, v2):
        v1.extend(v2)
        return v1

def print_dynamodb_table_info(source_table, target_table):
    source_table_info = get_and_print_dynamodb_table_info(source_table)
    scan_cost = get_and_print_table_scan_cost(source_table_info)

    target_table_info = get_and_print_dynamodb_table_info(target_table)
    write_cost = get_and_print_table_copy_write_cost(source_table_info, target_table_info)

    total_cost = scan_cost + write_cost
    print(f"TOTAL DynamoDB cost for scanning '{source_table}' and writing to '{target_table}' (approx): ${total_cost:,.2f}")
    print()

def run(job, spark_context, glue_context, parsed_args):
    source_table = parsed_args.get('source')
    target_table = parsed_args.get('target')

    # Rate limiter configuration
    bucket_name = parsed_args.get('s3-bucket-name')
    job_run_id = parsed_args.get("JOB_RUN_ID")

    print_dynamodb_table_info(source_table, target_table)

    source_rate_limiter_shared_config = RateLimiterSharedConfig(
        bucket=bucket_name,
        job_run_id=f"{job_run_id}-source"
    )

    target_rate_limiter_shared_config = RateLimiterSharedConfig(
        bucket=bucket_name,
        job_run_id=f"{job_run_id}-target"
    )

    source_rate_limiter_aggregator = RateLimiterAggregator(shared_config=source_rate_limiter_shared_config)
    target_rate_limiter_aggregator = RateLimiterAggregator(shared_config=target_rate_limiter_shared_config)

    # Get monitor options for rate limiting
    source_monitor_options = get_dynamodb_throughput_configs(parsed_args, source_table, modes=["read"], format="monitor")
    target_monitor_options = get_dynamodb_throughput_configs(parsed_args, target_table, modes=["write"], format="monitor")

    total_matched_accumulator = spark_context.accumulator(0)

    # Since each task might generate errors, let's accumulate them and report intelligently
    error_accumulator = spark_context.accumulator([], ListAccumulator())

    # Distribute work among partitions, each knowing what segment it's to handle
    try:
        parallelize_count = 400
        rdd = spark_context.parallelize(range(parallelize_count), parallelize_count)
        rdd.foreach(lambda worker_id: _copy_data(source_table, target_table, source_monitor_options, target_monitor_options, worker_id, parallelize_count, total_matched_accumulator, error_accumulator, source_rate_limiter_shared_config, target_rate_limiter_shared_config))
        #rdd.count()
    except Exception as e:
        raise Exception(f"Error in parallel execution: {get_error_message(e)}") from None
    finally:
        source_rate_limiter_aggregator.shutdown()
        target_rate_limiter_aggregator.shutdown()
    if error_accumulator.value:
        first_error = error_accumulator.value[0]
        raise Exception(first_error) from None

    print(f"Total records copied: {total_matched_accumulator.value:,}")

def _copy_data(source_table, target_table, source_monitor_options, target_monitor_options, segment, total_segments, total_matched_accumulator, error_accumulator, source_rate_limiter_shared_config, target_rate_limiter_shared_config):

    # Let's hit the gas harder for this verb, at least for now XXX
    source_rl = RateLimiterWorker(
        shared_config=source_rate_limiter_shared_config,
        **source_monitor_options,
        worker_max_read_rate=2500, # up from 1,500 default
    )
    target_rl = RateLimiterWorker(
        shared_config=target_rate_limiter_shared_config,
        **target_monitor_options,
        worker_max_write_rate=800, # up from 500 default
    )

    source_session = source_rl.get_session()
    target_session = target_rl.get_session()

    cfg = Config(
        connect_timeout=4.0,
        read_timeout=4.0,
        retries={"mode": "standard", "total_max_attempts": 50},
    )

    # Talk to the right region if the table name is an ARN to a diff region
    source_region = _region_from_table_ref(source_table) or source_session.region_name
    target_region = _region_from_table_ref(target_table) or target_session.region_name

    source_ddb = source_session.resource("dynamodb", config=cfg, region_name=source_region)
    target_ddb   = target_session.resource("dynamodb", config=cfg, region_name=target_region)

    src = source_ddb.Table(source_table)
    dst = target_ddb.Table(target_table)

    local_count = 0
    scan_kwargs = {"Segment": segment, "TotalSegments": total_segments}

    try:
        with dst.batch_writer() as batch:
            while True:
                resp = src.scan(**scan_kwargs)

                items = resp.get("Items", [])
                for item in items:
                    # optionally transform item here
                    batch.put_item(Item=item)
                local_count += len(items)

                lek = resp.get("LastEvaluatedKey")
                if not lek:
                    break
                scan_kwargs["ExclusiveStartKey"] = lek
    except Exception as e:
        error_accumulator.add([f"Error in worker {segment}: {get_error_message(e)}"])
        # Let control drop down to exit
    finally:
        source_rl.shutdown()
        target_rl.shutdown()

    total_matched_accumulator.add(local_count)
    print(f"Worker {segment}/{total_segments} copied {local_count} records.")
    return local_count
