import importlib
import json
import math
import sys

import boto3
import botocore
from awsglue.context import GlueContext
from awsglue.job import Job
from botocore.config import Config
from pyspark.context import SparkContext
from python_modules.shared.bulk_executor_error import BulkExecutorError
from python_modules.shared.errors import *
from python_modules.shared.pricing import PricingUtility
from python_modules.shared.table_info import get_and_print_dynamodb_table_info

# Custom Library Imports
sys.path.append('/server/src')
from python_modules.shared.logger import log
from python_modules.shared.rate_limiter import (
    RateLimiterAggregator,
    RateLimiterSharedConfig,
    RateLimiterWorker
)
from python_modules.shared.table_info import get_dynamodb_throughput_configs
from python_modules.shared.worker_errors import (
    raise_first_worker_error,
    record_understood_failure,
    record_worker_failure
)


DYNAMO_DB_THROTTLE_EXCEPTION = 'ProvisionedThroughputExceededException'
DYNAMO_DB_VALIDATION_EXCEPTION = 'ValidationException'

def print_dynamodb_table_info(session, table_name, numitems, avg_size):
    region_name = session.region_name
    table_info = get_and_print_dynamodb_table_info(table_name)

    #avg_size = max(math.ceil(table_size_bytes / (item_count+1)), 1000) # avoid any division by 0
    avg_write_units_per_item = math.ceil(avg_size / 1024)
    write_units = numitems * avg_write_units_per_item

    pricing_utility = PricingUtility()
    ondemand_pricing = pricing_utility.get_on_demand_capacity_pricing(region_name)
    wru_cost = float(ondemand_pricing.get(table_info['write_pricing_category']))
    od_cost = write_units * wru_cost
    prov_cost = od_cost / 1.5 # very rough, look into updating this
    log.info("DynamoDB fill costs depend on how many items are being written and the size of the items.")
    log.info(f"Here we assume the command will insert {numitems:,} items")
    log.info(f" with average size {int(avg_size):,} bytes (based on peeking at generator output);")
    log.info(f" each write incurs an average of {avg_write_units_per_item} write units")
    log.info(f"Write units required to do such a fill (approx): {write_units:,}")
    log.info("This does not include costs for secondary indexes!")
    if table_info['billing_mode'] == "PROVISIONED":
        log.info(f"Approx DynamoDB cost for provisioned writes consuming {write_units:,} WCUs (using {region_name} prices): ${prov_cost:,.2f}")
    elif table_info['billing_mode'] == "PAY_PER_REQUEST":
        log.info(f"Approx DynamoDB cost for On-demand writes consuming {write_units:,} WRUs (using {region_name} prices): ${od_cost:,.2f}")
    print() # empty print intentional
    return table_info

def _as_item_collection(item_collection):
    # A generator may return one item as a dict, or many items as a list. Iterating
    # a dict yields its keys, not the item, so wrap the single-item case. Both the
    # cost estimate and the fill itself have to agree on this, or the estimate
    # measures attribute names instead of items.
    if isinstance(item_collection, dict):
        item_collection = [item_collection] # in case the user returns one item
    return item_collection


def check_generator_output_avg_size(generate):
    # Generate returns a set of items, call it 10 times and sum things up
    total_count = 0
    total_size = 0

    from boto3.dynamodb.types import TypeSerializer
    serializer = TypeSerializer()

    for i in range(10):
        for item in _as_item_collection(generate()):
            total_count += 1
            total_size += len(json.dumps(serializer.serialize(item), default=str).encode('UTF-8'))

    return total_size / total_count


def run(job, spark_context, glue_context, parsed_args):
    table_name = parsed_args.get('table')
    num_items = int(parsed_args.get('numitems', 1000))

    generator_name = parsed_args.get('generator', 'default')
    generator_function_name = parsed_args.get('generatorfunctionname', 'generate') # Recommend _not_ passing unless there's a specific reason.

    # Rate limiter configuration
    bucket_name = parsed_args.get('s3-bucket-name')
    job_run_id = parsed_args.get("JOB_RUN_ID")

    module = importlib.import_module(f"python_modules.fill.{generator_name}")
    generate = getattr(module, generator_function_name)
    record_count = num_items

    avg_size = check_generator_output_avg_size(generate)

    session = boto3.Session()
    table_info = print_dynamodb_table_info(session, table_name, num_items, avg_size)
    key_names = [v['name'] for v in table_info['key_schema'].values()]

    # Divide the work into groups of ~10,000 item inserts
    parallelize_count = math.ceil(record_count / 10000)  # Assuming each worker will handle around 10,000 items

    # Calculate the exact number of items each worker task should load
    items_per_worker = [record_count // parallelize_count] * parallelize_count
    for i in range(record_count % parallelize_count):
        items_per_worker[i] += 1  # Distribute any remainder

    total_inserted_accumulator = spark_context.accumulator(0)

    # Since each task might generate errors, let's accumulate them and report intelligently
    error_accumulator = spark_context.accumulator([], ListAccumulator())

    rate_limiter_shared_config = RateLimiterSharedConfig(
        bucket=bucket_name,
        job_run_id=job_run_id
    )

    rate_limiter_aggregator = RateLimiterAggregator(shared_config=rate_limiter_shared_config)

    monitor_options = get_dynamodb_throughput_configs(parsed_args, table_name, modes=["write"], format="monitor")

    # Distribute work among partitions, each told how many items to load
    # Handle exceptions (raised or in accumulator) here so we process once instead of once per worker
    try:
        rdd = spark_context.parallelize(list(enumerate(items_per_worker)), parallelize_count).map(
                lambda x: _fill_data(monitor_options, table_name, x[1], generate, total_inserted_accumulator, error_accumulator, rate_limiter_shared_config, key_names)).collect()
    except Exception as e:
        raise Exception(f"Error in parallel execution: {get_error_message(e)}") from None
    finally:
        rate_limiter_aggregator.shutdown()
    raise_first_worker_error(error_accumulator)

    # Print the total records inserted using the accumulator after all tasks complete
    log.info(f"Total records filled: {total_inserted_accumulator.value:,}")

def _fill_data(monitor_options, table_name, num_items, generate, total_inserted_accumulator, error_accumulator, rate_limiter_shared_config, key_names):
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

    local_count = 0

    try:
        with table.batch_writer(overwrite_by_pkeys=key_names) as batch:
            while local_count < num_items:
                try:
                    item_collection = _as_item_collection(generate())
                    for item in item_collection:
                        if local_count >= num_items:
                            break
                        missing = [name for name in key_names if name not in item]
                        if missing:
                            # A common mistake, and worth naming precisely: boto3
                            # would raise a bare KeyError from batch_writer's
                            # de-duplication, and DynamoDB's own rejection arrives as
                            # "The provided key element does not match the schema".
                            raise BulkExecutorError(
                                f"Generated item is missing the table's key attribute(s) "
                                f"{missing}; the item has {sorted(map(str, item))}. The generator "
                                f"has to produce the key schema of '{table_name}'.")
                        batch.put_item(Item=item)
                        local_count += 1

                except botocore.exceptions.ClientError as e:
                    if get_error_code(e) == DYNAMO_DB_THROTTLE_EXCEPTION:
                        log.info('Persistent throttling, loop again with same batch_writer...')
                    raise # Others can get handled below

    except botocore.exceptions.ClientError as e:
        if get_error_code(e) == DYNAMO_DB_THROTTLE_EXCEPTION:
            log.info('Persistent throttling on batch_writer exit, give up on last few item inserts...')
        elif get_error_code(e) == DYNAMO_DB_VALIDATION_EXCEPTION:
            msg = get_error_message(e)
            # Offer a little help for "The provided key element does not match" which can be confusing
            if "The provided key element does not match" in msg:
                msg = f"Perhaps generated items don't match table schema? {msg}"
            record_understood_failure(error_accumulator, f"Schema validation error: {msg}")
        else:
            record_worker_failure(error_accumulator, e, "Error during writing")
    except Exception as e:
        # Anything the ClientError handler above doesn't cover -- including whatever a
        # user's generator raises, which is reported with its traceback.
        record_worker_failure(error_accumulator, e, "Error in worker")
    finally:
        rate_limiter_worker.shutdown()

    total_inserted_accumulator.add(local_count)
    return local_count
