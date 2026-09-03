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
from python_modules.shared.worker_errors import (
    raise_first_worker_error,
    record_understood_failure,
    record_worker_failure
)

from python_modules.shared.transform_loader import load_transform_module

TRANSFORM_PACKAGE = 'python_modules.copy.transform'

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

    return source_table_info, target_table_info

def run(job, spark_context, glue_context, parsed_args):
    source_table = parsed_args.get('source')
    target_table = parsed_args.get('target')
    transform_name = parsed_args.get('transform')

    # Rate limiter configuration
    bucket_name = parsed_args.get('s3-bucket-name')
    job_run_id = parsed_args.get("JOB_RUN_ID")

    _source_table_info, target_table_info = print_dynamodb_table_info(source_table, target_table)

    target_key_names = None
    if transform_name:
        # Fail fast on the driver. A typo in --transform should cost one
        # sentence, not a Glue job that dies 400 Spark tasks deep with the
        # cause buried in a Py4J wrapper. load_transform_module raises
        # BulkExecutorError for a bad name; the per-worker load stays too.
        load_transform_module(transform_name, TRANSFORM_PACKAGE)
        target_key_names = frozenset(
            k['name'] for k in target_table_info['key_schema'].values()
        )
        print(
            f"Note: --transform '{transform_name}' runs per item, so it can drop, "
            f"reshape or fan out rows. The cost estimate above assumes a 1:1 "
            f"source-to-target copy and no longer holds."
        )
        print()

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
    transform_excluded_accumulator = spark_context.accumulator(0) if transform_name else None

    # Since each task might generate errors, let's accumulate them and report intelligently
    error_accumulator = spark_context.accumulator([], ListAccumulator())

    # Distribute work among partitions, each knowing what segment it's to handle
    try:
        parallelize_count = 400
        rdd = spark_context.parallelize(range(parallelize_count), parallelize_count)
        rdd.foreach(lambda worker_id: _copy_data(source_table, target_table, source_monitor_options, target_monitor_options, worker_id, parallelize_count, total_matched_accumulator, error_accumulator, source_rate_limiter_shared_config, target_rate_limiter_shared_config, transform_name=transform_name, target_key_names=target_key_names, transform_excluded_accumulator=transform_excluded_accumulator))
        #rdd.count()
    except Exception as e:
        raise Exception(f"Error in parallel execution: {get_error_message(e)}") from None
    finally:
        source_rate_limiter_aggregator.shutdown()
        target_rate_limiter_aggregator.shutdown()
    raise_first_worker_error(error_accumulator)

    print(f"Total records copied: {total_matched_accumulator.value:,}")
    if transform_name:
        print(f"Items excluded by transform: {transform_excluded_accumulator.value:,}")

def _copy_data(source_table, target_table, source_monitor_options, target_monitor_options, segment, total_segments, total_matched_accumulator, error_accumulator, source_rate_limiter_shared_config, target_rate_limiter_shared_config, transform_name=None, target_key_names=None, transform_excluded_accumulator=None):

    transform_fn = None
    if transform_name:
        transform_module = load_transform_module(transform_name, TRANSFORM_PACKAGE)
        transform_fn = transform_module.transform_item

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
    excluded_count = 0
    # A broken transform raises on every item; record the first failure of each
    # kind and stay quiet after that, so a 10M-row table does not ship 10M
    # strings to the driver (which only ever reads the first).
    transform_error_recorded = False
    key_error_recorded = False
    scan_kwargs = {"Segment": segment, "TotalSegments": total_segments}

    try:
        with dst.batch_writer() as batch:
            while True:
                resp = src.scan(**scan_kwargs)

                items = resp.get("Items", [])
                for item in items:
                    if transform_fn:
                        try:
                            transformed = transform_fn(item)
                        except Exception as e:
                            if not transform_error_recorded:
                                record_worker_failure(
                                    error_accumulator, e,
                                    f"Transform '{transform_name}' raised an exception in worker {segment}",
                                    understood=False,
                                )
                                transform_error_recorded = True
                            continue
                        # Contract matches the export verbs: return a list;
                        # [] skips the item. A bare item is coerced for
                        # convenience, but None is not a skip signal.
                        if not isinstance(transformed, list):
                            transformed = [transformed]
                        if not transformed:
                            excluded_count += 1
                            continue
                    else:
                        transformed = [item]

                    for out_item in transformed:
                        if target_key_names is not None and not target_key_names.issubset(out_item):
                            if not key_error_recorded:
                                missing = sorted(target_key_names.difference(out_item))
                                record_understood_failure(
                                    error_accumulator,
                                    f"Transform '{transform_name}' produced an item missing key "
                                    f"attribute(s) {missing} in worker {segment}",
                                )
                                key_error_recorded = True
                            continue
                        batch.put_item(Item=out_item)
                        local_count += 1

                lek = resp.get("LastEvaluatedKey")
                if not lek:
                    break
                scan_kwargs["ExclusiveStartKey"] = lek
    except Exception as e:
        record_worker_failure(error_accumulator, e, f"Error in worker {segment}")
        # Let control drop down to exit
    finally:
        source_rl.shutdown()
        target_rl.shutdown()

    total_matched_accumulator.add(local_count)
    if transform_excluded_accumulator is not None:
        transform_excluded_accumulator.add(excluded_count)
    print(f"Worker {segment}/{total_segments} copied {local_count} records.")
    return local_count
