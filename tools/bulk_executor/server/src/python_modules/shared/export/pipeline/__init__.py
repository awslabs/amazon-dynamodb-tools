import time

from ...bulk_executor_error import BulkExecutorError
from ...logger import log
from ...errors import ListAccumulator
from ...table_info import get_dynamodb_throughput_configs
from ...rate_limiter import RateLimiterAggregator, RateLimiterSharedConfig

from .validator import validate
from .cost_estimator import estimate_cost
from .reader import read_and_parse
from .writer import write
from .reporter import report
from .transform_loader import load_transform_module
from ..utils.enums import ExportLoadType, Operation
from ..utils.export_path_resolver import ExportPathResolver


def _apply_transform_and_resolve(spark_context, records_rdd, export_load_type, parser, transform_name, transform_package, key_schema, error_accumulator, post_transform=None):
    """
    Apply optional user transform, optional post_transform, then resolve records into items.

    Args:
        post_transform: optional callable(record) → record, applied via map after the user transform.
    """
    transform_active = bool(transform_name)
    transformed_excluded_accumulator = spark_context.accumulator(0) if transform_active else None
    transformed_modified_or_included_accumulator = spark_context.accumulator(0) if transform_active else None

    if transform_active:
        log.debug(f"Loading transform module: {transform_name}")
        transform_module = load_transform_module(transform_name, transform_package)

        if export_load_type == ExportLoadType.FULL:
            transform_fn = transform_module.transform_full_record
        else:
            transform_fn = transform_module.transform_incremental_record

        log.debug(f"Applying transform: {transform_name}.{transform_fn.__name__}")
        def _apply_transform(record):
            try:
                result = transform_fn(record)
            except Exception as e:
                error_accumulator.add([f"Transform function raised an exception: {e}"])
                return []

            if not isinstance(result, list):
                result = [result]

            if not result:
                transformed_excluded_accumulator.add(1)
                return []

            transformed_modified_or_included_accumulator.add(len(result))
            return result

        records_rdd = records_rdd.flatMap(_apply_transform)
        log.debug("Transform applied (counts will be determined during processing)")
    else:
        log.debug("No transform specified, processing all records")

    if post_transform:
        records_rdd = records_rdd.map(post_transform)

    expected_keys = {key_schema[k]['name'] for k in ('pk', 'sk') if k in key_schema}

    def _resolve_and_validate(record):
        item = parser.resolve(record)
        if item["operation"] == Operation.PUT:
            missing = expected_keys - item["data"].keys()
            if missing:
                error_accumulator.add([f"Item missing key attributes after resolve: {missing}"])
                return None
        elif item["operation"] == Operation.DELETE:
            missing = expected_keys - item["data"].keys()
            if missing:
                error_accumulator.add([f"DELETE item missing key attributes: {missing}"])
                return None
            extra = item["data"].keys() - expected_keys
            if extra:
                error_accumulator.add([f"DELETE item has non-key attributes: {extra}"])
                return None
        return item

    items_rdd = records_rdd.map(_resolve_and_validate).filter(lambda x: x is not None)
    return items_rdd, transform_active, transformed_excluded_accumulator, transformed_modified_or_included_accumulator


def run_export_pipeline(spark_context, parsed_args, transform_package, post_validate=None, post_transform=None):
    """
    Shared orchestration for export-based verbs.

    Args:
        spark_context: Spark context
        parsed_args: Parsed Glue job arguments
        transform_package: Fully qualified package path for transforms (e.g. 'python_modules.load_export.transform')
        post_validate: optional callable(validation) that runs after validate() succeeds.
                       Can raise ValueError to fail fast.
        post_transform: optional callable(record) → record, applied via map after the user transform
                        but before resolve. Used by revert-export to swap new_image = old_image.
    """
    log.info(f"parsed_args {parsed_args}")
    table_name = parsed_args.get('table')
    s3_path = parsed_args.get('s3_path')
    transform_name = parsed_args.get('transform')

    bucket_name = parsed_args.get('s3-bucket-name')
    job_run_id = parsed_args.get("JOB_RUN_ID")
    rate_limiter_shared_config = RateLimiterSharedConfig(bucket=bucket_name, job_run_id=job_run_id)
    rate_limiter_aggregator = RateLimiterAggregator(shared_config=rate_limiter_shared_config)

    monitor_options = get_dynamodb_throughput_configs(parsed_args, table_name, modes=["write"], format="monitor")
    log.debug(f"monitor_options {monitor_options}")

    debug_enabled = parsed_args.get('XDebug', 'false').lower() == 'true'
    error_accumulator = spark_context.accumulator([], ListAccumulator())
    debug_accumulator = spark_context.accumulator([], ListAccumulator()) if debug_enabled else None

    start_time = time.time()
    path_resolver = ExportPathResolver(s3_path)

    log.debug(f"S3 Source Bucket: {path_resolver.get_bucket()}")
    log.debug(f"S3 Source Bucket Prefix: {path_resolver.get_prefix()}")
    log.debug(f"S3 Source Bucket Export ID: {path_resolver.get_export_id()}")
    log.debug(f"Export Path: {path_resolver.get_data_base_path()}")

    current_phase = "initialization"
    try:
        log.debug("=" * 80)
        log.info(f"Destination Table: {table_name}")
        log.debug("=" * 80)

        current_phase = "validation"
        validation = validate(path_resolver, table_name)
        if validation is None:
            return

        if post_validate:
            post_validate(validation)

        current_phase = "cost estimation"
        estimate_cost(validation['table_info'], validation['manifest_data'], validation['key_schema_result'])

        current_phase = "data reading"
        records_rdd, export_load_type, parser, total_expected_items = read_and_parse(
            spark_context, validation['manifest_data'], path_resolver, validation['key_schema']
        )

        current_phase = "transform"
        items_rdd, transform_active, transformed_excluded_acc, transformed_modified_or_included_acc = _apply_transform_and_resolve(
            spark_context, records_rdd, export_load_type, parser, transform_name,
            transform_package, validation['key_schema'], error_accumulator, post_transform
        )

        current_phase = "data processing"
        written_items_acc = write(
            spark_context, items_rdd, export_load_type, table_name,
            rate_limiter_shared_config, monitor_options,
            error_accumulator, debug_accumulator
        )

        report(
            validation['manifest_data'], total_expected_items, written_items_acc,
            transform_active, transform_name, transformed_excluded_acc, transformed_modified_or_included_acc,
            start_time
        )

    except (ValueError, BulkExecutorError) as e:
        execution_time = time.time() - start_time
        log.error("=" * 80)
        log.error("JOB FAILED")
        log.error("=" * 80)
        log.error(f"Failure Summary:")
        log.error(f"  - Phase: {current_phase}")
        log.error(f"  - Error: {str(e)}")
        log.error(f"  - Execution time: {execution_time:.1f} seconds")
        log.error("=" * 80)
        log.error("Job terminated due to validation failure")
        raise

    except Exception as e:
        execution_time = time.time() - start_time
        log.error("=" * 80)
        log.error("JOB FAILED")
        log.error("=" * 80)
        log.error(f"Failure Summary:")
        log.error(f"  - Phase: {current_phase}")
        log.error(f"  - Error: {str(e)}")
        log.error(f"  - Execution time: {execution_time:.1f} seconds")
        log.error("=" * 80)
        log.error("Job terminated due to unexpected error")
        raise
    finally:
        for debug_msg in (debug_accumulator.value if debug_accumulator else []):
            log.debug(debug_msg)
        rate_limiter_aggregator.shutdown()
