import boto3
import time

from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext

from ..shared.logger import log
from ..shared.errors import ListAccumulator
from ..shared.table_info import get_dynamodb_throughput_configs, get_and_print_dynamodb_table_info, get_and_print_table_write_cost
from ..shared.rate_limiter import RateLimiterAggregator, RateLimiterSharedConfig

from .validators.manifest_validator import ManifestValidator
from .validators.data_file_validator import DataFileValidator
from .validators.key_schema_validator import KeySchemaValidator
from .validators.s3_validator import S3Validator
from .utils.file_loader import FileLoader
from .utils.enums import ImportType, Operation, VALID_OPERATIONS
from .utils.export_path_resolver import ExportPathResolver
from .readers.export_reader import get_export_file_paths
from .writers.writer_factory import WriterFactory
from .parsers.parser_factory import ParserFactory
from .transform.transform_loader import load_transform_module


def _validate(path_resolver, table_name, step):
    """Validate S3 path, table, manifests, checksums, and key schema."""
    s3_client = boto3.client('s3')
    file_loader = FileLoader(s3_client=s3_client)
    s3_validator = S3Validator(s3_client)

    step += 1
    log.info(f"Step {step}: Validating S3 export path exists...")
    s3_validator.validate_path_exists(path_resolver)

    step += 1
    log.info(f"Step {step}: Validating destination table exists...")
    table_info = get_and_print_dynamodb_table_info(table_name, quiet=True)
    key_schema = table_info['key_schema']
    log.info(f"Destination table validation completed successfully: {key_schema}")

    step += 1
    log.info(f"Step {step}: Validating and parsing manifest files...")
    manifest_validator = ManifestValidator(file_loader)
    manifest_data = manifest_validator.validate_and_parse_manifests(path_resolver)
    log.info(f"Step {step}: Manifest validation completed successfully")

    if manifest_data['total_item_count'] == 0:
        log.info("Export contains 0 items, nothing to import. Exiting.")
        return None

    step += 1
    log.info(f"Step {step}: Validating data file checksums...")
    data_file_validator = DataFileValidator(file_loader)
    checksum_result = data_file_validator.validate(
        data_files=manifest_data['data_files'],
        base_path=path_resolver.get_base_path()
    )
    log.info(f"Step {step}: Data file checksum validation completed successfully")

    step += 1
    log.info(f"Step {step}: Validating key schema against verified data files...")
    key_schema_validator = KeySchemaValidator(file_loader)
    key_schema_result = key_schema_validator.validate(
        verified_files=checksum_result['verified_files'],
        base_path=path_resolver.get_base_path(),
        key_schema=key_schema,
        export_type=manifest_data['export_type']
    )
    log.info(f"Step {step}: Key schema validation completed successfully")

    step += 1
    log.info("=" * 80)
    log.info(f"Step {step}: Validation Summary:")
    log.info(f"  - Total items to import: {manifest_data['total_item_count']:,}")
    log.info(f"  - Number of data files: {len(manifest_data['data_files']):,}")
    log.info(f"  - Output format: {manifest_data['output_format']}")
    log.info(f"  - Export format: {manifest_data['export_type']}")
    log.info("=" * 80)
    log.info("All validations passed successfully")

    return {
        'table_info': table_info,
        'key_schema': key_schema,
        'manifest_data': manifest_data,
        'key_schema_result': key_schema_result,
        'step': step,
    }


def _estimate_cost(table_info, manifest_data, key_schema_result, step):
    """Estimate and log DynamoDB write costs."""
    step += 1
    log.info(f"Step {step}: Estimating DynamoDB write costs...")
    avg_item_size = key_schema_result.get('avg_item_size', 0)
    estimated_size_bytes = avg_item_size * manifest_data['total_item_count']
    get_and_print_table_write_cost(table_info, manifest_data['total_item_count'], estimated_size_bytes)
    return step


def _read_and_parse(spark_context, manifest_data, path_resolver, key_schema, step):
    """Read export files into record RDD. Returns records_rdd, import_type, parser, total_expected_items, step."""
    step += 1
    log.info(f"Step {step}: Resolving export file paths...")
    file_paths, total_expected_items = get_export_file_paths(
        data_files=manifest_data['data_files'],
        file_base_path=path_resolver.get_base_path()
    )

    step += 1
    log.info(f"Step {step}: Reading and parsing export files with Spark...")
    all_lines_rdd = spark_context.textFile(",".join(file_paths))

    export_type = manifest_data['export_type']
    import_type = ImportType.INCREMENTAL if export_type == 'INCREMENTAL_EXPORT' else ImportType.FULL
    parser = ParserFactory.get_parser(import_type, key_schema)
    log.info(f"Parser of type {type(parser).__name__} returned successfully...")

    records_rdd = all_lines_rdd.map(parser.parse_to_record)
    return records_rdd, import_type, parser, total_expected_items, step


def _apply_transform_stage(spark_context, records_rdd, import_type, parser, transform_name, key_schema, error_accumulator):
    """Apply transform then resolve records into items. Returns items_rdd, transform_active, transformed_out_accumulator, transformed_in_accumulator."""
    transform_active = bool(transform_name)
    transformed_out_accumulator = spark_context.accumulator(0) if transform_active else None
    transformed_in_accumulator = spark_context.accumulator(0) if transform_active else None

    if transform_active:
        log.info(f"Loading transform module: {transform_name}")
        transform_module = load_transform_module(transform_name)

        if import_type == ImportType.FULL:
            transform_fn = transform_module.transform_full_record
        else:
            transform_fn = transform_module.transform_incremental_record

        log.info(f"Applying transform: {transform_name}.{transform_fn.__name__}")
        def _apply_transform(record):
            try:
                result = transform_fn(record)
            except Exception as e:
                error_accumulator.add([f"Transform function raised an exception: {e}"])
                return []

            if not isinstance(result, list):
                result = [result]

            if not result:
                transformed_out_accumulator.add(1)
                return []

            transformed_in_accumulator.add(len(result))
            return result

        records_rdd = records_rdd.flatMap(_apply_transform)
        log.info("Transform applied (counts will be determined during processing)")
    else:
        log.info("No transform specified, processing all records")

    # Resolve records into {"operation", "data"} and validate keys
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
    return items_rdd, transform_active, transformed_out_accumulator, transformed_in_accumulator


def _write(spark_context, items_rdd, import_type, table_name, rate_limiter_shared_config, monitor_options, error_accumulator, debug_accumulator, step):
    """Repartition and write items to DynamoDB."""
    step += 1
    num_partitions = min(items_rdd.getNumPartitions(), spark_context.defaultParallelism * 2)
    log.info(f"Step {step}: Using {num_partitions:,} partitions (based on mins of #gz files {items_rdd.getNumPartitions():,} and defaultParallelism {spark_context.defaultParallelism:,})")
    items_rdd = items_rdd.repartition(num_partitions)
    log.info(f"Step {step}: Repartitioned to {num_partitions:,} partitions")

    step += 1
    log.info(f"Step {step}: Writing items to DynamoDB in parallel...")
    writer = WriterFactory.create_writer()
    log.info(f"Using batch writer for {import_type.value} import")

    written_items_accumulator = spark_context.accumulator(0)
    log.info("Writing items to DynamoDB...")

    items_rdd.foreachPartition(
        lambda partition: writer.write_partition_to_dynamodb(
            partition,
            table_name,
            rate_limiter_shared_config,
            monitor_options,
            error_accumulator,
            debug_accumulator,
            written_items_accumulator
        )
    )

    if error_accumulator.value:
        first_error = error_accumulator.value[0]
        raise Exception(first_error) from None

    return written_items_accumulator, step


def _report(manifest_data, total_expected_items, written_items_accumulator, transform_active, transform_name, transformed_out_accumulator, transformed_in_accumulator, start_time):
    """Log final summary."""
    total_item_count = manifest_data['total_item_count']
    written_count = written_items_accumulator.value

    log.info(f"Successfully wrote {written_count:,} items to DynamoDB")
    if transform_active:
        log.info(f"Transform '{transform_name}': {transformed_in_accumulator.value:,} items produced, {transformed_out_accumulator.value:,} items excluded out of {total_item_count:,} total")
    log.info("Data processing and writing completed")

    log.info("=" * 80)
    log.info("Data Processing Summary:")
    log.info(f"  - Manifest items: {total_item_count:,}")
    log.info(f"  - Expected items: {total_expected_items:,}")
    log.info(f"  - Parsed items: {total_item_count:,}")
    log.info("=" * 80)

    execution_time = time.time() - start_time

    log.info("=" * 80)
    log.info("JOB COMPLETED SUCCESSFULLY")
    log.info("=" * 80)
    log.info("Success Summary:")
    log.info(f"  - Total items in export: {total_item_count:,}")
    if transform_active:
        log.info(f"  - Items excluded by transform: {transformed_out_accumulator.value:,}")
        log.info(f"  - Items produced by transform: {transformed_in_accumulator.value:,}")
    log.info(f"  - Total items written: {written_count:,}")
    log.info(f"  - Execution time: {execution_time:.1f} seconds")
    log.info(f"  - All validations passed")
    log.info("=" * 80)


def run(job, spark_context, glue_context, parsed_args):
    log.info(f"parsed_args {parsed_args}")
    table_name = parsed_args.get('table')
    s3_path = parsed_args.get('s3_path')
    transform_name = parsed_args.get('transform')

    bucket_name = parsed_args.get('s3-bucket-name')
    job_run_id = parsed_args.get("JOB_RUN_ID")
    rate_limiter_shared_config = RateLimiterSharedConfig(bucket=bucket_name, job_run_id=job_run_id)
    rate_limiter_aggregator = RateLimiterAggregator(shared_config=rate_limiter_shared_config)

    monitor_options = get_dynamodb_throughput_configs(parsed_args, table_name, modes=["write"], format="monitor")
    log.info(f"monitor_options {monitor_options}")

    debug_enabled = parsed_args.get('XDebug', 'false').lower() == 'true'
    error_accumulator = spark_context.accumulator([], ListAccumulator())
    debug_accumulator = spark_context.accumulator([], ListAccumulator()) if debug_enabled else None

    start_time = time.time()
    path_resolver = ExportPathResolver(s3_path)

    log.info(f"S3 Source Bucket: {path_resolver.get_bucket()}")
    log.info(f"S3 Source Bucket Prefix: {path_resolver.get_prefix()}")
    log.info(f"S3 Source Bucket Export ID: {path_resolver.get_export_id()}")
    log.info(f"Export Path: {path_resolver.get_data_base_path()}")

    current_phase = "initialization"
    try:
        log.info("=" * 80)
        log.info("DynamoDB Export Importer - Job Started")
        log.info(f"Destination Table: {table_name}")
        log.info("=" * 80)

        # Validate
        current_phase = "validation"
        validation = _validate(path_resolver, table_name, step=1)
        if validation is None:
            return
        step = validation['step']

        # Cost estimate
        current_phase = "cost estimation"
        step = _estimate_cost(validation['table_info'], validation['manifest_data'], validation['key_schema_result'], step)

        # Read & parse into records
        current_phase = "data reading"
        records_rdd, import_type, parser, total_expected_items, step = _read_and_parse(
            spark_context, validation['manifest_data'], path_resolver, validation['key_schema'], step
        )

        # Transform records then resolve into items
        current_phase = "transform"
        items_rdd, transform_active, transformed_out_acc, transformed_in_acc = _apply_transform_stage(
            spark_context, records_rdd, import_type, parser, transform_name,
            validation['key_schema'], error_accumulator
        )

        # Write
        current_phase = "data processing"
        written_items_acc, step = _write(
            spark_context, items_rdd, import_type, table_name,
            rate_limiter_shared_config, monitor_options,
            error_accumulator, debug_accumulator, step
        )

        # Report
        _report(
            validation['manifest_data'], total_expected_items, written_items_acc,
            transform_active, transform_name, transformed_out_acc, transformed_in_acc,
            start_time
        )

    except ValueError as e:
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
