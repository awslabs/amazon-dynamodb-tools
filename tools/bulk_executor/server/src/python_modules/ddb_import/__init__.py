import math
import boto3
import time

from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext

from ..shared.logger import log
from ..shared.errors import ListAccumulator
from ..shared.pricing import PricingUtility
from ..shared.table_info import get_dynamodb_throughput_configs
from ..shared.rate_limiter import RateLimiterAggregator, RateLimiterSharedConfig

from .validators.table_validator import TableValidator
from .validators.manifest_validator import ManifestValidator
from .validators.data_file_validator import DataFileValidator
from .validators.key_schema_validator import KeySchemaValidator
from .validators.s3_validator import S3Validator
from .utils.file_loader import FileLoader
from .utils.enums import ImportType
from .utils.export_path_resolver import ExportPathResolver
from .readers.export_reader import get_export_file_paths
from .writers.writer_factory import WriterFactory
from .parsers.parser_factory import ParserFactory
from .filter.filter_loader import load_filter_function

def _is_filter_specified(filter_name, filter_function_name):
    return filter_name and filter_name != "None" and filter_function_name and filter_function_name != "None"

def run(job, spark_context, glue_context, parsed_args):
    log.info(f"parsed_args {parsed_args}")
    table_name = parsed_args.get('table')
    s3_path = parsed_args.get('s3_path')
    
    # Filter configuration
    filter_name = parsed_args.get('filter')
    filter_function_name = parsed_args.get('filterfunctionname')

    # Rate limiter configuration
    bucket_name = parsed_args.get('s3-bucket-name')
    job_run_id = parsed_args.get("JOB_RUN_ID")

    rate_limiter_shared_config = RateLimiterSharedConfig(
        bucket=bucket_name,
        job_run_id=job_run_id
    )
    rate_limiter_aggregator = RateLimiterAggregator(shared_config=rate_limiter_shared_config)

    # Get throughput configuration for rate limiting
    monitor_options = get_dynamodb_throughput_configs(parsed_args, table_name, modes=["write"], format="monitor")
    log.info(f"monitor_options {monitor_options}")

    debug_enabled = parsed_args.get('XDebug', 'false').lower() == 'true'

    error_accumulator = spark_context.accumulator([], ListAccumulator()) # Error accumulator for collecting errors from workers
    debug_accumulator = spark_context.accumulator([], ListAccumulator()) if debug_enabled else None # Debug accumulators for worker info

    # Track execution time from job start
    start_time = time.time()

    path_resolver = ExportPathResolver(s3_path)

    log.info(f"S3 Source Bucket: {path_resolver.get_bucket()}")
    log.info(f"S3 Source Bucket Prefix: {path_resolver.get_prefix()}")
    log.info(f"S3 Source Bucket Export ID: {path_resolver.get_export_id()}")
    log.info(f"Export Path: {path_resolver.get_data_base_path()}")

    try:
        # Log job start
        log.info("=" * 80)
        log.info("DynamoDB Export Importer - Job Started")
        log.info(f"Destination Table: {table_name}")
        log.info("=" * 80)

        # Step 1: Initialize components
        current_phase = "initialization"
        log.info("Step 1: Initializing components...")
        dynamodb_client = boto3.client('dynamodb')

        s3_client = boto3.client('s3')
        
        file_loader = FileLoader(s3_client=s3_client)
        table_validator = TableValidator(dynamodb_client)
        manifest_validator = ManifestValidator(file_loader)
        data_file_validator = DataFileValidator(file_loader)
        key_schema_validator = KeySchemaValidator(file_loader)
        s3_validator = S3Validator(s3_client)

        log.info("Components initialized successfully")

        # Step 2: Validate S3 export path exists
        current_phase = "s3 path validation"
        log.info("Step 2: Validating S3 export path exists...")
        s3_validator.validate_path_exists(path_resolver)

        # Step 3: Validate target table exists and get key schema
        current_phase = "table validation"
        log.info("Step 3: Validating destination table exists...")
        key_schema = table_validator.validate_table_exists(table_name)
        log.info(f"Destination table validation completed successfully: {key_schema}")

        # Step 4: Validate and parse manifests
        current_phase = "manifest validation"
        log.info("Step 4: Validating and parsing manifest files...")
        manifest_data = manifest_validator.validate_and_parse_manifests(path_resolver)
        log.info("Step 4: Manifest validation completed successfully")

        if manifest_data['total_item_count'] == 0:
            log.info("Export contains 0 items, nothing to import. Exiting.")
            return

        # Step 5: Validate data file checksums
        current_phase = "data file validation"
        log.info("Step 5: Validating data file checksums...")
        checksum_result = data_file_validator.validate(
            data_files=manifest_data['data_files'],
            base_path=path_resolver.get_base_path()
        )
        log.info("Step 5: Data file checksum validation completed successfully")

        # Step 5b: Validate key schema against verified files
        current_phase = "key schema validation"
        log.info("Step 5b: Validating key schema against verified data files...")
        key_schema_validator.validate(
            verified_files=checksum_result['verified_files'],
            base_path=path_resolver.get_base_path(),
            key_schema=key_schema,
            export_type=manifest_data['export_type']
        )
        log.info("Step 5b: Key schema validation completed successfully")

        # Step 6: Print validation summary
        log.info("=" * 80)
        log.info("Step 6: Validation Summary:")
        log.info(f"  - Total items to import: {manifest_data['total_item_count']}")
        log.info(f"  - Number of data files: {len(manifest_data['data_files'])}")
        log.info(f"  - Output format: {manifest_data['output_format']}")
        log.info(f"  - Export format: {manifest_data['export_type']}")
        log.info("=" * 80)
        log.info("All validations passed successfully")
        log.info("Ready to proceed with data import")

        # Step 7: Get export file paths
        current_phase = "file path resolution"
        log.info("Step 7: Resolving export file paths...")

        file_paths, total_expected_items = get_export_file_paths(
            data_files=manifest_data['data_files'],
            file_base_path=path_resolver.get_base_path()
        )
        
        # Step 8: Read and parse files using Spark
        current_phase = "data reading"
        log.info("Step 8: Reading and parsing export files with Spark...")
        
        # Read all data files as text lines using Spark (handles gzip automatically)
        log.info("Reading data files from S3 using Spark textFile...")
        all_lines_rdd = spark_context.textFile(",".join(file_paths))
        
        # Parse each line to extract items and deserialize to plain Python format
        log.info("Parsing export lines to extract items...")

        # Get the appropriate parser for the import type
        export_type = manifest_data['export_type']
        import_type = ImportType.INCREMENTAL if export_type == 'INCREMENTAL_EXPORT' else ImportType.FULL
        parser = ParserFactory.get_parser(import_type, output_view=manifest_data.get('output_view'))
        log.info(f"Parser of type {type(parser).__name__} returned successfully...")
        
        def parse_line(line):
            """Parse a line from the export file using the appropriate parser."""
            result = parser.parse_export_line(line)
            operation, item_data, condition = result
            # Return the full operation info for the writer to handle
            return {"operation": operation, "data": item_data, "condition": condition}

        items_rdd = all_lines_rdd.map(parse_line)

        # Apply filter if specified
        if _is_filter_specified(filter_name, filter_function_name):
            log.info(f"Loading filter function: {filter_name}.{filter_function_name}")
            filter_function = load_filter_function(filter_name, filter_function_name)
            
            log.info("Applying filter to items...")
            filtered_items_rdd = items_rdd.filter(filter_function)
            log.info("Filter applied (filtered count will be determined during processing)")
            
            # Use filtered items
            final_items_rdd = filtered_items_rdd
        else:
            log.info("No filter specified, processing all items")
            final_items_rdd = items_rdd

        # Use manifest count (already validated)
        total_item_count = manifest_data['total_item_count']
        log.info(f"Prepared to process items from export (original manifest count: {total_item_count})")

        # Step 9: Repartition for optimal parallelism
        current_phase = "repartitioning"

        num_partitions = min(final_items_rdd.getNumPartitions(), spark_context.defaultParallelism*2)
        log.info(f"Step 9: Using {num_partitions} partitions (based on mins of #gz files {final_items_rdd.getNumPartitions()} and defaultParallelism {spark_context.defaultParallelism})")
        items_rdd = final_items_rdd.repartition(num_partitions)
        log.info(f"Step 9: Repartitioned to {num_partitions} partitions")

        # Step 10: Write items in parallel
        current_phase = "data processing"
        log.info("Step 10: Writing items to DynamoDB in parallel...")
        
        # Get the appropriate writer based on import type
        writer = WriterFactory.create_writer(import_type)
        writer_type = "batch writer" if import_type == ImportType.FULL else "item writer"
        log.info(f"Using {writer_type} for {import_type.value} import")

        # Create accumulator to track written items
        written_items_accumulator = spark_context.accumulator(0)

        # Execute parallel writes with rate limiting
        log.info("Writing items to DynamoDB...")

        # Use the selected writer
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
        
        # Check for errors from workers
        if error_accumulator.value:
            first_error = error_accumulator.value[0]
            raise Exception(first_error) from None

        # Log final counts
        written_count = written_items_accumulator.value
        log.info(f"Successfully wrote {written_count:,} items to DynamoDB table '{table_name}'")
        if _is_filter_specified(filter_name, filter_function_name):
            log.info(f"Filter '{filter_name}' processed {written_count:,} items out of {total_item_count:,} original items")
        log.info("Data processing and writing completed")

        # Log final summary
        log.info("=" * 80)
        log.info("Data Processing Summary:")
        log.info(f"  - Manifest items: {manifest_data['total_item_count']}")
        log.info(f"  - Expected items: {total_expected_items}")
        log.info(f"  - Parsed items: {total_item_count}")
        log.info("=" * 80)

        # Calculate execution time
        end_time = time.time()
        execution_time = end_time - start_time

        # Log job completion with success summary
        log.info("=" * 80)
        log.info("JOB COMPLETED SUCCESSFULLY")
        log.info("=" * 80)
        log.info("Success Summary:")
        log.info(f"  - Total items expected: {manifest_data['total_item_count']}")
        log.info(f"  - Total items written: {written_items_accumulator.value}")
        log.info(f"  - Execution time: {execution_time:.1f} seconds")
        log.info(f"  - All validations passed")
        log.info("=" * 80)

    except ValueError as e:
        # Validation error - log and terminate with failure summary
        end_time = time.time()
        execution_time = end_time - start_time

        log.error("=" * 80)
        log.error("JOB FAILED")
        log.error("=" * 80)
        log.error("Failure Summary:")
        log.error(f"  - Phase: {current_phase}")
        log.error(f"  - Error: {str(e)}")
        log.error(f"  - Execution time: {execution_time:.1f} seconds")
        log.error("=" * 80)
        log.error("Job terminated due to validation failure")
        raise

    except Exception as e:
        # Unexpected error - log and terminate with failure summary
        end_time = time.time()
        execution_time = end_time - start_time

        log.error("=" * 80)
        log.error("JOB FAILED")
        log.error("=" * 80)
        log.error("Failure Summary:")
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

