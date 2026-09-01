import sys
import boto3
from pyspark.sql import SparkSession

# Custom Library Imports
sys.path.append('/server/src')
from python_modules.shared.pricing import PricingUtility
from python_modules.shared.table_info import get_and_print_dynamodb_table_info, get_dynamodb_throughput_configs, get_and_print_table_scan_cost
from python_modules.shared.glue_connector import read_dynamodb_dataframe
from python_modules.shared.bulk_executor_error import BulkExecutorError
from python_modules.shared.errors import *

def run(job, spark_context, glue_context, parsed_args):
    DYNAMO_DB_NUMBER_OF_SPLITS = parsed_args.get('splits', '200')
    DYNAMO_DB_TABLE_NAME = parsed_args.get('table')
    QUERY = parsed_args.get('query')
    LIMIT = parsed_args.get('limit', None)

    # Print the table info
    region_name = boto3.Session().region_name
    table_info = get_and_print_dynamodb_table_info(DYNAMO_DB_TABLE_NAME)
    _ = get_and_print_table_scan_cost(table_info, region_name)

    # The "DataFrame constructor is internal" warning is suppressed once in
    # server/src/root.py for every verb.

    # Read directly into a DataFrame and register as temp table
    records = read_dynamodb_dataframe(
        glue_context, DYNAMO_DB_TABLE_NAME, parsed_args,
        splits=DYNAMO_DB_NUMBER_OF_SPLITS)
    table_alias = DYNAMO_DB_TABLE_NAME.replace('-', '_').replace('.', '_')
    records.createOrReplaceTempView(table_alias)
    
    # Create Spark session
    spark = SparkSession(spark_context)
    
    try:
        # Validate query starts with SELECT for safety
        query_upper = QUERY.upper().strip()
        if not query_upper.startswith('SELECT'):
            raise BulkExecutorError("Only SELECT queries are supported")

        # Execute the SQL query
        result = spark.sql(QUERY)

        # Apply limit if specified
        if LIMIT:
            try:
                limit = int(LIMIT)
            except (ValueError, TypeError):
                raise BulkExecutorError(f"Invalid 'limit': {LIMIT!r} is not an integer") from None
            if limit <= 0:
                raise BulkExecutorError(f"Invalid 'limit': must be positive, got {limit}")
            result = result.limit(limit)
        
        # Cache the result for multiple actions
        result.cache()
        count = result.count()
        
        # Calculate the S3 output location
        bucket_name = parsed_args.get('s3-bucket-name')
        job_run_id = parsed_args.get("JOB_RUN_ID")
        s3_output_location = f"s3://{bucket_name}/output/{job_run_id}"
        
        # Print the top N results first
        TOP_N = 10
        if (count <= TOP_N):
            print(f"{count} result rows:")
        else:
            print(f"First {TOP_N} result rows:")
            
        if count > 0:
            top_n_records = result.limit(TOP_N).toJSON().collect()
            for record in top_n_records:
                print(record)
                
            if (count > TOP_N):
                print(f"...and {count - TOP_N} more rows not printed")
        
        # Write results to S3
        result.write.mode("overwrite").json(s3_output_location)
            
        print()
        print(f"Wrote {count:,} rows in JSON format to {s3_output_location}/")
        print()
        
        # Ensure proper cleanup
        result.unpersist()
        
    except BulkExecutorError:
        # User-parameter errors are already clean; let root.py surface them
        # without wrapping them into an opaque "SQL query error".
        raise
    except Exception as e:
        # The query is the user's to fix, so this is a sentence, not a stack trace.
        raise BulkExecutorError("SQL query error: " + get_error_message(e)) from None
    finally:
        # Ensure Spark session cleanup
        try:
            spark.stop()
        except:
            pass
