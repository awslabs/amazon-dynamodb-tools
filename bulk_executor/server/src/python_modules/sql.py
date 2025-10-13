import sys
import warnings
import boto3
from pyspark.sql import SparkSession

# Custom Library Imports
sys.path.append('/server/src')
from python_modules.shared.pricing import PricingUtility
from python_modules.shared.table_info import get_and_print_dynamodb_table_info, get_dynamodb_throughput_configs, get_and_print_table_scan_cost
from python_modules.shared.errors import *

def run(job, spark_context, glue_context, parsed_args):
    DYNAMO_DB_NUMBER_OF_SPLITS = parsed_args.get('splits', '200')
    DYNAMO_DB_TABLE_NAME = parsed_args.get('table')
    QUERY = parsed_args.get('query')
    LIMIT = parsed_args.get('limit', None)

    # Print the table info
    region_name = boto3.Session().region_name
    table_info = get_and_print_dynamodb_table_info(DYNAMO_DB_TABLE_NAME)
    _ = get_and_print_table_scan_cost(table_info, region_name, numberOfScans=2)

    connection_options = {
        "dynamodb.input.tableName": DYNAMO_DB_TABLE_NAME,
        "dynamodb.splits": str(DYNAMO_DB_NUMBER_OF_SPLITS),
        "dynamodb.consistentRead": "false",
        **get_dynamodb_throughput_configs(parsed_args, DYNAMO_DB_TABLE_NAME, modes=["read"])
    }

    # Create a DynamoDB data source
    dynamo_data_source = glue_context.create_dynamic_frame.from_options(
        connection_type="dynamodb",
        connection_options=connection_options
    )

    # Suppress dataframe.py warning
    warnings.filterwarnings("ignore", message="DataFrame constructor is internal. Do not directly use it.")
    
    # Convert to DataFrame and register as temp table
    records = dynamo_data_source.toDF()
    table_alias = DYNAMO_DB_TABLE_NAME.replace('-', '_').replace('.', '_')
    records.createOrReplaceTempView(table_alias)
    
    # Create Spark session
    spark = SparkSession(spark_context)
    
    try:
        # Validate query starts with SELECT for safety
        query_upper = QUERY.upper().strip()
        if not query_upper.startswith('SELECT'):
            raise Exception("Only SELECT queries are supported")
            
        # Execute the SQL query
        result = spark.sql(QUERY)
        
        # Apply limit if specified
        if LIMIT:
            try:
                limit = int(LIMIT)
                if limit <= 0:
                    raise ValueError("Limit must be positive")
                result = result.limit(limit)
            except ValueError as e:
                raise Exception(f"Invalid 'limit': {str(e)}") from None
            except Exception as e:
                raise Exception("Invalid 'limit': " + get_error_message(e)) from None
        
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
        
    except Exception as e:
        raise Exception("SQL query error: " + get_error_message(e)) from None
    finally:
        # Ensure Spark session cleanup
        try:
            spark.stop()
        except:
            pass
