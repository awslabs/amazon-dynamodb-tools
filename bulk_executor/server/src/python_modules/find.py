import json
import math
import re
import sys
import warnings

import boto3
from awsglue.transforms import *
from botocore.config import Config
from pyspark.sql import SparkSession
from pyspark.sql.functions import asc, desc

# Custom Library Imports
sys.path.append('/server/src')
from python_modules.shared.errors import *
from python_modules.shared.pricing import PricingUtility
from python_modules.shared.rate_limiter import (
    RateLimiterAggregator,
    RateLimiterSharedConfig,
    RateLimiterWorker
)

from python_modules.shared.table_info import (
    get_and_print_dynamodb_table_info, get_and_print_table_scan_cost,
    get_dynamodb_throughput_configs)


def print_dynamodb_table_info(table_name, is_delete, **kwargs):
    region_name = boto3.Session().region_name
    table_info = get_and_print_dynamodb_table_info(table_name)
    _ = get_and_print_table_scan_cost(table_info, region_name, **kwargs)

    # If a delete, print how much a delete would likely cost
    if is_delete:
        # Wait for the scan to get the count, then we'll resume here to say what the deletes will cost
        delete_count = yield

        avg_size = math.ceil(table_info['size_bytes'] / (table_info['item_count'] + 1)) # avoid any division by 0
        avg_write_units_per_item = math.ceil(avg_size / 1024)
        write_units = delete_count * avg_write_units_per_item
        pricing_utility = PricingUtility()
        ondemand_pricing = pricing_utility.get_on_demand_capacity_pricing(region_name)
        wru_cost = float(ondemand_pricing.get(table_info['write_pricing_category']))
        od_cost = write_units * wru_cost
        prov_cost = od_cost / 1.5 # very rough, look into updating this
        print("DynamoDB delete costs depend on how many items are being deleted and the size of the items.")
        print(f"Here we observe the command will delete approx {delete_count:,} matched items")
        print(f" with the table's average item size of {avg_size:,} bytes;")
        print(f" each write incurs an average of {avg_write_units_per_item} write units")
        print(f"Write units required to do such a delete (approx): {write_units:,}")
        print("This does not include costs for secondary indexes!")
        if table_info['billing_mode'] == "PROVISIONED":
            print(f"Approx DynamoDB cost for provisioned delete consuming {write_units:,} WCUs (using {region_name} prices): ${prov_cost:,.2f}")
        elif table_info['billing_mode'] == "PAY_PER_REQUEST":
            print(f"Approx DynamoDB cost for On-demand delete consuming {write_units:,} WRUs (using {region_name} prices): ${od_cost:,.2f}")
        print()
    yield  # to avoid a StopIteration exception


def run(job, spark_context, glue_context, parsed_args):
    DYNAMO_DB_NUMBER_OF_SPLITS = parsed_args.get('splits', '200')
    DYNAMO_DB_TABLE_NAME = parsed_args.get('table')
    WHERE = parsed_args.get('where', None)
    ORDERBY = parsed_args.get('orderby', None)
    LIMIT = parsed_args.get('limit', None)

    glue_job_action = parsed_args.get('XAction')
    DO_COUNT = glue_job_action == 'count'
    DO_DELETE = glue_job_action == 'delete'
    DO_FIND = glue_job_action == 'find'

    # This verb usually requires two scans except for plain count calls
    kwargs = {}
    if not(DO_COUNT and not (WHERE or ORDERBY or LIMIT)):
        kwargs['numberOfScans'] = 2

    # Print the table info, and use a generator in case we need to resume for the deletes
    print_pricing_generator = print_dynamodb_table_info(DYNAMO_DB_TABLE_NAME, DO_DELETE, **kwargs)
    table_desc = next(print_pricing_generator)

    # We want to convert a string like "foo asc, bar desc" into an object array [asc(foo), desc(bar)]
    def parse_sort_order(sort_order_str):
        sort_order_list = []
        pattern = re.compile(r'(.+?)(?:\s+(asc|desc))?$', re.IGNORECASE)
        for spec in sort_order_str.split(','):
            match = pattern.match(spec.strip())
            if match:
                column = match.group(1).strip()
                order = match.group(2).strip().lower() if match.group(2) else 'asc'
                if order == 'asc':
                    sort_order_list.append(asc(column))
                elif order == 'desc':
                    sort_order_list.append(desc(column))
                else:
                    raise ValueError(f"Invalid sort order: {order}")
            else:
                raise ValueError(f"Invalid sort specification: {spec}")
        return sort_order_list

    if ORDERBY:
        ORDERBY = parse_sort_order(ORDERBY)

    connection_options = {
        "dynamodb.input.tableName": DYNAMO_DB_TABLE_NAME,
        "dynamodb.splits": str(DYNAMO_DB_NUMBER_OF_SPLITS),
        "dynamodb.consistentRead": "false",
        **get_dynamodb_throughput_configs(parsed_args, DYNAMO_DB_TABLE_NAME, modes=["read"])
    }
    #print(f"Connection options: {connection_options}...")

    # Create a DynamoDB data source
    dynamo_data_source = glue_context.create_dynamic_frame.from_options(
        connection_type="dynamodb",
        connection_options=connection_options
    )

    # Shortcut: if it's a simple full table count we don't need to convert to a DataFrame, just count directly
    if DO_COUNT and not (WHERE or ORDERBY or LIMIT):
        print(f"Count of matching items: {dynamo_data_source.count():,}")

    # OK, we're gonna convert the DynamicFrame to a DataFrame for processing
    else:
        # Suppress dataframe.py warning that might confuse users
        warnings.filterwarnings("ignore", message="DataFrame constructor is internal. Do not directly use it.")
        records = dynamo_data_source.toDF()

        needsRepartitioning = False

        # Filter the DataFrame
        if WHERE:
            try:
                records = records.filter(WHERE)
            except Exception as e:
                raise Exception("Invalid 'where': " + get_error_message(e)) from None
        if ORDERBY:
            try:
                records = records.orderBy(ORDERBY)
                needsRepartitioning = True
            except Exception as e:
                raise Exception("Invalid 'orderby': " + get_error_message(e)) from None
        if LIMIT:
            try:
                limit = int(LIMIT)
                records = records.limit(limit)
                if limit > 1000: # Don't bother for tiny sizes
                    needsRepartitioning = True
            except Exception as e:
                raise Exception("Invalid 'limit': " + get_error_message(e)) from None

        def get_table_keys(table_name):
            client = boto3.client('dynamodb')
            response = client.describe_table(TableName=table_name)
            key_schema = response['Table']['KeySchema']
            keys = [key['AttributeName'] for key in key_schema]
            return keys

        if DO_COUNT:
            print(f"Count of matching items: {records.count():,}")

        elif DO_FIND:
            records.cache()
            count = records.count()

            # Calculate the S3 output location
            bucket_name = parsed_args.get('s3-bucket-name')
            job_run_id = parsed_args.get("JOB_RUN_ID")
            s3_output_location = f"s3://{bucket_name}/output/{job_run_id}"

            # With a --limit, this produces one file with a name like part-00000-8c460443-6d45-4d11-b9ef-0cd84c21a45a-c000.json
            # because the limit moves all the data to a single worker
            # Without a limit, this produces about 200 files with names similar to that
            # Adding coalesce(10) gets us down to 10 files, but testing against a large table showed that slower
            spark = SparkSession(spark_context)
            json_rdd = records.toJSON()
            json_df = spark.read.json(json_rdd)
            json_df.write.mode("overwrite").json(s3_output_location)

            # Print the top N many
            TOP_N = 10
            if count <= TOP_N:
                print(f"{count} matching items:")
            else:
                print(f"First {TOP_N} matching items:")
            top_n_records = records.limit(TOP_N).toJSON().collect()
            for record in top_n_records:
                print(record)
            if count > TOP_N:
                print(f"...and {count - TOP_N} more not printed")
            print()
            print(f"Wrote {count:,} items in JSON format to {s3_output_location}/")
            print()

        elif DO_DELETE:
            keys = get_table_keys(DYNAMO_DB_TABLE_NAME)

            def delete_partition(monitor_options, partition, shared_config):
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

                table = dynamodb_resource.Table(DYNAMO_DB_TABLE_NAME)
                try:
                    with table.batch_writer() as batch:
                        for record in partition:
                            try:
                                item = json.loads(record)
                                key = {k: item[k] for k in keys}
                                batch.delete_item(Key=key)
                            except Exception as e:
                                print(f"Error deleting item {item}: {e}")
                finally:
                    rate_limiter_worker.shutdown()

            if needsRepartitioning:
                # The orderby and limit tend to have results on one worker, so shuffle them around
                # We first select only the keys attributes so we have less data to shuffle around
                records = records.select(*keys)
                records = records.repartition(200)
            records.cache()
            count = records.count()

            # Finish the printing now including delete cost
            print_pricing_generator.send(count)

            bucket_name = parsed_args.get('s3-bucket-name')
            job_run_id = parsed_args.get("JOB_RUN_ID")

            rate_limiter_shared_config = RateLimiterSharedConfig(
                bucket=bucket_name,
                job_run_id=job_run_id
            )

            rate_limiter_aggregator = RateLimiterAggregator(shared_config=rate_limiter_shared_config)

            monitor_options = get_dynamodb_throughput_configs(parsed_args, DYNAMO_DB_TABLE_NAME, modes=["write"], format="monitor")
            try:
                records.toJSON().foreachPartition(
                    lambda partition: delete_partition(monitor_options, partition, rate_limiter_shared_config)
                )
            finally:
                rate_limiter_aggregator.shutdown()
            print(f"Deleted {count:,} items")

        else:
            raise ValueError("Logic error, don't know what action to take")
