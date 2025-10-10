import boto3
import json
import math
import re
import sys
from awsglue.transforms import Filter, Map
from botocore.exceptions import ClientError
from python_modules.shared.errors import *
from python_modules.shared.logger import log
from python_modules.shared.pricing import PricingUtility
from python_modules.shared.table_info import get_dynamodb_throughput_configs
from python_modules.shared.table_info import get_and_print_dynamodb_table_info


def read_data(glueContext, path, parsed_args):
    format_options = {}

    def set_bool_option(arg_name, default = False):
        nonlocal format_options
        if default is not None:
            #print(f"Alternative default value for {arg_name} parameter was provided: {default}")
            format_options[arg_name] = default
        if parsed_args.get(arg_name) is not None:
            #print(f"{arg_name} parameter was provided")
            format_options[arg_name] = str(parsed_args.get(arg_name)).lower() == 'true'

    def set_str_option(arg_name):
        nonlocal format_options
        if parsed_args.get(arg_name) is not None:
            #print(f"{arg_name} parameter was provided")
            format_options[arg_name] = parsed_args.get(arg_name)

    def set_int_option(arg_name):
        nonlocal format_options
        if parsed_args.get(arg_name) is not None:
            #print(f"{arg_name} parameter was provided")
            try:
                format_options[arg_name] = int(parsed_args.get(arg_name))
            except ValueError:
                raise ValueError(f"Invalid integer for {arg_name}: {parsed_args.get(arg_name)}")

    # Parse the params based on the format
    fmt = parsed_args.get('format')
    if fmt == 'csv':
        set_bool_option('withHeader', True)
        set_bool_option('multiLine')
        set_bool_option('skipFirst')
        set_str_option('separator')
        set_str_option('escaper')
        set_str_option('quoteChar')
    elif fmt == 'json':
        set_bool_option('multiline')
    elif fmt == 'parquet':
        set_str_option('compression')
        set_int_option('blockSize')
        set_int_option('pageSize')
    else:
        raise ValueError(f"Unexpected format {fmt!r}")

    log.debug(f"About to create DynamicFrame from {fmt} at {path} using options {format_options}...")

    dynamicFrame = glueContext.create_dynamic_frame.from_options(
        connection_type="s3",
        connection_options={"paths": [path]},
        format=fmt,
        format_options=format_options,
    )

    mappings_s3_path = parsed_args.get('mappings')
    if mappings_s3_path is not None:
        dynamicFrame = dynamicFrame.apply_mapping(get_mappings_from_s3(mappings_s3_path))
    return dynamicFrame

def run(job, spark_context, glue_context, parsed_args):
    log.debug(f"parsed_args {parsed_args}")
    table_name = parsed_args.get('table')
    s3_path = parsed_args.get('s3_path')

    if not check_s3_file_exists(s3_path):
        log.error("The S3 uri provided doesn't exist / is not a file")
        return

    # Get throughput configuration (put this early so any output prints ahead of work)
    connection_options = get_dynamodb_throughput_configs(parsed_args, table_name, modes=["write"])
    connection_options["dynamodb.output.tableName"] = table_name

    dynamicFrame = read_data(glue_context, s3_path, parsed_args)

    count = 0
    try:
        count = dynamicFrame.count()
        if count == 0:
            log.error("No data found, please check your data source") # Should perhaps check that the path exists
            return
        log.info(f"\nPreparing to load {count} items")
        log.info("Schema is:")
        dynamicFrame.printSchema()

    except Exception as e:
        raise Exception(f"Failed to create DynamicFrame {e}")

    if parsed_args.get('removeEmptyStringAttributes') is not None:
        log.debug(f"removeEmptyStringAttributes parameter was provided")
        dynamicFrame = Map.apply(frame = dynamicFrame, f = remove_empty_fields)

    try:
        session = boto3.Session()
        print_dynamodb_table_info(session, table_name, count, check_dynamic_frame_avg_size(dynamicFrame))

        dynamicFrame = dynamicFrame.repartition(30)
        glue_context.write_dynamic_frame_from_options(
            frame=dynamicFrame,
            connection_type="dynamodb",
            connection_options=connection_options
        )
        log.info(f"Wrote {count} items to '{table_name}'")
    except Exception as e:
        raise Exception(f"Error in writing to table: {get_error_message(e)}") from None

def check_s3_file_exists(s3_uri):
    """
    Check if a specific file exists in S3 using an S3 URI

    Args:
        s3_uri (str): The S3 URI in the format s3://bucket-name/key/path

    Returns:
        bool: True if the file exists, False otherwise
    """
    # Parse the S3 URI to extract bucket name and key
    uri_pattern = r"s3://([^/]+)/(.*)"
    match = re.match(uri_pattern, s3_uri)

    if not match:
        raise ValueError(f"Invalid S3 URI format: {s3_uri}. Expected format: s3://bucket-name/key")

    bucket_name = match.group(1)
    key = match.group(2)

    # Initialize S3 client
    s3 = boto3.client('s3')

    try:
        s3.head_object(Bucket=bucket_name, Key=key)
        return True
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            return False
        else:
            # Something else went wrong
            raise

def get_mappings_from_s3(s3_uri):
    # Initialize S3 client
    s3_client = boto3.client('s3')

    try:
        # Parse the S3 URI (format: s3://bucket-name/key/path)
        if not s3_uri.startswith('s3://'):
            raise ValueError("S3 URI must start with 's3://'")

        path_parts = s3_uri[5:].split('/', 1)  # Remove 's3://' and split
        bucket_name = path_parts[0]
        key_name = path_parts[1] if len(path_parts) > 1 else ''

        # Get the object from S3
        response = s3_client.get_object(Bucket=bucket_name, Key=key_name)
        # Read the JSON content
        mappings_json = response['Body'].read().decode('utf-8')

        # Parse JSON to Python object
        mappings_data = json.loads(mappings_json)

        # Convert each mapping array to tuple
        return [tuple(mapping) for mapping in mappings_data['mappings']]
    except Exception as e:
        log.error(f"Error reading from S3: {str(e)}")
        return None

def remove_empty_fields(rec):
    cleaned = {k: v for k, v in rec.items() if v != ""}
    return cleaned

def check_dynamic_frame_avg_size(dynamicFrame):

    # Sample up to 100 items
    sample_frame = dynamicFrame.toDF().limit(100)
    # Convert DynamicFrame to DataFrame, then collect as list
    items = sample_frame.collect()

    total_size = 0
    item_count = 0

    for item in items:
        # Convert to dict then to JSON to simulate DynamoDB storage
        item_dict = item.asDict()
        # Calculate size in bytes
        item_size = sys.getsizeof(json.dumps(item_dict))
        total_size += item_size
        item_count += 1

    if item_count > 0:
        average_size = total_size / item_count
        return average_size

    else:
        # this code path should not happen because we check at the start if the source is not empty
        raise Exception("can't determine an average size without any items")

def print_dynamodb_table_info(session, table_name, num_items, avg_size):
    region_name = session.region_name
    table_info = get_and_print_dynamodb_table_info(table_name)

    avg_write_units_per_item = math.ceil(avg_size / 1024)
    write_units = num_items * avg_write_units_per_item

    pricing_utility = PricingUtility()
    ondemand_pricing = pricing_utility.get_on_demand_capacity_pricing(region_name)
    wru_cost = float(ondemand_pricing.get(table_info['write_pricing_category']))
    od_cost = write_units * wru_cost
    prov_cost = od_cost / 1.5 # very rough, look into updating this
    log.info("DynamoDB load costs depend on how many items are being written and the size of the items.")
    log.info(f"Here we assume the command will insert {num_items:,} items")
    log.info(f" with average size {int(avg_size):,} bytes (based on peeking at reader output);")
    log.info(f" each write incurs an average of {avg_write_units_per_item} write units")
    log.info(f"Write units required to do such a load (approx): {write_units:,}")
    log.info("This does not include costs for secondary indexes!")
    if table_info['billing_mode'] == "PROVISIONED":
        log.info(f"Approx DynamoDB cost for provisioned writes consuming {write_units:,} WCUs (using {region_name} prices): ${prov_cost:,.2f}")
    elif table_info['billing_mode'] == "PAY_PER_REQUEST":
        log.info(f"Approx DynamoDB cost for On-demand writes consuming {write_units:,} WRUs (using {region_name} prices): ${od_cost:,.2f}")
    print() # empty print intentional
