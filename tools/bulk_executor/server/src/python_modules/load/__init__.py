import boto3
import json
import math
import re
import sys
from awsglue.transforms import Filter, Map
from botocore.exceptions import ClientError
from python_modules.shared.bulk_executor_error import BulkExecutorError
from python_modules.shared.errors import *
from python_modules.shared.logger import log
from python_modules.shared.pricing import PricingUtility
from python_modules.shared.table_info import get_dynamodb_throughput_configs
from python_modules.shared.table_info import get_and_print_dynamodb_table_info
from python_modules.shared.glue_connector import write_dynamodb_dataframe


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
                raise BulkExecutorError(f"Invalid integer for {arg_name}: {parsed_args.get(arg_name)}") from None

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
        raise BulkExecutorError(f"Unexpected format {fmt!r}")

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
        raise BulkExecutorError(f"The S3 path '{s3_path}' doesn't exist or is not accessible")

    # Inside the same handler as the count below: some formats fail here instead. Parquet
    # reads its footer while the frame is being created, so `--format parquet` at a CSV
    # file raises from read_data, while a JSON or CSV mismatch only surfaces at count().
    # Measured before this was wrapped: the Parquet case reported as an unexpected failure
    # ("Py4JJavaError: ... is not a Parquet file") rather than as the user's own mistake.
    count = 0
    try:
        dynamicFrame = read_data(glue_context, s3_path, parsed_args)
        count = dynamicFrame.count()
        if count == 0:
            # Zero rows means one of two different things, and they deserve different
            # outcomes (#340). An empty drop is a legitimate input -- the export pipeline
            # treats a 0-item export the same way -- so it succeeds with a warning rather
            # than an ERROR line above "Job completed successfully". A source that holds
            # bytes and yields nothing is the reader failing to understand it, which is the
            # user's mistake and worth failing on: otherwise a mistyped --format is
            # indistinguishable from an empty file, and Spark's JSON reader returns no rows
            # where its Parquet reader would have raised.
            source_bytes = s3_source_bytes(s3_path)
            if source_bytes:
                raise BulkExecutorError(
                    f"Read 0 items from '{s3_path}', but it holds {source_bytes:,} bytes. "
                    f"The data is probably not {parsed_args.get('format')!r} -- check "
                    f"--format. (A header-only CSV also reads as 0 items.)")
            log.warning(
                f"Read 0 items from '{s3_path}': the source is empty, so nothing was "
                f"loaded. Check the path if that is unexpected.")
            return
        log.info(f"\nPreparing to load {count} items")
        log.info("Schema is:")
        dynamicFrame.printSchema()

    except BulkExecutorError:
        # Already phrased -- the zero-row branch above raises from inside this try, and
        # wrapping it again produced "Could not read the source ... as 'json': Read 0 items
        # from ..., but it holds 29 bytes ..." in a live run.
        raise
    except Exception as e:
        # This is where Spark actually reads the source, so it fires on the everyday
        # mistakes: --format json pointed at CSV, malformed JSON, an unreadable Parquet
        # file. (A path that does not exist is caught earlier, before the job starts.)
        raise BulkExecutorError(
            f"Could not read the source at '{s3_path}' as {parsed_args.get('format')!r}: "
            f"{get_error_message(e)}") from None

    if parsed_args.get('removeEmptyStringAttributes') is not None:
        log.debug(f"removeEmptyStringAttributes parameter was provided")
        dynamicFrame = Map.apply(frame = dynamicFrame, f = remove_empty_fields)

    try:
        session = boto3.Session()
        print_dynamodb_table_info(session, table_name, count, check_dynamic_frame_avg_size(dynamicFrame))

        throughput = get_dynamodb_throughput_configs(
            parsed_args, table_name, modes=["write"], format="connector")
        write_rate = throughput.get("dynamodb.throughput.write")
        write_rate = int(write_rate) if write_rate is not None else None

        df = dynamicFrame.repartition(100).toDF()
        write_dynamodb_dataframe(
            glue_context, df, table_name, parsed_args, write_rate=write_rate)
        log.info(f"Wrote {count} items to '{table_name}'")
    except Exception as e:
        # The connector write: a read-only role or persistent throttling lands here, and
        # get_error_message unwraps the Py4J stack to AWS's own sentence.
        raise BulkExecutorError(f"Error in writing to table: {get_error_message(e)}") from None

def _split_s3_uri(s3_uri):
    """Return (bucket, key) for an s3:// URI."""
    match = re.match(r"s3://([^/]+)/(.*)", s3_uri)
    if not match:
        raise BulkExecutorError(f"Invalid S3 URI format: {s3_uri}. Expected format: s3://bucket-name/key")
    return match.group(1), match.group(2)


def s3_source_bytes(s3_uri):
    """Total bytes behind the path: one object, or every object under a prefix.

    Only called when a read produced no rows, so the happy path pays nothing for it. The
    number separates "there was nothing to load" from "the reader did not understand what
    is there", which otherwise look identical (#340).
    """
    bucket_name, key = _split_s3_uri(s3_uri)
    s3 = boto3.client('s3')
    try:
        return s3.head_object(Bucket=bucket_name, Key=key)['ContentLength']
    except ClientError as e:
        if e.response['Error']['Code'] != '404':
            raise

    total = 0
    for page in s3.get_paginator('list_objects_v2').paginate(Bucket=bucket_name, Prefix=key):
        total += sum(obj['Size'] for obj in page.get('Contents', []))
    return total


def check_s3_file_exists(s3_uri):
    """
    Check if a specific file exists in S3 using an S3 URI

    Args:
        s3_uri (str): The S3 URI in the format s3://bucket-name/key/path

    Returns:
        bool: True if the file exists, False otherwise
    """
    bucket_name, key = _split_s3_uri(s3_uri)

    # Initialize S3 client
    s3 = boto3.client('s3')

    try:
        s3.head_object(Bucket=bucket_name, Key=key)
        return True
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == '404':
            # Check if it's a prefix containing objects
            resp = s3.list_objects_v2(Bucket=bucket_name, Prefix=key, MaxKeys=1)
            return resp.get('KeyCount', 0) > 0
        elif error_code in ('403', 'AccessDenied'):
            raise BulkExecutorError(
                f"Access denied to S3 path 's3://{bucket_name}/{key}'. "
                f"Check that your IAM role has s3:GetObject permission on this bucket."
            ) from None
        else:
            raise BulkExecutorError(
                f"S3 error checking 's3://{bucket_name}/{key}': {e.response['Error'].get('Message', str(e))}"
            ) from None

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
