import argparse
import logging as log

import utils
from utils.custom_parser import BulkArgumentParser

ALLOWED_ARGUMENTS_CSV_LOAD = {'verb', 'table','format','s3_path','removeEmptyStringAttributes','separator','escaper','quoteChar','multiLine','withHeader', 'mappings', 'skipFirst'}
ALLOWED_ARGUMENTS_JSON_LOAD = {'verb', 'table','format','s3_path','remove_empty_string_attributes','jsonPath','multiLine'}
ALLOWED_ARGUMENTS_PARQUET_LOAD = {'verb', 'table','format','s3_path','remove_empty_string_attributes','useGlueParquetWriter','compression','blockSize','pageSize'}
ALLOWED_VALUES_FORMAT = { 'csv', 'json', 'parquet' }
ALLOWED_VALUES_COMPRESSION = { 'uncompressed', 'snappy', 'gzip', 'lzo' }

help_text = f"""
    Purpose of "load":
        Load items in a DynamoDB table using the DynamoDB Connector
        Parameters:
        Required --table parameter
        Required --format parameter: 'csv', 'json' or 'parquet'
        Optional --removeEmptyStringAttributes
        IF format=csv
        Optional --separator parameter
        Optional --escaper parameter
        Optional --quoteChar parameter
        Optional --multiLine parameter
        Optional --withHeader parameter
        Optional --mappings parameter
        Optional --skipFirst parameter
        IF format=json
        Optional --jsonPath parameter
        Optional --multiLine parameter
        TODO
        IF format=parquet
        Optional --compression parameter
        Optional --blockSize parameter
        Optional --pageSize parameter

    Examples:
        # Import data from a csv file
        bulk load --table orders --format csv

    See parameter explanations:
            CSV: https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-format-csv-home.html
           JSON: https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-format-json-home.html
        Parquet: https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-format-parquet-home.html
    """

def single_char(value):
    if len(value) != 1:
        raise argparse.ArgumentTypeError(f"Expected a single character, got '{value}'")
    return value

def run(env_configs):
    glue_job_parent = utils.glue_job_arguments()
    environment_parent = utils.environment_arguments()

    # The Bulk Executor Action to be performed.
    parser = BulkArgumentParser("bulk load", help_text=help_text, parents=[glue_job_parent, environment_parent])
    parser.add_argument('verb', help=argparse.SUPPRESS)
    parser.add_argument('--table', required=True, type=str, help='Table name')
    parser.add_argument('--format', required=True, type=str, choices=['csv', 'json', 'parquet'], help='Supported input file formats: csv, json or parquet')
    parser.add_argument('--s3-path', required=True, type=str, help='Amazon S3 path in format like s3://aws-glue-target/temp')
    parser.add_argument('--removeEmptyStringAttributes', action='store_true', default=argparse.SUPPRESS, help='Indicates that attributes with empty values, as can occur especially with CSV files when a value is missing, should not be written as attributes on the loaded item')
    # OPTIONAL ARGUMENTS for CSV import
    parser.add_argument('--separator', type=str, default=argparse.SUPPRESS, help='Separator char used in CSV files. The default is a comma, but any other character can be specified.')
    parser.add_argument('--escaper', type=single_char, default=argparse.SUPPRESS, help='Specifies a character to use for escaping. If enabled, the character that immediately follows is used as-is, except for a small set of well-known escapes (\n, \r, \t, and \0).')
    parser.add_argument('--quoteChar', type=single_char, default=argparse.SUPPRESS, help='Quote char used in CSV files. The default is a double quote. Set this to -1 to turn off quoting entirely.')
    parser.add_argument('--multiLine', action='store_true', default=argparse.SUPPRESS, help='Specifies whether a single record can span multiple lines. his can occur when a field contains a quoted new-line character. You must set this option to True if any record spans multiple lines. Enabling multiLine might decrease performance because it requires more cautious file-splitting while parsing.')
    parser.add_argument('--withHeader', action='store_true', dest='withHeader',  default=argparse.SUPPRESS, help='Specifies to treat the first line as a header (Default)')
    parser.add_argument('--withoutHeader', action='store_false', dest='withHeader', default=argparse.SUPPRESS, help='Specifies not to treat the first line as a header.')
    parser.add_argument('--mappings', default=argparse.SUPPRESS, help='Amazon S3 path in format like s3://aws-glue-target/mappings.json that points to a file describing the mappings between the auto generated column names (col0, col1, col2, etc) and it allows to specify a different type than String')
    parser.add_argument('--skipFirst', action='store_true', default=argparse.SUPPRESS, help='Specifies whether to skip the first data line. Default is FALSE')
    # OPTIONAL ARGUMENTS for JSON import
    parser.add_argument('--jsonPath', type=str, default=argparse.SUPPRESS, help='A JsonPath expression that identifies an object to be read into records. This is particularly useful when a file contains records nested inside an outer array. For example, the following JsonPath expression targets the id field of a JSON object: `format="json", format_options={"jsonPath": "$.id"}`')
    parser.add_argument('--multiline', action='store_true', default=argparse.SUPPRESS, help='Boolean value that specifies whether a single record can span multiple lines. This can occur when a field contains a quoted new-line character. You must set this option to "true" if any record spans multiple lines.')
    # OPTIONAL ARGUMENTS for PARQUET import
    parser.add_argument('--compression', type=str, default=argparse.SUPPRESS, choices=['uncompressed', 'snappy', 'gzip', 'lzo'], help='Default is "snappy". Values are fully compatible with org.apache.parquet.hadoop.metadata.CompressionCodecName. Values: "uncompressed", "snappy", "gzip", and "lzo"')
    parser.add_argument('--blockSize', type=int, default=argparse.SUPPRESS, help='Default is "134217728". The default value is equal to 128 MB. Specifies the size in bytes of a row group being buffered in memory. You use this for tuning performance. Size should divide exactly into a number of megabytes.')
    parser.add_argument('--pageSize', type=int, default=argparse.SUPPRESS, help='Default is "1048576". The default value is equal to 1 MB. Specifies the size in bytes of a page. You use this for tuning performance. A page is the smallest unit that must be read fully to access a single record.')
    args = parser.parse_args()

    format_type = getattr(args, 'format')

    if format_type == "csv":
        for arg in vars(args):
            log.info(f"arg: {arg} with value {getattr(args, arg)}")

            if arg not in ALLOWED_ARGUMENTS_CSV_LOAD and not arg.startswith("X"):
                parser.error(f'argument [{arg}] is not allowed for load commands using CSV format')
            elif arg == "withHeader":
                if getattr(args, "withHeader") == False and "mappings" not in vars(args):
                    parser.error(f'argument [withoutHeader] is only allowed if argument [mappings] is also specified')
            elif arg == "mappings":
                if  getattr(args, "withHeader", True) == True:
                    parser.error(f'argument [{arg}] is only allowed when argument [withoutHeader] is specified')
    elif format_type == "json":
        for arg in vars(args):
            if arg not in ALLOWED_ARGUMENTS_JSON_LOAD and not arg.startswith("X"):
                parser.error(f'argument [{arg}] is not allowed for load commands using JSON format')
    elif format_type == "parquet":
        for arg in vars(args):
            if arg not in ALLOWED_ARGUMENTS_PARQUET_LOAD and not arg.startswith("X"):
                parser.error(f'argument [{arg}] is not allowed for load commands using PARQUET format')
    else:
        parser.error('--format should be "csv", "json" or "parquet"')

    result = args.__dict__
    utils.validate_tables(env_configs, parser, result['table'])

    log.info(f"Running action '{result['verb']}' with arguments: {result}")

    # If all checks pass
    return True, result
