import argparse
import logging as log

import utils
from utils.custom_parser import BulkArgumentParser

help_text = f"""
    Purpose of "import":
        Imports a full export from S3 to an existing DynamoDB table.
        Required --table parameter to specify the name of destination DynamoDB table.
        Required --s3-source-bucket parameter to specify the name of the S3 bucket where the export resides.
        Required --s3-source-bucket-export-id the export ID (prefix) within the S3 bucket which needs to be imported.
        Required --import-type the type of import to be run (full-only, incremental-only, full-incremental).
        Optional --s3-source-bucket-prefix parameter to specify the prefix of the S3 bucket where the export resides.
        Optional --filter parameter to specify the module/file in which the custom filter logic resides.
        Optional --filterfunctionname parameter to specify the name of the function to use for filtering.

    Examples:
        Assuming you have your exports in s3://exported-data/prod/AWSDynamoDB/01716790307109-5f9d6aaa
        bulk import --table users --s3-source-bucket exported-data --s3-source-bucket-export-id 01716790307109-5f9d6aaa [--s3-source-bucket-prefix prod] [--filter example] [--filterfunctionname filter_item]
    """

def run(env_configs):
    glue_job_parent = utils.glue_job_arguments()
    environment_parent = utils.environment_arguments()

    # The Bulk Executor Action to be performed.
    parser = BulkArgumentParser("bulk import", help_text=help_text, parents=[glue_job_parent, environment_parent])
    parser.add_argument('verb', help=argparse.SUPPRESS)
    parser.add_argument('--table', required=True, type=str, help='Table name')
    parser.add_argument('--s3-source-bucket', required=True, type=str, help='S3 bucket name where DynamoDB export resides')
    parser.add_argument('--s3-source-bucket-export-id', required=True, type=str, help='The export ID')
    parser.add_argument('--import-type', required=True, type=str, choices=['full-only', 'incremental-only', 'full-incremental'], help='The type of import to be run')
    parser.add_argument('--s3-source-bucket-prefix', required=False, type=str, help='S3 bucket prefix where DynamoDB export resides')  # Don't advertise this one
    parser.add_argument('--filter', required=False, type=str, help='Specify the module/file in which the custom filter logic resides')  # Don't advertise this one
    parser.add_argument('--filterfunctionname', required=False, type=str, help='Specify the name of the function to use for filtering')  # Don't advertise this one

    # TODO: We might need to introduce some margin here
    # default is _now_, date/time when the execution occurs
    parser.add_argument('--import-upto', required=False, type=str, help='Import incremental imports upto this date/time value (format: 2024-05-27T06:26:47.109Z)')  # Don't advertise this one

    args = parser.parse_args()

    result = args.__dict__

    utils.validate_tables(env_configs, parser, result['table'])

    log.info(f"Running action '{result['verb']}' with arguments: {result}")

    return True, result