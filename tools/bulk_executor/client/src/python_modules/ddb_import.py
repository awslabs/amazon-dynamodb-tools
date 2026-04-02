import argparse
import logging as log

import utils
from utils.custom_parser import BulkArgumentParser

help_text = f"""
    Purpose of "import":
        Imports a full export from S3 to an existing DynamoDB table.
        Required --table parameter to specify the name of destination DynamoDB table.
        Required --s3-path to specify the name of the S3 path where the export resides.
        Optional --filter parameter to specify the module/file in which the custom filter logic resides.
        Optional --filterfunctionname parameter to specify the name of the function to use for filtering.

    Examples:
        Assuming you have your exports in s3://exported-data/prod/AWSDynamoDB/01716790307109-5f9d6aaa
        bulk import --table users --s3-path s3://bucket/prefix/AWSDynamoDB/01716790307109-5f9d6aaa [--filter example --filterfunctionname filter_item]
    """

def run(env_configs):
    glue_job_parent = utils.glue_job_arguments()
    environment_parent = utils.environment_arguments()

    # The Bulk Executor Action to be performed.
    parser = BulkArgumentParser("bulk import", help_text=help_text, parents=[glue_job_parent, environment_parent])
    parser.add_argument('verb', help=argparse.SUPPRESS)
    parser.add_argument('--table', required=True, type=str, help='Table name')
    parser.add_argument('--s3-path', required=True, type=str, help='Amazon S3 path in format like s3://bucket-name/prefix/AWSDynamoDB/01716790307109-5f9d6aaa')
    parser.add_argument('--filter', type=str, default=argparse.SUPPRESS, help=argparse.SUPPRESS)  # Don't advertise this one
    parser.add_argument('--filterfunctionname', type=str, default=argparse.SUPPRESS, help=argparse.SUPPRESS)  # Don't advertise this one

    args = parser.parse_args()

    result = args.__dict__

    utils.validate_tables(env_configs, parser, result['table'], pitr_enabled=True)

    log.info(f"Running action '{result['verb']}' with arguments: {result}")

    return True, result