import argparse
import logging as log

import utils
from utils.custom_parser import BulkArgumentParser

help_text = f"""
    Purpose of "fill":
        Fill synthetic items into a DynamoDB table
        Required --table parameter
        Required --numitems parameter to specify how many items to fill
        Required --generator parameter to specify a Python module to generate the synthetic items

    Examples:
        bulk fill --table users --generator fakeusers --numitems 100000000
    """

def run(env_configs):
    glue_job_parent = utils.glue_job_arguments()
    environment_parent = utils.environment_arguments()

    # The Bulk Executor Action to be performed.
    parser = BulkArgumentParser("bulk fill", help_text=help_text, parents=[glue_job_parent, environment_parent])
    parser.add_argument('verb', help=argparse.SUPPRESS)
    parser.add_argument('--table', required=True, type=str, help='Table name')
    parser.add_argument('--numitems', required=True, type=int, help='Number of items to insert')
    parser.add_argument('--generator', required=True, type=str, help='Generator file')
    parser.add_argument('--generatorfunctionname', type=str, default=argparse.SUPPRESS, help=argparse.SUPPRESS) # Don't advertise this one
    args = parser.parse_args()

    #result = {k: v for k, v in vars(args).items() if v is not None}
    result = args.__dict__

    utils.validate_tables(env_configs, parser, result['table'], pitr_enabled=True)

    log.info(f"Running action '{result['verb']}' with arguments: {result}")

    # If all checks pass
    return True, result
