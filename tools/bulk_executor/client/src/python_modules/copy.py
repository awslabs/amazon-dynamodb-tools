import argparse
import logging as log

import utils
from utils.custom_parser import BulkArgumentParser

help_text = f"""
    Purpose of "copy":
        Copy items from one DynamoDB table to another (already-existing) table
        Required --source parameter
        Required --target parameter

    Examples:
        bulk copy --source tableOne --target tableTwo
    """

def run(env_configs):
    glue_job_parent = utils.glue_job_arguments()
    environment_parent = utils.environment_arguments()

    # The Bulk Executor Action to be performed.
    parser = BulkArgumentParser("bulk copy", help_text=help_text, parents=[glue_job_parent, environment_parent])
    parser.add_argument('verb', help=argparse.SUPPRESS)
    parser.add_argument('--source', required=True, type=str, help='Source table name')
    parser.add_argument('--target', required=True, type=str, help='Target table name')
    args = parser.parse_args()

    #result = {k: v for k, v in vars(args).items() if v is not None}
    result = args.__dict__

    if result["source"] == result["target"]:
        parser.error("--source and --target must be different DynamoDB tables")

    utils.validate_tables(env_configs, parser, result['source'], result['target'], pitr_enabled=True, schemas_match=True)

    log.info(f"Running action '{result['verb']}' with arguments: {result}")

    # If all checks pass
    return True, result
