import argparse
import logging as log

import utils
from utils.custom_parser import BulkArgumentParser

help_text = f"""
    Purpose of "update":
        Update items in a DynamoDB table, such as to add, delete, or modify an attribute.
        Performs parallel update of items, based on the update expression keys returned by a Python script.
        The Python script gets handed in turn each item in the table and returns empty if no update is needed
        or the core update expression kwargs if an update is needed.
        Required --table parameter
        Required --generator parameter to specify a Python generator that acts on each Item; the script
                 must exist in the S3 bucket as prepared during the bootstrap. In the likely event you
                 want to use your own generator script, make sure the entity with bootstrap permissions
                 runs bootstrap and includes your generator. The generator scripts are found under the
                 update folder.

    Examples:
        bulk update --table tickets --generator backfillSeatPK
    """

def run(env_configs):
    glue_job_parent = utils.glue_job_arguments()
    environment_parent = utils.environment_arguments()

    # The Bulk Executor Action to be performed.
    parser = BulkArgumentParser("bulk update", help_text=help_text, parents=[glue_job_parent, environment_parent])
    parser.add_argument('verb', help=argparse.SUPPRESS)
    parser.add_argument('--table', required=True, type=str, help='Table name')
    parser.add_argument('--generator', required=True, type=str, help='Generator file')
    parser.add_argument('--generatorfunctionname', type=str, default=argparse.SUPPRESS, help=argparse.SUPPRESS) # Don't advertise this one
    args = parser.parse_args()

    #result = {k: v for k, v in vars(args).items() if v is not None}
    result = args.__dict__

    utils.validate_tables(env_configs, parser, result['table'], pitr_enabled=True)

    log.info(f"Running action '{result['verb']}' with arguments: {result}")

    # If all checks pass
    return True, result
