import argparse
import logging as log

import utils
from infrastructure import TeardownInfrastructure
from utils.custom_parser import BulkArgumentParser

help_text = f"""
    Purpose of "teardown":
        Attempt to delete the S3 bucket holding the execution scripts. The bucket will follow the naming pattern aws-glue-bulk-dynamodb-<aws_region>-<aws_account_id>-<random>.
            Note if the bucket contains additional objects outside of the '<root>/server' path, the bucket will be left intact for manual review and deletion within the AWS Console.
        Delete the service role for the Glue execution, unless an existing custom role was specified.
        Delete the Bulk DynamoDB Glue job.

        See documentation for additional details.

    Examples:
        bulk teardown
    """

def run(env_configs):
    environment_parent = utils.environment_arguments()

    # The Bulk Executor Action to be performed.
    parser = BulkArgumentParser("bulk teardown", help_text=help_text, parents=[environment_parent])
    parser.add_argument('verb', help=argparse.SUPPRESS)

    #result = {k: v for k, v in vars(args).items() if v is not None}
    result = parser.parse_args().__dict__

    log.info(f"Running action '{result['verb']}' with arguments: {result}")

    TeardownInfrastructure(env_configs).teardown()
    return False, {}
