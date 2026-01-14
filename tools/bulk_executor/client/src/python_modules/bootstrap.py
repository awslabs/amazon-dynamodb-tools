import argparse
import logging as log

import utils
from infrastructure import BootstrapInfrastructure
from infrastructure.constants import READ_WRITE_ROLE_TYPES
from utils.custom_parser import BulkArgumentParser

help_text = f"""
    Purpose of "bootstrap":
        Create an S3 bucket to hold the execution scripts. The bucket will follow the naming pattern aws-glue-bulk-dynamodb-<aws_region>-<aws_account_id>-<random>.
        Upload the S3 scripts from the local folder to the right location within the S3 bucket. The Glue job will use these scripts for execution.
        Create a service role for the Glue execution, unless an existing service role is specified.
        Create a Glue job that uses the specified role and points at the specified scripts.

        See documentation for additional details.

    Examples:
        bulk bootstrap
    """

def validate_role(value):
    if value in READ_WRITE_ROLE_TYPES or value.startswith("AWSGlueServiceRole"):
        return value
    raise argparse.ArgumentTypeError(
        f"Invalid role: '{value}'. Must be READ-ONLY, READ-WRITE, or start with 'AWSGlueServiceRole'."
    )

def run(env_configs):
    glue_job_parent = utils.glue_job_arguments()
    environment_parent = utils.environment_arguments()

    # The Bulk Executor Action to be performed.
    parser = BulkArgumentParser("bulk bootstrap", help_text=help_text, parents=[glue_job_parent, environment_parent])
    parser.add_argument('verb', help=argparse.SUPPRESS)

    # Glue Job Bootstrap Args

    # Action Execution Role
    parser.add_argument("--XRole", type=validate_role, default=argparse.SUPPRESS, help="The AWS Role to use for executing the action. Specify a custom role name, or use the special keywords READ-ONLY or READ-WRITE to generate a managed role.")
    parser.add_argument("--XMaxConcurrentRuns", type=int, default=argparse.SUPPRESS, help="The maximum number of concurrent runs to allow for the single Glue Job (default is 20).")
    parser.add_argument("--XRetries", type=int, default=argparse.SUPPRESS, help="The max number of Glue Job retries. Defaults to zero to fail a misconfigured Glue Job quickly.")

    #result = {k: v for k, v in vars(args).items() if v is not None}
    result = parser.parse_args().__dict__

    log.info(f"Running action '{result['verb']}' with arguments: {result}")

    BootstrapInfrastructure(env_configs).bootstrap(result)
    return False, {}
