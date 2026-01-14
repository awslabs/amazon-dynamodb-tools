import argparse
import json
import os
import re
import sys

import boto3
import botocore
from botocore.exceptions import ClientError
from clients import Clients
# project files
from utils.logger import ColorCodes, log

SUPPORTED_EXECUTION_CLASSES = ['STANDARD', 'FLEX']
SUPPORTED_WORKER_TYPES = ['G.1X', 'G.2X', 'G.4X', 'G.8X', 'G.12X', 'G.16X', 'R.1X', 'R.2X', 'R.4X', 'R.8X']

LOG_PATTERN_IGNORE_LIST = [
    r"Running autoDebugger shutdown hook.",
    r"Error while invoking RpcHandler#receive() for one-way message.",
]

# Intentional nuanced configs:
# - PascaleCase Keys
# - Suffix symbols
UNHEALTHY_STATE_LOG_MESSAGE_KEYS = [
    "AccessDeniedException:",
    "ModuleNotFoundError:",
    "OutOfMemoryError:",
    "ProvisionedThroughputExceededException:"
]

STD_ERROR_MESSAGE_KEYS = [ # Lowercase Keys Intentional
    " ERROR ", # Surrounding spaces intentional.
    "exception",
    "timeout"
]

# Intentional nuanced configs:
# - Lowercase Keys
# - Suffix symbols
CONFIG_LOG_MESSAGE_KEYS = [
    "timeout=",
    "arguments:",
]

WARN_LOG_MESSAGE_KEYS = [
    " WARN ", # Surrounding spaces intentional.
]

_ENV_OR_SCRIPT_KEYS = set([
    'XMaxWriteRate',
    'XMaxReadRate'
])

# We can perhaps move this somewhere else later
def warn(s):
    print(ColorCodes.YELLOW + "[WARN] " + s + ColorCodes.RESET, file=sys.stderr)

def convert_client_dict_to_script_args(client_dict):
    script_args = []
    for key, value in client_dict.items():
        if not key.startswith('X') or key in _ENV_OR_SCRIPT_KEYS:
            script_args.extend([f'--{key}', str(value)])
    return script_args

def get_args_from_processed_args(processed_args):
    return {k: v for k, v in processed_args.items() if k.startswith('X')}

def filter_none_or_false_values(args):
    return {k: v for k, v in args.items() if v}

def validate_timeout(x):
    try:
        value = int(x)
        if 1 <= value <= 10080:
            return value
        else:
            raise argparse.ArgumentTypeError(f"Timeout must be between 1 and 10080 minutes (7 days)")
    except ValueError:
        raise argparse.ArgumentTypeError(f"Timeout must be an integer")


# The defaults stated below should perhaps be dynamic from constants.py
def parse_action():
    parser = argparse.ArgumentParser(
        description="The Bulk Executor action argument.",
        add_help=False  # Allow individual verbs to provide their own help functionality (ex. `bulk find --help`)
    )

    # The Bulk Executor Action to be performed.
    parser.add_argument("XAction", type=str, help="The Action to perform.", nargs='?')

    return parser.parse_known_args()

def glue_job_arguments():
    parser = argparse.ArgumentParser(description="The Bulk Executor Glue Job arguments.", add_help=False)

    parser.add_argument("--XExecutionClass", type=str, default=argparse.SUPPRESS, help="Set to STANDARD (default) or FLEX (lower DPU cost by using spare capacity, may take longer).", choices=SUPPORTED_EXECUTION_CLASSES)
    parser.add_argument("--XTimeout", type=validate_timeout, default=argparse.SUPPRESS, help="The Glue Job timeout (in minutes). Must be between 1 and 10080 minutes (7 days is the max allowed timeout). Default is 60 minutes.")
    parser.add_argument("--XNumberOfWorkers", type=int, default=argparse.SUPPRESS, help="The number of Glue workers (default 220).")
    parser.add_argument("--XWorkerType", type=str, default=argparse.SUPPRESS, help="The Glue worker type. ex. G.1X", choices=SUPPORTED_WORKER_TYPES)
    parser.add_argument("--XWaitForDPU", action='store_true', default=argparse.SUPPRESS, help="Causes execution to wait 40 seconds at the end of execution for DPU metrics to be available.")

    # DynamoDB Read/Write Overrides
    parser.add_argument("--XMaxWriteRate", type=int, default=argparse.SUPPRESS, help="Maximum amount of write units to consume per second.")
    parser.add_argument("--XMaxReadRate", type=int, default=argparse.SUPPRESS, help="Maximum amount of read units to consume per second.")
    return parser

def environment_arguments():
    parser = argparse.ArgumentParser(description="The Bulk Executor environment variable arguments.", add_help=False)

    # Environment Args
    parser.add_argument("--XAccount", type=str, default=argparse.SUPPRESS, help="The AWS Account for the Glue Job. Only needed if different from pre-configured environment variables.")
    parser.add_argument("--XRegion", type=str, default=argparse.SUPPRESS, help="The AWS Region for the Glue Job. Only needed if different from pre-configured environment variables.")

    # Bulk Executor Tooling Args
    parser.add_argument("--XDebug", action='store_true', default=argparse.SUPPRESS, help=argparse.SUPPRESS) # Enable debug logs.
    parser.add_argument("--XDev", action='store_true', default=argparse.SUPPRESS, help=argparse.SUPPRESS) # Enable development mode. Useful for quickly pushing updated script code into a bootstrapped environment without doing a full bootstrap.
    return parser

# CLI Arguments will always override environment configurations
def parse_environment_arguments():
    return environment_arguments().parse_known_args()

def _get_table_info(dynamodb_client, table_name):
    """Get table information or return None if table doesn't exist"""
    try:
        return dynamodb_client.describe_table(TableName=table_name)['Table']
    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceNotFoundException':
            return None
        raise

def validate_tables(env_configs, parser, *tables, index=None, pitr_enabled=False, schemas_match=False):
    # This is kinda messy
    aws_region = env_configs.aws_region
    clients = Clients(aws_region)
    dynamodb_client = clients.dynamodb_client

    reference_schema = None
    reference_table = None

    for table_name in tables:
        # Get all table info upfront
        table_info = _get_table_info(dynamodb_client, table_name)

        # Check table exists
        if table_info is None:
            sys.exit(f"Table '{table_name}' does not exist")

        # Check index exists (if specified)
        if index:
            gsis = table_info.get('GlobalSecondaryIndexes', [])
            if not any(gsi['IndexName'] == index for gsi in gsis):
                sys.exit(f"Index '{index}' does not exist in table '{table_name}'")

        # Check PITR (if required)
        if pitr_enabled:
            try:
                response = dynamodb_client.describe_continuous_backups(TableName=table_name)
                pitr_status = response['ContinuousBackupsDescription']['PointInTimeRecoveryDescription']['PointInTimeRecoveryStatus']
                if pitr_status != 'ENABLED':
                    sys.exit(f"For safety, point in time recovery (PITR) must be enabled for table '{table_name}' before performing bulk mutations against it")
            except ClientError:
                sys.exit(f"Could not check PITR status for table '{table_name}'")

        # Check all schemas match (if required)
        if schemas_match:

            # We only care about definitions for the keys, not indexes
            def _extract_key_attributes(schema):
                return {
                    entry["AttributeName"]: entry["AttributeType"]
                    for entry in schema.get("AttributeDefinitions", [])
                    if entry["AttributeName"] in {k["AttributeName"] for k in schema.get("KeySchema", [])}
                }

            def _index_map(indexes):
                return {index["IndexName"]: index for index in indexes}

            current_key_schema = sorted(table_info.get("KeySchema", []), key=lambda x: x["AttributeName"])
            current_key_attributes = _extract_key_attributes(table_info)

            current_gsis = _index_map(table_info.get("GlobalSecondaryIndexes", []))
            current_lsis = _index_map(table_info.get("LocalSecondaryIndexes", []))

            if reference_schema is None:
                reference_schema = {
                    "KeySchema": current_key_schema,
                    "KeyAttributes": current_key_attributes,
                    "GSIs": current_gsis,
                    "LSIs": current_lsis,
                }
                reference_table = table_name
            else:
                if (
                    reference_schema["KeySchema"] != current_key_schema
                    or reference_schema["KeyAttributes"] != current_key_attributes
                ):
                    sys.exit(
                        f"Primary key schema mismatch between '{reference_table}' and '{table_name}'\n\n"
                        f"{reference_table}:\n{json.dumps(reference_schema, indent=2)}\n\n"
                        f"{table_name}:\n{json.dumps({'KeySchema': current_key_schema, 'KeyAttributes': current_key_attributes}, indent=2)}"
                    )

                # Check GSIs
                gsi_a = reference_schema["GSIs"]
                gsi_b = current_gsis

                missing_in_b = set(gsi_a) - set(gsi_b)
                missing_in_a = set(gsi_b) - set(gsi_a)
                shared = set(gsi_a) & set(gsi_b)

                for name in missing_in_b:
                    warn(f"GSI '{name}' is in '{reference_table}' but missing from '{table_name}'")
                for name in missing_in_a:
                    warn(f"GSI '{name}' is in '{table_name}' but missing from '{reference_table}'")
                for name in shared:
                    if gsi_a[name] != gsi_b[name]:
                        warn(f"GSI '{name}' differs between '{reference_table}' and '{table_name}'")

                # Check LSIs
                lsi_a = reference_schema["LSIs"]
                lsi_b = current_lsis

                missing_in_b = set(lsi_a) - set(lsi_b)
                missing_in_a = set(lsi_b) - set(lsi_a)
                shared = set(lsi_a) & set(lsi_b)

                for name in missing_in_b:
                    warn(f"LSI '{name}' is in '{reference_table}' but missing from '{table_name}'")
                for name in missing_in_a:
                    warn(f"LSI '{name}' is in '{table_name}' but missing from '{reference_table}'")
                for name in shared:
                    if lsi_a[name] != lsi_b[name]:
                        warn(f"LSI '{name}' differs between '{reference_table}' and '{table_name}'")

