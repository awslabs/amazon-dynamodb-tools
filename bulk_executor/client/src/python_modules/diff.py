import argparse
import logging as log

import utils
from utils.custom_parser import BulkArgumentParser

help_text = f"""
    Purpose of "diff":
        Compare items in two DynamoDB tables
        Required --table parameter
        Required --table2 parameter
        Optional --format parameter to specify 'full' or 'keys' view. Default is keys.
        Optional --s3 flag to specify if S3 should be used to store the diff output
        Optional --sample-fraction fraction of the table to compare, 1.0 meaning full

    Examples:
        bulk diff --table tableBeforeRestore --table2 tableAfterRestore
    """

def positive_fraction(value):
    f = float(value)
    if f <= 0.0 or f > 1.0:
        raise argparse.ArgumentTypeError('--sample-fraction must be > 0 and ≤ 1.0')
    return f

def run(env_configs):
    glue_job_parent = utils.glue_job_arguments()
    environment_parent = utils.environment_arguments()

    # The Bulk Executor Action to be performed.
    parser = BulkArgumentParser("bulk diff", help_text=help_text, parents=[glue_job_parent, environment_parent])
    parser.add_argument('verb', help=argparse.SUPPRESS)
    parser.add_argument('--table', required=True, type=str, help='First table name')
    parser.add_argument('--table2', required=True, type=str, help='Second table name')
    parser.add_argument('--format', type=str, default=argparse.SUPPRESS, choices=['full', 'keys'], help='Output format (full or keys)')
    parser.add_argument('--s3', dest='s3', action='store_true', help='Store the diff output in S3')
    parser.add_argument('--sample-fraction', type=positive_fraction, default=1.0, help='Fraction of segments to diff (e.g., 0.5 for 50%% of segments), must be > 0 and ≤ 1.0, default 1.0')
    args = parser.parse_args()

    #result = {k: v for k, v in vars(args).items() if v is not None}
    result = args.__dict__

    utils.validate_tables(env_configs, parser, result['table'], result['table2'], schemas_match=True)

    log.info(f"Running action '{result['verb']}' with arguments: {result}")

    # If all checks pass
    return True, result
