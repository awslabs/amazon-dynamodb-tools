import argparse
import logging as log

import utils
from utils.custom_parser import BulkArgumentParser

help_text=f"""
    Purpose of "find":
        Find items in a DynamoDB table
        Required --table parameter
        Optional --where parameter to specify a match criteria, using Spark SQL syntax
        Optional --orderby parameter to specify a sort attribute, with optional asc/desc suffix
        Optional --limit parameter to limit the number of items processed
        Saves full output to S3 and prints the top few items to console

    Examples:
        bulk find --table products
        bulk find --table users --where "age > 21"
        bulk find --table orders --where "status = 'pending'"
    """

def validate_orderby(parser, values):
    if len(values) == 1:
        return values[0]
    elif len(values) == 2 and values[1] in ['asc', 'desc']:
        return ' '.join(values)
    elif len(values) == 2:
        parser.error('--orderby direction must be \'asc\' or \'desc\'')
    else:
        parser.error('--orderby takes at most two arguments')

def run(env_configs, verb="find", help_text=help_text):
    glue_job_parent = utils.glue_job_arguments()
    environment_parent = utils.environment_arguments()

    # The Bulk Executor Action to be performed
    parser = BulkArgumentParser(f"bulk {verb}", help_text=help_text, parents=[glue_job_parent, environment_parent])
    parser.add_argument('verb', help=argparse.SUPPRESS)
    parser.add_argument('--table', required=True, type=str, help='Table name')
    parser.add_argument('--where', type=str, default=argparse.SUPPRESS, help='Where clause')
    parser.add_argument('--orderby', nargs='+', type=str, default=argparse.SUPPRESS, metavar=('COLUMN', 'DIRECTION'), help='Order by clause (e.g., "column" or "column asc/desc")')
    parser.add_argument('--limit', type=int, default=argparse.SUPPRESS, help='Limit number')
    args = parser.parse_args()

    if hasattr(args, 'orderby'):
        args.orderby = validate_orderby(parser, args.orderby)

    #result = {k: v for k, v in vars(args).items() if v is not None}
    result = args.__dict__

    utils.validate_tables(env_configs, parser, result['table'], pitr_enabled=result['verb'] == 'delete')

    log.info(f"Running action '{result['verb']}' with arguments: {result}")


    # If all checks pass
    return True, result
