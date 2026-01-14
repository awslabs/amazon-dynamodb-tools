import argparse
import json
import logging as log

import utils
from utils.custom_parser import BulkArgumentParser

help_text = f"""
    Purpose of "scancount":
        Count items in a DynamoDB table, using a scan instead of a DynamoDB Connector (so usually faster)
        Required --table parameter
        Optional --index parameter if scanning a secondary index
        Optional --filter-expression parameter to specify a push-down FilterExpression predicate
        Optional --expression-names parameter to specify the expression names used in the filter-expression
        Optional --expression-values parameter to specify the expression values used in the filter-expression

    Examples:
        # Count all items in a table
        bulk scancount --table orders

        # Count using a filter expression (uses DynamoDB FilterExpression syntax)
        bulk scancount --table audit --filter-expression "#touched > :touched" --expression-names '{{"#touched": "touched"}}' --expression-values '{{":touched":1742359403.0}}'
    """

def json_type(s):
    try:
        json.loads(s)
        return s
    except json.JSONDecodeError as e:
        raise argparse.ArgumentTypeError(f"JSON type parameter held invalid JSON: {e} | Parsed string: {s}")

def run(env_configs):
    glue_job_parent = utils.glue_job_arguments()
    environment_parent = utils.environment_arguments()

    # The Bulk Executor Action to be performed.
    parser = BulkArgumentParser("bulk scancount", help_text=help_text, parents=[glue_job_parent, environment_parent])
    parser.add_argument('verb', help=argparse.SUPPRESS)
    parser.add_argument('--table', required=True, type=str, help='Table name')
    parser.add_argument('--filter-expression', type=str, default=argparse.SUPPRESS, help='Filter expression to push down')
    parser.add_argument('--expression-names', type=json_type, default=argparse.SUPPRESS, help='Expression names to use')
    parser.add_argument('--expression-values', type=json_type, default=argparse.SUPPRESS, help='Expression values to use')
    parser.add_argument('--index', type=str, default=argparse.SUPPRESS, help='Index to use')
    args = parser.parse_args()

    if hasattr(args, "filter_expression"):
        if "#" in args.filter_expression and not hasattr(args, "expression_names"):
            parser.error("--filter-expression having name substitution requires --expression-names")
        if ":" in args.filter_expression and not hasattr(args, "expression_values"):
            parser.error("--filter-expression having value substitution requires --expression-values")

    #result = {k: v for k, v in vars(args).items() if v is not None}
    result = args.__dict__

    if 'index' in result:
        utils.validate_tables(env_configs, parser, result['table'], index=result['index'])
    else:
        utils.validate_tables(env_configs, parser, result['table'])

    log.info(f"Running action '{result['verb']}' with arguments: {result}")

    # If all checks pass
    return True, result
