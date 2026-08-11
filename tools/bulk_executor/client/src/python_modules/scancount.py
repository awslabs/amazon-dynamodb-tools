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
        Optional --per-segment flag to print item counts per segment (reveals data skew)
        Optional --segments parameter to control how many parallel scan segments to use (default 200)
        Optional --sample-fraction to scan only a fraction of segments and extrapolate an estimated
            total (e.g. 0.1 scans 10% of segments), with a 95% confidence interval. Intended for a
            rough *filtered* count of a large table: an unfiltered count is already available for
            free (and exact) from DescribeTable, so pair --sample-fraction with --filter-expression
            to estimate how many items match a predicate without scanning the whole table.
            --per-segment reveals the skew that widens the error margin.

    Examples:
        # Count all items in a table
        bulk scancount --table orders

        # Count using a filter expression (uses DynamoDB FilterExpression syntax)
        bulk scancount --table audit --filter-expression "#ts > :ts" --expression-names '{{"#ts": "timestamp"}}' --expression-values '{{":ts":"2025-01-01"}}'

        # Show per-segment counts to diagnose hot partitions
        bulk scancount --table orders --per-segment

        # Use fewer segments for a smaller table
        bulk scancount --table orders --segments 10

        # Estimate how many items are from 2024 or earlier by scanning only 10% of segments
        bulk scancount --table audit --filter-expression "#ts < :cutoff" --expression-names '{{"#ts": "timestamp"}}' --expression-values '{{":cutoff":"2025-01-01"}}' --sample-fraction 0.1

        # Same estimate, plus the per-segment skew behind its error margin
        bulk scancount --table audit --filter-expression "#ts < :cutoff" --expression-names '{{"#ts": "timestamp"}}' --expression-values '{{":cutoff":"2025-01-01"}}' --sample-fraction 0.1 --per-segment
    """

def json_type(s):
    try:
        json.loads(s)
        return s
    except json.JSONDecodeError as e:
        raise argparse.ArgumentTypeError(f"JSON type parameter held invalid JSON: {e} | Parsed string: {s}")

def positive_fraction(value):
    f = float(value)
    if f <= 0.0 or f > 1.0:
        raise argparse.ArgumentTypeError('--sample-fraction must be > 0 and ≤ 1.0')
    return f

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
    # store_true with SUPPRESS (matching --XWaitForDPU): the flag is only placed
    # in the arg dict when set, so it is only forwarded to the Glue job when the
    # user asks for it. A plain default=False would serialize to the string
    # "False" over the argv wire (root.py parses values as strings), which the
    # server would read as truthy — firing the per-segment report unconditionally.
    parser.add_argument('--per-segment', action='store_true', default=argparse.SUPPRESS, help='Print item count per segment to reveal data skew')
    parser.add_argument('--segments', type=int, default=200, help='Number of parallel scan segments (default 200)')
    parser.add_argument('--sample-fraction', type=positive_fraction, default=1.0, help='Scan only this fraction of segments and extrapolate an estimated total (e.g., 0.1 for 10%%), must be > 0 and ≤ 1.0, default 1.0 (full scan)')
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
