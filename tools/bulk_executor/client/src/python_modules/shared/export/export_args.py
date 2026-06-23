import argparse
import logging as log

import utils
from utils.custom_parser import BulkArgumentParser


def parse_export_args(verb_name, help_text, env_configs):
    glue_job_parent = utils.glue_job_arguments()
    environment_parent = utils.environment_arguments()

    parser = BulkArgumentParser(f"bulk {verb_name}", help_text=help_text, parents=[glue_job_parent, environment_parent])
    parser.add_argument('verb', help=argparse.SUPPRESS)
    parser.add_argument('--table', required=True, type=str, help='Table name')
    parser.add_argument('--s3-path', required=True, type=str, help='Amazon S3 path in format like s3://bucket-name/prefix/AWSDynamoDB/01716790307109-5f9d6aaa')
    parser.add_argument('--transform', type=str, default=argparse.SUPPRESS, help=argparse.SUPPRESS)

    args = parser.parse_args()

    result = args.__dict__

    utils.validate_tables(env_configs, parser, result['table'], pitr_enabled=True)
    utils.validate_s3_export_path(result['s3_path'])

    if 'transform' in result:
        result['transform'] = utils.sanitize_arg(result['transform'], r'\.py$')

    log.info(f"Running action '{result['verb']}' with arguments: {result}")

    return True, result
