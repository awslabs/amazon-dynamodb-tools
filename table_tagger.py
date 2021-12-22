#!/bin/env python

import argparse
import json
import logging
import sys

from ddbtools import constants
from ddbtools.table import TableUtility


class DynamoDBEponymousTagger(object):
    """Iterate through all DynamoDB tables in a region, 
       and tag each table with its own name if not already tagged."""
    def __init__(self, args: argparse.Namespace):
            self.args = args
            self.table_utility = TableUtility(region_name=self.args.region)
            
            # Setup logging
            log_level = logging.INFO
            root_logger = logging.getLogger()
            root_logger.setLevel(log_level)

            root_handler = logging.StreamHandler(sys.stdout)
            root_handler.setLevel(log_level)
            formatter = logging.Formatter('%(asctime)s: %(message)s')
            root_handler.setFormatter(formatter)
            root_logger.addHandler(root_handler)


    def eponymously_tag_all_tables(self, tag_name:str, table_names:list, dry_run:bool) -> list:
        """Tag all tables in the region with their own name if not already tagged."""
        tagged_tables = []

        for table_name in table_names:
            table_arn = self.table_utility.get_table_arn(table_name)
            table_tags = self.table_utility.get_table_tags(table_arn=table_arn)

            if tag_name not in table_tags:
                name_tag = [{'Key': tag_name, 'Value': table_name}]

                if not self.args.dry_run:
                    self.table_utility.add_tags_to_table(table_arn, name_tag)
                    logging.info(f"table_tagger: Tagged {table_arn} with Key: {tag_name}, Value: {table_name}")

                tag_info = {'table_arn': table_arn,
                            'tag_key': tag_name,
                            'tag_value': table_name}
                tagged_tables.append(tag_info)

        return tagged_tables


    def run(self):
        """Main program entry point"""
        table_names = []

        try:
            if self.args.table_name is not None:
                table_names = [self.args.table_name]
            else:
                table_names = self.table_utility.get_table_names()

            tagged_tables = self.eponymously_tag_all_tables(self.args.tag_name, 
                                                            table_names,
                                                            self.args.dry_run)
            output = json.dumps(tagged_tables, indent=2)
            print(output)

            exit(0)

        except Exception as e:
            print(f"Table tagging failed: {e}")
            exit(0)


def main():
    parser = argparse.ArgumentParser(description='Tag all DynamoDB tables in a region with their own name.')

    parser.add_argument(
        '--dry-run', required=False, action='store_true', help='output results but do not actually tag tables')

    parser.add_argument(
        '--region', required=False, type=str, default='us-east-1', help='tag tables in REGION (default: us-east-1)')

    parser.add_argument(
        '--table-name', required=False, type=str, help='tag only TABLE_NAME (defaults to all tables in region)')

    parser.add_argument(
        '--tag-name', required=False, type=str, default='table_name',  help='tag table with tag TAG_NAME (default is "table_name")')

    args = parser.parse_args()
    calculator = DynamoDBEponymousTagger(args)
    calculator.run()

if __name__ == "__main__":
    main()
