#!/bin/env python

import argparse
import json
import logging
import sys

import boto3

from ddbtools import constants
from ddbtools.table import TableUtility
from ddbtools.util import DecimalEncoder


class DynamoDBTableClassCalculator(object):
    """Calculate pricing for all table classes, and make optimization recommendations
       for tables that may save money by using a different table class.

       Note: This tool assumes a table is not overprovisioned when calculating costs."""
    def __init__(self, args: argparse.Namespace):
            self.args = args

            # Versions of boto3 older than 1.20.18 will still run, but don't support the table class attribute in
            # the result of describe_table, which would result in assuming all tables used the Standard table class.
            # Check the Boto version after import to avoid this situation.
            boto_version_elements = boto3.__version__.split('.')
            major_version = int(boto_version_elements[0])
            minor_version = int(boto_version_elements[1])
            patch_version = int(boto_version_elements[2])

            if ((major_version < 1) or
                (major_version == 1 and minor_version < 20) or
                (major_version == 1 and minor_version == 20 and patch_version < 18)):
                    message = f"Error: Boto3 >= 1.20.18 required. See https://aws.amazon.com/sdk-for-python/ for more."
                    print(message)
                    exit(0)

            self.table_utility = TableUtility(region_name=self.args.region, profile_name=self.args.profile)


            # Setup logging
            log_level = logging.INFO

            root_logger = logging.getLogger()
            root_logger.setLevel(log_level)

            root_handler = logging.StreamHandler(sys.stdout)
            root_handler.setLevel(log_level)
            formatter = logging.Formatter('%(asctime)s: %(message)s')
            root_handler.setFormatter(formatter)
            root_logger.addHandler(root_handler)


    def run(self):
        """Main program entry point"""
        table_names = []

        try:
            if self.args.table_name is not None:
                table_names = [self.args.table_name]
            else:
                table_names = self.table_utility.get_table_names()

            table_cost_estimates = self.table_utility.estimate_table_costs_for_region(table_names, self.args.region)

            if not table_cost_estimates:
                print("No table cost results returned.")
                exit(0)

            if self.args.estimates_only:
                print(json.dumps(table_cost_estimates, cls=DecimalEncoder, indent=2))
                exit(0)

            recommendations = []

            # evaluate tables costs for storage classes
            for table_estimate in table_cost_estimates:
                table_pricing_data = table_estimate[constants.PRICING_DATA]

                # skip on-demand tables
                if table_pricing_data[constants.BILLING_MODE] == constants.ON_DEMAND_BILLING:
                    continue

                table_class = table_pricing_data[constants.TABLE_CLASS]
                monthly_cost_estimates = table_estimate[constants.ESTIMATED_MONTHLY_COSTS]

                ia_cost_differential = (monthly_cost_estimates[constants.IA_MO_TOTAL_COST]
                                        - monthly_cost_estimates[constants.STD_MO_TOTAL_COST])

                if ia_cost_differential < 0:
                    if table_class == constants.STD_TABLE_CLASS:
                        recommendation = {constants.RECOMMENDATION_TYPE: constants.TABLE_CLASS_CHANGE_RECOMMENDATION,
                                          constants.RECOMMENDED_TABLE_CLASS: constants.IA_TABLE_CLASS,
                                          constants.ESTIMATED_MO_SAVINGS: abs(ia_cost_differential),
                                          constants.ESTIMATE_DETAIL: table_estimate}
                        recommendations.append(recommendation)

                elif ia_cost_differential > 0:
                    if table_class == constants.IA_TABLE_CLASS:
                        recommendation = {constants.RECOMMENDATION_TYPE: constants.TABLE_CLASS_CHANGE_RECOMMENDATION,
                                          constants.RECOMMENDED_TABLE_CLASS: constants.STD_TABLE_CLASS,
                                          constants.ESTIMATED_MO_SAVINGS: ia_cost_differential,
                                          constants.ESTIMATE_DETAIL: table_estimate}
                        recommendations.append(recommendation)


            output = json.dumps(recommendations, cls=DecimalEncoder, indent=2)
            print(output)
            exit(0)
        except Exception as e:
            print(f"Table evaluation failed: {e}")
            import traceback
            traceback.print_exc()
            exit(0)


def main():
    parser = argparse.ArgumentParser(description='Recommend Amazon DynamoDB table class changes to optimize costs.')

    parser.add_argument(
        '--estimates-only', required=False, action='store_true',
                         help='print table cost estimates instead of change recommendations')

    parser.add_argument(
        '--region', required=False, type=str, default='us-east-1',
                    help='evaluate tables in REGION (default: us-east-1)')

    parser.add_argument(
        '--table-name', required=False, type=str,
                        help='evaluate TABLE_NAME (defaults to all tables in region)')

    parser.add_argument('--profile', required=False, type=str, default='default', help='set a custom profile name to perform the operation under')

    args = parser.parse_args()
    calculator = DynamoDBTableClassCalculator(args)
    calculator.run()

if __name__ == "__main__":
    main()
