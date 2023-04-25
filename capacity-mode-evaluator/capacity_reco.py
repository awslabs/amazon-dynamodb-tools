import argparse
from datetime import datetime
from src.dynamodb import DDBScalingInfo
from src.getmetrics import get_metrics
from src.cost_estimates import recommendation_summary
import pandas as pd
import pytz
import os


dir_path = "output"

if not os.path.exists(dir_path):
    os.makedirs(dir_path)


def get_params(args):
    params = {}
    params['dynamodb_tablename'] = args.dynamodb_tablename
    params['dynamodb_read_utilization'] = args.dynamodb_read_utilization
    params['dynamodb_write_utilization'] = args.dynamodb_write_utilization
    params['dynamodb_minimum_write_unit'] = args.dynamodb_minimum_write_unit
    params['dynamodb_maximum_write_unit'] = args.dynamodb_maximum_write_unit
    params['dynamodb_minimum_read_unit'] = args.dynamodb_minimum_read_unit
    params['dynamodb_maximum_read_unit'] = args.dynamodb_maximum_read_unit
    params['number_of_days_look_back'] = args.number_of_days_look_back
    params['max_concurrent_tasks'] = args.max_concurrent_tasks

    now = datetime.utcnow()
    midnight = datetime(now.year, now.month, now.day, 0, 0, 0, tzinfo=pytz.UTC)
    params['cloudwatch_metric_end_datatime'] = midnight.strftime(
        '%Y-%m-%d %H:%M:%S')

    return params


def process_dynamodb_table(dynamodb_table_info: pd.DataFrame, params: dict, debug: bool) -> pd.DataFrame:
    print('starting process to get dynamodb table metrics')
    result = get_metrics(params)
    metric_df = result[0]
    estimate_df = result[1]
    print('Estimating cost...')
    summary_result = recommendation_summary(
        params, metric_df, estimate_df, dynamodb_table_info)
    cost_estimate_df = summary_result[1]
    if debug:
        filename_metrics = os.path.join(dir_path, 'metrics.csv')
        filename_estimate = os.path.join(dir_path, 'estimate.csv')
        filename_cost_estimate = os.path.join(dir_path, 'cost_estimate.csv')
        metric_df.to_csv(filename_metrics, index=False)
        estimate_df.to_csv(filename_estimate, index=False)
        cost_estimate_df.to_csv(filename_cost_estimate, index=False)
    filename_summary = os.path.join(dir_path, 'analysis_summary.csv')
    summary_result[0].to_csv(filename_summary, index=False)
    return summary_result[0]


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Process DynamoDB table metrics.')

    parser.add_argument('--debug', action='store_true',
                        help='Save metrics and estimates as CSV files in debug mode')
    parser.add_argument('--dynamodb-tablename', type=str,
                        default=None, help='DynamoDB table name')
    parser.add_argument('--dynamodb-read-utilization',
                        type=int, default=70, help='DynamoDB read utilization')
    parser.add_argument('--dynamodb-write-utilization',
                        type=int, default=70, help='DynamoDB write utilization')
    parser.add_argument('--dynamodb-minimum-write-unit',
                        type=int, default=1, help='DynamoDB minimum write unit')
    parser.add_argument('--dynamodb-maximum-write-unit',
                        type=int, default=80000, help='DynamoDB maximum write unit')
    parser.add_argument('--dynamodb-minimum-read-unit',
                        type=int, default=1, help='DynamoDB minimum read unit')
    parser.add_argument('--dynamodb-maximum-read-unit',
                        type=int, default=80000, help='DynamoDB maximum read unit')
    parser.add_argument('--number-of-days-look-back', type=int,
                        default=14, help='Number of days to look back')
    parser.add_argument('--max-concurrent-tasks', type=int,
                        default=5, help='Maximum number of tasks to run concurrently')
    args = parser.parse_args()

    params = get_params(args)
    print(params)
    DDBinfo = DDBScalingInfo()
    dynamo_tables_result = DDBinfo.get_all_dynamodb_autoscaling_settings_with_indexes(
        params['dynamodb_tablename'], params['max_concurrent_tasks'] )

    dynamo_tables_result.to_csv(
        os.path.join(dir_path, 'dynamodb_table_info.csv'), index=False)
    process_dynamodb_result = process_dynamodb_table(
        dynamo_tables_result, params, args.debug)
