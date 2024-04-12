import argparse
import logging
from datetime import datetime
from src.dynamodb import DDBScalingInfo
from src.getmetrics import get_metrics
from src.cost_estimates import recommendation_summary
import pandas as pd
import pytz
import os
from tqdm.contrib.concurrent import thread_map

# Initialize logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def setup_output_directory(output_dir):
    try:
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
    except Exception as e:
        logger.error(f"Error creating directory {output_dir}: {str(e)}")


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


def process_table(args):
    table_name, params, debug, filename, dynamodb_table_info = args
    current_params = params.copy()
    current_params['dynamodb_tablename'] = table_name

    try:
        result = get_metrics(current_params)

        metric_df = result[0]
        estimate_df = result[1]

        summary_result = recommendation_summary(
            current_params, metric_df, estimate_df, dynamodb_table_info)

        if debug:
            filename_metrics = os.path.join(output_path, f'metrics_{table_name}.csv')
            filename_estimate = os.path.join(output_path, f'metrics_estimate_{table_name}.csv')
            filename_cost_estimate = os.path.join(output_path, f'cost_estimate_{table_name}.csv')
            metric_df.to_csv(filename_metrics, index=False)
            estimate_df.to_csv(filename_estimate, index=False)
            summary_result[1].to_csv(filename_cost_estimate, index=False)

        with open(filename, 'a') as analysis_summary:
            summary_result[0].to_csv(analysis_summary, index=False, header=not os.path.exists(filename))

        return summary_result[0]
    except Exception as e:
        logger.error(f"Error processing table {table_name}: {str(e)}")
        return None


def process_dynamodb_table(dynamodb_table_info: pd.DataFrame, params: dict, output_path: str, debug: bool) -> pd.DataFrame:
    filename = os.path.join(output_path, f'analysis_summary{timestamp}.csv')
    with open(filename, 'w') as analysis_summary:
        analysis_summary.write('base_table_name,index_name,class,metric_name,est_provisioned_cost,current_provisioned_cost,ondemand_cost,recommended_mode,current_mode,status,savings_pct,current_cost,recommended_cost,number_of_days,current_min_capacity,simulated_min_capacity,current_max_capacity,simulated_max_capacity,current_target_utilization,simulated_target_utilizatio,autoscaling_enabled,Note\n')
    unique_tables = dynamodb_table_info['base_table_name'].unique()

    args_list = [(table_name, params, debug, filename, dynamodb_table_info) for table_name in unique_tables]

    results = thread_map(process_table, args_list, total=len(args_list), desc="Processing Tables", max_workers=params['max_concurrent_tasks'])

    # Filter out None values and concatenate the valid results
    valid_results = [result for result in results if result is not None]
    concatenated_summary_result = pd.concat(valid_results)

    return concatenated_summary_result


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

    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    output_path = "output"
    setup_output_directory(output_path)
    logger.info(f"Output directory: {output_path}")
    params = get_params(args)
    logger.info(f"Parameters: {params}")
    DDBinfo = DDBScalingInfo()
    dynamo_tables_result = DDBinfo.get_all_dynamodb_autoscaling_settings_with_indexes(
        params['dynamodb_tablename'], params['max_concurrent_tasks'])

    dynamo_tables_result.to_csv(
        os.path.join(output_path, 'dynamodb_table_info.csv'), index=False)

    process_dynamodb_result = process_dynamodb_table(
        dynamo_tables_result, params, output_path, args.debug)
