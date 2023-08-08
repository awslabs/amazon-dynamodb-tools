import datetime
from datetime import datetime, timedelta
from queue import Queue
import boto3
import src.metrics_estimates as estimates
import pandas as pd
from tqdm.contrib.concurrent import thread_map
import logging


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def list_metrics(tablename: str) -> list:
    cw = boto3.client('cloudwatch')
    metrics_list = []

    paginator = cw.get_paginator('list_metrics')

    if not tablename:
        operation_parameters = {'Namespace': 'AWS/DynamoDB'}
    else:
        operation_parameters = {'Namespace': 'AWS/DynamoDB',
                                'Dimensions': [{'Name': 'TableName', 'Value': tablename}]}

    for response in paginator.paginate(**operation_parameters):
        metrics_list.extend(response['Metrics'])

    return metrics_list


def process_results(metrics_list, metric, metric_result_queue, estimate_result_queue, read_utilization, write_utilization, read_min, write_min, read_max, write_max):

    metrics_result = []
    for result in metrics_list['MetricDataResults']:

        try:
            name = str(metric[0]['Value']) + ":" + str(metric[1]['Value'])
        except:
            name = str(metric[0]['Value'])
        metric_list = list(zip(result['Timestamps'], result['Values']))
        tmdf = pd.DataFrame(metric_list, columns=['timestamp', 'unit'])
        tmdf['unit'] = tmdf['unit'].astype(float)
        tmdf['timestamp'] = pd.to_datetime(tmdf['timestamp'], unit='ms')
        tmdf['name'] = name
        tmdf['metric_name'] = result['Label']
        tmdf = tmdf[['metric_name', 'timestamp', 'name', 'unit']]
        metrics_result.append(tmdf)
        metric_result_queue.put(tmdf)
    metrics_result = pd.concat(metrics_result)
    estimate_units = estimates.estimate(
        metrics_result, read_utilization, write_utilization, read_min, write_min, read_max, write_max)

    estimate_result_queue.put(estimate_units)


def fetch_metric_data(metric, start_time, end_time, consumed_period, provisioned_period):
    cw = boto3.client('cloudwatch')

    if metric['MetricName'] == 'ProvisionedWriteCapacityUnits':
        result = cw.get_metric_data(MetricDataQueries=[
            {
                'Id': 'provisioned_rcu',
                'MetricStat': {
                    'Metric': {
                        'Namespace': 'AWS/DynamoDB',
                        'MetricName': 'ProvisionedReadCapacityUnits',
                        'Dimensions': metric['Dimensions']
                    },
                    'Period': provisioned_period,
                    'Stat': 'Average'
                },
            },
            {
                'Id': 'provisioned_wcu',
                'MetricStat': {
                    'Metric': {
                        'Namespace': 'AWS/DynamoDB',
                        'MetricName': 'ProvisionedWriteCapacityUnits',
                        'Dimensions': metric['Dimensions']
                    },
                    'Period': provisioned_period,
                    'Stat': 'Average'
                }
            }
        ], StartTime=start_time, EndTime=end_time)
        return (result, metric['Dimensions'])

    elif metric['MetricName'] == 'ConsumedReadCapacityUnits':
        result = cw.get_metric_data(MetricDataQueries=[
            {
                'Id': 'consumed_rcu',
                'MetricStat': {
                    'Metric': {
                        'Namespace': 'AWS/DynamoDB',
                        'MetricName': 'ConsumedReadCapacityUnits',
                        'Dimensions': metric['Dimensions']
                    },
                    'Period': consumed_period,
                    'Stat': 'Sum'
                },
            },
            {
                'Id': 'consumed_wcu',
                'MetricStat': {
                    'Metric': {
                        'Namespace': 'AWS/DynamoDB',
                        'MetricName': 'ConsumedWriteCapacityUnits',
                        'Dimensions': metric['Dimensions']
                    },
                    'Period': consumed_period,
                    'Stat': 'Sum'
                }
            }
        ], StartTime=start_time, EndTime=end_time)
        return (result, metric['Dimensions'])

    return None


def get_table_metrics(metrics, start_time, end_time, consumed_period, provisioned_period, read_utilization, write_utilization, read_min, write_min, read_max, write_max, max_concurrent_tasks,dynamodb_tablename):
    metric_result_queue = Queue()
    estimate_result_queue = Queue()
    metric_data_list = thread_map(lambda metric: fetch_metric_data(metric, start_time, end_time, consumed_period, provisioned_period),
                                  metrics, max_workers=max_concurrent_tasks, desc="Fetching CloudWatch metrics for: " + dynamodb_tablename)

    metric_data_list = [
        result for result in metric_data_list if result is not None]

    #print("starting process to estimate dynamodb table provisioned metrics")
    thread_map(lambda result: process_results(result[0], result[1], metric_result_queue, estimate_result_queue, read_utilization, write_utilization, read_min, write_min, read_max, write_max),
               metric_data_list, max_workers=max_concurrent_tasks, desc="Estimating DynamoDB table provisioned metrics for: " + dynamodb_tablename)

    processed_metric = []
    processed_estimate = []
    while not metric_result_queue.empty():
        processed_metric.append(metric_result_queue.get())
    while not estimate_result_queue.empty():
        processed_estimate.append(estimate_result_queue.get())
    if all(df.empty for df in processed_metric):
        logger.info("No metrics were retrieved from CloudWatch.")
    else:
        metric_df = pd.concat(processed_metric, ignore_index=True)
        estimate_df = pd.concat(processed_estimate, ignore_index=True)
        return [metric_df, estimate_df]


def get_metrics(params):

    provisioned_period = 3600
    consumed_period = 60
    read_min = params['dynamodb_minimum_read_unit']
    write_min = params['dynamodb_minimum_write_unit']
    read_max = params['dynamodb_maximum_read_unit']
    write_max = params['dynamodb_maximum_write_unit']
    read_utilization = params['dynamodb_read_utilization']
    write_utilization = params['dynamodb_write_utilization']
    dynamodb_tablename = params['dynamodb_tablename']
    interval = params['number_of_days_look_back']
    now = params['cloudwatch_metric_end_datatime']
    now = datetime.strptime(now, '%Y-%m-%d %H:%M:%S')
    end_time = now
    start_time = end_time - timedelta(days=interval)
    end_time = end_time.strftime('%Y-%m-%dT%H:%M:%SZ')
    start_time = start_time.strftime('%Y-%m-%dT%H:%M:%SZ')
    max_concurrent_tasks = params['max_concurrent_tasks']

    metrics = list_metrics(dynamodb_tablename)
    result = get_table_metrics(metrics, start_time, end_time, consumed_period,
                               provisioned_period, read_utilization, write_utilization, read_min, write_min, read_max, write_max, max_concurrent_tasks,dynamodb_tablename)

    return result
