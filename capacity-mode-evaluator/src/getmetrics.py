import datetime
from datetime import datetime, timedelta
from queue import Queue
import boto3
import src.metrics_estimates as estimates
import pandas as pd
from tqdm.contrib.concurrent import thread_map


# list metrics
def list_metrics(tablename: str) -> list:
    # Create a client for the AWS CloudWatch service using the specified region
    cw = boto3.client('cloudwatch')

    # Create an empty list to store the metrics
    metrics_list = []

    # Create a paginator for the "list_metrics" operation
    paginator = cw.get_paginator('list_metrics')

    # Set the operation parameters based on the provided tablename
    if not tablename:
        operation_parameters = {'Namespace': 'AWS/DynamoDB'}
    else:
        operation_parameters = {'Namespace': 'AWS/DynamoDB',
                                'Dimensions': [{'Name': 'TableName', 'Value': tablename}]}

    # Iterate through the paginated responses, appending each response's metrics to the list
    for response in paginator.paginate(**operation_parameters):
        metrics_list.extend(response['Metrics'])

    # Return the list of metrics
    return metrics_list


def process_results(metr_list, metric, metric_result_queue, estimate_result_queue, readutilization, writeutilization, read_min, write_min):

    metrics_result = []
    for result in metr_list['MetricDataResults']:

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
        tmdf = tmdf[['metric_name',  'timestamp', 'name', 'unit']]
        metrics_result.append(tmdf)
        metric_result_queue.put(tmdf)
    metrics_result = pd.concat(metrics_result)
    estimate_units = estimates.estimate(
        metrics_result, readutilization, writeutilization, read_min, write_min)

    estimate_result_queue.put(estimate_units)


def fetch_metric_data(metric, starttime, endtime, consumed_period, provisioned_period):
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
        ], StartTime=starttime, EndTime=endtime)
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
        ], StartTime=starttime, EndTime=endtime)
        return (result, metric['Dimensions'])

    return None


def get_table_metrics(metrics, starttime, endtime, consumed_period, provisioned_period, readutilization, writeutilization, read_min, write_min):
    metric_result_queue = Queue()
    estimate_result_queue = Queue()
    # Using tqdm.contrib.concurrent.thread_map to fetch metric data with progress bar
    metric_data_list = thread_map(lambda metric: fetch_metric_data(metric, starttime, endtime, consumed_period, provisioned_period), 
                                  metrics, max_workers=10)
    
    # Filter out None values from the metric_data_list
    metric_data_list = [result for result in metric_data_list if result is not None]
    
    print("starting process to estimate dynamodb table provisioned metrics")
    thread_map(lambda result: process_results(result[0], result[1], metric_result_queue, estimate_result_queue, readutilization, writeutilization, read_min, write_min),
               metric_data_list, max_workers=10)

    # create an empty list to hold the dataframe
    processed_metric = []
    processed_estimate = []
    # get the elements from the queue
    while not metric_result_queue.empty():
        processed_metric.append(metric_result_queue.get())
    while not estimate_result_queue.empty():
        processed_estimate.append(estimate_result_queue.get())
    # convert the processed_metric list to dataframe
    if all(df.empty for df in processed_metric):
        print("No Metrics were retrived in check end date provided for CloudWatch.")
    else:
        metric_df = pd.concat(processed_metric, ignore_index=True)
        estimate_df = pd.concat(processed_estimate, ignore_index=True)
        return [metric_df, estimate_df]


# Getting  Metrics
def get_metrics(params):

    provisioned_period = 3600
    consumed_period = 60
    read_min = params['dynamodb_minimum_read_unit']
    write_min = params['dynamodb_minimum_write_unit']
    readutilization = params['dynamodb_read_utilization']
    writeutilization = params['dynamodb_write_utilization']
    dynamodb_tablename = params['dynamodb_tablename']
    interval = params['number_of_days_look_back']
    now = params['cloudwatch_metric_end_datatime']
    now = datetime.strptime(now, '%Y-%m-%d %H:%M:%S')
    endtime = now
    starttime = endtime - timedelta(days=interval)
    endtime = endtime.strftime('%Y-%m-%dT%H:%M:%SZ')
    starttime = starttime.strftime('%Y-%m-%dT%H:%M:%SZ')

    metrics = list_metrics(dynamodb_tablename)
    result = get_table_metrics(metrics, starttime, endtime, consumed_period,
                               provisioned_period, readutilization, writeutilization, read_min, write_min)

    return result
