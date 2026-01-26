import pandas as pd
from concurrent.futures import ThreadPoolExecutor
import numpy as np
from tqdm import tqdm
import boto3
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DDBScalingInfo:
    def __init__(self):
        self.dynamodb_client = boto3.client('dynamodb')
        self.app_autoscaling = boto3.client('application-autoscaling')

    def get_dynamodb_autoscaling_settings(self, base_table_name: str, table_storage_class: str, index_name: str = None):

        app_autoscaling = self.app_autoscaling

        resource_id = f"table/{base_table_name}"
        if index_name:
            resource_id = f"{resource_id}/index/{index_name}"
        # Get the current autoscaling settings for the table
        response = app_autoscaling.describe_scalable_targets(
            ResourceIds=[resource_id], ServiceNamespace='dynamodb')
        autoscaling_settings = response['ScalableTargets']
        scalable_targets = response.get('ScalableTargets', [])
        if not scalable_targets:
            return [[base_table_name, index_name, table_storage_class, None, None, None, None, 'False', 'Provisioned']]
        data = []
        for setting in autoscaling_settings:
            policy_response = app_autoscaling.describe_scaling_policies(
                ServiceNamespace='dynamodb',
                ResourceId=setting['ResourceId'],
                ScalableDimension=setting['ScalableDimension']
            )
            try:
                policy = policy_response['ScalingPolicies'][0]["TargetTrackingScalingPolicyConfiguration"]

                data.append([
                    base_table_name,
                    index_name,
                    table_storage_class,
                    setting['ScalableDimension'],
                    setting['MinCapacity'],
                    setting['MaxCapacity'],
                    policy['TargetValue'],
                    'True',
                    'Provisioned'
                ])
            except:
                data.append([
                    base_table_name,
                    index_name,
                    table_storage_class,
                    None,
                    None,
                    None,
                    None,
                    'policy_missing',
                    'Provisioned'
                ])
        return data

    def _process_table(self, name):
        try:
            desc_table = self.dynamodb_client.describe_table(TableName=name)
            table_data = desc_table.get('Table', {})
            table_storage_class = table_data.get('TableClassSummary', {}).get('TableClass', 'STANDARD')
            global_indexes = table_data.get('GlobalSecondaryIndexes', [])
            billing_mode = table_data.get('BillingModeSummary', {}).get('BillingMode', 'PROVISIONED')

            # Initialize DataFrame columns
            columns = ['base_table_name', 'index_name', 'class', 'metric_name', 'min_capacity', 'max_capacity', 'target_utilization', 'autoscaling_enabled', 'throughput_mode']
            result_data = []

            if billing_mode == 'PAY_PER_REQUEST':
                result_data.append([name, None, table_storage_class, None, None, None, None, None, 'Ondemand'])
                for index in global_indexes:
                    result_data.append([name, index['IndexName'], table_storage_class, None, None, None, None, None, 'Ondemand'])
            else:
                table_settings = self.get_dynamodb_autoscaling_settings(name, table_storage_class)
                if table_settings:
                    result_data.extend(table_settings)

                for index in global_indexes:
                    index_settings = self.get_dynamodb_autoscaling_settings(name, table_storage_class, index_name=index['IndexName'])
                    if index_settings:
                        result_data.extend(index_settings)
            result_df = pd.DataFrame(result_data, columns=columns)
            return result_df
        except Exception as e:
            logger.error(f"Error processing table {name}: {e}")
            return pd.DataFrame(columns=columns)

    def get_all_dynamodb_autoscaling_settings_with_indexes(self, table_name: str, max_concurrent_tasks: int) -> pd.DataFrame:

        dynamodb_client = self.dynamodb_client

        # Get a list of all DynamoDB tables
        table_names = []
        last_evaluated_table_name = None
        if not table_name:
            while last_evaluated_table_name != '':
                params = {}
                if last_evaluated_table_name:
                    params['ExclusiveStartTableName'] = last_evaluated_table_name
                response = dynamodb_client.list_tables(**params)
                table_names += response['TableNames']
                last_evaluated_table_name = response.get(
                    'LastEvaluatedTableName', '')

        else:
            table_names = [table_name]

        settings_list = []
        if len(table_names) != 0:
            # Create a thread pool to execute _process_table() for each table in parallel
            with ThreadPoolExecutor(max_workers=max_concurrent_tasks) as executor:
                futures = [executor.submit(self._process_table, name)
                           for name in table_names]
                progress_bar = tqdm(total=len(table_names),
                                    desc=f"Getting DynamoDB Tables info ...")

                settings_list = []
                for future in futures:
                    progress_bar.update(1)
                    try:
                        result = future.result()
                        if result is not None:
                            settings_list.append(result)
                    except Exception as e:
                        logger.error(f"Error processing table: {e}")
                progress_bar.close()
            if len(settings_list) > 0:
                settings = pd.concat(settings_list, axis=0)
                settings['index_name'] = settings.apply(lambda x: x['base_table_name'] if pd.isnull(
                    x['index_name']) else x['base_table_name'] + ':' + x['index_name'], axis=1)
                if settings['metric_name'].notnull().any():
                    settings['metric_name'] = settings['metric_name'].replace(
                        {'dynamodb:table:ReadCapacityUnits': 'ProvisionedReadCapacityUnits', 'dynamodb:index:ReadCapacityUnits': 'ProvisionedReadCapacityUnits'}, regex=True)
                    settings['metric_name'] = settings['metric_name'].replace(
                        {'dynamodb:table:WriteCapacityUnits': 'ProvisionedWriteCapacityUnits', 'dynamodb:index:WriteCapacityUnits': 'ProvisionedWriteCapacityUnits'}, regex=True)
            else:
                settings = pd.DataFrame()
            return settings
        else:
            logger.info("No DynamoDB tables found in this region")
            raise ValueError("No DynamoDB tables found in this region")
