import pandas as pd
from concurrent.futures import ThreadPoolExecutor
import numpy as np
from tqdm import tqdm
import boto3


class DDBScalingInfo:
    def __init__(self):
        self.dynamodb_client = boto3.client('dynamodb')
        self.app_autoscaling = boto3.client('application-autoscaling')

    def get_dynamodb_autoscaling_settings(self,  base_table_name: str, table_storage_class: str, index_name: str = None) -> pd.DataFrame:

        app_autoscaling = self.app_autoscaling

        resource_id = f"table/{base_table_name}"
        if index_name:
            resource_id = f"{resource_id}/index/{index_name}"
        # Get the current autoscaling settings for the table
        response = app_autoscaling.describe_scalable_targets(
            ResourceIds=[resource_id], ServiceNamespace='dynamodb')
        autoscaling_settings = response['ScalableTargets']
        data = []
        for setting in autoscaling_settings:
            policy_response = app_autoscaling.describe_scaling_policies(
                ServiceNamespace='dynamodb',
                ResourceId=setting['ResourceId'],
                ScalableDimension=setting['ScalableDimension']
            )
            try:
                policy = policy_response['ScalingPolicies'][0]["TargetTrackingScalingPolicyConfiguration"]

                data.append({
                    'base_table_name': base_table_name,
                    'index_name': index_name,
                    'class': table_storage_class,
                    'metric_name': setting['ScalableDimension'],
                    'min_capacity': setting['MinCapacity'],
                    'max_capacity': setting['MaxCapacity'],
                    'target_utilization': policy['TargetValue'],
                    'autoscaling_enabled': 'True',
                    'throughput_mode': 'Provisioned'
                })
            except:
                data.append({
                    'base_table_name': base_table_name,
                    'index_name': index_name,
                    'class': table_storage_class,
                    'metric_name': None,
                    'min_capacity': None,
                    'max_capacity': None,
                    'target_utilization': None,
                    'autoscaling_enabled': 'policy_missing',
                    'throughput_mode': 'Provisioned'
                })

        return pd.DataFrame(data)

    def _process_table(self, name):
        dynamodb_client = self.dynamodb_client
        app_autoscaling = self.app_autoscaling

        desc_table = dynamodb_client.describe_table(TableName=name)

        # Get the global secondary indexes (if any)
        table_data = desc_table['Table']
        try:
            table_storage_class = table_data['TableClassSummary']['TableClass']
        except:
            table_storage_class = 'STANDARD'

        global_indexes = table_data.get('GlobalSecondaryIndexes', [])

        try:
            BillingModeSummary = desc_table['Table']['BillingModeSummary']
        except:
            BillingModeSummary = None

        if BillingModeSummary is not None:
            if desc_table['Table']['BillingModeSummary']['BillingMode'] == 'PAY_PER_REQUEST':
                result_df = pd.DataFrame({'base_table_name': [name], 'index_name': [np.nan], 'class': [table_storage_class],  'metric_name': [np.nan], 'min_capacity': [np.nan], 'max_capacity': [
                    np.nan], 'target_utilization': [np.nan], 'autoscaling_enabled': [np.nan],
                    'throughput_mode': 'Ondemand'})
                if global_indexes is not None:
                    for index in global_indexes:
                        index_name = index['IndexName']
                        index_settings = pd.DataFrame({'base_table_name': [name], 'index_name': [index_name], 'class': [table_storage_class], 'metric_name': [np.nan], 'min_capacity': [np.nan], 'max_capacity': [
                            np.nan], 'target_utilization': [np.nan], 'autoscaling_enabled': [np.nan],
                            'throughput_mode': ['Ondemand']})
                        result_df = pd.concat(
                            [result_df, index_settings], axis=0)

                return result_df
            else:
                result = []
                response = app_autoscaling.describe_scalable_targets(
                    ResourceIds=[f"table/{name}"], ServiceNamespace='dynamodb')
                if len(response['ScalableTargets']) == 0:
                    result_df = pd.DataFrame({'base_table_name': [name], 'index_name': [np.nan], 'class': [table_storage_class], 'metric_name': [np.nan], 'min_capacity': [np.nan], 'max_capacity': [
                        np.nan], 'target_utilization': [np.nan], 'autoscaling_enabled': ['False'], 'throughput_mode': ['Provisioned']})
                    result = [result_df]
                else:
                    settings = self.get_dynamodb_autoscaling_settings(
                        base_table_name=name, table_storage_class=table_storage_class)
                    result = [settings]

                # Get autoscaling settings for each index (if any)
                if global_indexes is not None:
                    for index in global_indexes:
                        index_name = index['IndexName']
                        response = app_autoscaling.describe_scalable_targets(
                            ResourceIds=[f"table/{name}/index/{index_name}"], ServiceNamespace='dynamodb')

                        if len(response['ScalableTargets']) == 0:
                            index_settings = pd.DataFrame({'base_table_name': [name], 'index_name': [index_name], 'class': [table_storage_class], 'metric_name': [np.nan], 'min_capacity': [np.nan], 'max_capacity': [
                                np.nan], 'target_utilization': [np.nan], 'autoscaling_enabled': ['False'], 'throughput_mode': ['Provisioned']})
                        else:
                            index_settings = self.get_dynamodb_autoscaling_settings(
                                base_table_name=name, table_storage_class=table_storage_class, index_name=index_name)

                        if index_settings is not None:
                            result.append(index_settings)

            if len(result) > 0:
                result_df = pd.concat(result, axis=0)
            return result_df
        else:

            result = []
            response = app_autoscaling.describe_scalable_targets(
                ResourceIds=[f"table/{name}"], ServiceNamespace='dynamodb')
            if len(response['ScalableTargets']) == 0:
                result_df = pd.DataFrame({'base_table_name': [name], 'index_name': [np.nan], 'class': [table_storage_class], 'metric_name': [np.nan], 'min_capacity': [np.nan], 'max_capacity': [
                    np.nan], 'target_utilization': [np.nan], 'autoscaling_enabled': ['False'], 'throughput_mode': ['Provisioned']})
                result = [result_df]
            else:
                # Get autoscaling settings for the table
                settings = self.get_dynamodb_autoscaling_settings(
                    base_table_name=name, table_storage_class=table_storage_class)
                result = [settings]

            # Get autoscaling settings for each index (if any)
            if global_indexes is not None:
                for index in global_indexes:
                    index_name = index['IndexName']
                    response = app_autoscaling.describe_scalable_targets(
                        ResourceIds=[f"table/{name}/index/{index_name}"], ServiceNamespace='dynamodb')

                    if len(response['ScalableTargets']) == 0:
                        index_settings = pd.DataFrame({'base_table_name': [name], 'index_name': [index_name], 'class': [table_storage_class], 'metric_name': [np.nan], 'min_capacity': [np.nan], 'max_capacity': [
                            np.nan], 'target_utilization': [np.nan], 'autoscaling_enabled': ['False'], 'throughput_mode': ['Provisioned']})
                    else:
                        index_settings = self.get_dynamodb_autoscaling_settings(
                            base_table_name=name, table_storage_class=table_storage_class, index_name=index_name)
                    if index_settings is not None:
                        result.append(index_settings)

            if len(result) > 0:
                result_df = pd.concat(result, axis=0)
            return result_df

    def get_all_dynamodb_autoscaling_settings_with_indexes(self, table_name: str) -> pd.DataFrame:

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
            with ThreadPoolExecutor(max_workers=10) as executor:
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
                        print(f"Error processing table: {e}")
                progress_bar.close()

            if len(settings_list) > 0:
                settings = pd.concat(settings_list, axis=0)
                settings['index_name'] = settings.apply(lambda x: x['base_table_name'] if pd.isnull(
                    x['index_name']) else x['base_table_name'] + ':' + x['index_name'], axis=1)
                settings['metric_name'] = settings['metric_name'].replace(
                    {'dynamodb:table:ReadCapacityUnits': 'ProvisionedReadCapacityUnits', 'dynamodb:index:ReadCapacityUnits': 'ProvisionedReadCapacityUnits'}, regex=True)
                settings['metric_name'] = settings['metric_name'].replace(
                    {'dynamodb:table:WriteCapacityUnits': 'ProvisionedWriteCapacityUnits', 'dynamodb:index:WriteCapacityUnits': 'ProvisionedWriteCapacityUnits'}, regex=True)
            else:
                settings = pd.DataFrame()

            return settings
        else:
            raise ValueError("No DynamoDB tables found in this region")
