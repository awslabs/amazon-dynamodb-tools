import pandas as pd
import numpy as np
from src.pricing import PricingUtility
import boto3


def cost_estimate(results_metrics_df, results_estimates_df, read_util, write_util, read_min, write_min, provisioned_pricing, ondemand_pricing):
    consumed_write_capacity_unit_pricing = float(
        ondemand_pricing.get('std_wcu_pricing'))
    consumed_read_capacity_unit_pricing = float(
        ondemand_pricing.get('std_rcu_pricing'))
    provisioned_read_capacity_unit_pricing = float(
        provisioned_pricing.get('std_rcu_pricing'))
    provisioned_write_capacity_unit_pricing = float(
        provisioned_pricing.get('std_wcu_pricing'))

    metric_map = {
        'ConsumedWriteCapacityUnits': ('ProvisionedWriteCapacityUnits', write_min, write_util, provisioned_write_capacity_unit_pricing, consumed_write_capacity_unit_pricing),
        'ConsumedReadCapacityUnits': ('ProvisionedReadCapacityUnits', read_min, read_util, provisioned_read_capacity_unit_pricing, consumed_read_capacity_unit_pricing)
    }

    q1 = (
        results_estimates_df.groupby([
            pd.Grouper(key='timestamp', freq='H', offset=0),
            'name', 'metric_name'
        ])
        .agg({
            'unit': 'sum',
            'estunit': 'mean'
        })
        .reset_index()
    )

    q1['timestamp'] = q1['timestamp'].dt.floor('H')
    q1['Consumed_unit'] = q1['unit']
    q1['est_provisioned_unit'] = q1['estunit']

    q1['metric_name'], q1['min_capacity'], q1['target_utilization'], q1['est_provisioned_cost'], q1['ondemand_cost'] = zip(*[
        metric_map.get(metric, (None, None, None, None, None))
        for metric in q1['metric_name']
    ])

    q1['ondemand_cost'] = q1['Consumed_unit'] * q1['ondemand_cost']
    q1['est_provisioned_cost'] = q1['est_provisioned_unit'] * \
        q1['est_provisioned_cost']

    q2 = (
        results_metrics_df.groupby([
            pd.Grouper(key='timestamp', freq='H', offset=0),
            'name', 'metric_name'
        ])
        .agg({
            'unit': 'mean'
        })
        .reset_index()
    )

    q2['timestamp'] = q2['timestamp'].dt.floor('H')
    q2['provisioned_cost'] = (
        q2.apply(
            lambda x: x['unit'] * provisioned_read_capacity_unit_pricing if x['metric_name'] == 'ProvisionedReadCapacityUnits' else x['unit'] *
            provisioned_write_capacity_unit_pricing if x[
                'metric_name'] == 'ProvisionedWriteCapacityUnits' else 0,
            axis=1
        )
    )

    q2 = q2.rename(columns={'unit': 'provisioned_unit'})

    df = q1.merge(q2, how='left', on=[
                  'name',  'timestamp', 'metric_name'])

    df['current_provisioned_cost'] = df['provisioned_cost']
    df['ondemand_unit'] = df['Consumed_unit']
    df['est_provisioned_cost'] = df['est_provisioned_cost']

    df['current_cost'] = df.apply(
        lambda x: x['provisioned_cost'] if x['provisioned_cost'] else x['ondemand_cost'], axis=1)

    return df[['name',  'timestamp', 'metric_name', 'est_provisioned_unit', 'provisioned_unit', 'ondemand_unit', 'current_provisioned_cost', 'est_provisioned_cost', 'ondemand_cost', 'current_cost', 'min_capacity', 'target_utilization']
              ]


def recommendation_summary(params, results_metrics_df, results_estimates_df, dynamodb_info_df):
    region_name = boto3.Session().region_name
    pricing_utility = PricingUtility(region_name=region_name)
    ondemand_pricing = pricing_utility.get_on_demand_capacity_pricing(
        region_name)
    provisioned_pricing = pricing_utility.get_provisioned_capacity_pricing(
        region_name)

    # Extract the required parameters from the input dictionary
    read_min = params.get('dynamodb_minimum_read_unit', 0)
    write_min = params.get('dynamodb_minimum_write_unit', 0)
    read_util = params.get('dynamodb_read_utilization', 0)
    write_util = params.get('dynamodb_write_utilization', 0)

    # Compute the cost estimates
    cost_estimate_df = cost_estimate(
        results_metrics_df, results_estimates_df, read_util, write_util, read_min, write_min, provisioned_pricing, ondemand_pricing)

    # Preprocess the cost estimate data
    cost_estimate_df = cost_estimate_df.rename(
        columns={cost_estimate_df.columns[0]: "index_name"})
    cost_estimate_df["base_table_name"] = cost_estimate_df["index_name"].str.split(
        ':').str[0]
    # Aggregate the cost estimates
    q1 = cost_estimate_df.groupby(['index_name', 'base_table_name',  'metric_name']).agg(
        est_provisioned_cost=('est_provisioned_cost', 'sum'),
        current_provisioned_cost=('current_provisioned_cost', 'sum'),
        ondemand_cost=('ondemand_cost', 'sum'),
        timestamp_min=('timestamp', 'min'),
        timestamp_max=('timestamp', 'max'),
        min_capacity=('min_capacity', 'mean'),
        target_utilization=('target_utilization', 'mean')
    ).reset_index()

    # Compute additional metrics
    q1['number_of_days'] = (q1['timestamp_max'] -
                            q1['timestamp_min']).dt.days + 1


    q1['recommended_mode'] = np.where(
        (q1['est_provisioned_cost'] < q1['current_provisioned_cost']) &
        (q1['est_provisioned_cost'] < q1['ondemand_cost']) &
        (np.divide((q1['current_provisioned_cost'] - q1['est_provisioned_cost']),
         q1['current_provisioned_cost'], where=q1['current_provisioned_cost'] != 0) > 1.5e-1),
        'Provisioned_Modify',
        np.where(
            (q1['current_provisioned_cost'] != 0) &
            (q1['current_provisioned_cost'] < q1['ondemand_cost']),
            'Provisioned',
            np.where(q1['est_provisioned_cost'] <
                     q1['ondemand_cost'], 'Provisioned', 'Ondemand')
        )
    )

    # Select relevant columns
    q1 = q1[['index_name', 'base_table_name',  'metric_name', 'est_provisioned_cost',
             'current_provisioned_cost', 'ondemand_cost', 'recommended_mode', 'number_of_days', 'min_capacity',
             'target_utilization']]

    # Merge with additional data
    q2 = dynamodb_info_df.rename(columns={'table_name': 'base_table_name'})[
        ['index_name', 'base_table_name',  'metric_name', 'min_capacity', 'target_utilization', 'throughput_mode', 'autoscaling_enabled']]
    q2 = q2.rename(columns={'min_capacity': 'current_min_capacity',
                   'target_utilization': 'current_target_utilization'})

    q2['metric_name'] = q2['metric_name'].astype(str)
    view_df = pd.merge(q1, q2, how='left', on=[
                       'base_table_name', 'index_name', 'metric_name'])
    view_df.rename(columns={'min_capacity': 'simulated_min_capacity',
                   'target_utilization': 'simulated_target_utilizatio'}, inplace=True)

    view_df['current_mode'] = np.where(
        view_df['index_name'].isin(
            q2.loc[q2['throughput_mode'] != 'Ondemand', 'index_name']),
        'Provisioned',
        'Ondemand'
    )

    view_df['status'] = np.where(
        view_df['recommended_mode'] == view_df['current_mode'], 'Optimized', 'Not Optimized')
    view_df['savings_pct'] = np.where(
        (view_df['current_mode'] == 'Ondemand') & (
            view_df['recommended_mode'] == 'Provisioned'),
        (view_df['ondemand_cost'] - view_df['est_provisioned_cost']) /
        view_df['ondemand_cost'],
        np.where(
            (view_df['current_mode'] == 'Provisioned') & (
                view_df['recommended_mode'] == 'Ondemand'),
            np.divide((view_df['current_provisioned_cost'] - view_df['ondemand_cost']),
                      view_df['current_provisioned_cost'], where=view_df['current_provisioned_cost'] != 0),
            np.where(
                (view_df['current_mode'] == 'Provisioned') & (
                    view_df['recommended_mode'] == 'Provisioned_Modify'),
                np.divide((view_df['current_provisioned_cost'] - view_df['est_provisioned_cost']),
                          view_df['current_provisioned_cost'], where=view_df['current_provisioned_cost'] != 0),
                np.nan
            )
        )
    )

    view_df['current_cost'] = np.where(
        view_df['current_mode'] == 'Provisioned',
        view_df['current_provisioned_cost'],
        view_df['ondemand_cost']
    )

    view_df['recommended_cost'] = np.where(
        view_df['recommended_mode'] == 'Ondemand',
        view_df['ondemand_cost'],
        np.where(
            (view_df['current_mode'] == 'Provisioned') &
            (view_df['recommended_mode'] == 'Provisioned'),
            view_df['current_provisioned_cost'],
            np.where(
                (view_df['current_mode'] == 'Ondemand') &
                (view_df['recommended_mode'] == 'Provisioned'),
                view_df['est_provisioned_cost'],
                view_df['est_provisioned_cost']
            )
        )
    )
    view_df.loc[view_df['current_mode'] == 'Provisioned',
                'autoscaling_enabled'] = view_df['autoscaling_enabled'].fillna(False)

    view_df = view_df.reindex(columns=['index_name', 'base_table_name', 'metric_name', 'est_provisioned_cost', 'current_provisioned_cost', 'ondemand_cost', 'recommended_mode',
                              'current_mode', 'status', 'savings_pct', 'current_cost', 'recommended_cost', 'number_of_days', 'current_min_capacity', 'simulated_min_capacity', 'current_target_utilization', 'simulated_target_utilizatio', 'autoscaling_enabled'])

    return view_df, cost_estimate_df
