import aioboto3
from datetime import datetime, timedelta
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def get_metrics_for_table(session, table_name, region, start_time, end_time):
    async with session.client("cloudwatch", region_name=region) as cloudwatch:
        try:
            metric_data_queries = [
                {
                    "Id": "consumed_read",
                    "MetricStat": {
                        "Metric": {
                            "Namespace": "AWS/DynamoDB",
                            "MetricName": "ConsumedReadCapacityUnits",
                            "Dimensions": [{"Name": "TableName", "Value": table_name}],
                        },
                        "Period": 300,
                        "Stat": "Average",
                    },
                },
                {
                    "Id": "provisioned_read",
                    "MetricStat": {
                        "Metric": {
                            "Namespace": "AWS/DynamoDB",
                            "MetricName": "ProvisionedReadCapacityUnits",
                            "Dimensions": [{"Name": "TableName", "Value": table_name}],
                        },
                        "Period": 300,
                        "Stat": "Average",
                    },
                },
                {
                    "Id": "read_utilization",
                    "Expression": "consumed_read / provisioned_read",
                    "Label": "Read Capacity Utilization",
                },
                {
                    "Id": "consumed_write",
                    "MetricStat": {
                        "Metric": {
                            "Namespace": "AWS/DynamoDB",
                            "MetricName": "ConsumedWriteCapacityUnits",
                            "Dimensions": [{"Name": "TableName", "Value": table_name}],
                        },
                        "Period": 300,
                        "Stat": "Average",
                    },
                },
                {
                    "Id": "provisioned_write",
                    "MetricStat": {
                        "Metric": {
                            "Namespace": "AWS/DynamoDB",
                            "MetricName": "ProvisionedWriteCapacityUnits",
                            "Dimensions": [{"Name": "TableName", "Value": table_name}],
                        },
                        "Period": 300,
                        "Stat": "Average",
                    },
                },
                {
                    "Id": "write_utilization",
                    "Expression": "consumed_write / provisioned_write",
                    "Label": "Write Capacity Utilization",
                },
            ]

            all_results = []
            next_token = None

            while True:
                if next_token:
                    response = await cloudwatch.get_metric_data(
                        MetricDataQueries=metric_data_queries,
                        StartTime=start_time,
                        EndTime=end_time,
                        NextToken=next_token,
                    )
                else:
                    response = await cloudwatch.get_metric_data(
                        MetricDataQueries=metric_data_queries,
                        StartTime=start_time,
                        EndTime=end_time,
                    )

                metric_data_dict = {
                    result["Id"]: result for result in response["MetricDataResults"]
                }

                base_metric = max(
                    metric_data_dict.values(), key=lambda x: len(x["Timestamps"])
                )

                for i, timestamp in enumerate(base_metric["Timestamps"]):
                    result = {"Timestamp": timestamp}
                    for metric_id, metric_data in metric_data_dict.items():
                        if i < len(metric_data["Values"]):
                            result[metric_id] = metric_data["Values"][i]
                        else:
                            result[metric_id] = None
                    all_results.append(result)

                next_token = response.get("NextToken")
                if not next_token:
                    break

            return sorted(all_results, key=lambda x: x["Timestamp"])

        except Exception as e:
            logger.error(
                f"Error fetching metrics for table {table_name} in region {region}: {str(e)}"
            )
            return []  # Return an empty list if we encounter any errors
