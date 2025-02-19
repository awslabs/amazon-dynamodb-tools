"""
DynamoDB Metrics Retrieval Module

This module provides functionality to retrieve and process CloudWatch metrics for DynamoDB tables.
It uses the AWS CloudWatch API to fetch metrics and performs custom calculations based on the provided configuration.

The main function, get_metrics_for_table, retrieves metrics for a specific DynamoDB table
within a given time range and processes them according to the provided configuration.

Dependencies:
    - logging: For error and info logging
    - simpleeval: For safely evaluating simple expressions

Note: This module is designed to be used asynchronously with aioboto3.
"""

import logging
from simpleeval import simple_eval

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def get_metrics_for_table(
    session, table_name, region, start_time, end_time, config
):
    """
    Retrieve and process CloudWatch metrics for a specific DynamoDB table.

    This function fetches metrics from CloudWatch for the specified table and time range,
    then processes and calculates additional metrics based on the provided configuration.

    Args:
        session (aioboto3.Session): An aioboto3 session object.
        table_name (str): The name of the DynamoDB table.
        region (str): The AWS region of the table.
        start_time (datetime): The start time for the metric query.
        end_time (datetime): The end time for the metric query.
        config (dict): A configuration dictionary containing metric and calculation definitions.

    Returns:
        list: A list of dictionaries, each containing metric data for a specific timestamp,
              sorted by timestamp.

    Raises:
        Exception: If there's an error fetching or processing the metrics.
    """
    async with session.client("cloudwatch", region_name=region) as cloudwatch:
        try:
            metric_data_queries = [
                {
                    "Id": metric["Id"],
                    "MetricStat": {
                        "Metric": {
                            "Namespace": "AWS/DynamoDB",
                            "MetricName": metric["MetricName"],
                            "Dimensions": [{"Name": "TableName", "Value": table_name}],
                        },
                        "Period": config["period"],
                        "Stat": metric["Stat"],
                    },
                }
                for metric in config["metrics"]
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

                # Find the metric with the most data points to use as a base
                base_metric = max(
                    metric_data_dict.values(), key=lambda x: len(x["Timestamps"])
                )

                for i, timestamp in enumerate(base_metric["Timestamps"]):
                    result = {"Timestamp": timestamp}
                    for metric in config["metrics"]:
                        metric_id = metric["Id"]
                        result[metric_id] = (
                            metric_data_dict[metric_id]["Values"][i]
                            if i < len(metric_data_dict[metric_id]["Values"])
                            else None
                        )

                    # Perform calculations
                    for calc in config["calculations"]:
                        try:
                            # Check if all required values are not None before performing calculation
                            if all(
                                result.get(var) is not None
                                for var in calc.get("required_vars", [])
                            ):
                                result[calc["id"]] = simple_eval(
                                    calc["formula"],
                                    names={**result, "period": config["period"]},
                                )
                            else:
                                result[calc["id"]] = None
                        except Exception as e:
                            logger.error(f"Error in calculation {calc['id']}: {str(e)}")
                            result[calc["id"]] = None

                    all_results.append(result)

                next_token = response.get("NextToken")
                if not next_token:
                    break

            return sorted(all_results, key=lambda x: x["Timestamp"])

        except Exception as e:
            logger.error(
                f"Error fetching metrics for table {table_name} in region {region}: {str(e)}"
            )
            return []
