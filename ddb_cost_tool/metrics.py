"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: MIT-0
Permission is hereby granted, free of charge, to any person obtaining a copy of this
software and associated documentation files (the "Software"), to deal in the Software
without restriction, including without limitation the rights to use, copy, modify,
merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
permit persons to whom the Software is furnished to do so.
THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

"""
import json
import logging
import os
from datetime import datetime, timedelta

import boto3
import pandas as pd

session = boto3.Session()
cw_client = session.client("cloudwatch")

logging.getLogger().setLevel(logging.INFO)
logger = logging.getLogger("metrics")
log = logging.StreamHandler()
logger.addHandler(log)

METRICS_FILE = "config/metrics.json"


def format_metric_query(  # pylint: disable=dangerous-default-value
    metrics: dict,  # pylint: disable=redefined-outer-name
    dimension: dict,
    periods: list = [60, 300],  # pylint: disable=dangerous-default-value
) -> dict:
    """Helper function that formats the metrics as required by CloudWatch

    Args:
        metrics (dict): metrics dict
        dimension (dict): dimension dict
        periods (list, optional): List of periods. Defaults to [300, 3600].

    Returns:
        dict: The JSON required by CW.
    """
    metric_data_query = []
    for period in periods:
        for metric in metrics:
            metric_data_query.append(
                {
                    "Id": metric["metric_name"].lower(),
                    "MetricStat": {
                        "Metric": {
                            "Namespace": "AWS/DynamoDB",
                            "MetricName": metric["metric_name"],
                            "Dimensions": dimension,
                        },
                        "Period": period,
                        "Stat": metric["stat"],
                    },
                    "Label": metric["metric_name"],
                    "ReturnData": True,
                }
            )
    return metric_data_query


def get_metrics_file():
    with open(METRICS_FILE, "r") as jsonfile:
        data = json.load(jsonfile)

    return data["dimensionMetrics"]


def get_local_metric_data(  # pylint: disable=too-many-arguments, inconsistent-return-statements
    metric_data_query: dict,
    period: int,
    client: object = cw_client,
) -> pd.DataFrame:  # pylint: disable=too-many-arguments
    """Captures the table metrics and returns them as a dataframe ready to staore in S3,
    Json format that can be imported later.

    Args:
        get_local_metric_data (dict): Provides the dimesion that will be sent to CW
        period (int): period 60 or 300
        client (object, optional): CloudWatch Client. Defaults to cw_client.

    Returns:
        pd.DataFrame: The dataframe object with the cloudwatch metrics
    """
    try:
        results = {}
        metric_data_query = format_metric_query(
            get_metrics_file(), metric_data_query, [period]
        )
        start_date, end_date = get_start_end_date(period)
        logger.debug("Getting metric data from %s, to %s", start_date, end_date)
        logger.debug("Metric Data Query: %s", metric_data_query)
        logger.debug("Period: %s", period)
        response = client.get_metric_data(
            MetricDataQueries=metric_data_query,
            StartTime=start_date,
            EndTime=end_date,
        )
        # print(response["MetricDataResults"])
        for metric in response["MetricDataResults"]:
            results[metric["Label"]] = {"Timestamps": [], "Values": []}
            results[metric["Label"]]["Values"] += metric["Values"]
            results[metric["Label"]]["Timestamps"] += metric["Timestamps"]
        while "NextToken" in response:
            response = client.get_metric_data(
                MetricDataQueries=metric_data_query,
                StartTime=start_date,
                EndTime=end_date,
                NextToken=response["NextToken"],
            )
            for metric in response["MetricDataResults"]:
                results[metric["Label"]]["Values"] += metric["Values"]
                results[metric["Label"]]["Timestamps"] += metric["Timestamps"]

        time_series_pd = []
        for res, data in results.items():
            time_series_pd.append(
                pd.Series(
                    data["Values"],
                    name=res,
                    dtype="float64",
                    index=data["Timestamps"],
                )
            )

        result = pd.concat([i for i in time_series_pd], axis=1)
        # result.index = pd.to_datetime(result.index)
        # https://github.com/pandas-dev/pandas/issues/39537
        # result.index = pd.to_datetime(result.index).tz_convert("UTC")
        result = result.fillna(0)

        if result.empty:
            return_value = None
        else:
            return_value = result.to_json(orient="table")
            # return_value = result.to_json()
        return return_value
    except client.exceptions.InvalidParameterValueException as exception:
        logger.exception(exception)
        # To Do
        # pass
    except client.exceptions.InternalServiceFault as exception:
        logger.exception(exception)
        # To Do
        # pass


def get_start_end_date(period: int) -> str:
    """Obtain the start and end date for the API calls,
    for 1 minute, return the last 15 days, and for 5 minutes, return the last 63.

    Args:
        period (int): 60 | 300

    Returns:
        str: Start and end date in isoformat, start_date, end_date
    """
    # TODO - Update this to 15 days for 1 minute
    base = {60: 15, 300: 63}
    now = datetime.now()

    def round_down_to_nearest_multiple(value, multiple):
        """Round down to the nearest multiple of a number"""
        return value - (value % multiple)

    # Round down to the nearest minute based on the period (in multiples of 5)
    # This improves CW response times.
    rounded_minute = round_down_to_nearest_multiple(now.minute, int(period / 60))
    end_date = now.replace(minute=rounded_minute, second=0, microsecond=0)

    time_delta = timedelta(days=base[period])
    start_date = end_date - time_delta

    return start_date.isoformat(), end_date.isoformat()
