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

region.py core logic for the ddb_cost_tool

"""

import json
import logging
import os
import subprocess
from datetime import date, datetime

import boto3
from botocore.config import Config
from ddb_table import get_metric_dimensions, prettify_describe_json
from metrics import get_local_metric_data

logging.getLogger().setLevel(logging.INFO)
logger = logging.getLogger("region")
log = logging.StreamHandler()
logger.addHandler(log)


def create_ddb_client(region):
    """Creates a client for the specified region"""
    my_config = Config(region_name=region)
    ddb_client = boto3.client("dynamodb", config=my_config)
    return ddb_client


def create_cw_client(region):
    """Creates a client for the specified region"""
    my_config = Config(region_name=region)
    cw_client = boto3.client("cloudwatch", config=my_config)
    return cw_client


def json_serial(obj: object) -> object:
    """JSON serializer for objects not serializable by default json code

    Args:
        obj (object): JSON object

    Raises:
        TypeError: When the object is not serializable

    Returns:
        object: Returns the date objects in format ISO8601
    """

    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError(f"Type {type(obj)} not serializable")


def get_local_tables(region) -> list:
    """Retrieves Amazon DynamoDB Tablers in the account and region where this
    client session was created.

    Args:
        None

    Returns:
        list: A list of tables
    """
    try:
        ddb_client = create_ddb_client(region)
        tables = []
        response = ddb_client.list_tables()
        tables += response["TableNames"]
        while "LastEvaluatedTableName" in response:
            response = ddb_client.list_tables(
                ExclusiveStartTableName=response["LastEvaluatedTableName"]
            )
            tables += response["TableNames"]
        logger.debug("Found %s tables", len(tables))
        logger.debug(tables)
        return tables
    except Exception as e:  # pylint: disable=invalid-name, broad-exception-caught
        logger.error("There was an error retrieving the tables")
        logger.error(e)
        return []


def describe_table(table_name: str, ddb_client) -> dict:
    """Retrieves the table description as per the DescribeTable API

    Args:
        table_name (str): the table you want the descripton on

    Returns:
        dict: The result of the API call.
    """
    try:
        logger.info("Obtaining information for table %s", table_name)
        response = ddb_client.describe_table(TableName=table_name)
        return response["Table"]
    except Exception as e:  # pylint: disable=invalid-name, broad-exception-caught
        logger.error("There was an error retrieving the table description")
        logger.error(e)


def get_table_tags(table_arn: str, ddb_client) -> list:
    """Retrieves the Tags associated to this DynamoDB Table.

    Args:
        table_arn (str): Table ARN of the table you want to retrieve tags from
        client (object, optional): DynamoDB Client. Defaults to ddb_client.

    Returns:
        list: A list of tags
    """
    try:
        tags = []
        logger.debug("Obtaining information for tags for table %s", table_arn)
        response = ddb_client.list_tags_of_resource(ResourceArn=table_arn)
        tags += response["Tags"]
        while "NextToken" in response:
            response = ddb_client.list_tags_of_resource(
                ResourceArn=table_arn, NextToken=response["NextToken"]
            )
            tags += response["Tags"]
        return {"Tags": tags}
    except Exception as e:  # pylint: disable=invalid-name, broad-exception-caught
        logger.error("There was an error retrieving the tags for this table arn")
        logger.error(e)


def get_pitr_status(table_name: str, ddb_client) -> dict:
    """Retrieves the PITR status of this table

    Args:
        table_name (str): the table you want the PITR status
        client (object, optional): DynamoDB Client. Defaults to ddb_client.

    Returns:
        dict: The restul of the DescribeContinousBackup
    """
    try:
        logger.info(
            "Obtaining information for Continuous Backups for table %s", table_name
        )
        response = ddb_client.describe_continuous_backups(TableName=table_name)
        logger.debug(response)
        return response["ContinuousBackupsDescription"]
    except Exception as e:  # pylint: disable=invalid-name, broad-exception-caught
        logger.error(
            "There was an error retrieving the Continuous Backups for this table"
        )
        logger.error(e)


def get_ddb_base_object(table_name, region) -> dict:
    """This method creates a class that will be used across the script for each table.
    This object will store DynamoDB table properties to make them more accessible.

    Args:
        table_name (str): DynamoDB table name

    Raises:
        TypeError: _description_

    Returns:
        Table: DynamoDB Table Class, an object that represents a DDB table
    """
    try:
        ddb_client = create_ddb_client(region)
        table_config = json.loads(
            json.dumps(
                describe_table(table_name, ddb_client),
                default=json_serial,
            )
        )
        pitr = json.loads(
            json.dumps(get_pitr_status(table_name, ddb_client), default=json_serial)
        )
        tags = get_table_tags(table_config["TableArn"], ddb_client)
        table_config.update(pitr)
        table_config.update(tags)
        return table_config
    except Exception as e:  # pylint: disable=invalid-name, broad-exception-caught
        logger.error("There was an error retrieving the table description")
        logger.error(e)
        return None


def create_folder(path):
    """Creates a folder if it does not exists

    Args:
        path (string): OS file path
    """
    if not os.path.exists(path):
        os.mkdir(path)


def capture_metrics(table):
    """Captures metrics for a given table. The function will create folders per table and
    interval, using the DynamoDB table_id as root and 60 and 300 as subfolders.

    Args:
        table (dict): DynamoDB table metadata
    """
    region = table["TableArn"].split(":")[3]
    cloudwatch_client = create_cw_client(region)
    metric_dimensions = get_metric_dimensions(table)
    table_id = table["TableId"]
    logger.info("Working with table_id %s", table_id)
    output_dir = "output"

    create_folder(f"{output_dir}")
    create_folder(f"{output_dir}/{table_id}")
    create_folder(f"{output_dir}/{table_id}/60")
    create_folder(f"{output_dir}/{table_id}/300")

    output_file = f"{output_dir}/{table_id}/table_data.json"
    with open(output_file, "w") as outfile:
        json.dump(prettify_describe_json(table), outfile)
        outfile.close()

    for metric in metric_dimensions:
        object_name = metric[-1]["Value"]
        for period in [60, 300]:
            output_file = f"{output_dir}/{table_id}/{period}/{object_name}.json"
            with open(output_file, "w") as outfile:
                json.dump(
                    get_local_metric_data(metric, period, cloudwatch_client),
                    outfile,
                )
                outfile.close()

    # Subprocess is faster than doing this in purely python
    subprocess.call(
        [
            "tar",
            "-C",
            "output",
            "-zcvf",
            f"./{output_dir}/{table_id}.tar.gz",
            str(table_id),
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.STDOUT,
    )

    # TODO: Make this command less scary
    subprocess.call(
        ["rm", "-rf", f"./{output_dir}/{table_id}"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.STDOUT,
    )
    logger.info("Finished processing table %s", table_id)
