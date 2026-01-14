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

DynamoDB Cost Optimization Tool.

"""
import argparse
import logging
import os
import shutil
import sys
from multiprocessing import Pool

import region

logging.getLogger().setLevel(logging.INFO)
logger = logging.getLogger("cost-optimization-tool")
log = logging.StreamHandler()
logger.addHandler(log)

REGIONS = [
    "all",  # All Regions"
    "us-east-2",  # US East (Ohio)
    "us-east-1",  # US East (N. Virginia)
    "us-west-1",  # US West (N. California)
    "us-west-2",  # US West (Oregon)
    "af-south-1",  # Africa (Cape Town)
    "ap-east-1",  # Asia Pacific (Hong Kong)
    "ap-south-2",  # Asia Pacific (Hyderabad)
    "ap-southeast-3",  # Asia Pacific (Jakarta)
    "ap-southeast-4",  # Asia Pacific (Melbourne)
    "ap-south-1",  # Asia Pacific (Mumbai)
    "ap-northeast-3",  # Asia Pacific (Osaka)
    "ap-northeast-2",  # Asia Pacific (Seoul)
    "ap-southeast-1",  # Asia Pacific (Singapore)
    "ap-southeast-2",  # Asia Pacific (Sydney)
    "ap-northeast-1",  # Asia Pacific (Tokyo)
    "ca-central-1",  # Canada (Central)
    "ca-west-1",  # Canada West (Calgary)
    "eu-central-1",  # Europe (Frankfurt)
    "eu-west-1",  # Europe (Ireland)
    "eu-west-2",  # Europe (London)
    "eu-south-1",  # Europe (Milan)
    "eu-west-3",  # Europe (Paris)
    "eu-south-2",  # Europe (Spain)
    "eu-north-1",  # Europe (Stockholm)
    "eu-central-2",  # Europe (Zurich)
    "il-central-1",  # Israel (Tel Aviv)
    "me-south-1",  # Middle East (Bahrain)
    "me-central-1",  # Middle East (UAE)
    "sa-east-1",  # South America (São Paulo)
    "us-gov-east-1",  # AWS GovCloud (US-East)
    "us-gov-west-1",  # AWS GovCloud (US-West)
]


def main():
    """Main function that will run when the script is run

    Raises:
        argparse.ArgumentError: validates if the region provided is valid

    Returns:
        [list]: List of regions provided by the user
    """
    parser = argparse.ArgumentParser(description="DynamoDB Cost Optimization Tool")
    parser.add_argument("--regions", nargs="+", help="Provide an array of values")
    try:
        args = parser.parse_args()

        regions = args.regions
        print(regions)
        if "all" in regions:
            REGIONS.pop(0)
            return REGIONS
        else:
            tmp = [i for i in regions if i in REGIONS]
            if len(tmp) == len(regions):
                return regions
            else:
                raise argparse.ArgumentError(
                    None, "You have one or more invalid region names"
                )
    except argparse.ArgumentError as e:
        logger.error(e)
        sys.exit(1)


def get_local_files(tables):
    """Core logic that runs the capture metrics method in parallel.

    Args:
        tables (dict): Table metadata as result of describe table and other describe* api call.
    """
    logger.info("Get metrics for 1 and 5 minutes")
    pool = Pool()  # Defaults to max CPUs available
    results = pool.map(region.capture_metrics, tables)
    pool.close()  # Prevents any more tasks being submitted to the pool
    pool.join()  # Waits for the worker process to exit, you need to call close() or terminate() before using join
    return results


def clean_env():
    """Removes the files in the output folder path"""
    try:
        output_path = "./output"
        shutil.rmtree(output_path)
        print(f"The directory '{output_path}' has been successfully deleted.")
        os.mkdir(output_path)
    except FileNotFoundError:
        print(f"The directory '{output_path}' does not exist.")
    except Exception as e:
        print(f"An error occurred while deleting the directory: {e}")


def get_ddb_table_metrics(region_name):
    """Obtains DynamoDB tables and its metadata for later use in the calculations

    Returns:
        list: An array containing all the describe table information for all the tables
    """
    logger.info("Collecting DynamoDB tables metadata in {0}:".format(region_name))
    local_tables = region.get_local_tables(region_name)
    fn_arguments = [(i, region_name) for i in local_tables]

    with Pool() as pool:
        return pool.starmap(region.get_ddb_base_object, fn_arguments)


if __name__ == "__main__":
    region_names = main()
    table_metadata = []
    clean_env()
    for region_name in region_names:
        print(region_name)
        table_metadata.extend(get_ddb_table_metrics(region_name))
    get_local_files(table_metadata)

    sys.exit(0)
