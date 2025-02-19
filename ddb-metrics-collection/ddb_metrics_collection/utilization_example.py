"""
DynamoDB Utilization Metrics Collector

This script collects and analyzes DynamoDB table utilization metrics across all AWS regions.
It identifies tables with low utilization and generates CSV reports for both raw metrics and low-utilization tables.

Usage:
    python utilization_example.py [options]

Options:
    --start-time STR    Start time for metric collection in ISO8601 format
    --end-time STR      End time for metric collection in ISO8601 format
    --storage STR       Storage type: 'disk' or 'memory' (default: 'disk') (Experimental)
    --output STR        Custom output file name for the utilization CSV
    --config STR        Path to the metric configuration JSON file (default: 'metric_config.json')

The script performs the following main tasks:
1. Collects DynamoDB metrics for all tables across all regions
2. Identifies tables with low utilization (below 45%)
3. Generates two CSV reports:
   - A report of tables with low utilization
   - A raw metrics report for all tables

Dependencies:
    - asyncio: For asynchronous operations
    - argparse: For parsing command-line arguments
    - csv: For writing CSV files
    - json: For reading the configuration file
    - datetime: For date and time operations
    - ddb_metrics_collection.collector: Custom module for collecting DynamoDB metrics
    - ddb_metrics_collection.storage: Custom module for storing metrics (currently unused)

Functions:
    parse_iso8601(date_string: str) -> datetime:
        Parses an ISO8601 formatted date string to a datetime object with UTC timezone.

    write_csv(data: List[List], header: List[str], output_file: str) -> str:
        Writes data to a CSV file with the given header and returns the output file path.

    write_raw_metrics_csv(all_metrics: Dict, config: Dict, output_file: Optional[str] = None) -> str:
        Generates a CSV file with raw metrics data for all tables and returns the file path.

    write_utilization_csv(low_utilization_tables: Dict, output_file: Optional[str] = None) -> str:
        Generates a CSV file with low utilization table data and returns the file path.

    main() -> None:
        The main function that orchestrates the metric collection and report generation process.

Example:
    python utilization_example.py --start-time 2023-05-01T00:00:00Z --end-time 2023-05-02T00:00:00Z --config my_config.json
"""

import asyncio
import argparse
import csv
import json
from datetime import datetime, timezone, timedelta
from ddb_metrics_collection.collector import DynamoDBMetricsCollector

# from ddb_metrics_collection.storage import MetricsStorage


def parse_iso8601(date_string):
    """
    Parse an ISO8601 formatted date string to a datetime object with UTC timezone.

    Args:
        date_string (str): A date string in ISO8601 format.

    Returns:
        datetime: A datetime object representing the input date string, with UTC timezone.
    """
    return datetime.fromisoformat(date_string).replace(tzinfo=timezone.utc)


def write_csv(data, header, output_file):
    """
    Write data to a CSV file with the given header.

    Args:
        data (List[List]): The data to write to the CSV file.
        header (List[str]): The header row for the CSV file.
        output_file (str): The path to the output CSV file.

    Returns:
        str: The path to the written CSV file.
    """
    with open(output_file, "w", newline="") as csvfile:
        csvwriter = csv.writer(csvfile)
        csvwriter.writerow(header)
        csvwriter.writerows(data)
    return output_file


def write_raw_metrics_csv(all_metrics, config, output_file=None):
    """
    Generate a CSV file with raw metrics data for all tables.

    Args:
        all_metrics (Dict): A dictionary containing all collected metrics.
        config (Dict): The configuration dictionary containing metric definitions.
        output_file (str, optional): The path to the output CSV file. If None, a default name is generated.

    Returns:
        str: The path to the written CSV file.
    """
    if output_file is None:
        output_file = (
            f"dynamodb_raw_metrics_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        )

    header = ["Timestamp", "Region", "Table Name"]
    header.extend(
        f"{metric['MetricName']} ({metric['Stat']})" for metric in config["metrics"]
    )
    header.extend(calc["id"] for calc in config["calculations"])

    data = []
    for region, tables in all_metrics.items():
        for table, metrics in tables.items():
            for metric in metrics:
                row = [metric["Timestamp"], region, table]
                row.extend(metric.get(m["Id"], "N/A") for m in config["metrics"])
                row.extend(
                    metric.get(calc["id"], "N/A") for calc in config["calculations"]
                )
                data.append(row)

    return write_csv(data, header, output_file)


def write_utilization_csv(low_utilization_tables, output_file=None):
    """
    Generate a CSV file with low utilization table data.

    Args:
        low_utilization_tables (Dict): A dictionary containing tables with low utilization.
        output_file (str, optional): The path to the output CSV file. If None, a default name is generated.

    Returns:
        str: The path to the written CSV file.
    """
    if output_file is None:
        output_file = (
            f"dynamodb_utilization_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        )

    header = ["Region", "Table Name", "Read Utilization", "Write Utilization"]
    data = [
        [region, table, f"{read_util:.2f}", f"{write_util:.2f}"]
        for region, tables in low_utilization_tables.items()
        for table, read_util, write_util in tables
    ]

    return write_csv(data, header, output_file)


async def main():
    """
    The main function that orchestrates the metric collection and report generation process.

    This function:
    1. Parses command-line arguments
    2. Loads the configuration file
    3. Initializes the DynamoDBMetricsCollector
    4. Determines the time range for metric collection
    5. Collects metrics for all tables
    6. Generates CSV reports for low utilization tables and raw metrics
    """
    parser = argparse.ArgumentParser(description="Collect DynamoDB metrics")
    parser.add_argument("--start-time", type=str, help="Start time in ISO8601 format")
    parser.add_argument("--end-time", type=str, help="End time in ISO8601 format")
    parser.add_argument(
        "--storage",
        choices=["disk", "memory"],
        default="disk",
        help="Storage type (default: disk)",
    )
    parser.add_argument(
        "--output",
        type=str,
        help="Output CSV file name (default: dynamodb_utilization_YYYYMMDD_HHMMSS.csv)",
    )
    parser.add_argument(
        "--config",
        type=str,
        default="metric_config.json",
        help="Path to the metric configuration JSON file",
    )
    args = parser.parse_args()

    with open(args.config, "r") as config_file:
        config = json.load(config_file)

    collector = DynamoDBMetricsCollector(config)
    # Experimental
    # storage = MetricsStorage(storage_type=args.storage, base_path="metrics_data")

    now = datetime.now(timezone.utc)
    start_time = (
        parse_iso8601(args.start_time)
        if args.start_time
        else (now - timedelta(days=1)).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
    )
    end_time = (
        parse_iso8601(args.end_time)
        if args.end_time
        else now.replace(hour=23, minute=59, second=59, microsecond=999999)
    )

    print(f"Collecting metrics from {start_time} to {end_time}")

    all_metrics, low_utilization_tables = await collector.collect_all_metrics(
        start_time, end_time
    )

    print("Metrics collected and stored successfully.\n")

    total_tables = sum(len(tables) for tables in low_utilization_tables.values())
    print(f"Found {total_tables} tables with utilization below 45%")

    csv_file = write_utilization_csv(low_utilization_tables, args.output)
    print(f"Tables with low utilization are written to {csv_file}")

    raw_csv_file = write_raw_metrics_csv(all_metrics, config)
    print(f"\nRaw metrics data written to {raw_csv_file}")


if __name__ == "__main__":
    asyncio.run(main())
