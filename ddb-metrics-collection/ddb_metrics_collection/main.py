import asyncio
import argparse
import csv
from datetime import datetime, timezone, timedelta
from ddb_metrics_collection.collector import DynamoDBMetricsCollector
from ddb_metrics_collection.storage import MetricsStorage


def parse_iso8601(date_string):
    return datetime.fromisoformat(date_string).replace(tzinfo=timezone.utc)


def write_utilization_csv(low_utilization_tables, output_file=None):
    if output_file is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = f"dynamodb_utilization_{timestamp}.csv"

    with open(output_file, "w", newline="") as csvfile:
        csvwriter = csv.writer(csvfile)
        csvwriter.writerow(
            ["Region", "Table Name", "Read Utilization", "Write Utilization"]
        )

        for region, tables in low_utilization_tables.items():
            for table, read_util, write_util in tables:
                csvwriter.writerow(
                    [region, table, f"{read_util:.2f}", f"{write_util:.2f}"]
                )

    return output_file


async def main():
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
    args = parser.parse_args()

    collector = DynamoDBMetricsCollector()
    storage = MetricsStorage(storage_type=args.storage, base_path="metrics_data")

    if args.start_time is None:
        # If no start time is provided, default to the start of yesterday
        start_time = datetime.now(timezone.utc).replace(
            hour=0, minute=0, second=0, microsecond=0
        ) - timedelta(days=1)
        start_iso = start_time.isoformat()
    else:
        start_iso = args.start_time
        start_time = parse_iso8601(start_iso)

    if args.end_time is None:
        # If no end time is provided, use the end of the start day
        end_time = datetime.now(timezone.utc).replace(
            hour=23, minute=59, second=59, microsecond=999999
        )
        end_iso = end_time.isoformat()
    else:
        end_iso = args.end_time
        end_time = parse_iso8601(end_iso)

    print(f"Collecting metrics from {start_iso} to {end_iso}")

    all_metrics, low_utilization_tables = await collector.collect_all_metrics(
        start_time, end_time
    )

    # await storage.store(metrics)

    print("Metrics collected and stored successfully.")

    # Write utilization data to CSV
    csv_file = write_utilization_csv(low_utilization_tables, args.output)
    print(f"\nUtilization data written to: {csv_file}")

    total_tables = sum(len(tables) for tables in low_utilization_tables.values())
    print(
        f"\nTotal tables with low utilization (0 <= utilization <= 0.45): {total_tables}"
    )


if __name__ == "__main__":
    asyncio.run(main())
