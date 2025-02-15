import asyncio
import aioboto3
from tqdm import tqdm
from .metrics import get_metrics_for_table
import logging

MAX_CONCURRENT_REGIONS = 10
MAX_CONCURRENT_TABLE_CHECKS = 1000

logging.basicConfig(level=logging.ERROR)
logger = logging.getLogger(__name__)


class DynamoDBMetricsCollector:
    def __init__(self):
        self.session = aioboto3.Session()
        self.table_semaphore = asyncio.Semaphore(MAX_CONCURRENT_TABLE_CHECKS)
        self.region_semaphore = asyncio.Semaphore(MAX_CONCURRENT_REGIONS)

    async def get_all_regions(self):
        async with self.session.client("ec2") as ec2:
            response = await ec2.describe_regions()
            return [region["RegionName"] for region in response["Regions"]]

    async def get_tables_in_region(self, region):
        async with self.session.client("dynamodb", region_name=region) as dynamodb:
            tables = []
            paginator = dynamodb.get_paginator("list_tables")
            page_count = 0
            try:
                async for page in paginator.paginate():
                    page_count += 1
                    new_tables = page.get("TableNames", [])
                    tables.extend(new_tables)
                    # logger.info(
                    #     f"Region {region}: Retrieved {len(new_tables)} tables (Page {page_count})"
                    # )

                # logger.info(f"Region {region}: Total tables retrieved: {len(tables)}")
                return tables
            except Exception as e:
                logger.error(f"Error retrieving tables for region {region}: {str(e)}")
                return []

    async def get_table_billing_mode(self, region, table_name):
        async with self.table_semaphore:
            async with self.session.client("dynamodb", region_name=region) as dynamodb:
                try:
                    response = await dynamodb.describe_table(TableName=table_name)
                    return table_name, response["Table"].get(
                        "BillingModeSummary", {}
                    ).get("BillingMode", "PROVISIONED")
                except Exception as e:
                    logger.error(
                        f"Error getting billing mode for table {table_name} in region {region}: {str(e)}"
                    )
                    return table_name, None

    async def count_total_tables(self):
        regions = await self.get_all_regions()
        total_tables = 0
        for region in regions:
            tables = await self.get_tables_in_region(region)
            total_tables += len(tables)
        return total_tables

    async def collect_table_metrics(self, region, table, start_date, end_date=None):
        try:
            metrics = await get_metrics_for_table(
                self.session, table, region, start_date, end_date
            )
            return {table: metrics}
        except Exception as e:
            print(
                f"Error collecting metrics for table {table} in region {region}: {str(e)}"
            )
            return {table: None}

    async def collect_region_metrics(self, region, start_date, end_date=None):
        tables = await self.get_tables_in_region(region)
        tasks = [
            self.collect_table_metrics(region, table, start_date, end_date)
            for table in tables
        ]
        table_metrics = await asyncio.gather(*tasks)
        return {region: {k: v for d in table_metrics for k, v in d.items()}}

    async def get_provisioned_tables(self, region):
        tables = await self.get_tables_in_region(region)
        tasks = [self.get_table_billing_mode(region, table) for table in tables]
        results = await asyncio.gather(*tasks)
        provisioned_tables = [
            table for table, billing_mode in results if billing_mode == "PROVISIONED"
        ]

        # logger.info(
        #     f"Region {region}: Found {len(provisioned_tables)} provisioned tables out of {len(tables)} total tables."
        # )

        return provisioned_tables

    async def get_tables_and_metrics(self, region, start_time, end_time):
        async with self.region_semaphore:
            provisioned_tables = await self.get_provisioned_tables(region)
            tasks = [
                get_metrics_for_table(self.session, table, region, start_time, end_time)
                for table in provisioned_tables
            ]
            table_metrics = await asyncio.gather(*tasks)
            return region, provisioned_tables, table_metrics

    async def collect_all_metrics(self, start_time, end_time):
        all_metrics = {}
        low_utilization_tables = {}
        regions = await self.get_all_regions()

        # Get the total number of provisioned tables across all regions
        total_provisioned_tables = 0
        provisioned_tables_by_region = {}
        for region in regions:
            provisioned_tables = await self.get_provisioned_tables(region)
            total_provisioned_tables += len(provisioned_tables)
            provisioned_tables_by_region[region] = provisioned_tables

        # Create tasks for all regions
        region_tasks = [
            self.get_tables_and_metrics(region, start_time, end_time)
            for region in regions
        ]

        # Now collect metrics with progress bar
        with tqdm(total=total_provisioned_tables, desc="Collecting metrics") as pbar:
            for future in asyncio.as_completed(region_tasks):
                region, tables, table_metrics = await future
                all_metrics[region] = {}
                low_utilization_tables[region] = []

                for table, metrics in zip(tables, table_metrics):
                    if metrics:  # Only process if we got metrics
                        all_metrics[region][table] = metrics
                        avg_read_util = sum(
                            m["read_utilization"]
                            for m in metrics
                            if m["read_utilization"] is not None
                        ) / len(metrics)
                        avg_write_util = sum(
                            m["write_utilization"]
                            for m in metrics
                            if m["write_utilization"] is not None
                        ) / len(metrics)
                        if 0 <= avg_read_util <= 0.45 or 0 <= avg_write_util <= 0.45:
                            low_utilization_tables[region].append(
                                (table, avg_read_util, avg_write_util)
                            )
                    pbar.update(1)

        return all_metrics, low_utilization_tables
