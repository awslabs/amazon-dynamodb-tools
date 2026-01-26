"""
Multi-region DynamoDB table and GSI discovery system with state management.

Provides async discovery of DynamoDB tables and Global Secondary Indexes
across all AWS regions with resumable operations and progress tracking.
"""

import asyncio
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional

from ..core.state import (
    CollectionState,
    GSIMetadata,
    OperationState,
    StateManager,
    StateManagerMixin,
    TableMetadata,
    ensure_state_manager_consistency,
)
from ..database.connection import get_database_manager
from ..logging import get_logger
from ..utils.progress import ActivityIndicator, ProgressTracker, format_duration
from .client import AWSClientManager

logger = get_logger("dmetrics.aws.discovery")


class DiscoveryError(Exception):
    """Exception raised during discovery operations."""

    pass


class DiscoveryManager(StateManagerMixin):
    """
    Manages multi-region DynamoDB table and GSI discovery with state management.

    Provides resumable discovery operations with checkpoint persistence,
    progress tracking, and comprehensive error handling.
    """

    def __init__(
        self,
        aws_client_manager: Optional[AWSClientManager] = None,
        state_manager: Optional[StateManager] = None,
        checkpoint_interval: Optional[int] = None,
        credentials: Optional[Dict[str, str]] = None,
        account_id: Optional[str] = None,
    ):
        """
        Initialize discovery manager.

        Args:
            aws_client_manager: AWS client manager instance
            state_manager: State manager for checkpoints
            checkpoint_interval: Save checkpoint every N tables discovered
            credentials: AWS credentials for cross-account access
            account_id: AWS account ID for multi-account discovery
        """
        # Initialize StateManagerMixin first
        super().__init__(state_manager=state_manager)

        # Lazy import to avoid circular dependency
        from ..config import get_settings

        settings = get_settings()
        
        # Create AWS client manager with credentials if provided
        if credentials:
            self.aws_client_manager = AWSClientManager(
                aws_access_key_id=credentials.get("aws_access_key_id"),
                aws_secret_access_key=credentials.get("aws_secret_access_key"),
                aws_session_token=credentials.get("aws_session_token")
            )
        else:
            self.aws_client_manager = aws_client_manager or AWSClientManager()
        
        self.checkpoint_interval = checkpoint_interval or (
            settings.checkpoint_save_interval * 2
        )  # Discovery uses 2x interval
        self.db_manager = get_database_manager()
        self.account_id = account_id  # Store for multi-account discovery

    @ensure_state_manager_consistency
    async def discover_all_resources(
        self,
        regions: List[str],
        operation_id: Optional[str] = None,
        resume_from_checkpoint: bool = False,
    ) -> OperationState:
        """
        Discover all DynamoDB tables and GSIs across specified regions.

        Args:
            regions: List of AWS regions to discover
            operation_id: Operation ID for state tracking
            resume_from_checkpoint: Whether to resume from existing checkpoint

        Returns:
            OperationState with discovery results and progress
        """
        # Load or create operation state
        if resume_from_checkpoint and operation_id:
            state = self.state_manager.load_checkpoint(operation_id)
            if not state:
                logger.warning(
                    f"No checkpoint found for operation {operation_id}, "
                    "starting fresh discovery"
                )
                state = self.state_manager.create_operation_state(
                    "DISCOVERY", operation_id=operation_id
                )
        else:
            state = self.state_manager.create_operation_state(
                "DISCOVERY", operation_id=operation_id
            )

        # Initialize collection state if needed
        if not state.collection_state:
            state.collection_state = CollectionState(start_time=datetime.now())

        # Set up regions to discover
        if not state.collection_state.regions_to_discover:
            state.collection_state.regions_to_discover = regions.copy()

        # Get AWS account ID from STS only if not already set (for multi-account support)
        if not self.account_id:
            import boto3
            try:
                sts_client = boto3.client('sts')
                account_info = sts_client.get_caller_identity()
                self.account_id = account_info['Account']
                logger.info(f"Discovering resources for AWS account: {self.account_id}")
            except Exception as e:
                logger.warning(f"Failed to get account ID from STS: {e}")
                self.account_id = "unknown"
        else:
            logger.info(f"Using pre-configured account ID for discovery: {self.account_id}")

        logger.info(
            f"Starting discovery operation {state.operation_id}",
            regions=len(state.collection_state.regions_to_discover),
            resume=resume_from_checkpoint,
        )

        try:
            # Validate AWS credentials for all regions
            validation_result = (
                await self.aws_client_manager.validate_credentials_comprehensive(
                    state.collection_state.regions_to_discover
                )
            )

            if not validation_result.is_valid:
                error_msg = (
                    f"AWS credential validation failed: "
                    f"{validation_result.error_messages}"
                )
                state.status = "FAILED"
                state.error_message = error_msg
                self.state_manager.save_checkpoint(state)
                raise DiscoveryError(error_msg)

            # Update regions based on validation results
            valid_regions = validation_result.valid_regions
            if set(valid_regions) != set(state.collection_state.regions_to_discover):
                logger.warning(
                    f"Some regions failed validation, proceeding with valid regions: "
                    f"{valid_regions}"
                )
                state.collection_state.regions_to_discover = valid_regions

            # Perform discovery
            await self._discover_regions(state)

            # Mark operation as completed
            state.status = "COMPLETED"
            state.completion_percentage = 100.0
            state.estimated_completion = datetime.now()

            # Save final state
            self.state_manager.save_checkpoint(state)

            # Store discovered metadata in database
            await self._store_discovered_metadata(state)

            logger.info(
                f"Discovery operation {state.operation_id} completed successfully",
                total_tables=sum(
                    len(tables)
                    for tables in state.collection_state.tables_discovered.values()
                ),
                total_gsis=sum(
                    len(gsis)
                    for gsis in state.collection_state.gsis_discovered.values()
                ),
                regions=len(state.collection_state.regions_completed),
            )

            return state

        except Exception as e:
            logger.error(f"Discovery operation {state.operation_id} failed: {e}")
            state.status = "FAILED"
            state.error_message = str(e)
            self.state_manager.save_checkpoint(state)
            raise

    async def _discover_regions(self, state: OperationState) -> None:
        """Discover tables and GSIs across all regions with progress tracking."""
        collection_state = state.collection_state

        # Calculate remaining regions
        remaining_regions = [
            region
            for region in collection_state.regions_to_discover
            if region not in collection_state.regions_completed
        ]

        total_regions = len(collection_state.regions_to_discover)
        completed_regions = len(collection_state.regions_completed)

        logger.info(
            "Discovering tables across regions",
            remaining_regions=len(remaining_regions),
            completed_regions=completed_regions,
            total_regions=total_regions,
        )

        # Initialize activity indicator for overall discovery (unknown total tables)
        activity = ActivityIndicator("🔍 Discovering DynamoDB resources")

        # Set initial status if resuming
        if completed_regions > 0:
            activity.update(
                f"Resuming discovery "
                f"({completed_regions}/{total_regions} regions completed)"
            )
        else:
            activity.update(f"🔍 Starting discovery across {total_regions} regions")

        for i, region in enumerate(remaining_regions):
            region_start_time = time.time()

            # Update progress with current region
            current_total_tables = sum(
                len(tables) for tables in collection_state.tables_discovered.values()
            )

            logger.info(
                f"Discovering tables in region {region}... "
                f"({completed_regions + i + 1}/{total_regions} regions, "
                f"{current_total_tables} tables found)"
            )

            try:
                # Update activity indicator for current region
                activity.update(
                    f"🔍 Processing {region} "
                    f"({completed_regions + i + 1}/{total_regions} regions)"
                )

                # Discover tables in this region (use ProgressTracker)
                tables = await self._discover_tables_in_region(region, state)
                collection_state.tables_discovered[region] = tables

                # Discover GSIs for all tables in this region with progress tracking
                if tables:
                    gsi_progress = ProgressTracker(
                        total=len(tables),
                        description=f"Discovering GSIs in {region}",
                        show_eta=True,
                        show_percentage=True,
                        show_count=True,
                    )

                    for table in tables:
                        gsis = await self._discover_gsis_for_table(table, state)
                        table_key = f"{region}#{table.table_name}"
                        collection_state.gsis_discovered[table_key] = gsis

                        # Update GSI progress
                        gsi_progress.update(
                            1, current_item=f"GSIs for {table.table_name}"
                        )

                        # Small delay to make updates visible
                        await asyncio.sleep(0.01)

                    gsi_progress.finish(
                        f"Discovered GSIs for {len(tables)} tables in {region}"
                    )

                # Mark region as completed
                collection_state.regions_completed.append(region)

                # Update progress
                completed_regions = len(collection_state.regions_completed)
                state.completion_percentage = (completed_regions / total_regions) * 100

                # Calculate ETA
                region_duration = time.time() - region_start_time
                remaining_regions_count = total_regions - completed_regions
                if completed_regions > 0:
                    avg_time_per_region = (
                        datetime.now() - collection_state.start_time
                    ).total_seconds() / completed_regions
                    eta_seconds = avg_time_per_region * remaining_regions_count
                    state.estimated_completion = datetime.now() + timedelta(
                        seconds=eta_seconds
                    )

                # Update activity indicator with completion status
                final_total_tables = sum(
                    len(tables)
                    for tables in collection_state.tables_discovered.values()
                )
                region_gsis = sum(
                    len(gsis)
                    for key, gsis in collection_state.gsis_discovered.items()
                    if key.startswith(f"{region}#")
                )

                completion_status = (
                    f"{region}: {len(tables)} tables, {region_gsis} GSIs "
                    f"({format_duration(region_duration)}) - "
                    f"{completed_regions}/{total_regions} regions complete, "
                    f"{final_total_tables} total tables"
                )
                activity.update(completion_status)

                # Save checkpoint periodically
                if (
                    completed_regions % max(1, total_regions // 10) == 0
                ):  # Every 10% of regions
                    self.state_manager.save_checkpoint(state)
                    logger.info(
                        f"Checkpoint saved - {completed_regions}/{total_regions} "
                        "regions completed"
                    )

                logger.info(
                    f"Completed discovery for region {region} "
                    f"({len(tables)} tables, {region_gsis} GSIs, "
                    f"{region_duration:.1f}s)"
                )

            except Exception as e:
                logger.error(f"Failed to discover region {region}: {e}")
                # Update activity indicator with error
                activity.update(f"{region}: Failed ({str(e)[:50]}...)")

                # Continue with other regions but log the failure
                collection_state.failed_collections.append(
                    {
                        "region": region,
                        "error": str(e),
                        "timestamp": datetime.now().isoformat(),
                    }
                )

        # Finish activity indicator
        final_total_tables = sum(
            len(tables) for tables in collection_state.tables_discovered.values()
        )
        final_total_gsis = sum(
            len(gsis) for gsis in collection_state.gsis_discovered.values()
        )
        activity.finish(
            f"Discovery complete: {final_total_tables} tables, "
            f"{final_total_gsis} GSIs across "
            f"{len(collection_state.regions_completed)} regions"
        )

        # Final checkpoint save
        self.state_manager.save_checkpoint(state)

    async def _discover_tables_in_region(
        self, region: str, state: OperationState
    ) -> List[TableMetadata]:
        """Discover all DynamoDB tables in a specific region."""
        tables = []

        try:
            async with await self.aws_client_manager.get_async_client(
                "dynamodb", region
            ) as dynamodb:
                # First, get all table names (scanning phase)
                activity = ActivityIndicator(f"🔍 Scanning tables in {region}")
                table_names_list = []
                paginator_params = {"Limit": 100}  # Process in batches

                while True:
                    response = await dynamodb.list_tables(**paginator_params)
                    table_names = response.get("TableNames", [])

                    if not table_names:
                        break

                    table_names_list.extend(table_names)
                    activity.update(
                        f"Found {len(table_names_list)} table names in {region}..."
                    )

                    # Handle pagination
                    last_evaluated_table_name = response.get("LastEvaluatedTableName")
                    if last_evaluated_table_name:
                        paginator_params["ExclusiveStartTableName"] = (
                            last_evaluated_table_name
                        )
                    else:
                        break

                activity.finish(
                    f"Found {len(table_names_list)} tables to process in {region}"
                )

                # Now process each table with progress bar (known total)
                if table_names_list:
                    progress = ProgressTracker(
                        total=len(table_names_list),
                        description=f"📋 Processing tables in {region}",
                        show_eta=True,
                        show_percentage=True,
                        show_count=True,
                    )

                    for i, table_name in enumerate(table_names_list):
                        try:
                            table_metadata = await self._get_table_metadata(
                                table_name, region, dynamodb
                            )
                            tables.append(table_metadata)

                            # Update progress bar with table name at the end
                            progress.update(1, current_item=f"Processing {table_name}")

                            # Small delay to make updates visible
                            await asyncio.sleep(0.02)

                            # Save checkpoint every N tables
                            discovered_tables = (
                                state.collection_state.tables_discovered.values()
                            )
                            total_tables = sum(len(t) for t in discovered_tables) + len(
                                tables
                            )

                            if total_tables % self.checkpoint_interval == 0:
                                self.state_manager.save_checkpoint(state)
                                logger.debug(
                                    f"Checkpoint saved after discovering "
                                    f"{total_tables} tables"
                                )

                        except Exception as e:
                            logger.warning(
                                f"Failed to get metadata for table {table_name} "
                                f"in {region}: {e}"
                            )
                            # Continue with other tables
                            progress.update(1, current_item=f"Failed: {table_name}")

                    progress.finish(
                        f"Processed {len(tables)}/{len(table_names_list)} "
                        f"tables in {region}"
                    )

        except Exception as e:
            logger.error(f"Failed to list tables in region {region}: {e}")
            raise DiscoveryError(f"Failed to discover tables in region {region}: {e}")

        return tables

    async def _get_table_metadata(
        self, table_name: str, region: str, dynamodb_client
    ) -> TableMetadata:
        """Get detailed metadata for a DynamoDB table."""
        try:
            response = await dynamodb_client.describe_table(TableName=table_name)
            table_info = response["Table"]

            # Extract billing mode information
            billing_mode_summary = table_info.get("BillingModeSummary", {})
            billing_mode = billing_mode_summary.get("BillingMode", "PROVISIONED")

            # Extract provisioned throughput if available
            provisioned_throughput = table_info.get("ProvisionedThroughput", {})
            provisioned_read_capacity = provisioned_throughput.get("ReadCapacityUnits")
            provisioned_write_capacity = provisioned_throughput.get(
                "WriteCapacityUnits"
            )

            # Count GSIs
            gsi_count = len(table_info.get("GlobalSecondaryIndexes", []))

            # Extract table status and creation time
            table_status = table_info.get("TableStatus", "UNKNOWN")
            creation_date_time = table_info.get("CreationDateTime")

            return TableMetadata(
                table_name=table_name,
                region=region,
                billing_mode=billing_mode,
                provisioned_read_capacity=provisioned_read_capacity,
                provisioned_write_capacity=provisioned_write_capacity,
                gsi_count=gsi_count,
                status=table_status,
                last_updated=creation_date_time or datetime.now(),
            )

        except Exception as e:
            logger.error(f"Failed to describe table {table_name} in {region}: {e}")
            raise DiscoveryError(f"Failed to get metadata for table {table_name}: {e}")

    async def _discover_gsis_for_table(
        self, table: TableMetadata, state: OperationState
    ) -> List[GSIMetadata]:
        """Discover all GSIs for a specific table."""
        gsis = []

        try:
            async with await self.aws_client_manager.get_async_client(
                "dynamodb", table.region
            ) as dynamodb:
                response = await dynamodb.describe_table(TableName=table.table_name)
                table_info = response["Table"]

                gsi_list = table_info.get("GlobalSecondaryIndexes", [])

                for gsi_info in gsi_list:
                    gsi_name = gsi_info["IndexName"]

                    # Extract provisioned throughput if available
                    provisioned_throughput = gsi_info.get("ProvisionedThroughput", {})
                    provisioned_read_capacity = provisioned_throughput.get(
                        "ReadCapacityUnits"
                    )
                    provisioned_write_capacity = provisioned_throughput.get(
                        "WriteCapacityUnits"
                    )

                    # Extract projection information
                    projection = gsi_info.get("Projection", {})
                    projection_type = projection.get("ProjectionType", "ALL")

                    gsi_metadata = GSIMetadata(
                        table_name=table.table_name,
                        gsi_name=gsi_name,
                        region=table.region,
                        provisioned_read_capacity=provisioned_read_capacity,
                        provisioned_write_capacity=provisioned_write_capacity,
                        projection_type=projection_type,
                        last_updated=datetime.now(),
                    )

                    gsis.append(gsi_metadata)

        except Exception as e:
            logger.warning(
                f"Failed to discover GSIs for table {table.table_name} "
                f"in {table.region}: {e}"
            )
            # Don't raise exception, just log warning and continue

        return gsis

    async def _store_discovered_metadata(self, state: OperationState) -> None:
        """Store discovered table and GSI metadata in the database."""
        collection_state = state.collection_state

        try:
            # Prepare table metadata for batch insert
            table_records = []
            for region, tables in collection_state.tables_discovered.items():
                for table in tables:
                    table_records.append(
                        {
                            "table_name": table.table_name,
                            "region": table.region,
                            "billing_mode": table.billing_mode,
                            "provisioned_read_capacity": (
                                table.provisioned_read_capacity
                            ),
                            "provisioned_write_capacity": (
                                table.provisioned_write_capacity
                            ),
                            "last_updated": table.last_updated or datetime.now(),
                            "configuration": {
                                "gsi_count": table.gsi_count,
                                "status": table.status,
                            },
                        }
                    )

            # Prepare GSI metadata for batch insert
            gsi_records = []
            for table_key, gsis in collection_state.gsis_discovered.items():
                for gsi in gsis:
                    resource_name = f"{gsi.table_name}#{gsi.gsi_name}"
                    gsi_records.append(
                        {
                            "table_name": gsi.table_name,
                            "gsi_name": gsi.gsi_name,
                            "resource_name": resource_name,
                            "region": gsi.region,
                            "provisioned_read_capacity": gsi.provisioned_read_capacity,
                            "provisioned_write_capacity": (
                                gsi.provisioned_write_capacity
                            ),
                            "projection_type": gsi.projection_type,
                            "last_updated": gsi.last_updated or datetime.now(),
                        }
                    )

            # Batch insert table metadata
            if table_records:
                # Use INSERT with ON CONFLICT to handle duplicates
                with self.db_manager.get_connection_context() as conn:
                    for record in table_records:
                        conn.execute(
                            """
                            INSERT INTO table_metadata
                            (account_id, table_name, region, billing_mode,
                             provisioned_read_capacity,
                             provisioned_write_capacity, last_updated, configuration)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                            ON CONFLICT (account_id, table_name, region)
                            DO UPDATE SET
                                account_id = EXCLUDED.account_id,
                                billing_mode = EXCLUDED.billing_mode,
                                provisioned_read_capacity = (
                                    EXCLUDED.provisioned_read_capacity
                                ),
                                provisioned_write_capacity = (
                                    EXCLUDED.provisioned_write_capacity
                                ),
                                last_updated = EXCLUDED.last_updated,
                                configuration = EXCLUDED.configuration
                        """,
                            [
                                self.account_id,
                                record["table_name"],
                                record["region"],
                                record["billing_mode"],
                                record["provisioned_read_capacity"],
                                record["provisioned_write_capacity"],
                                record["last_updated"],
                                record["configuration"],
                            ],
                        )

                logger.info(f"Stored {len(table_records)} table metadata records")

            # Batch insert GSI metadata
            if gsi_records:
                with self.db_manager.get_connection_context() as conn:
                    for record in gsi_records:
                        conn.execute(
                            """
                            INSERT INTO gsi_metadata
                            (account_id, region, table_name, gsi_name, resource_name,
                             provisioned_read_capacity, provisioned_write_capacity,
                             projection_type, discovered_at, last_updated)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, ?)
                            ON CONFLICT (account_id, region, table_name, gsi_name) DO UPDATE SET
                                resource_name = EXCLUDED.resource_name,
                                provisioned_read_capacity = (
                                    EXCLUDED.provisioned_read_capacity
                                ),
                                provisioned_write_capacity = (
                                    EXCLUDED.provisioned_write_capacity
                                ),
                                projection_type = EXCLUDED.projection_type,
                                last_updated = EXCLUDED.last_updated
                        """,
                            [
                                self.account_id,
                                record["region"],
                                record["table_name"],
                                record["gsi_name"],
                                record["resource_name"],
                                record["provisioned_read_capacity"],
                                record["provisioned_write_capacity"],
                                record["projection_type"],
                                record["last_updated"],
                            ],
                        )

                logger.info(f"Stored {len(gsi_records)} GSI metadata records")

        except Exception as e:
            logger.error(f"Failed to store discovered metadata: {e}")
            raise DiscoveryError(f"Failed to store metadata in database: {e}")

    def get_discovery_status(self, operation_id: str) -> Optional[Dict]:
        """Get current status of a discovery operation."""
        state = self.state_manager.load_checkpoint(operation_id)
        if not state:
            return None

        collection_state = state.collection_state
        total_tables = sum(
            len(tables) for tables in collection_state.tables_discovered.values()
        )
        total_gsis = sum(
            len(gsis) for gsis in collection_state.gsis_discovered.values()
        )

        return {
            "operation_id": state.operation_id,
            "status": state.status,
            "completion_percentage": state.completion_percentage,
            "start_time": state.start_time,
            "last_checkpoint_time": state.last_checkpoint_time,
            "estimated_completion": state.estimated_completion,
            "regions_total": len(collection_state.regions_to_discover),
            "regions_completed": len(collection_state.regions_completed),
            "tables_discovered": total_tables,
            "gsis_discovered": total_gsis,
            "error_message": state.error_message,
        }

    async def resume_discovery(self, operation_id: str) -> OperationState:
        """Resume a paused or failed discovery operation."""
        state = self.state_manager.load_checkpoint(operation_id)
        if not state:
            raise DiscoveryError(f"No checkpoint found for operation {operation_id}")

        if state.status == "COMPLETED":
            logger.info(f"Discovery operation {operation_id} already completed")
            return state

        logger.info(f"Resuming discovery operation {operation_id}")
        state.status = "RUNNING"

        # Continue discovery from where we left off
        remaining_regions = [
            region
            for region in state.collection_state.regions_to_discover
            if region not in state.collection_state.regions_completed
        ]

        return await self.discover_all_resources(
            remaining_regions, operation_id=operation_id, resume_from_checkpoint=True
        )

    async def discover_account_resources(
        self,
        account_id: str,
        account_name: str,
        regions: List[str],
        credentials: Optional[Dict[str, str]] = None,
    ) -> Dict[str, any]:
        """
        Discover DynamoDB resources in a specific AWS account.
        
        This method is designed for multi-account discovery where each account
        is processed independently with its own credentials.
        
        Args:
            account_id: AWS account ID
            account_name: AWS account name (for logging)
            regions: List of AWS regions to discover
            credentials: AWS credentials for cross-account access
        
        Returns:
            Dictionary with discovery results including counts and errors
        """
        logger.info(
            f"Starting discovery for account {account_name} ({account_id})",
            regions=len(regions)
        )
        
        try:
            # Create discovery manager instance with account-specific credentials
            account_discovery = DiscoveryManager(
                credentials=credentials,
                account_id=account_id,
                state_manager=self.state_manager,
                checkpoint_interval=self.checkpoint_interval
            )
            
            # Run discovery for this account
            operation_id = f"discovery-{account_id}-{datetime.now().strftime('%Y%m%d-%H%M%S')}"
            state = await account_discovery.discover_all_resources(
                regions=regions,
                operation_id=operation_id,
                resume_from_checkpoint=False
            )
            
            # Calculate totals
            total_tables = sum(
                len(tables)
                for tables in state.collection_state.tables_discovered.values()
            )
            total_gsis = sum(
                len(gsis)
                for gsis in state.collection_state.gsis_discovered.values()
            )
            
            logger.info(
                f"Completed discovery for account {account_name} ({account_id})",
                total_tables=total_tables,
                total_gsis=total_gsis,
                regions_completed=len(state.collection_state.regions_completed)
            )
            
            return {
                "account_id": account_id,
                "account_name": account_name,
                "status": "success",
                "tables_discovered": total_tables,
                "gsis_discovered": total_gsis,
                "regions_completed": len(state.collection_state.regions_completed),
                "regions_failed": len(state.collection_state.failed_collections),
                "operation_id": operation_id
            }
            
        except Exception as e:
            logger.error(
                f"Failed to discover resources in account {account_name} ({account_id}): {e}"
            )
            return {
                "account_id": account_id,
                "account_name": account_name,
                "status": "failed",
                "tables_discovered": 0,
                "gsis_discovered": 0,
                "regions_completed": 0,
                "regions_failed": len(regions),
                "error": str(e)
            }

    def list_discovered_resources(self) -> Dict[str, any]:
        """List all discovered resources from the database."""
        try:
            # Get table counts by region
            table_query = """
                SELECT region, COUNT(*) as table_count,
                       COUNT(CASE WHEN billing_mode = 'PAY_PER_REQUEST'
                             THEN 1 END) as on_demand_count,
                       COUNT(CASE WHEN billing_mode = 'PROVISIONED'
                             THEN 1 END) as provisioned_count
                FROM table_metadata
                GROUP BY region
                ORDER BY region
            """
            table_stats = self.db_manager.execute_query(table_query)

            # Get GSI counts by region
            gsi_query = """
                SELECT region, COUNT(*) as gsi_count
                FROM gsi_metadata
                GROUP BY region
                ORDER BY region
            """
            gsi_stats = self.db_manager.execute_query(gsi_query)

            # Combine results
            region_stats = {}
            for stat in table_stats:
                region = stat["region"]
                region_stats[region] = {
                    "tables": stat["table_count"],
                    "on_demand_tables": stat["on_demand_count"],
                    "provisioned_tables": stat["provisioned_count"],
                    "gsis": 0,
                }

            for stat in gsi_stats:
                region = stat["region"]
                if region in region_stats:
                    region_stats[region]["gsis"] = stat["gsi_count"]

            # Get totals
            total_tables = sum(stats["tables"] for stats in region_stats.values())
            total_gsis = sum(stats["gsis"] for stats in region_stats.values())

            return {
                "total_tables": total_tables,
                "total_gsis": total_gsis,
                "regions": region_stats,
                "last_updated": datetime.now(),
            }

        except Exception as e:
            logger.error(f"Failed to list discovered resources: {e}")
            return {"error": str(e)}
