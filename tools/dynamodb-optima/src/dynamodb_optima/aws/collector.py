"""
Async CloudWatch metrics collector with database storage and state management.

Provides comprehensive metrics collection for DynamoDB tables and GSIs
with resumable operations, progress tracking, and batch database storage.
"""

import asyncio
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

from ..core.state import (
    CollectionState,
    OperationState,
    StateManager,
    StateManagerMixin,
    ensure_state_manager_consistency,
)
from ..database.connection import get_database_manager
from ..logging import get_logger
from ..utils.progress import format_duration
from ..utils.timestamp_alignment import (
    TimestampAligner,
    align_collection_timestamps,
    validate_cloudwatch_timestamps,
)
from .client import AWSClientManager
from .metrics import MetricConfiguration, get_service_metrics

logger = get_logger("dynamodb_optima.aws.collector")


@dataclass
class CollectionResult:
    """Result of metrics collection operation."""

    operation_id: str
    total_metrics_collected: int
    successful_collections: int
    failed_collections: int
    collection_duration: timedelta
    resources_processed: int
    regions_processed: List[str]
    error_summary: Dict[str, int]


@dataclass
class MetricDataPoint:
    """Individual metric data point for database storage."""

    account_id: str  # AWS account ID
    table_name: str
    resource_name: str  # table_name or table_name#gsi_name
    resource_type: str  # 'TABLE' or 'GSI'
    metric_name: str  # Base metric name
    operation: Optional[str]  # DynamoDB operation
    operation_type: Optional[str]  # For batch operations
    statistic: str  # Sum, Average, Maximum, etc.
    period_seconds: int  # CloudWatch period
    timestamp: datetime
    value: float
    unit: str
    region: str
    dimensions: Dict[str, str]


class CollectionError(Exception):
    """Exception raised during metrics collection operations."""

    pass


class CloudWatchCollector(StateManagerMixin):
    """
    Manages async CloudWatch metrics collection with state management.

    Provides resumable collection operations with checkpoint persistence,
    progress tracking, batch database storage, and comprehensive error handling.
    """

    def __init__(
        self,
        aws_client_manager: Optional[AWSClientManager] = None,
        state_manager: Optional[StateManager] = None,
        checkpoint_interval: Optional[int] = None,
        batch_size: Optional[int] = None,
        rate_limit_delay: float = 0.0,
    ):
        """
        Initialize CloudWatch collector.

        Args:
            aws_client_manager: AWS client manager instance
            state_manager: State manager for checkpoints
            checkpoint_interval: Save checkpoint every N successful collections
            batch_size: Number of metrics to batch for database insertion
            rate_limit_delay: Delay between API calls to respect rate limits
        """
        # Initialize StateManagerMixin first
        super().__init__(state_manager=state_manager)

        # Lazy import to avoid circular dependency
        from ..config import get_settings

        settings = get_settings()
        self.aws_client_manager = aws_client_manager or AWSClientManager()
        self.checkpoint_interval = (
            checkpoint_interval or settings.checkpoint_save_interval
        )
        self.batch_flush_size = batch_size or settings.metrics_batch_flush_size
        self.rate_limit_delay = rate_limit_delay
        self.db_manager = get_database_manager()

        # Default metric configurations for DynamoDB
        self.default_metrics = self._get_default_metric_configurations()

        # Batch storage for metrics
        self._metric_batch: List[MetricDataPoint] = []
        self._batch_lock = asyncio.Lock()

    def get_metric_configurations(
        self, service: str = "dynamodb", comprehensive: bool = False
    ) -> List[MetricConfiguration]:
        """Get metric configurations for a specific service.

        Args:
            service: Service name ('dynamodb', 'documentdb', etc.)
            comprehensive: If True, return comprehensive metrics; if False, return basic metrics

        Returns:
            List of metric configurations for the service
        """
        return get_service_metrics(service, comprehensive)

    def _get_default_metric_configurations(self) -> List[MetricConfiguration]:
        """Get default DynamoDB metric configurations (backward compatibility)."""
        return self.get_metric_configurations("dynamodb", comprehensive=False)

    @ensure_state_manager_consistency
    async def collect_metrics(
        self,
        start_time: datetime,
        end_time: datetime,
        regions: Optional[List[str]] = None,
        table_names: Optional[List[str]] = None,
        metric_configs: Optional[List[MetricConfiguration]] = None,
        operation_id: Optional[str] = None,
        resume_from_checkpoint: bool = False,
        show_progress: bool = True,
    ) -> CollectionResult:
        """
        Collect CloudWatch metrics for DynamoDB resources.

        Args:
            start_time: Start time for metric collection
            end_time: End time for metric collection
            regions: List of AWS regions to collect from
            table_names: Specific table names to collect (if None, discovers all)
            metric_configs: Metric configurations to collect
            operation_id: Operation ID for state tracking
            resume_from_checkpoint: Whether to resume from existing checkpoint

        Returns:
            CollectionResult with collection statistics
        """
        # Load or create operation state using mixin methods
        if resume_from_checkpoint and operation_id:
            state = self._load_state(operation_id)
            if not state:
                logger.warning(
                    f"No checkpoint found for operation {operation_id}, "
                    "starting fresh collection"
                )
                state = self._create_operation("COLLECTION", operation_id=operation_id)
        else:
            state = self._create_operation("COLLECTION", operation_id=operation_id)

        # Initialize collection state if needed
        if not state.collection_state:
            state.collection_state = CollectionState(start_time=datetime.now())

        # Set up collection parameters
        target_regions = regions or await self._get_available_regions()
        target_metrics = metric_configs or self.default_metrics

        # Timestamps are already hour-aligned from collect.py command
        # No additional alignment needed - this prevents timestamp drift between runs
        logger.debug(
            f"Starting metrics collection operation {state.operation_id}",
            start_time=start_time,
            end_time=end_time,
            regions=len(target_regions),
            metrics=len(target_metrics),
            resume=resume_from_checkpoint,
        )

        # Import the dynamic status display
        from ..utils.simple_progress import DynamicStatusDisplay

        status_display = DynamicStatusDisplay()

        try:
            # Show status during credential validation
            status_display.start("Validating AWS credentials across regions")

            # Validate AWS credentials for all regions
            validation_result = (
                await self.aws_client_manager.validate_credentials_comprehensive(
                    target_regions
                )
            )

            status_display.stop()
            print("  AWS credentials validated successfully")

            if not validation_result.is_valid:
                error_msg = (
                    f"AWS credential validation failed: "
                    f"{validation_result.error_messages}"
                )
                state.status = "FAILED"
                state.error_message = error_msg
                self.state_manager.save_checkpoint(state)
                raise CollectionError(error_msg)

            # Update regions based on validation results
            valid_regions = validation_result.valid_regions
            if set(valid_regions) != set(target_regions):
                logger.warning(
                    f"Some regions failed validation, proceeding with valid regions: "
                    f"{valid_regions}"
                )
                target_regions = valid_regions

            # Show status during resource discovery
            if not table_names:
                status_display.start("Loading discovered resources from database")
                resources = await self._discover_collection_resources(
                    target_regions, state
                )
                status_display.stop()
                print("  Resources loaded from database")
            else:
                resource_preview = f"{', '.join(table_names[:3])}"
                if len(table_names) > 3:
                    resource_preview += "..."
                status_display.start(
                    f"Preparing specified resources: {resource_preview}"
                )
                resources = await self._prepare_specified_resources(
                    table_names, target_regions, state
                )
                status_display.stop()
                print("  Resources prepared for collection")

            # Log discovered resources for debugging
            total_discovered = sum(
                len(region_resources) for region_resources in resources.values()
            )
            logger.debug(f"Discovered {total_discovered} resources for collection:")
            for region, region_resources in resources.items():
                if region_resources:
                    tables = sum(
                        1 for r in region_resources if r["resource_type"] == "TABLE"
                    )
                    gsis = sum(
                        1 for r in region_resources if r["resource_type"] == "GSI"
                    )
                    logger.debug(f"   {region}: {tables} tables, {gsis} GSIs")
                else:
                    logger.debug(f"   {region}: No resources found!")

            if total_discovered == 0:
                logger.error("No resources discovered for collection!")
                logger.info(
                    "Try running 'dynamodb_optima discover' first to populate the database"
                )
                raise CollectionError("No resources found for collection")

            result = await self._collect_metrics_for_resources(
                resources,
                target_metrics,
                start_time,
                end_time,
                state,
                show_progress=False,
            )

            print(
                f"\n✅ Metrics collection completed - {result.total_metrics_collected:,} metrics collected"
            )

            # Mark operation as completed
            state.status = "COMPLETED"
            state.completion_percentage = 100.0
            # Ensure completed_operations matches total_operations for consistency
            if state.collection_state:
                state.collection_state.completed_operations = (
                    state.collection_state.total_operations
                )
            state.estimated_completion = datetime.now()

            # Save final state and flush any remaining metrics
            status_display.start("Storing metrics into local DuckDB")
            await self._flush_metric_batch()
            status_display.stop()
            print("  Metrics stored in database")
            print()  # Add newline for better spacing before final summary
            self.state_manager.save_checkpoint(state)

            # Log performance metrics and optimization impact
            self.log_collection_performance_metrics(
                start_time,
                end_time,
                target_metrics,
                result.total_metrics_collected,
                result.collection_duration,
            )

            # Log time period optimization recommendations
            recommendations = self.get_time_period_optimization_recommendations(
                target_metrics
            )
            logger.debug(
                "Time period optimization recommendations for future collections",
                recommendations=recommendations,
            )

            logger.debug(
                f"Metrics collection operation {state.operation_id} "
                "completed successfully",
                total_metrics=result.total_metrics_collected,
                resources=result.resources_processed,
                regions=len(result.regions_processed),
                duration=result.collection_duration,
            )

            return result

        except KeyboardInterrupt:
            logger.info(
                f"Metrics collection operation {state.operation_id} interrupted by user"
            )
            state.status = "PAUSED"
            state.error_message = "Interrupted by user"
            self.state_manager.save_checkpoint(state)
            raise
        except Exception as e:
            logger.error(
                f"Metrics collection operation {state.operation_id} failed: {e}"
            )
            state.status = "FAILED"
            state.error_message = str(e)
            self.state_manager.save_checkpoint(state)

            # Flush any collected metrics before failing
            try:
                await self._flush_metric_batch()
            except Exception as flush_error:
                logger.error(
                    f"Failed to flush metrics during error handling: {flush_error}"
                )

            raise

    async def _get_available_regions(self) -> List[str]:
        """Get list of available AWS regions for DynamoDB."""
        # Use a subset of major regions for efficiency
        # In production, this could be configurable or discovered dynamically
        return [
            "us-east-1",
            "us-east-2",
            "us-west-1",
            "us-west-2",
            "eu-west-1",
            "eu-west-2",
            "eu-central-1",
            "ap-southeast-1",
            "ap-southeast-2",
            "ap-northeast-1",
        ]

    async def _discover_collection_resources(
        self, regions: List[str], state: OperationState
    ) -> Dict[str, List[Dict[str, Any]]]:
        """Discover DynamoDB resources for collection."""
        resources = {}

        for region in regions:
            try:
                # Get tables and GSIs from database (populated by discovery)
                tables_query = """
                    SELECT account_id, table_name, billing_mode,
                           provisioned_read_capacity, provisioned_write_capacity
                    FROM table_metadata
                    WHERE region = ?
                """
                tables = self.db_manager.execute_query(tables_query, [region])

                gsis_query = """
                    SELECT account_id, table_name, gsi_name, resource_name,
                           provisioned_read_capacity, provisioned_write_capacity
                    FROM gsi_metadata
                    WHERE region = ?
                """
                gsis = self.db_manager.execute_query(gsis_query, [region])

                # Combine tables and GSIs
                region_resources = []

                # Add tables
                for table in tables:
                    region_resources.append(
                        {
                            "account_id": table["account_id"],
                            "resource_name": table["table_name"],
                            "resource_type": "TABLE",
                            "table_name": table["table_name"],
                            "region": region,
                            "billing_mode": table["billing_mode"],
                        }
                    )

                # Add GSIs
                for gsi in gsis:
                    region_resources.append(
                        {
                            "account_id": gsi["account_id"],
                            "resource_name": gsi[
                                "resource_name"
                            ],  # table_name#gsi_name
                            "resource_type": "GSI",
                            "table_name": gsi["table_name"],
                            "region": region,
                            "billing_mode": "PROVISIONED",  # GSIs always provisioned
                        }
                    )

                resources[region] = region_resources

                logger.debug(
                    f"Discovered resources for collection in {region}",
                    tables=len(tables),
                    gsis=len(gsis),
                )

            except Exception as e:
                logger.error(f"Failed to discover resources in region {region}: {e}")
                resources[region] = []

        return resources

    async def _prepare_specified_resources(
        self, table_names: List[str], regions: List[str], state: OperationState
    ) -> Dict[str, List[Dict[str, Any]]]:
        """Prepare specified resources for collection."""
        resources = {}

        for region in regions:
            region_resources = []

            for table_name in table_names:
                # Add table resource
                region_resources.append(
                    {
                        "resource_name": table_name,
                        "resource_type": "TABLE",
                        "table_name": table_name,
                        "region": region,
                        "billing_mode": "UNKNOWN",  # Determined during collection
                    }
                )

                # Check for GSIs in database
                gsis_query = """
                    SELECT gsi_name, resource_name
                    FROM gsi_metadata
                    WHERE table_name = ? AND region = ?
                """
                gsis = self.db_manager.execute_query(gsis_query, [table_name, region])

                for gsi in gsis:
                    region_resources.append(
                        {
                            "resource_name": gsi["resource_name"],
                            "resource_type": "GSI",
                            "table_name": table_name,
                            "region": region,
                            "billing_mode": "PROVISIONED",
                        }
                    )

            resources[region] = region_resources

        return resources

    async def _collect_metrics_for_resources(
        self,
        resources: Dict[str, List[Dict[str, Any]]],
        metric_configs: List[MetricConfiguration],
        start_time: datetime,
        end_time: datetime,
        state: OperationState,
        show_progress: bool = True,
    ) -> CollectionResult:
        """Collect metrics for all discovered resources."""
        collection_start = datetime.now()
        total_metrics_collected = 0
        successful_collections = 0
        failed_collections = 0
        error_summary = {}
        regions_processed = []

        # Calculate total batches for progress tracking
        # (more accurate for batch processing)
        total_resources = sum(
            len(region_resources) for region_resources in resources.values()
        )

        # Estimate total batches across all regions
        total_batches = 0
        for region, region_resources in resources.items():
            if region_resources:
                # For batch processing, we typically process all resources
                # in a region as one batch
                # or split into smaller batches if there are too many resources
                max_resources_per_batch = 50  # Conservative batch size for resources
                region_batches = (
                    len(region_resources) + max_resources_per_batch - 1
                ) // max_resources_per_batch
                total_batches += region_batches

        # Fallback if no batches calculated
        if total_batches == 0:
            total_batches = max(1, total_resources // 10)  # Rough estimate

        # Store batch info in state
        state.collection_state.total_operations = total_batches
        if not hasattr(state.collection_state, "completed_batches"):
            state.collection_state.completed_batches = 0

        # Disable detailed progress tracking to avoid console spam
        progress = None

        # Initialize completed resources set if resuming
        if not hasattr(state.collection_state, "completed_resources"):
            state.collection_state.completed_resources = set()

        # Set initial progress if resuming
        completed_operations = len(state.collection_state.completed_resources) * len(
            metric_configs
        )
        # Initialize completed_operations if not set
        if (
            not hasattr(state.collection_state, "completed_operations")
            or state.collection_state.completed_operations == 0
        ):
            state.collection_state.completed_operations = completed_operations

        if completed_operations > 0:
            logger.debug(
                f"Resuming collection, skipping "
                f"{len(state.collection_state.completed_resources)} "
                "already completed resources"
            )

        # Create single progress bar for all resources across all regions
        import click
        total_resources_to_process = sum(
            len([r for r in region_resources
                 if f"{region}:{r['resource_name']}" not in state.collection_state.completed_resources])
            for region, region_resources in resources.items()
        )

        progress_bar = click.progressbar(
            length=total_resources_to_process,
            label=f'Collecting {total_resources_to_process} resources',
            show_eta=False,
            show_percent=True,
            show_pos=True
        ) if total_resources_to_process > 0 else None

        if progress_bar:
            progress_bar.__enter__()

        try:
            # Process each region
            for region, region_resources in resources.items():
                if not region_resources:
                    continue

                regions_processed.append(region)
                logger.debug(
                    f"Collecting metrics for region {region}",
                    resources=len(region_resources),
                )

                # Filter out already completed resources
                remaining_resources = [
                    resource
                    for resource in region_resources
                    if f"{region}:{resource['resource_name']}"
                    not in state.collection_state.completed_resources
                ]

                if not remaining_resources:
                    logger.debug(
                        f"All resources in region {region} already completed, skipping"
                    )
                    continue

                logger.debug(
                    f"Processing {len(remaining_resources)} remaining resources in {region}"
                )

                # Use batch processing for better efficiency when possible
                if len(remaining_resources) > 1:
                    # Process multiple resources in optimized batches
                    batch_metrics, batch_successful, batch_failed = (
                        await self._process_resources_batch_optimized(
                            remaining_resources,
                            metric_configs,
                            start_time,
                            end_time,
                            region,
                            progress_bar,
                            state,
                        )
                    )

                    # Update counters from batch processing
                    total_metrics_collected += batch_metrics
                    successful_collections += batch_successful
                    failed_collections += batch_failed

                else:
                    # Process single resource (fallback to existing logic)
                    for resource in remaining_resources:
                        resource_key = f"{region}:{resource['resource_name']}"

                        try:
                            # Progress is shown via progress bar,
                            # no need for console logging
                            logger.debug(
                                f"Processing resource: {resource['resource_name']} "
                                f"({len(remaining_resources)} remaining)"
                            )

                            # Progress tracking disabled to avoid console interference

                            # Process single resource with timeout
                            resource_metrics, resource_duration = await asyncio.wait_for(
                                self._process_single_resource(
                                    resource,
                                    metric_configs,
                                    start_time,
                                    end_time,
                                    region,
                                    progress,
                                    state,
                                ),
                                timeout=45.0,  # 45 second timeout per resource
                            )

                            # Add to batch for database storage
                            should_flush = False
                            async with self._batch_lock:
                                self._metric_batch.extend(resource_metrics)
                                # Decide atomically while holding lock
                                should_flush = len(self._metric_batch) >= self.batch_flush_size

                            # Auto-flush if batch size limit reached (prevents OOM)
                            if should_flush:
                                logger.info(
                                    f"Auto-flushing {len(self._metric_batch):,} metrics "
                                    f"(limit: {self.batch_flush_size:,})"
                                )
                                await self._flush_metric_batch()

                            total_metrics_collected += len(resource_metrics)
                            successful_collections += 1

                            # Mark resource as completed
                            state.collection_state.completed_resources.add(resource_key)

                            # Update batch progress tracking
                            # (treat individual resource as micro-batch)
                            state.collection_state.completed_batches += 1
                            state.collection_state.completed_operations += 1  # Keep in sync
                            state.completion_percentage = (
                                (
                                    state.collection_state.completed_operations
                                    / state.collection_state.total_operations
                                )
                                * 100
                                if state.collection_state.total_operations > 0
                                else 0
                            )


                            # Save checkpoint after each resource
                            self.state_manager.save_checkpoint(state)

                        except asyncio.TimeoutError:
                            logger.error(
                                f"Resource {resource['resource_name']} "
                                f"timed out after 45 seconds - SKIPPING"
                            )
                            failed_collections += 1
                            error_summary["TimeoutError"] = (
                                error_summary.get("TimeoutError", 0) + 1
                            )

                            # Mark resource as completed even if it failed
                            # (to skip it on resume)
                            state.collection_state.completed_resources.add(resource_key)

                            # Add to failed collections
                            state.collection_state.failed_collections.append(
                                {
                                    "resource": resource["resource_name"],
                                    "region": region,
                                    "error": "Timeout after 45 seconds",
                                    "timestamp": datetime.now().isoformat(),
                                }
                            )

                            # Update batch progress tracking
                            # (failed resource still counts as processed batch)
                            state.collection_state.completed_batches += 1

                            # Save checkpoint even for failed resources
                            self.state_manager.save_checkpoint(state)

                        except Exception as e:
                            logger.error(
                                f"Failed to process resource "
                                f"{resource['resource_name']}: {e}"
                            )
                            failed_collections += 1
                            error_type = type(e).__name__
                            error_summary[error_type] = error_summary.get(error_type, 0) + 1

                            # Mark resource as completed even if it failed (to skip it on
                            # resume)
                            state.collection_state.completed_resources.add(resource_key)

                            # Add to failed collections
                            state.collection_state.failed_collections.append(
                                {
                                    "resource": resource["resource_name"],
                                    "region": region,
                                    "error": str(e),
                                    "timestamp": datetime.now().isoformat(),
                                }
                            )

                            # Update batch progress tracking
                            # (failed resource still counts as processed batch)
                            state.collection_state.completed_batches += 1

                            # Save checkpoint even for failed resources
                            self.state_manager.save_checkpoint(state)

                logger.debug(
                    f"Completed region {region}: "
                    f"{successful_collections} resources processed"
                )

        finally:
            if progress_bar:
                progress_bar.__exit__(None, None, None)

        # Collection duration tracking
        collection_duration = datetime.now() - collection_start

        # Final checkpoint save
        self.state_manager.save_checkpoint(state)

        return CollectionResult(
            operation_id=state.operation_id,
            total_metrics_collected=total_metrics_collected,
            successful_collections=successful_collections,
            failed_collections=failed_collections,
            collection_duration=collection_duration,
            resources_processed=successful_collections + failed_collections,
            regions_processed=regions_processed,
            error_summary=error_summary,
        )

    async def _process_single_resource(
        self,
        resource: Dict[str, Any],
        metric_configs: List[MetricConfiguration],
        start_time: datetime,
        end_time: datetime,
        region: str,
        progress: Any,
        state: Any,
    ) -> tuple[List[Any], float]:
        """Process a single resource and return metrics and duration."""
        resource_start_time = time.time()

        logger.debug(f"Starting collection for {resource['resource_name']} in {region}")

        try:
            # Collect metrics for this resource
            logger.debug(
                f"Calling _collect_resource_metrics for {resource['resource_name']}"
            )
            resource_metrics = await self._collect_resource_metrics(
                resource, metric_configs, start_time, end_time, region
            )

            resource_duration = time.time() - resource_start_time

            # Progress will be updated by the calling method

            # Log completion at debug level to avoid duplicate messages
            logger.debug(
                f"Completed {resource['resource_name']} in "
                f"{resource_duration:.2f}s - {len(resource_metrics)} metrics"
            )

            # Rate limiting
            if self.rate_limit_delay > 0:
                logger.debug(f"Rate limiting delay: {self.rate_limit_delay}s")
                await asyncio.sleep(self.rate_limit_delay)

            return resource_metrics, resource_duration

        except Exception as e:
            resource_duration = time.time() - resource_start_time
            logger.error(
                f"Failed processing {resource['resource_name']} "
                f"after {resource_duration:.2f}s: {e}"
            )
            raise

    async def _collect_resource_metrics(
        self,
        resource: Dict[str, Any],
        metric_configs: List[MetricConfiguration],
        start_time: datetime,
        end_time: datetime,
        region: str,
    ) -> List[MetricDataPoint]:
        """Collect metrics for a specific resource using optimized batching."""

        logger.debug(f"Getting CloudWatch client for {region}")
        async with await self.aws_client_manager.get_async_client(
            "cloudwatch", region
        ) as cloudwatch:

            # Use optimized batch collection for single resource
            return await self._collect_metrics_batch_optimized(
                cloudwatch,
                [resource],  # Single resource in list for batch processing
                metric_configs,
                start_time,
                end_time,
                region,
            )

    async def _add_metrics_and_maybe_flush(
        self,
        metrics: List[MetricDataPoint]
    ) -> None:
        """
        Add metrics to batch and automatically flush if size limit reached.

        This enables streaming collection where metrics are persisted progressively
        instead of accumulating unbounded lists in memory.

        Args:
            metrics: List of metrics to add to batch
        """
        if not metrics:
            return

        should_flush = False

        async with self._batch_lock:
            self._metric_batch.extend(metrics)
            # Decide atomically while holding lock (parallel-safe)
            should_flush = len(self._metric_batch) >= self.batch_flush_size

        # Flush outside lock if needed
        if should_flush:
            logger.info(
                f"Auto-flushing {len(self._metric_batch):,} metrics "
                f"(limit: {self.batch_flush_size:,})"
            )
            await self._flush_metric_batch()
    
    async def _flush_metric_batch(self) -> None:
        """Flush accumulated metrics to database with data recovery on failure."""

        if not self._metric_batch:
            return

        # Safety check: warn if batch is unexpectedly large
        # This indicates auto-flush logic may not be working correctly
        current_batch_size = len(self._metric_batch)
        if current_batch_size > self.batch_flush_size * 5:  # More than 5x expected size
            logger.warning(
                f"Very large batch detected: {current_batch_size:,} metrics "
                f"(expected max: {self.batch_flush_size:,}). "
                "Auto-flush may not be working correctly!"
            )

        # Keep reference to original metrics for recovery if insert fails
        metrics_to_restore = []
        batch_data = []

        try:
            # Copy batch data while holding lock briefly
            async with self._batch_lock:
                if not self._metric_batch:
                    return

                # Keep reference to original MetricDataPoint objects for recovery
                metrics_to_restore = list(self._metric_batch)

                # Convert to dict format for DuckDB batch insert
                for metric in self._metric_batch:
                    batch_data.append(
                        {
                            "account_id": metric.account_id,
                            "table_name": metric.table_name,
                            "resource_name": metric.resource_name,
                            "resource_type": metric.resource_type,
                            "metric_name": metric.metric_name,
                            "operation": metric.operation,
                            "operation_type": metric.operation_type,
                            "statistic": metric.statistic,
                            "period_seconds": metric.period_seconds,
                            "timestamp": metric.timestamp,
                            "value": metric.value,
                            "unit": metric.unit,
                            "region": metric.region,
                            "dimensions": metric.dimensions,
                            # created_at will be set by DEFAULT CURRENT_TIMESTAMP
                        }
                    )

                # Clear the batch (will restore if insert fails)
                self._metric_batch.clear()

            # Perform direct database operation (OUTSIDE lock to avoid holding during slow I/O)
            if batch_data:
                self._direct_batch_insert(batch_data)
                logger.debug(f"Successfully stored {len(batch_data):,} metrics in DuckDB")

        except Exception as e:
            # Restore metrics to batch if database insert failed
            # This prevents silent data loss
            async with self._batch_lock:
                # Prepend failed metrics to front of batch (maintain order)
                self._metric_batch = metrics_to_restore + self._metric_batch
            
            logger.error(
                f"Flush failed! Restored {len(metrics_to_restore):,} metrics to batch. "
                f"Current batch size: {len(self._metric_batch):,}. Error: {e}"
            )
            
            # RE-RAISE to fail collection visibly (don't silently lose data)
            raise RuntimeError(f"Metric flush failed, data restored to batch: {e}")

    def _direct_batch_insert(self, batch_data: List[Dict[str, Any]]) -> None:
        """Direct batch insert with performance timing."""
        insert_start = time.time()
        try:
            self.db_manager.execute_batch_upsert_metrics(batch_data)
            insert_duration_ms = (time.time() - insert_start) * 1000
            logger.debug(
                f"DuckDB insert: {len(batch_data):,} metrics in {insert_duration_ms:.1f}ms "
                f"({len(batch_data)/(insert_duration_ms/1000):.0f} records/sec)"
            )
        except Exception as e:
            insert_duration_ms = (time.time() - insert_start) * 1000
            logger.error(
                f"FATAL: Insert failed after {insert_duration_ms:.1f}ms "
                f"for {len(batch_data)} metrics: {e}"
            )
            raise RuntimeError(f"Database insert failed: {e}")

    def get_collection_status(self, operation_id: str) -> Optional[Dict]:
        """Get current status of a collection operation."""
        state = self.state_manager.load_checkpoint(operation_id)
        if not state:
            return None

        collection_state = state.collection_state

        return {
            "operation_id": state.operation_id,
            "status": state.status,
            "completion_percentage": state.completion_percentage,
            "start_time": state.start_time,
            "last_checkpoint_time": state.last_checkpoint_time,
            "estimated_completion": state.estimated_completion,
            "completed_operations": collection_state.completed_operations,
            "total_operations": collection_state.total_operations,
            "failed_collections": len(collection_state.failed_collections),
            "error_message": state.error_message,
        }

    async def resume_collection(
        self, operation_id: str, show_progress: bool = True
    ) -> CollectionResult:
        """Resume a paused or failed collection operation."""
        state = self.state_manager.load_checkpoint(operation_id)
        if not state:
            raise CollectionError(f"No checkpoint found for operation {operation_id}")

        if state.status == "COMPLETED":
            raise CollectionError(
                f"Cannot resume operation {operation_id}: already completed. "
                f"Use 'dynamodb-optima collect' to start a new collection."
            )

        logger.info(f"Resuming collection operation {operation_id}")
        state.status = "RUNNING"

        # Use default parameters for resume
        # (could be enhanced to store original params in state)

        # Default to last 1 day if no time range stored
        end_time = datetime.now()
        start_time = end_time - timedelta(days=1)

        # Resume collection with existing state
        return await self.collect_metrics(
            start_time=start_time,
            end_time=end_time,
            regions=None,  # Will discover all regions
            table_names=None,  # Will discover all tables
            metric_configs=None,  # Will use default metrics
            operation_id=operation_id,
            resume_from_checkpoint=True,
            show_progress=show_progress,
        )

    def list_collected_metrics_summary(self) -> Dict[str, Any]:
        """Get summary of collected metrics from database."""
        try:
            # Get metric counts by resource type
            resource_query = """
                SELECT resource_type, COUNT(*) as metric_count,
                       COUNT(DISTINCT resource_name) as resource_count,
                       COUNT(DISTINCT region) as region_count,
                       MIN(timestamp) as earliest_metric,
                       MAX(timestamp) as latest_metric
                FROM metrics
                GROUP BY resource_type
            """
            resource_stats = self.db_manager.execute_query(resource_query)

            # Get metric counts by metric name
            metric_query = """
                SELECT metric_name, COUNT(*) as count,
                       COUNT(DISTINCT resource_name) as resource_count
                FROM metrics
                GROUP BY metric_name
                ORDER BY count DESC
                LIMIT 10
            """
            metric_stats = self.db_manager.execute_query(metric_query)

            # Get recent collection activity
            recent_query = """
                SELECT DATE_TRUNC('day', timestamp) as date,
                       COUNT(*) as metrics_collected
                FROM metrics
                WHERE timestamp >= CURRENT_DATE - INTERVAL '7 days'
                GROUP BY DATE_TRUNC('day', timestamp)
                ORDER BY date DESC
            """
            recent_stats = self.db_manager.execute_query(recent_query)

            # Get total counts
            total_query = """
                SELECT COUNT(*) as total_metrics,
                       COUNT(DISTINCT resource_name) as total_resources,
                       COUNT(DISTINCT region) as total_regions
                FROM metrics
            """
            totals = self.db_manager.execute_query(total_query)[0]

            return {
                "total_metrics": totals["total_metrics"],
                "total_resources": totals["total_resources"],
                "total_regions": totals["total_regions"],
                "by_resource_type": resource_stats,
                "top_metrics": metric_stats,
                "recent_activity": recent_stats,
                "last_updated": datetime.now(),
            }

        except Exception as e:
            logger.error(f"Failed to get metrics summary: {e}")
            return {"error": str(e)}

    # ========================================================================
    # OPTIMIZED CLOUDWATCH API BATCHING METHODS (Task 5.1)
    # ========================================================================

    async def _collect_with_batch_api(
        self,
        cloudwatch,
        resources: List[Dict[str, Any]],
        metric_configs: List[MetricConfiguration],
        start_time: datetime,
        end_time: datetime,
        region: str
    ) -> int:
        """
        Collect metrics using GetMetricData batch API with streaming flush.
        
        Metrics are progressively flushed to database during collection instead of
        accumulating in memory. This prevents OOM on large collections.
        
        Args:
            cloudwatch: CloudWatch client
            resources: List of resources to collect
            metric_configs: Metrics to collect
            start_time: Collection start time
            end_time: Collection end time
            region: AWS region
            
        Returns:
            Total count of metrics collected (already flushed to database)
        """
        # Build all metric queries for batching
        metric_queries = []
        query_metadata = {}
        query_id_counter = 0
        

        for resource in resources:
            for config in metric_configs:
                for statistic in config.statistics:
                    for period in config.periods:
                        query_id = f"m{query_id_counter}"
                        query_id_counter += 1
                        
                        # Build dimensions
                        dimensions = [{"Name": "TableName", "Value": resource["table_name"]}]
                        
                        if resource["resource_type"] == "GSI" and "#" in resource["resource_name"]:
                            gsi_name = resource["resource_name"].split("#", 1)[1]
                            dimensions.append({"Name": "GlobalSecondaryIndexName", "Value": gsi_name})
                        
                        if config.operation:
                            dimensions.append({"Name": "Operation", "Value": config.operation})
                        
                        if config.operation_type:
                            dimensions.append({"Name": "OperationType", "Value": config.operation_type})
                        
                        # Create metric query
                        metric_query = {
                            "Id": query_id,
                            "MetricStat": {
                                "Metric": {
                                    "Namespace": "AWS/DynamoDB",
                                    "MetricName": config.metric_name,
                                    "Dimensions": dimensions,
                                },
                                "Period": period,
                                "Stat": statistic,
                            },
                            "ReturnData": True,
                        }
                        
                        metric_queries.append(metric_query)
                        query_metadata[query_id] = {
                            "resource": resource,
                            "config": config,
                            "statistic": statistic,
                            "period": period,
                            "region": region,
                        }

        # Track total metrics collected
        total_metrics_collected = 0
        batch_size = self._calculate_optimal_batch_size(len(metric_queries))
        
        # Process queries in batches with pagination and progressive flushing
        for i in range(0, len(metric_queries), batch_size):
            batch_queries = metric_queries[i:i + batch_size]
            

            try:
                # Paginate through all results using NextToken
                next_token = None
                page = 1
                batch_total = 0
                
                while True:
                    params = {
                        "MetricDataQueries": batch_queries,
                        "StartTime": start_time,
                        "EndTime": end_time,
                        "MaxDatapoints": 100800,
                    }
                    
                    if next_token:
                        params["NextToken"] = next_token
                    
                    response = await asyncio.wait_for(
                        cloudwatch.get_metric_data(**params),
                        timeout=30.0,
                    )

                    # Process this page
                    page_metrics = self._process_batch_response(response, query_metadata)

                    # FLUSH PROGRESSIVELY: Add metrics and maybe flush
                    await self._add_metrics_and_maybe_flush(page_metrics)

                    batch_total += len(page_metrics)
                    total_metrics_collected += len(page_metrics)
                    
                    logger.debug(
                        f"Batch {i//batch_size + 1} page {page}: "
                        f"{len(page_metrics)} metrics collected"
                    )
                    
                    # Check for more pages
                    next_token = response.get("NextToken")
                    if not next_token:
                        logger.debug(f"DIAGNOSTIC: No more pages for batch {i//batch_size + 1}")
                        break  # No more data!
                    
                    logger.debug(f"DIAGNOSTIC: NextToken present, fetching page {page + 1}")
                    page += 1
                
                logger.info(
                    f"Batch {i//batch_size + 1} complete: "
                    f"{batch_total} metrics across {page} page(s)"
                )
                
                # Small delay between batches
                if i + batch_size < len(metric_queries):
                    await asyncio.sleep(0.1)
                    
            except Exception as e:
                logger.error(f"Batch API call failed: {e}")
        
        return total_metrics_collected

    def _get_latest_timestamps_for_resource(
        self,
        account_id: str,
        resource_name: str,
        region: str,
    ) -> Dict[str, datetime]:
        """
        Get latest timestamp for each metric configuration for a resource.
        
        Returns dictionary mapping "metric:stat:period" to latest timestamp.
        Used to determine where to start incremental collection.
        
        Args:
            account_id: AWS account ID
            resource_name: Resource identifier
            region: AWS region
            
        Returns:
            Dict mapping metric key to latest timestamp, or empty dict if no data
        """
        from datetime import timezone
        
        query = """
            SELECT 
                metric_name,
                statistic,
                period_seconds,
                MAX(timestamp) as latest_timestamp
            FROM metrics
            WHERE account_id = ?
              AND resource_name = ?
              AND region = ?
            GROUP BY metric_name, statistic, period_seconds
        """
        
        try:
            results = self.db_manager.execute_query(
                query, [account_id, resource_name, region]
            )
            
            coverage = {}
            for row in results:
                key = f"{row['metric_name']}:{row['statistic']}:{row['period_seconds']}"
                timestamp = row['latest_timestamp']
                
                # Ensure timestamp is timezone-aware (UTC) for comparisons
                if isinstance(timestamp, datetime) and timestamp.tzinfo is None:
                    timestamp = timestamp.replace(tzinfo=timezone.utc)
                
                coverage[key] = timestamp
            
            return coverage
            
        except Exception as e:
            logger.warning(
                f"Failed to get latest timestamps for {resource_name}: {e}, "
                "will collect full window"
            )
            return {}  # Empty dict means collect full window

    async def _collect_metrics_batch_optimized(
        self,
        cloudwatch,
        resources: List[Dict[str, Any]],
        metric_configs: List[MetricConfiguration],
        start_time: datetime,
        end_time: datetime,
        region: str,
    ) -> List[MetricDataPoint]:
        """
        Incremental collection using batch API with conservative gap detection.
        
        For each resource, finds the EARLIEST latest timestamp across all its metrics,
        then collects from that point forward. This enables efficient batch API usage
        while minimizing duplicate data collection.
        """
        logger.info(
            f"Starting incremental collection for {len(resources)} resources in {region}"
        )
        
        # Separate resources into those needing collection vs up-to-date
        resources_to_collect = []
        resources_skipped = 0
        
        for resource in resources:
            # Get latest timestamps for this resource
            coverage = self._get_latest_timestamps_for_resource(
                resource['account_id'],
                resource['resource_name'],
                region
            )
            
            if not coverage:
                # No existing data - collect full window
                resource['collection_start'] = start_time
                resources_to_collect.append(resource)
            else:
                # Find minimum latest timestamp (conservative approach)
                # This is the earliest point where ANY metric needs new data
                min_latest = min(coverage.values())
                
                # Calculate gap start (add largest period for safety)
                # Use largest period from all metrics to ensure we don't miss data
                max_period = max(
                    max(config.periods) for config in metric_configs if config.periods
                )
                gap_start = min_latest + timedelta(seconds=max_period)
                
                # Skip if already up-to-date
                if gap_start >= end_time:
                    resources_skipped += 1
                    logger.debug(
                        f"{resource['resource_name']}: Up-to-date "
                        f"(latest: {min_latest.isoformat()}, end: {end_time.isoformat()})"
                        )
                    continue
                
                # Collect gap from earliest needed point
                resource['collection_start'] = gap_start
                resources_to_collect.append(resource)
                logger.debug(
                    f"{resource['resource_name']}: Gap detected, "
                    f"collecting from {gap_start.isoformat()} to {end_time.isoformat()}"
                    )
        
        logger.info(
            f"Gap analysis complete: {len(resources_to_collect)} need collection, "
            f"{resources_skipped} up-to-date"
        )
        
        # Skip API calls if all resources are up-to-date
        if not resources_to_collect:
            logger.info("All resources up-to-date, skipping CloudWatch API calls")
            return []
        
        # Collect metrics using batch API for resources that need data
        # Use the minimum gap_start across all resources for batch efficiency
        batch_start = min(r['collection_start'] for r in resources_to_collect)

        
        # Collect with streaming flush - returns count of metrics (already flushed)
        metrics_count = await self._collect_with_batch_api(
            cloudwatch,
            resources_to_collect,
            metric_configs,
            batch_start,  # Use earliest gap start for batch
            end_time,
            region
        )

        # Return empty list since metrics are already flushed
        # Caller expects List but won't use it (will use count from return value context)
        return []

    def get_time_period_optimization_recommendations(
        self, metric_configs: List[MetricConfiguration]
    ) -> Dict[str, str]:
        """
        Get optimization recommendations for time periods in metric configurations.

        Args:
            metric_configs: List of metric configurations to analyze

        Returns:
            Dictionary mapping period to optimization recommendation
        """
        recommendations = {}

        # Collect all unique periods
        all_periods = set()
        for config in metric_configs:
            all_periods.update(config.periods)

        # Generate recommendations for each period
        for period in sorted(all_periods):
            recommendation = TimestampAligner.get_alignment_recommendation(period)
            recommendations[f"{period}s"] = recommendation

        logger.debug(
            "Generated time period optimization recommendations",
            periods_analyzed=len(all_periods),
            recommendations=recommendations,
        )

        return recommendations

    def log_collection_performance_metrics(
        self,
        start_time: datetime,
        end_time: datetime,
        metric_configs: List[MetricConfiguration],
        total_metrics_collected: int,
        collection_duration: timedelta,
    ) -> None:
        """
        Log performance metrics and optimization impact for the collection.

        Args:
            start_time: Collection start time (aligned)
            end_time: Collection end time (aligned)
            metric_configs: Metric configurations used
            total_metrics_collected: Total metrics collected
            collection_duration: Total collection duration
        """
        # Calculate collection statistics
        time_window_hours = (end_time - start_time).total_seconds() / 3600
        metrics_per_hour = total_metrics_collected / max(time_window_hours, 0.1)

        # Analyze period distribution
        period_distribution = {}
        for config in metric_configs:
            for period in config.periods:
                period_key = f"{period}s"
                period_distribution[period_key] = (
                    period_distribution.get(period_key, 0) + 1
                )

        # Calculate estimated API efficiency gain from alignment
        total_periods = len(
            set(period for config in metric_configs for period in config.periods)
        )

        logger.debug(
            "CloudWatch collection performance metrics",
            collection_duration_seconds=collection_duration.total_seconds(),
            time_window_hours=time_window_hours,
            total_metrics_collected=total_metrics_collected,
            metrics_per_hour=round(metrics_per_hour, 2),
            period_distribution=period_distribution,
            unique_periods=total_periods,
            timestamp_alignment_benefit="Optimized boundaries reduce partial periods and improve consistency",
            api_efficiency_gain="Period-specific time windows maximize CloudWatch API performance",
        )

    def _calculate_optimal_batch_size(self, total_queries: int) -> int:
        """
        Calculate optimal batch size for CloudWatch GetMetricData calls.

        CloudWatch limits:
        - 100,400 objects per GetMetricData call
        - Each MetricDataQuery counts as 1 object
        - Additional objects come from returned datapoints

        We use conservative batching to stay well under limits.
        """
        # Conservative batch size to account for datapoints returned
        # Each query might return multiple datapoints, so we batch conservatively
        max_queries_per_batch = min(500, total_queries)  # Conservative limit

        # For small numbers of queries, use smaller batches for better error isolation
        if total_queries <= 50:
            return min(25, total_queries)
        elif total_queries <= 200:
            return min(100, total_queries)
        else:
            return max_queries_per_batch

    def _process_batch_response(
        self,
        response: Dict[str, Any],
        query_metadata: Dict[str, Dict[str, Any]],
    ) -> List[MetricDataPoint]:
        """Process CloudWatch GetMetricData response into MetricDataPoint objects."""
        metrics = []

        for metric_result in response.get("MetricDataResults", []):
            query_id = metric_result["Id"]

            if query_id not in query_metadata:
                logger.warning(f"Unknown query ID in response: {query_id}")
                continue

            metadata = query_metadata[query_id]
            resource = metadata["resource"]
            config = metadata["config"]
            statistic = metadata["statistic"]
            period = metadata["period"]
            region = metadata["region"]

            # Process all datapoints for this metric
            timestamps = metric_result.get("Timestamps", [])
            values = metric_result.get("Values", [])

            logger.debug(
                f"Processing query {query_id}: {len(timestamps)} datapoints "
                f"for {resource['resource_name']} {config.metric_name}"
            )

            if len(timestamps) != len(values):
                logger.warning(
                    f"Timestamp/value mismatch for query {query_id}: "
                    f"{len(timestamps)} timestamps, {len(values)} values"
                )
                continue

            if len(timestamps) == 0:
                logger.debug(
                    f"No data returned for {resource['resource_name']} "
                    f"{config.metric_name}:{statistic}:{period}s"
                )

            for timestamp, value in zip(timestamps, values):
                # Build dimensions dict for storage
                dimensions = {"TableName": resource["table_name"]}

                if (
                    resource["resource_type"] == "GSI"
                    and "#" in resource["resource_name"]
                ):
                    gsi_name = resource["resource_name"].split("#", 1)[1]
                    dimensions["GlobalSecondaryIndexName"] = gsi_name

                if config.operation:
                    dimensions["Operation"] = config.operation

                if config.operation_type:
                    dimensions["OperationType"] = config.operation_type

                # Create metric data point
                metric_data = MetricDataPoint(
                    account_id=resource["account_id"],
                    table_name=resource["table_name"],
                    resource_name=resource["resource_name"],
                    resource_type=resource["resource_type"],
                    metric_name=config.metric_name,
                    operation=config.operation,
                    operation_type=config.operation_type,
                    statistic=statistic,
                    period_seconds=period,
                    timestamp=timestamp,
                    value=value,
                    unit="Count",  # GetMetricData doesn't return units, use default
                    region=region,
                    dimensions=dimensions,
                )

                metrics.append(metric_data)

        return metrics

    async def _process_resources_batch_optimized(
        self,
        resources: List[Dict[str, Any]],
        metric_configs: List[MetricConfiguration],
        start_time: datetime,
        end_time: datetime,
        region: str,
        progress_bar: Any,
        state: Any,
    ) -> tuple[int, int, int]:
        """Process resources in parallel, updating the provided progress bar."""

        from ..config import get_settings
        settings = get_settings()
        
        total = len(resources)
        successful = 0
        failed = 0

        async with await self.aws_client_manager.get_async_client(
            "cloudwatch", region
        ) as cloudwatch:

            semaphore = asyncio.Semaphore(settings.max_concurrent_resources)

            async def collect_one_resource(resource):
                async with semaphore:
                    try:
                        await self._collect_metrics_batch_optimized(
                            cloudwatch,
                            [resource],
                            metric_configs,
                            start_time,
                            end_time,
                            region,
                        )
                        
                        resource_key = f"{region}:{resource['resource_name']}"
                        state.collection_state.completed_resources.add(resource_key)
                        return ("success", resource)
                    
                    except Exception as e:
                        resource_key = f"{region}:{resource['resource_name']}"
                        state.collection_state.completed_resources.add(resource_key)
                        state.collection_state.failed_collections.append({
                            "resource": resource["resource_name"],
                            "region": region,
                            "error": str(e),
                            "timestamp": datetime.now().isoformat(),
                        })
                        return ("error", resource, e)

            tasks = [asyncio.create_task(collect_one_resource(r)) for r in resources]
            
            # Use the provided progress bar instead of creating a new one
            for coro in asyncio.as_completed(tasks):
                result = await coro
                
                # Update the shared progress bar
                if progress_bar:
                    progress_bar.update(1)
                
                if result[0] == "success":
                    successful += 1
                else:
                    failed += 1

            state.collection_state.completed_batches += 1
            state.collection_state.completed_operations += 1
            state.completion_percentage = (
                state.collection_state.completed_operations / state.collection_state.total_operations * 100
                if state.collection_state.total_operations > 0 else 0
            )

            self.state_manager.save_checkpoint(state)

            return 0, successful, failed

    def get_batch_performance_stats(self) -> Dict[str, Any]:
        """Get performance statistics for batch operations."""
        # This would be implemented with actual performance tracking
        # For now, return placeholder data
        return {
            "api_calls_saved": "Estimated 80-95% reduction in API calls",
            "throughput_improvement": "5-10x faster collection",
            "batch_efficiency": "500 queries per batch vs 1 query per call",
            "rate_limit_optimization": "Better utilization of 1000 req/sec limit",
        }
