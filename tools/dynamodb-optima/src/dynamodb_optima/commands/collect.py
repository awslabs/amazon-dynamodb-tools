"""
Collection command for CloudWatch metrics from DynamoDB tables.

Wires the CloudWatchCollector backend into a user-friendly CLI command.
"""

import asyncio
from datetime import datetime, timedelta, timezone
from typing import Optional

import click

from ..aws.collector import CloudWatchCollector
from ..config import get_settings
from ..logging import get_logger

logger = get_logger(__name__)


def calculate_stable_time_window(days: int) -> tuple[datetime, datetime]:
    """
    Calculate stable time windows aligned to hour boundaries in UTC.
    
    Uses end_time = now - 1 hour to avoid incomplete CloudWatch data.
    This ensures that multiple runs within the same hour use IDENTICAL
    time boundaries, preventing timestamp drift and enabling efficient
    database upserts.
    
    Args:
        days: Number of days to collect
        
    Returns:
        (start_time, end_time) tuple with hour-aligned UTC boundaries
        
    Examples:
        Run at 3:10 PM UTC: (Jan 24 14:00, Jan 26 14:00)
        Run at 3:45 PM UTC: (Jan 24 14:00, Jan 26 14:00) <- SAME!
        Run at 4:10 PM UTC: (Jan 24 15:00, Jan 26 15:00) <- Advances
    """
    # Get current time in UTC
    now_utc = datetime.now(timezone.utc)
    
    # Round DOWN to previous hour, then subtract 1 hour for safety buffer
    # This avoids collecting incomplete CloudWatch data
    end_time = now_utc.replace(minute=0, second=0, microsecond=0) - timedelta(hours=1)
    
    # Calculate start time (N days before end_time)
    start_time = end_time - timedelta(days=days)
    
    logger.debug(
        "Calculated stable time window with hour alignment and safety buffer",
        now_utc=now_utc.isoformat(),
        start_time=start_time.isoformat(),
        end_time=end_time.isoformat(),
        days_requested=days,
        actual_hours=(end_time - start_time).total_seconds() / 3600,
        safety_buffer_hours=1,
        alignment_strategy="Round down to previous hour minus 1 hour buffer"
    )
    
    return start_time, end_time


@click.command(name="collect")
@click.option(
    "--regions",
    help="Comma-separated list of AWS regions (leave empty to use discovered tables)",
)
@click.option(
    "--tables",
    help="Comma-separated list of table names (leave empty for all discovered)",
)
@click.option(
    "--days",
    default=14,
    type=int,
    help="Number of days of metrics to collect (default: 14)",
)
@click.option(
    "--profile",
    help="AWS profile name to use",
)
@click.option(
    "--resume",
    is_flag=True,
    help="Resume from last checkpoint",
)
@click.option(
    "--operation-id",
    help="Operation ID for resuming or tracking",
)
@click.option(
    "--comprehensive",
    is_flag=True,
    help="Collect comprehensive metrics (more detailed, slower)",
)
@click.option(
    "--truncate",
    is_flag=True,
    help="Truncate metrics table before collection (forces full re-collection)",
)
@click.pass_context
def collect(
    ctx: click.Context,
    regions: Optional[str],
    tables: Optional[str],
    days: int,
    profile: Optional[str],
    resume: bool,
    operation_id: Optional[str],
    comprehensive: bool,
    truncate: bool,
) -> None:
    """Collect CloudWatch metrics for DynamoDB tables.
    
    This command retrieves CloudWatch metrics for discovered DynamoDB tables
    and stores them in the local database for analysis. By default, it collects
    14 days of metrics with 5-minute granularity.
    
    Examples:
        # Collect 14 days of metrics for all discovered tables
        dynamodb-optima collect
        
        # Collect 30 days of metrics for specific tables
        dynamodb-optima collect --days 30 --tables table1,table2
        
        # Resume interrupted collection
        dynamodb-optima collect --resume --operation-id <id>
        
        # Collect comprehensive metrics (includes all operations)
        dynamodb-optima collect --comprehensive
    """
    settings = get_settings()
    
    # Parse regions and tables
    region_list = [r.strip() for r in regions.split(",")] if regions else None
    table_list = [t.strip() for t in tables.split(",")] if tables else None
    
    # Configure AWS profile if specified
    if profile:
        import os
        os.environ["AWS_PROFILE"] = profile
        click.echo(f"Using AWS profile: {profile}")
        click.echo()
    
    # Calculate stable time range aligned to hour boundaries
    start_time, end_time = calculate_stable_time_window(days)
    
    # Store operation_id for interrupt handling
    actual_operation_id = operation_id
    
    try:
        # Initialize collector
        collector = CloudWatchCollector()
        
        # Get metric configurations
        metric_configs = collector.get_metric_configurations(
            service="dynamodb",
            comprehensive=comprehensive
        )
        
        # Handle truncate flag
        metrics_before = 0
        if truncate:
            click.echo("⚠️  Truncating metrics table...")
            from ..database.connection import get_database_manager
            db_manager = get_database_manager()
            with db_manager.get_connection_context() as conn:
                conn.execute("DELETE FROM metrics")
            click.echo("✅ Metrics table truncated")
            click.echo()
        else:
            # Get metrics count before collection
            from ..database.connection import get_database_manager
            db_manager = get_database_manager()
            try:
                result_before = db_manager.execute_query("SELECT COUNT(*) as count FROM metrics")
                metrics_before = result_before[0]['count'] if result_before else 0
            except:
                metrics_before = 0
        
        # Get unique accounts from table_metadata (per user's suggestion)
        from ..database.connection import get_database_manager
        db_manager = get_database_manager()
        try:
            accounts_result = db_manager.execute_query(
                "SELECT COUNT(DISTINCT account_id) as count FROM table_metadata"
            )
            accounts_count = accounts_result[0]['count'] if accounts_result else 0
        except:
            accounts_count = 0
        
        # Get unique regions count
        try:
            regions_result = db_manager.execute_query(
                "SELECT COUNT(DISTINCT region) as count FROM table_metadata"
            )
            regions_count = regions_result[0]['count'] if regions_result else 0
        except:
            regions_count = 0
        
        # Get unique tables count
        try:
            tables_result = db_manager.execute_query(
                "SELECT COUNT(*) as count FROM table_metadata"
            )
            tables_count = tables_result[0]['count'] if tables_result else 0
        except:
            tables_count = 0
        
        # Display collection plan
        if not resume:
            click.echo(f"📊 CloudWatch Metrics Collection")
            click.echo(f"   Time range: {start_time.date()} to {end_time.date()} ({days} days)")
            click.echo(f"   Regions: {region_list if region_list else f'{regions_count} discovered'}")
            click.echo(f"   Tables: {table_list if table_list else f'{tables_count} discovered'}")
            click.echo(f"   Accounts: {accounts_count}")
            click.echo(f"   Metrics: {'Comprehensive' if comprehensive else 'Standard'} ({len(metric_configs)} configurations)")
            if truncate:
                click.echo(f"   Mode: Full re-collection (truncate enabled)")
            click.echo()
        else:
            click.echo(f"🔄 Resuming collection (operation: {operation_id})")
            click.echo()
        
        # Run collection
        result = asyncio.run(
            collector.collect_metrics(
                start_time=start_time,
                end_time=end_time,
                regions=region_list,
                table_names=table_list,
                metric_configs=metric_configs,
                operation_id=operation_id,
                resume_from_checkpoint=resume,
                show_progress=True,
            )
        )
        
        # Capture the actual operation_id from the result
        actual_operation_id = result.operation_id
        
        # Get metrics count after collection
        from ..database.connection import get_database_manager
        db_manager = get_database_manager()
        try:
            result_after = db_manager.execute_query("SELECT COUNT(*) as count FROM metrics")
            metrics_after = result_after[0]['count'] if result_after else 0
        except:
            metrics_after = 0
        
        new_metrics = metrics_after - metrics_before
        
        # Display results
        click.echo()
        click.echo("✅ Collection completed successfully!")
        click.echo()
        click.echo(f"📊 Collection Summary:")
        click.echo(f"   Accounts accessed: {accounts_count}")
        click.echo(f"   Total metrics collected: {result.total_metrics_collected:,}")
        if not truncate and metrics_before > 0:
            click.echo(f"   New metrics added: {new_metrics:,} (existing: {metrics_before:,})")
        click.echo(f"   Resources processed: {result.resources_processed}")
        click.echo(f"   Successful collections: {result.successful_collections}")
        click.echo(f"   Failed collections: {result.failed_collections}")
        click.echo(f"   Regions processed: {len(result.regions_processed)}")
        click.echo(f"   Duration: {result.collection_duration}")
        
        if result.error_summary:
            click.echo()
            click.echo("   Errors encountered:")
            for error_type, count in result.error_summary.items():
                click.echo(f"      {error_type}: {count}")
        
        # Get database summary
        click.echo()
        summary = collector.list_collected_metrics_summary()
        
        if "error" not in summary:
            click.echo("💾 Database Summary:")
            click.echo(f"   Total metrics in database: {summary['total_metrics']:,}")
            click.echo(f"   Total resources: {summary['total_resources']}")
            click.echo(f"   Total regions: {summary['total_regions']}")
        
        click.echo()
        click.echo("💡 Next steps:")
        click.echo("   1. Run 'dynamodb-optima analyze-capacity' to optimize capacity modes")
        click.echo("   2. Run 'dynamodb-optima analyze-utilization' to check usage patterns")
        
    except KeyboardInterrupt:
        click.echo()
        click.echo("⚠️  Collection interrupted by user")
        if actual_operation_id:
            click.echo(f"   Run with --resume --operation-id {actual_operation_id} to continue")
        else:
            click.echo("   Run with --resume --operation-id <id> to continue")
        raise click.Abort()
    
    except Exception as e:
        logger.error(f"Collection failed: {e}")
        click.echo(f"❌ Collection failed: {e}", err=True)
        click.echo()
        click.echo("💡 Troubleshooting:")
        click.echo("   1. Ensure you have run 'dynamodb-optima discover' first")
        click.echo("   2. Check AWS credentials are valid")
        click.echo("   3. Verify DynamoDB tables exist in specified regions")
        raise click.Abort()
