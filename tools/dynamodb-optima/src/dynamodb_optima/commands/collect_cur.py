"""
CUR collection command for gathering cost data from S3.

Collects Cost and Usage Reports data for table class analysis.
"""

import asyncio
from typing import Optional

import click
import boto3

from ..aws.cur_discovery import CURDiscovery
from ..aws.cur_collector import CURCollector
from ..config import get_settings
from ..database.connection import get_connection
from ..logging import get_logger

logger = get_logger(__name__)


@click.command(name="collect-cur")
@click.option(
    "--months",
    type=int,
    default=None,
    help="Number of months to collect (default: 3 from config)",
)
@click.option(
    "--force",
    is_flag=True,
    help="Force full refresh - delete and re-collect all data",
)
@click.option(
    "--profile",
    help="AWS profile name to use",
)
@click.pass_context
def collect_cur(
    ctx: click.Context,
    months: Optional[int],
    force: bool,
    profile: Optional[str],
) -> None:
    """Collect CUR data from S3 for table class analysis.
    
    This command reads Cost and Usage Reports from S3, filters for
    DynamoDB usage, and stores the data in the local database.
    
    Prerequisites:
        - Run 'dynamodb-optima discover' first to find CUR location
        - CUR must be enabled in AWS Billing Console
        - CUR format must be Parquet
        - IAM permissions for S3 access required
    
    Examples:
        # Collect default 3 months of CUR data
        dynamodb-optima collect-cur
        
        # Collect 6 months with force refresh
        dynamodb-optima collect-cur --months 6 --force
        
        # Use specific AWS profile
        dynamodb-optima collect-cur --profile production
    """
    settings = get_settings()
    
    # Configure AWS profile if specified
    if profile:
        import os
        os.environ["AWS_PROFILE"] = profile
        click.echo(f"Using AWS profile: {profile}")
        click.echo()
    
    # Use config default if not specified
    if months is None:
        months = settings.cur_collection_months
    
    try:
        # Step 1: Get CUR metadata from database
        click.echo("📋 Retrieving CUR metadata...")
        conn = get_connection()
        
        cur_metadata = conn.execute("""
            SELECT 
                management_account_id,
                cur_report_name,
                cur_s3_bucket,
                cur_s3_prefix,
                cur_format,
                cur_compression,
                cur_versioning,
                cur_granularity,
                has_resource_ids,
                collection_status
            FROM cur_metadata
            LIMIT 1
        """).fetchone()
        
        if not cur_metadata:
            click.echo("❌ No CUR metadata found")
            click.echo()
            click.echo("Please run 'dynamodb-optima discover' first to find CUR location")
            click.echo("Or use --cur-override to specify CUR S3 location")
            raise click.Abort()
        
        # Build CURLocation object
        from ..aws.cur_discovery import CURLocation
        cur_location = CURLocation(
            report_name=cur_metadata[1],
            s3_bucket=cur_metadata[2],
            s3_prefix=cur_metadata[3] or '',
            format=cur_metadata[4],
            management_account_id=cur_metadata[0],
            compression=cur_metadata[5] or 'Parquet',
            versioning=cur_metadata[6] or 'CREATE_NEW_REPORT',
            granularity=cur_metadata[7] or 'HOURLY',
            has_resource_ids=bool(cur_metadata[8]) if cur_metadata[8] is not None else False
        )
        
        click.echo(f"✓ CUR location: {cur_location.s3_uri}")
        click.echo(f"  Report: {cur_location.report_name}")
        click.echo(f"  Format: {cur_location.format}")
        click.echo()
        
        # Step 2: Get AWS credentials (assume role if needed)
        click.echo("🔑 Obtaining AWS credentials...")
        
        # For now, use current credentials
        # TODO: Add role assumption for management account in Phase 2
        sts_client = boto3.client('sts')
        caller_identity = sts_client.get_caller_identity()
        
        click.echo(f"  Account: {caller_identity['Account']}")
        click.echo(f"  ARN: {caller_identity['Arn']}")
        
        # Get temporary credentials via STS
        # This refreshes credentials and works with assumed roles
        session = boto3.Session()
        credentials = session.get_credentials()
        frozen_credentials = credentials.get_frozen_credentials()
        
        credentials_dict = {
            'AccessKeyId': frozen_credentials.access_key,
            'SecretAccessKey': frozen_credentials.secret_key,
            'SessionToken': frozen_credentials.token or ''
        }
        
        click.echo()
        
        # Step 3: Initialize CUR collector
        click.echo(f"📥 Starting CUR collection ({months} months)...")
        if force:
            click.echo("  ⚠️  Force refresh enabled - existing data will be deleted")
        click.echo()
        
        collector = CURCollector(conn)
        
        # Step 4: Collect CUR data
        rows_collected, status = asyncio.run(
            collector.collect_cur_data(
                location=cur_location,
                credentials=credentials_dict,
                months=months,
                force_refresh=force
            )
        )
        
        # Step 5: Update metadata
        conn.execute("""
            UPDATE cur_metadata
            SET 
                last_collected_date = CURRENT_DATE,
                last_updated_at = CURRENT_TIMESTAMP,
                collection_status = ?,
                rows_collected = ?
            WHERE management_account_id = ?
        """, (status, rows_collected, cur_location.management_account_id))
        conn.commit()
        
        click.echo()
        click.echo("✅ CUR collection completed successfully!")
        click.echo()
        
        # Step 6: Display summary
        summary = collector.get_collection_summary()
        
        click.echo("📊 Collection Summary:")
        click.echo(f"   Total rows: {summary.get('total_rows', 0):,}")
        click.echo(f"   Unique resources: {summary.get('unique_resources', 0)}")
        click.echo(f"   Unique accounts: {summary.get('unique_accounts', 0)}")
        
        if 'earliest_month' in summary and summary['earliest_month']:
            earliest = summary['earliest_month'].strftime('%Y-%m') if hasattr(summary['earliest_month'], 'strftime') else str(summary['earliest_month'])
            latest = summary['latest_month'].strftime('%Y-%m') if hasattr(summary['latest_month'], 'strftime') else str(summary['latest_month'])
            click.echo(f"   Date range: {earliest} to {latest}")
        
        # Display costs (prefer net_unblended if available)
        if 'total_net_unblended_cost_usd' in summary and summary['total_net_unblended_cost_usd'] > 0:
            click.echo(f"   Total DynamoDB cost: ${summary['total_net_unblended_cost_usd']:,.2f} (after discounts)")
        elif 'total_unblended_cost_usd' in summary:
            click.echo(f"   Total DynamoDB cost: ${summary['total_unblended_cost_usd']:,.2f}")
        
        click.echo()
        
        # Step 7: Validate data
        click.echo("🔍 Validating CUR data...")
        is_valid, issues = collector.validate_cur_data()
        
        if is_valid:
            click.echo("✓ Data validation passed")
        else:
            click.echo("⚠️  Data validation warnings:")
            for issue in issues:
                click.echo(f"   - {issue}")
        
        click.echo()
        click.echo("💡 Next steps:")
        click.echo("   1. Run 'dynamodb-optima analyze-table-class' to generate recommendations")
        click.echo("   2. Run 'dynamodb-optima health' to verify system status")
        
    except KeyboardInterrupt:
        click.echo()
        click.echo("⚠️  CUR collection interrupted by user")
        
        # Update status in database
        try:
            conn = get_connection()
            conn.execute("""
                UPDATE cur_metadata
                SET 
                    collection_status = 'interrupted',
                    last_updated_at = CURRENT_TIMESTAMP
            """)
            conn.commit()
        except:
            pass
        
        raise click.Abort()
    
    except Exception as e:
        logger.error(f"CUR collection failed: {e}")
        click.echo(f"❌ CUR collection failed: {e}", err=True)
        click.echo()
        click.echo("Common issues:")
        click.echo("  - S3 permissions: Ensure IAM role has s3:GetObject, s3:ListBucket")
        click.echo("  - CUR format: Must be Parquet (not CSV)")
        click.echo("  - CUR data: May take 24 hours after CUR is enabled")
        click.echo()
        
        # Update status in database
        try:
            conn = get_connection()
            conn.execute("""
                UPDATE cur_metadata
                SET 
                    collection_status = 'error',
                    error_message = ?,
                    last_updated_at = CURRENT_TIMESTAMP
            """, (str(e),))
            conn.commit()
        except:
            pass
        
        raise click.Abort()
