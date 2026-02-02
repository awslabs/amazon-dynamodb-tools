"""
Discovery command for DynamoDB tables and GSIs across AWS regions.

Wires the DiscoveryManager backend into a user-friendly CLI command.
"""

import asyncio
from typing import Optional

import click

from ..aws.discovery import DiscoveryManager
from ..aws.cur_discovery import CURDiscovery
from ..aws.organizations import OrganizationsManager
from ..aws.pricing_collector import PricingCollector
from ..config import get_settings
from ..database.connection import get_connection
from ..logging import get_logger

logger = get_logger(__name__)


@click.command(name="discover")
@click.option(
    "--regions",
    help="Comma-separated list of AWS regions (e.g., us-east-1,us-west-2)",
)
@click.option("--profile", help="AWS profile name to use")
@click.option(
    "--use-org",
    is_flag=True,
    help="Use AWS Organizations to discover accounts across entire organization",
)
@click.option(
    "--org-role",
    help="IAM role name to assume in member accounts (default: OrganizationAccountAccessRole)",
)
@click.option(
    "--skip-accounts",
    help="Comma-separated list of account IDs to skip during discovery",
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
    "--cur-override",
    help="Manual CUR S3 location override (s3://bucket/prefix)",
)
@click.pass_context
def discover(
    ctx: click.Context,
    regions: Optional[str],
    profile: Optional[str],
    use_org: bool,
    org_role: Optional[str],
    skip_accounts: Optional[str],
    resume: bool,
    operation_id: Optional[str],
    cur_override: Optional[str],
) -> None:
    """Discover DynamoDB tables and GSIs across AWS regions.
    
    This command scans AWS regions to find all DynamoDB tables and their
    Global Secondary Indexes (GSIs), storing the metadata in the local
    database for subsequent analysis.
    
    Examples:
        # Discover in specific regions
        dynamodb-optima discover --regions us-east-1,us-west-2
        
        # Discover in all configured regions
        dynamodb-optima discover
        
        # Resume interrupted discovery
        dynamodb-optima discover --resume --operation-id <id>
    """
    settings = get_settings()
    
    # Parse regions
    if regions:
        region_list = [r.strip() for r in regions.split(",")]
    else:
        # Use default regions from config or common regions
        region_list = [
            "us-east-1",
            "us-east-2",
            "us-west-1",
            "us-west-2",
            "eu-west-1",
            "eu-central-1",
            "ap-southeast-1",
            "ap-northeast-1",
        ]
        click.echo(f"No regions specified, using: {', '.join(region_list)}")
    
    # Configure AWS profile if specified
    if profile:
        import os
        os.environ["AWS_PROFILE"] = profile
        click.echo(f"Using AWS profile: {profile}")
        click.echo()
    
    # Parse skip accounts list
    skip_account_list = []
    if skip_accounts:
        skip_account_list = [a.strip() for a in skip_accounts.split(",")]
    
    try:
        # Multi-account Organizations discovery
        if use_org:
            click.echo("🏢 AWS Organizations Multi-Account Discovery")
            click.echo("=" * 80)
            
            # Get role name (use provided or default from config)
            role_name = org_role or settings.organizations_role_name
            click.echo(f"   Role to assume: {role_name}")
            click.echo(f"   Regions: {', '.join(region_list)}")
            if skip_account_list:
                click.echo(f"   Skipping accounts: {', '.join(skip_account_list)}")
            click.echo()
            
            # Initialize Organizations manager
            org_manager = OrganizationsManager()
            
            # Discover accounts from Organizations (don't store yet)
            click.echo("📋 Discovering accounts in organization...")
            accounts = asyncio.run(org_manager.list_accounts())
            
            # Filter out skipped accounts
            if skip_account_list:
                accounts = [a for a in accounts if a.account_id not in skip_account_list]
            
            click.echo(f"✅ Found {len(accounts)} accounts to process")
            click.echo()
            
            # Track results per account
            account_results = []
            total_tables = 0
            total_gsis = 0
            failed_accounts = []
            
            # Initialize discovery manager for multi-account orchestration
            discovery_manager = DiscoveryManager()
            
            # Process each account
            for idx, account in enumerate(accounts, 1):
                click.echo(f"[{idx}/{len(accounts)}] Processing account: {account.account_name} ({account.account_id})")
                
                try:
                    # Skip role assumption for management account
                    credentials = None
                    if not account.is_management_account:
                        click.echo(f"   Assuming role: {role_name}")
                        credentials = asyncio.run(
                            org_manager.get_account_credentials(
                                account.account_id,
                                role_name
                            )
                        )
                    else:
                        click.echo("   Using management account credentials (no role assumption)")
                    
                    # Discover resources in this account
                    result = asyncio.run(
                        discovery_manager.discover_account_resources(
                            account_id=account.account_id,
                            account_name=account.account_name,
                            regions=region_list,
                            credentials=credentials
                        )
                    )
                    
                    account_results.append(result)
                    
                    if result["status"] == "success":
                        total_tables += result["tables_discovered"]
                        total_gsis += result["gsis_discovered"]
                        click.echo(f"   ✅ Success: {result['tables_discovered']} tables, "
                                 f"{result['gsis_discovered']} GSIs")
                    else:
                        failed_accounts.append(account.account_name)
                        click.echo(f"   ❌ Failed: {result.get('error', 'Unknown error')}")
                    
                except Exception as e:
                    failed_accounts.append(account.account_name)
                    click.echo(f"   ❌ Failed: {e}")
                    logger.error(f"Failed to discover account {account.account_id}: {e}")
                    account_results.append({
                        "account_id": account.account_id,
                        "account_name": account.account_name,
                        "status": "failed",
                        "error": str(e)
                    })
                
                click.echo()
            
            # Display multi-account summary
            click.echo("=" * 80)
            click.echo("🎉 Multi-Account Discovery Complete!")
            click.echo("=" * 80)
            click.echo(f"   Total accounts processed: {len(accounts)}")
            click.echo(f"   Successful: {len(accounts) - len(failed_accounts)}")
            click.echo(f"   Failed: {len(failed_accounts)}")
            click.echo(f"   Total tables discovered: {total_tables}")
            click.echo(f"   Total GSIs discovered: {total_gsis}")
            
            if failed_accounts:
                click.echo()
                click.echo("   Failed accounts:")
                for account_name in failed_accounts:
                    click.echo(f"      - {account_name}")
            
            click.echo()
            
        else:
            # Single account discovery (original behavior)
            # Initialize discovery manager
            discovery_manager = DiscoveryManager()
            
            # Run discovery
            click.echo(f"🔍 Starting discovery across {len(region_list)} regions...")
            click.echo()
            
            # Run async discovery
            state = asyncio.run(
                discovery_manager.discover_all_resources(
                    regions=region_list,
                    operation_id=operation_id,
                    resume_from_checkpoint=resume,
                )
            )
            
            # Display results
            click.echo()
            click.echo("✅ Discovery completed successfully!")
            click.echo()
        
        # Collect comprehensive pricing data for all discovered regions
        click.echo("💰 Collecting DynamoDB pricing data...")
        click.echo()
        
        try:
            pricing_collector = PricingCollector()
            conn = get_connection()
            
            asyncio.run(
                pricing_collector.collect_all_regions(
                    regions=region_list,
                    connection=conn,
                    force_refresh=False  # Only refresh if stale
                )
            )
            
            click.echo("✅ Pricing data collection complete!")
            click.echo()
            
        except Exception as e:
            logger.error(f"Pricing collection failed: {e}", exc_info=True)
            click.echo(f"⚠️  Pricing collection failed: {e}")
            click.echo("   Analysis commands may fail without pricing data")
            click.echo()
        
        # Get summary from database
        summary = discovery_manager.list_discovered_resources()
        
        if "error" not in summary:
            click.echo(f"📊 Discovery Summary:")
            click.echo(f"   Total tables: {summary['total_tables']}")
            click.echo(f"   Total GSIs: {summary['total_gsis']}")
            click.echo()
            
            click.echo("   By Region:")
            for region, stats in sorted(summary['regions'].items()):
                click.echo(
                    f"      {region}: {stats['tables']} tables, "
                    f"{stats['gsis']} GSIs"
                )
                click.echo(
                    f"         ({stats['on_demand_tables']} On-Demand, "
                    f"{stats['provisioned_tables']} Provisioned)"
                )
        
        # CUR Discovery (Phase 4 - discover HOURLY CUR reports for table class analysis)
        click.echo()
        click.echo("📊 Discovering CUR reports with HOURLY granularity...")
        
        try:
            # Get management account ID (use STS to get current account for now)
            import boto3
            sts_client = boto3.client('sts')
            account_info = sts_client.get_caller_identity()
            management_account_id = account_info['Account']
            
            # Initialize CUR discovery
            cur_discovery = CURDiscovery()
            
            # Discover all HOURLY CUR reports
            cur_locations = asyncio.run(
                cur_discovery.discover_all_cur_reports(
                    management_account_id=management_account_id,
                    hourly_only=True
                )
            )
            
            cur_location = None
            
            if len(cur_locations) == 0:
                click.echo("⚠️  No HOURLY Parquet CUR reports found")
                click.echo("   To enable table class analysis:")
                click.echo("   1. Enable Cost and Usage Reports in AWS Billing Console")
                click.echo("   2. Configure report with:")
                click.echo("      - Format: Parquet")
                click.echo("      - Granularity: HOURLY (required)")
                click.echo("      - Include Resource IDs: Recommended")
                click.echo()
            elif len(cur_locations) == 1:
                # Only one report found - use it
                cur_location = cur_locations[0]
                click.echo(f"✅ Found 1 HOURLY CUR report: {cur_location.report_name}")
            else:
                # Multiple reports - let user choose
                click.echo(f"📋 Found {len(cur_locations)} HOURLY CUR reports:")
                click.echo()
                for idx, loc in enumerate(cur_locations, 1):
                    click.echo(f"[{idx}] {loc.report_name}")
                    click.echo(f"    Bucket: s3://{loc.s3_bucket}/{loc.s3_prefix}")
                    click.echo(f"    Format: {loc.format}, Granularity: {loc.granularity}")
                    click.echo(f"    Resources: {'Yes' if loc.has_resource_ids else 'No'}")
                    click.echo()
                
                # Prompt for selection
                while True:
                    try:
                        choice = click.prompt(
                            f"Select report to use [1-{len(cur_locations)}]",
                            type=int
                        )
                        if 1 <= choice <= len(cur_locations):
                            cur_location = cur_locations[choice - 1]
                            break
                        else:
                            click.echo(f"Please enter a number between 1 and {len(cur_locations)}")
                    except (ValueError, click.Abort):
                        click.echo("Invalid selection")
                        raise click.Abort()
            
            if cur_location:
                # Store CUR metadata in database
                conn = get_connection()
                conn.execute("""
                    INSERT OR REPLACE INTO cur_metadata (
                        management_account_id,
                        cur_report_name,
                        cur_s3_bucket,
                        cur_s3_prefix,
                        cur_format,
                        cur_compression,
                        cur_versioning,
                        cur_granularity,
                        has_resource_ids,
                        last_discovered_at,
                        collection_status
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, 'discovered')
                """, (
                    cur_location.management_account_id,
                    cur_location.report_name,
                    cur_location.s3_bucket,
                    cur_location.s3_prefix,
                    cur_location.format,
                    cur_location.compression,
                    cur_location.versioning,
                    cur_location.granularity,
                    cur_location.has_resource_ids
                ))
                conn.commit()
                
                click.echo()
                click.echo(f"✅ CUR configured: {cur_location.s3_uri}")
                click.echo(f"   Report: {cur_location.report_name}")
                click.echo(f"   Format: {cur_location.format}, Granularity: {cur_location.granularity}")
                click.echo(f"   Resources: {'Yes' if cur_location.has_resource_ids else 'No'}")
                click.echo()
                
        except Exception as e:
            # CUR discovery failure is not critical - gracefully continue
            logger.warning(f"CUR discovery failed: {e}")
            click.echo(f"⚠️  CUR discovery failed: {e}")
            click.echo("   Table class analysis will be unavailable without CUR data")
            click.echo()
        
        click.echo("💡 Next steps:")
        click.echo("   1. Run 'dynamodb-optima collect' to gather CloudWatch metrics")
        if cur_location:
            click.echo("   2. Run 'dynamodb-optima collect-cur' to gather CUR data")
            click.echo("   3. Run 'dynamodb-optima analyze-capacity' for capacity mode analysis")
            click.echo("   4. Run 'dynamodb-optima analyze-table-class' for table class analysis")
        else:
            click.echo("   2. Run 'dynamodb-optima analyze-capacity' for capacity mode analysis")
        
    except KeyboardInterrupt:
        click.echo()
        click.echo("⚠️  Discovery interrupted by user")
        click.echo("   Run with --resume --operation-id <id> to continue")
        raise click.Abort()
    
    except Exception as e:
        logger.error(f"Discovery failed: {e}")
        click.echo(f"❌ Discovery failed: {e}", err=True)
        raise click.Abort()
