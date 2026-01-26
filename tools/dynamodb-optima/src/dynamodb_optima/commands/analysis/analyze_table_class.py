"""
CLI command for table class analysis.

Analyzes CUR data to generate Standard ↔ Standard-IA recommendations.
"""

import click
from decimal import Decimal

from ...analysis.table_class import TableClassAnalyzer
from ...logging import get_logger

logger = get_logger(__name__)


@click.command(name="analyze-table-class")
@click.option(
    "--months",
    type=int,
    default=3,
    help="Number of months of CUR data to analyze (default: 3)",
)
@click.option(
    "--min-savings",
    type=float,
    default=50.0,
    help="Minimum monthly savings threshold in USD (default: 50)",
)
@click.option(
    "--account",
    multiple=True,
    help="Filter by AWS account ID (can specify multiple times)",
)
@click.option(
    "--region",
    multiple=True,
    help="Filter by AWS region (can specify multiple times)",
)
@click.option(
    "--table",
    multiple=True,
    help="Filter by table name (can specify multiple times)",
)
@click.option(
    "--save/--no-save",
    default=True,
    help="Save recommendations to database (default: True)",
)
@click.option(
    "--format",
    type=click.Choice(['table', 'json', 'csv']),
    default='table',
    help="Output format (default: table)",
)
@click.pass_context
def analyze_table_class(
    ctx,
    months,
    min_savings,
    account,
    region,
    table,
    save,
    format
):
    """
    Analyze table class and generate Standard ↔ Standard-IA recommendations.
    
    This command analyzes Cost and Usage Report (CUR) data to identify tables
    that should switch between Standard and Standard-IA storage classes based
    on their storage-to-throughput cost ratios.
    
    Examples:
        # Analyze all tables with default settings (3 months, $50 min savings)
        dynamodb-optima analyze-table-class
        
        # Analyze specific tables in us-east-1
        dynamodb-optima analyze-table-class --region us-east-1 --table MyTable
        
        # Analyze with higher savings threshold
        dynamodb-optima analyze-table-class --min-savings 100 --months 6
        
        # Output as JSON
        dynamodb-optima analyze-table-class --format json
    """
    try:
        click.echo("📊 Starting table class analysis...")
        click.echo()
        
        # Convert filters to lists
        account_ids = list(account) if account else None
        regions = list(region) if region else None
        table_names = list(table) if table else None
        
        # Display analysis parameters
        click.echo(f"Analysis Parameters:")
        click.echo(f"  Months to analyze: {months}")
        click.echo(f"  Minimum monthly savings: ${min_savings:.2f}")
        if account_ids:
            click.echo(f"  Accounts: {', '.join(account_ids)}")
        if regions:
            click.echo(f"  Regions: {', '.join(regions)}")
        if table_names:
            click.echo(f"  Tables: {', '.join(table_names)}")
        click.echo()
        
        # Initialize analyzer
        analyzer = TableClassAnalyzer(min_monthly_savings=Decimal(str(min_savings)))
        
        # Run analysis
        click.echo("🔍 Analyzing CUR data...")
        recommendations = analyzer.analyze_tables(
            months=months,
            account_ids=account_ids,
            regions=regions,
            table_names=table_names
        )
        
        if not recommendations:
            click.echo()
            click.echo("✅ No table class recommendations found")
            click.echo()
            click.echo("Possible reasons:")
            click.echo("  - All tables are already optimally configured")
            click.echo("  - Potential savings are below the minimum threshold")
            click.echo("  - No CUR data available for the specified filters")
            click.echo()
            click.echo("Try:")
            click.echo("  - Lowering --min-savings threshold")
            click.echo("  - Increasing --months to analyze more data")
            click.echo("  - Removing filters to analyze all tables")
            return
        
        # Save to database if requested
        if save:
            click.echo("💾 Saving recommendations to database...")
            analyzer.save_recommendations(recommendations)
            click.echo()
        
        # Display results
        click.echo()
        click.echo(f"✅ Found {len(recommendations)} table class recommendations")
        click.echo()
        
        # Calculate totals
        total_monthly_savings = sum(r.potential_monthly_savings for r in recommendations)
        total_annual_savings = sum(r.potential_annual_savings for r in recommendations)
        
        std_to_ia = [r for r in recommendations if r.recommended_class == 'STANDARD_IA']
        ia_to_std = [r for r in recommendations if r.recommended_class == 'STANDARD']
        
        click.echo("📈 Summary:")
        click.echo(f"   Total monthly savings potential: ${total_monthly_savings:,.2f}")
        click.echo(f"   Total annual savings potential: ${total_annual_savings:,.2f}")
        click.echo()
        click.echo(f"   Standard → Standard-IA: {len(std_to_ia)} tables (${sum(r.potential_monthly_savings for r in std_to_ia):,.2f}/month)")
        click.echo(f"   Standard-IA → Standard: {len(ia_to_std)} tables (${sum(r.potential_monthly_savings for r in ia_to_std):,.2f}/month)")
        click.echo()
        
        # Format output
        if format == 'table':
            _display_table(recommendations)
        elif format == 'json':
            _display_json(recommendations)
        elif format == 'csv':
            _display_csv(recommendations)
        
        click.echo()
        click.echo("💡 Next steps:")
        click.echo("   1. Review recommendations above")
        click.echo("   2. Verify table class assumptions with actual table configurations")
        if save:
            click.echo("   3. Query recommendations: SELECT * FROM table_class_recommendations")
        click.echo("   4. Implement changes via AWS Console or CLI")
        
    except Exception as e:
        logger.error(f"Table class analysis failed: {e}", exc_info=True)
        click.echo(f"❌ Analysis failed: {e}", err=True)
        raise click.Abort()


def _display_table(recommendations):
    """Display recommendations in table format."""
    import sys
    
    # Sort by savings descending
    recs = sorted(recommendations, key=lambda r: r.potential_monthly_savings, reverse=True)
    
    # Display top 20
    click.echo("Top Recommendations:")
    click.echo()
    
    # Header
    click.echo(f"{'Table':<40} {'Account':<15} {'Region':<15} {'Current':<12} {'→ Recommended':<15} {'Monthly $':<12}")
    click.echo("-" * 120)
    
    for rec in recs[:20]:
        click.echo(
            f"{rec.table_name:<40} "
            f"{rec.account_id:<15} "
            f"{rec.region:<15} "
            f"{rec.current_class:<12} "
            f"→ {rec.recommended_class:<13} "
            f"${rec.potential_monthly_savings:>10,.2f}"
        )
    
    if len(recs) > 20:
        click.echo()
        click.echo(f"... and {len(recs) - 20} more recommendations")


def _display_json(recommendations):
    """Display recommendations in JSON format."""
    import json
    from datetime import datetime
    
    def json_serializer(obj):
        if isinstance(obj, Decimal):
            return float(obj)
        if isinstance(obj, datetime):
            return obj.isoformat()
        raise TypeError(f"Type {type(obj)} not serializable")
    
    data = []
    for rec in recommendations:
        data.append({
            'table_name': rec.table_name,
            'account_id': rec.account_id,
            'region': rec.region,
            'current_class': rec.current_class,
            'recommended_class': rec.recommended_class,
            'current_monthly_storage_cost': float(rec.current_monthly_storage_cost),
            'current_monthly_throughput_cost': float(rec.current_monthly_throughput_cost),
            'projected_monthly_storage_cost': float(rec.projected_monthly_storage_cost),
            'projected_monthly_throughput_cost': float(rec.projected_monthly_throughput_cost),
            'potential_monthly_savings': float(rec.potential_monthly_savings),
            'potential_annual_savings': float(rec.potential_annual_savings),
            'storage_to_throughput_ratio': float(rec.storage_to_throughput_ratio),
            'recommendation_reason': rec.recommendation_reason,
        })
    
    click.echo(json.dumps(data, indent=2, default=json_serializer))


def _display_csv(recommendations):
    """Display recommendations in CSV format."""
    import csv
    import sys
    
    writer = csv.writer(sys.stdout)
    
    # Header
    writer.writerow([
        'table_name',
        'account_id',
        'region',
        'current_class',
        'recommended_class',
        'current_monthly_storage_cost',
        'current_monthly_throughput_cost',
        'projected_monthly_storage_cost',
        'projected_monthly_throughput_cost',
        'potential_monthly_savings',
        'potential_annual_savings',
        'storage_to_throughput_ratio',
        'recommendation_reason'
    ])
    
    # Data rows
    for rec in recommendations:
        writer.writerow([
            rec.table_name,
            rec.account_id,
            rec.region,
            rec.current_class,
            rec.recommended_class,
            float(rec.current_monthly_storage_cost),
            float(rec.current_monthly_throughput_cost),
            float(rec.projected_monthly_storage_cost),
            float(rec.projected_monthly_throughput_cost),
            float(rec.potential_monthly_savings),
            float(rec.potential_annual_savings),
            float(rec.storage_to_throughput_ratio),
            rec.recommendation_reason
        ])
