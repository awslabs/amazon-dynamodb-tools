"""
CLI command for utilization analysis.
"""

import click
from tabulate import tabulate

from ...database.connection import get_connection
from ...analysis.utilization import UtilizationAnalyzer
from ...logging import get_logger

logger = get_logger(__name__)


@click.command()
@click.option(
    "--days",
    type=int,
    default=14,
    help="Number of days of metrics to analyze (default: 14)"
)
@click.option(
    "--table",
    type=str,
    help="Analyze specific table (format: region:table_name)"
)
@click.option(
    "--threshold",
    type=float,
    default=45.0,
    help="Utilization threshold percentage (default: 45.0)"
)
@click.option(
    "--min-savings",
    type=float,
    default=10.0,
    help="Minimum savings threshold in USD (default: 10.0)"
)
@click.option(
    "--format",
    type=click.Choice(["table", "csv", "json"]),
    default="table",
    help="Output format"
)
@click.pass_context
def analyze_utilization(ctx, days, table, threshold, min_savings, format):
    """
    Analyze provisioned capacity utilization and identify underutilized resources.
    
    This command examines actual consumption vs provisioned capacity to identify
    opportunities for cost reduction through capacity adjustments or mode changes.
    
    Examples:
        # Analyze all provisioned tables
        dynamodb-optima analyze-utilization
        
        # Analyze with 30 days of metrics
        dynamodb-optima analyze-utilization --days 30
        
        # Analyze specific table
        dynamodb-optima analyze-utilization --table us-east-1:my-table
        
        # Show only recommendations with >$50 savings
        dynamodb-optima analyze-utilization --min-savings 50
        
        # Use 30% utilization threshold
        dynamodb-optima analyze-utilization --threshold 30
    """
    try:
        # Get database connection
        connection = get_connection()
        
        # Create analyzer
        analyzer = UtilizationAnalyzer(connection)
        
        # Run analysis
        if table:
            # Parse table specification
            try:
                region, table_name = table.split(":")
            except ValueError:
                click.echo(
                    "Error: Table must be in format 'region:table_name'",
                    err=True
                )
                ctx.exit(1)
            
            click.echo(f"Analyzing utilization: {table_name} in {region}")
            click.echo(f"Analysis window: {days} days")
            click.echo(f"Utilization threshold: {threshold}%")
            click.echo()
            
            recommendations = analyzer.analyze_table(
                region, table_name, days, threshold, min_savings
            )
        else:
            click.echo(f"Analyzing utilization for all provisioned tables...")
            click.echo(f"Analysis window: {days} days")
            click.echo(f"Utilization threshold: {threshold}%")
            click.echo()
            
            recommendations = analyzer.analyze_all_tables(
                days, threshold, min_savings
            )
        
        if not recommendations:
            click.echo("No underutilized resources found with the specified criteria.")
            click.echo()
            click.echo("💡 This is good news! Your provisioned capacity appears well-utilized.")
            return
        
        # Display results
        if format == "table":
            _display_table_format(recommendations)
        elif format == "csv":
            _display_csv_format(recommendations)
        elif format == "json":
            _display_json_format(recommendations)
        
        # Summary
        total_savings = sum(r.potential_monthly_savings for r in recommendations)
        reduce_count = sum(1 for r in recommendations if r.recommendation_type == "REDUCE_CAPACITY")
        switch_count = sum(1 for r in recommendations if r.recommendation_type == "SWITCH_TO_ON_DEMAND")
        
        click.echo()
        click.echo("=" * 80)
        click.echo(f"Total potential savings: ${total_savings:,.2f}/month")
        click.echo(f"Recommendations: {reduce_count} reduce capacity, {switch_count} switch to On-Demand")
        click.echo(f"Total underutilized resources: {len(recommendations)}")
        click.echo("=" * 80)
        
    except Exception as e:
        logger.error("Utilization analysis failed", error=str(e), exc_info=True)
        click.echo(f"Error: {str(e)}", err=True)
        ctx.exit(1)


def _display_table_format(recommendations):
    """Display recommendations in table format."""
    rows = []
    
    for rec in recommendations:
        # Truncate resource name if too long
        resource_name = rec.resource_name
        if len(resource_name) > 35:
            resource_name = resource_name[:32] + "..."
        
        # Determine action symbol
        if rec.recommendation_type == "SWITCH_TO_ON_DEMAND":
            action = "→ On-Demand"
        elif rec.recommendation_type == "REDUCE_CAPACITY":
            action = f"↓ {rec.recommended_read_capacity}/{rec.recommended_write_capacity}"
        else:
            action = "✓ OK"
        
        rows.append([
            resource_name,
            rec.region,
            rec.resource_type[:3],  # TAB or GSI
            f"{rec.read_utilization_pct:.1f}%",
            f"{rec.write_utilization_pct:.1f}%",
            f"{rec.provisioned_read_capacity}/{rec.provisioned_write_capacity}",
            f"{rec.avg_consumed_read_capacity:.1f}/{rec.avg_consumed_write_capacity:.1f}",
            action,
            f"${rec.potential_monthly_savings:.2f}",
            f"{rec.confidence_score:.0%}"
        ])
    
    # Sort by savings (highest first)
    rows.sort(key=lambda x: float(x[8].replace("$", "")), reverse=True)
    
    headers = [
        "Resource", "Region", "Type", "Read %", "Write %",
        "Prov (R/W)", "Avg (R/W)", "Recommendation", "Savings", "Conf"
    ]
    
    click.echo(tabulate(rows, headers=headers, tablefmt="grid"))


def _display_csv_format(recommendations):
    """Display recommendations in CSV format."""
    import csv
    import sys
    
    writer = csv.writer(sys.stdout)
    writer.writerow([
        "Region", "Table Name", "Resource Name", "Resource Type",
        "Read Utilization %", "Write Utilization %", "Avg Utilization %",
        "Provisioned Read", "Provisioned Write",
        "Avg Consumed Read", "Avg Consumed Write",
        "Recommendation Type", "Recommended Read", "Recommended Write",
        "Monthly Savings", "Confidence Score", "Analysis Days", "Data Points",
        "Rationale"
    ])
    
    for rec in recommendations:
        writer.writerow([
            rec.region,
            rec.table_name,
            rec.resource_name,
            rec.resource_type,
            f"{rec.read_utilization_pct:.2f}",
            f"{rec.write_utilization_pct:.2f}",
            f"{rec.avg_utilization_pct:.2f}",
            rec.provisioned_read_capacity,
            rec.provisioned_write_capacity,
            f"{rec.avg_consumed_read_capacity:.2f}",
            f"{rec.avg_consumed_write_capacity:.2f}",
            rec.recommendation_type,
            rec.recommended_read_capacity or "",
            rec.recommended_write_capacity or "",
            f"{rec.potential_monthly_savings:.2f}",
            f"{rec.confidence_score:.2f}",
            rec.analysis_days,
            rec.data_points,
            rec.rationale
        ])


def _display_json_format(recommendations):
    """Display recommendations in JSON format."""
    import json
    
    data = []
    for rec in recommendations:
        data.append({
            "region": rec.region,
            "table_name": rec.table_name,
            "resource_name": rec.resource_name,
            "resource_type": rec.resource_type,
            "read_utilization_pct": rec.read_utilization_pct,
            "write_utilization_pct": rec.write_utilization_pct,
            "avg_utilization_pct": rec.avg_utilization_pct,
            "provisioned_read_capacity": rec.provisioned_read_capacity,
            "provisioned_write_capacity": rec.provisioned_write_capacity,
            "avg_consumed_read_capacity": rec.avg_consumed_read_capacity,
            "avg_consumed_write_capacity": rec.avg_consumed_write_capacity,
            "recommendation_type": rec.recommendation_type,
            "recommended_read_capacity": rec.recommended_read_capacity,
            "recommended_write_capacity": rec.recommended_write_capacity,
            "potential_monthly_savings": rec.potential_monthly_savings,
            "confidence_score": rec.confidence_score,
            "analysis_days": rec.analysis_days,
            "data_points": rec.data_points,
            "rationale": rec.rationale,
            "created_at": rec.created_at.isoformat()
        })
    
    click.echo(json.dumps(data, indent=2))
