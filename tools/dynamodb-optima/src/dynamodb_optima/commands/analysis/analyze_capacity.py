"""
CLI command for capacity mode analysis.
"""

import asyncio
import click
from tabulate import tabulate

from ...database.connection import get_connection
from ...analysis.capacity_mode import CapacityModeAnalyzer
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
    help="Analyze specific table (format: account_id:region:table_name)"
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
def analyze_capacity(ctx, days, table, min_savings, format):
    """
    Analyze capacity modes and generate On-Demand vs Provisioned recommendations.
    
    This command simulates autoscaling behavior to accurately predict provisioned
    capacity costs, then compares them with On-Demand costs to generate
    optimization recommendations.
    
    Examples:
        # Analyze all tables with 14 days of metrics
        dynamodb-optima analyze-capacity
        
        # Analyze with 30 days of metrics
        dynamodb-optima analyze-capacity --days 30
        
        # Analyze specific table
        dynamodb-optima analyze-capacity --table 123456:us-east-1:my-table
        
        # Show only recommendations with >$100 savings
        dynamodb-optima analyze-capacity --min-savings 100
    """
    try:
        # Get database connection
        connection = get_connection()
        
        # Create analyzer
        analyzer = CapacityModeAnalyzer(connection)
        
        # Run analysis
        if table:
            # Parse table specification
            try:
                account_id, region, table_name = table.split(":")
            except ValueError:
                click.echo(
                    "Error: Table must be in format 'account_id:region:table_name'",
                    err=True
                )
                ctx.exit(1)
            
            click.echo(f"Analyzing table: {table_name} in {region} (account: {account_id})")
            click.echo(f"Analysis window: {days} days")
            click.echo()
            
            recommendations = asyncio.run(
                analyzer.analyze_table(account_id, region, table_name, days)
            )
        else:
            click.echo(f"Analyzing all tables...")
            click.echo(f"Analysis window: {days} days")
            click.echo()
            
            recommendations = asyncio.run(analyzer.analyze_all_tables(days))
        
        # Filter by minimum savings
        recommendations = [
            r for r in recommendations
            if r.estimated_savings >= min_savings
        ]
        
        if not recommendations:
            click.echo("No recommendations found with the specified criteria.")
            return
        
        # Display results
        if format == "table":
            _display_table_format(recommendations)
        elif format == "csv":
            _display_csv_format(recommendations)
        elif format == "json":
            _display_json_format(recommendations)
        
        # Summary
        total_savings = sum(r.estimated_savings for r in recommendations)
        not_optimized = sum(1 for r in recommendations if r.optimization_status == "NOT_OPTIMIZED")
        
        click.echo()
        click.echo("=" * 80)
        click.echo(f"Total potential savings: ${total_savings:,.2f}/month")
        click.echo(f"Not optimized: {not_optimized} out of {len(recommendations)} recommendations")
        click.echo("=" * 80)
        
    except Exception as e:
        logger.error("Capacity analysis failed", error=str(e), exc_info=True)
        click.echo(f"Error: {str(e)}", err=True)
        ctx.exit(1)


def _display_table_format(recommendations):
    """Display recommendations in table format."""
    # Group by table and combine READ/WRITE
    table_data = {}
    for rec in recommendations:
        key = (rec.account_id, rec.region, rec.table_name)
        if key not in table_data:
            table_data[key] = {
                "account_id": rec.account_id,
                "region": rec.region,
                "table_name": rec.table_name,
                "table_class": rec.table_class,
                "current_mode": rec.current_mode,
                "recommended_mode": rec.recommended_mode,
                "current_cost": 0,
                "recommended_cost": 0,
                "savings": 0,
                "status": rec.optimization_status
            }
        
        table_data[key]["current_cost"] += rec.current_cost
        recommended_cost = (
            rec.on_demand_cost if rec.recommended_mode == "ON_DEMAND"
            else rec.provisioned_cost
        )
        table_data[key]["recommended_cost"] += recommended_cost
        table_data[key]["savings"] += rec.estimated_savings
    
    # Convert to list for tabulate
    rows = []
    for data in table_data.values():
        savings_pct = (
            (data["savings"] / data["current_cost"] * 100)
            if data["current_cost"] > 0 else 0
        )
        
        # Truncate table name if too long
        table_name = data["table_name"]
        if len(table_name) > 30:
            table_name = table_name[:27] + "..."
        
        rows.append([
            table_name,
            data["region"],
            data["table_class"][:3],  # STD or SIA
            data["current_mode"],
            data["recommended_mode"],
            f"${data['current_cost']:.2f}",
            f"${data['recommended_cost']:.2f}",
            f"${data['savings']:.2f}",
            f"{savings_pct:.1f}%",
            "✓" if data["status"] == "OPTIMIZED" else "✗"
        ])
    
    # Sort by savings (highest first)
    rows.sort(key=lambda x: float(x[7].replace("$", "")), reverse=True)
    
    headers = [
        "Table", "Region", "Class", "Current", "Recommended",
        "Curr Cost", "Rec Cost", "Savings", "Save %", "OK"
    ]
    
    click.echo(tabulate(rows, headers=headers, tablefmt="grid"))


def _display_csv_format(recommendations):
    """Display recommendations in CSV format."""
    import csv
    import sys
    
    writer = csv.writer(sys.stdout)
    writer.writerow([
        "Account ID", "Region", "Table Name", "Table Class", "Metric Type",
        "Current Mode", "Recommended Mode", "Current Cost", "On-Demand Cost",
        "Provisioned Cost", "Estimated Savings", "Savings %", "Status",
        "Confidence", "Analysis Days"
    ])
    
    for rec in recommendations:
        writer.writerow([
            rec.account_id,
            rec.region,
            rec.table_name,
            rec.table_class,
            rec.metric_type,
            rec.current_mode,
            rec.recommended_mode,
            f"{rec.current_cost:.2f}",
            f"{rec.on_demand_cost:.2f}",
            f"{rec.provisioned_cost:.2f}",
            f"{rec.estimated_savings:.2f}",
            f"{rec.savings_percentage:.2f}",
            rec.optimization_status,
            f"{rec.confidence_score:.2f}",
            rec.analysis_days
        ])


def _display_json_format(recommendations):
    """Display recommendations in JSON format."""
    import json
    
    data = []
    for rec in recommendations:
        data.append({
            "account_id": rec.account_id,
            "region": rec.region,
            "table_name": rec.table_name,
            "table_class": rec.table_class,
            "metric_type": rec.metric_type,
            "current_mode": rec.current_mode,
            "recommended_mode": rec.recommended_mode,
            "current_cost": rec.current_cost,
            "on_demand_cost": rec.on_demand_cost,
            "provisioned_cost": rec.provisioned_cost,
            "estimated_savings": rec.estimated_savings,
            "savings_percentage": rec.savings_percentage,
            "optimization_status": rec.optimization_status,
            "confidence_score": rec.confidence_score,
            "analysis_days": rec.analysis_days,
            "analyzed_at": rec.analyzed_at.isoformat()
        })
    
    click.echo(json.dumps(data, indent=2))
