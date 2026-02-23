"""
Health command for DMetrics CLI.
"""

from datetime import datetime

import click


@click.command()
@click.option("--detailed", is_flag=True, help="Show detailed health information")
@click.option("--json", "output_json", is_flag=True, help="Output results as JSON")
@click.pass_context
def health(ctx: click.Context, detailed: bool, output_json: bool) -> None:
    """Check system health and operational status."""
    logger = ctx.obj["logger"]

    try:
        # Lazy import to avoid circular dependency
        from ...utils.health_checks import create_health_monitor

        monitor = create_health_monitor(logger)
        checks = monitor.run_all_checks()

        if output_json:
            # Lazy import - only needed conditionally
            import json

            result = {
                "timestamp": datetime.now().isoformat(),
                "checks": {name: check.to_dict() for name, check in checks.items()},
                "system_metrics": monitor.get_system_metrics().to_dict(),
            }
            click.echo(json.dumps(result, indent=2))
        else:
            report = monitor.format_health_report(checks)
            click.echo(report)

            if detailed:
                click.echo("\n  Detailed Check Results:")
                for name, check in checks.items():
                    click.echo(f"\n🔍 {name.replace('_', ' ').title()}:")
                    click.echo(f"   Status: {check.status.value}")
                    click.echo(f"   Duration: {check.duration_ms:.1f}ms")
                    if check.details:
                        for key, value in check.details.items():
                            if isinstance(value, (str, int, float, bool)):
                                click.echo(f"   {key}: {value}")

    except Exception as e:
        from ...utils.error_handling import ErrorContext

        error_handler = ctx.obj["error_handler"]
        context = ErrorContext(operation_type="HEALTH_CHECK")
        error_handler.handle_error(e, context, show_technical=detailed)
