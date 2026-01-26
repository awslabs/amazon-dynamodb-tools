"""
Command-line interface for Metrics Collector platform.

Provides commands for metrics collection, cost analysis, and system management
with state management and resumable operations.
"""

from typing import Optional

import click

# Import all core commands
from .commands.analysis.analyze_capacity import analyze_capacity as analyze_capacity_cmd
from .commands.analysis.analyze_table_class import analyze_table_class as analyze_table_class_cmd
from .commands.analysis.analyze_utilization import analyze_utilization as analyze_utilization_cmd
from .commands.collect import collect as collect_cmd
from .commands.collect_cur import collect_cur as collect_cur_cmd
from .commands.core.checkpoints import checkpoints
from .commands.core.health import health
from .commands.core.status import status
from .commands.core.version import version
from .commands.discover import discover as discover_cmd
from .config import get_settings
from .logging import configure_logging, get_logger


@click.group()
@click.option("--debug", is_flag=True, help="Enable debug mode")
@click.option("--log-level", default="INFO", help="Set logging level")
@click.pass_context
def main(ctx: click.Context, debug: bool, log_level: str) -> None:
    """Metrics Collector - DynamoDB cost optimization and analysis platform."""
    # Ensure context object exists
    ctx.ensure_object(dict)

    # Update settings
    settings = get_settings()
    settings.debug = debug
    settings.log_level = log_level

    # Configure logging
    configure_logging()
    logger = get_logger(__name__)

    # Store in context for commands to access
    ctx.obj["logger"] = logger
    ctx.obj["settings"] = settings


# Add core commands (always available)
main.add_command(checkpoints)
main.add_command(health)
main.add_command(status)
main.add_command(version)

# Add real discover and collect commands (Phase 1 & 2)
main.add_command(discover_cmd)
main.add_command(collect_cmd)

# Add CUR collection command (Phase 4B)
main.add_command(collect_cur_cmd, name="collect-cur")

# Add analysis commands (Phase 3+)
main.add_command(analyze_capacity_cmd, name="analyze-capacity")
main.add_command(analyze_table_class_cmd, name="analyze-table-class")
main.add_command(analyze_utilization_cmd, name="analyze-utilization")


@main.command(name="list-recommendations")
@click.option("--type", help="Filter by recommendation type (capacity, table-class, utilization)")
@click.option("--table", help="Filter by table name")
@click.option("--min-savings", type=float, help="Minimum monthly savings in USD")
@click.option("--format", default="table", type=click.Choice(["table", "json", "csv"]))
@click.pass_context
def list_recommendations(
    ctx: click.Context,
    type: Optional[str],
    table: Optional[str],
    min_savings: Optional[float],
    format: str,
) -> None:
    """List all cost optimization recommendations."""
    click.echo("� Recommendations list command")
    click.echo(f"  Type filter: {type or 'all'}")
    click.echo(f"  Table filter: {table or 'all'}")
    click.echo(f"  Min savings: ${min_savings or 0}")
    click.echo(f"  Format: {format}")
    click.echo("\n⚠️  Implementation pending (Phase 5)")


@main.command(name="gui")
@click.option("--port", default=8501, help="Port for Streamlit GUI (default: 8501)")
@click.option("--theme", default="light", type=click.Choice(["light", "dark"]))
@click.pass_context
def gui(ctx: click.Context, port: int, theme: str) -> None:
    """Launch interactive Streamlit GUI for analysis and visualization."""
    import subprocess
    import sys
    from pathlib import Path

    logger = ctx.obj.get("logger", get_logger(__name__))

    click.echo("🎨 Launching DynamoDB Cost Optimizer GUI...")
    click.echo(f"  Port: {port}")
    click.echo(f"  Theme: {theme}")
    click.echo()

    # Get path to GUI app
    gui_app_path = Path(__file__).parent / "gui" / "app.py"

    if not gui_app_path.exists():
        click.echo(f"❌ Error: GUI app not found at {gui_app_path}", err=True)
        sys.exit(1)

    # Build streamlit command
    cmd = [
        "streamlit",
        "run",
        str(gui_app_path),
        f"--server.port={port}",
        "--server.headless=true",
        f"--theme.base={theme}",
    ]

    click.echo(f"📡 Starting Streamlit server on port {port}...")
    click.echo("   Press Ctrl+C to stop the server")
    click.echo()

    try:
        # Run streamlit
        result = subprocess.run(cmd, check=True)
        sys.exit(result.returncode)
    except subprocess.CalledProcessError as e:
        logger.error(f"Failed to start GUI: {e}")
        click.echo(f"❌ Error launching GUI: {e}", err=True)
        sys.exit(1)
    except KeyboardInterrupt:
        click.echo("\n👋 GUI stopped by user")
        sys.exit(0)
    except FileNotFoundError:
        click.echo(
            "❌ Error: Streamlit not found. Install it with: pip install streamlit",
            err=True,
        )
        sys.exit(1)


if __name__ == "__main__":
    main()
