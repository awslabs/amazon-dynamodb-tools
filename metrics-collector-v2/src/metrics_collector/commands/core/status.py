"""
Status command for DMetrics CLI.
"""

from datetime import datetime
from typing import Optional

import click


@click.command()
@click.argument("operation_id", required=False)
@click.option("--detailed", is_flag=True, help="Show detailed status information")
@click.option("--throughput", is_flag=True, help="Show throughput metrics")
@click.pass_context
def status(
    ctx: click.Context, operation_id: Optional[str], detailed: bool, throughput: bool
) -> None:
    """Show current operation status and checkpoint information."""
    logger = ctx.obj["logger"]

    # Lazy import to avoid circular dependency
    from ...core.state import StateManager

    state_manager = StateManager()

    try:
        if operation_id:
            # Show specific operation status
            state = state_manager.load_checkpoint(operation_id)
            if not state:
                click.echo(f"Operation {operation_id} not found", err=True)
                return

            # Basic status info
            status_icon = {
                "RUNNING": "•",
                "PAUSED": "||",
                "COMPLETED": "✓",
                "FAILED": "✗",
            }.get(state.status, "?")

            click.echo(f"Operation Status: {operation_id}")
            click.echo(f"   {status_icon} Status: {state.status}")
            click.echo(f"   Progress: {state.completion_percentage:.1f}%")
            click.echo(f"   Started: {state.start_time.strftime('%Y-%m-%d %H:%M:%S')}")
            click.echo(
                f"   Last checkpoint: {state.last_checkpoint_time.strftime('%Y-%m-%d %H:%M:%S')}"
            )

            if state.estimated_completion:
                if isinstance(state.estimated_completion, str):
                    eta_str = state.estimated_completion
                else:
                    eta_str = state.estimated_completion.strftime("%Y-%m-%d %H:%M:%S")
                click.echo(f"   ETA: {eta_str}")

            if state.error_message:
                click.echo(f"   Error: {state.error_message}")

            # Detailed information
            if detailed and state.collection_state:
                click.echo("\n  Detailed Information:")
                cs = state.collection_state

                if cs.regions_completed:
                    click.echo(f"   Regions completed: {len(cs.regions_completed)}")
                    if cs.regions_to_discover:
                        remaining = len(cs.regions_to_discover) - len(
                            cs.regions_completed
                        )
                        click.echo(f"   Regions remaining: {remaining}")

                if cs.tables_discovered:
                    total_tables = sum(
                        len(tables) for tables in cs.tables_discovered.values()
                    )
                    click.echo(f"   Tables discovered: {total_tables}")

                if cs.gsis_discovered:
                    total_gsis = sum(len(gsis) for gsis in cs.gsis_discovered.values())
                    click.echo(f"   GSIs discovered: {total_gsis}")

                if cs.completed_resources:
                    click.echo(f"   Resources processed: {len(cs.completed_resources)}")

                if cs.failed_collections:
                    click.echo(f"   Failed collections: {len(cs.failed_collections)}")

            # Throughput metrics
            if throughput and state.collection_state:
                click.echo("\n⚡ Throughput Metrics:")
                cs = state.collection_state

                if cs.start_time and cs.completed_operations > 0:
                    elapsed = (datetime.now() - cs.start_time).total_seconds()
                    if elapsed > 0:
                        ops_per_sec = cs.completed_operations / elapsed
                        click.echo(f"   Operations/sec: {ops_per_sec:.2f}")

                        if cs.total_operations > 0:
                            remaining_ops = (
                                cs.total_operations - cs.completed_operations
                            )
                            eta_seconds = (
                                remaining_ops / ops_per_sec if ops_per_sec > 0 else 0
                            )
                            if eta_seconds > 0:
                                if eta_seconds < 60:
                                    eta_str = f"{eta_seconds:.0f}s"
                                elif eta_seconds < 3600:
                                    eta_str = f"{eta_seconds//60:.0f}m {eta_seconds % 60:.0f}s"
                                else:
                                    eta_str = f"{eta_seconds//3600:.0f}h {(eta_seconds % 3600)//60:.0f}m"
                                click.echo(f"   ⏰ Calculated ETA: {eta_str}")

        else:
            # Show all operations summary
            checkpoints = state_manager.list_checkpoints_with_details()

            if not checkpoints:
                click.echo("  No operations found")
                return

            click.echo("  Operations Summary")
            click.echo(f"   Total operations: {len(checkpoints)}")
            click.echo()

            # Group by status
            by_status = {}
            for checkpoint in checkpoints:
                status = checkpoint.get("status", "UNKNOWN")
                if status not in by_status:
                    by_status[status] = []
                by_status[status].append(checkpoint)

            # Show summary by status
            for status in ["RUNNING", "PAUSED", "FAILED", "COMPLETED", "CORRUPTED"]:
                if status not in by_status:
                    continue

                status_icon = {
                    "RUNNING": "🔄",
                    "PAUSED": "⏸️",
                    "COMPLETED": "✅",
                    "FAILED": "❌",
                    "CORRUPTED": "💥",
                }.get(status, "❓")

                count = len(by_status[status])
                click.echo(f"{status_icon} {status}: {count} operations")

                # Show recent operations for this status
                recent_ops = sorted(
                    by_status[status],
                    key=lambda x: x.get("modified_time", datetime.min),
                    reverse=True,
                )[:3]

                for checkpoint in recent_ops:
                    op_id = checkpoint["operation_id"]
                    op_type = checkpoint.get("operation_type", "UNKNOWN")
                    progress = checkpoint.get("completion_percentage", 0)

                    # Show full operation ID so users can copy/paste for resume
                    click.echo(f"   • {op_id} ({op_type}) - {progress:.1f}%")

            click.echo("\n  Commands:")
            click.echo("   metrics-collector status <operation_id>     # Detailed status")
            click.echo("   metrics-collector resume --latest           # Resume latest operation")
            click.echo("   metrics-collector monitor --watch           # Real-time monitoring")

    except Exception as e:
        logger.error("Failed to get status", error=str(e))
        click.echo(f" Error getting status: {e}", err=True)
