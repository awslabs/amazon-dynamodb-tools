"""
Checkpoints command for DMetrics CLI.
"""

import click


@click.command()
@click.option("--cleanup", is_flag=True, help="Clean up old checkpoint files")
@click.option("--max-age", default=7, help="Maximum age in days for cleanup")
@click.pass_context
def checkpoints(ctx: click.Context, cleanup: bool, max_age: int) -> None:
    """Manage operation checkpoints."""
    logger = ctx.obj["logger"]

    # Lazy import to avoid circular dependency
    from ...core.state import StateManager

    state_manager = StateManager()

    try:
        if cleanup:
            click.echo(f"  Cleaning up checkpoints older than {max_age} days...")
            cleaned_count = state_manager.cleanup_old_checkpoints(max_age)
            click.echo(f"  Cleaned up {cleaned_count} old checkpoint files")
            return

        # List all checkpoints with details
        checkpoints = state_manager.list_checkpoints_with_details()

        if not checkpoints:
            click.echo("  No checkpoints found")
            return

        click.echo(f"  Found {len(checkpoints)} checkpoints:")
        click.echo()

        for checkpoint in checkpoints:
            status_icon = {
                "RUNNING": "🔄",
                "PAUSED": "⏸️",
                "COMPLETED": "✅",
                "FAILED": "❌",
                "CORRUPTED": "💥",
            }.get(checkpoint["status"], "❓")

            click.echo(f"{status_icon} {checkpoint['operation_id']}")
            click.echo(f"   Type: {checkpoint['operation_type']}")
            click.echo(f"   Status: {checkpoint['status']}")
            click.echo(
                f"   Progress: {checkpoint.get('completion_percentage', 0):.1f}%"
            )
            click.echo(f"   Started: {checkpoint.get('start_time', 'Unknown')}")
            last_checkpoint = checkpoint.get("last_checkpoint_time", "Unknown")
            click.echo(f"   Last checkpoint: {last_checkpoint}")

            if checkpoint.get("estimated_completion"):
                click.echo(f"   ETA: {checkpoint['estimated_completion']}")

            if checkpoint.get("error"):
                click.echo(f"   ❌ Error: {checkpoint['error']}")

            # File info
            file_size_mb = checkpoint.get("file_size", 0) / (1024 * 1024)
            modified_time = checkpoint.get("modified_time", "Unknown")
            click.echo(f"   File: {file_size_mb:.2f} MB, Modified: {modified_time}")
            click.echo()

        # Show usage hints
        click.echo("  Commands:")
        click.echo("   metrics-collector resume --latest          # Resume latest checkpoint")
        click.echo("   metrics-collector resume <operation_id>    # Resume specific checkpoint")
        click.echo("   metrics-collector checkpoints --cleanup    # Clean up old checkpoints")

    except Exception as e:
        logger.error("Failed to manage checkpoints", error=str(e))
