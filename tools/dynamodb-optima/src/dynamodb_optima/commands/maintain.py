"""Manual lake maintenance: compact landing -> served across the whole lake."""

import click

from ..database import lake


@click.command(name="maintain")
@click.pass_context
def maintain(ctx: click.Context) -> None:
    """Compact the metrics lake (merge landing files into served month shards)."""
    click.echo("🧹 Compacting metrics lake (landing -> served)...")
    lake.compact_pending()
    click.echo("✅ Lake maintenance complete.")


def get_maintain_command():
    return maintain
