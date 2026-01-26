"""
Version command for Metrics Collector CLI.
"""

import click


@click.command()
@click.pass_context
def version(ctx: click.Context) -> None:
    """Show version information."""
    # Lazy import to avoid circular dependency with package initialization
    from ... import __version__

    click.echo(f"Metrics Collector version {__version__}")
