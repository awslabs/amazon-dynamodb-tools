"""
Version command for DynamoDB Optima CLI.
"""

import click


@click.command()
@click.pass_context
def version(ctx: click.Context) -> None:
    """Show version information."""
    # Lazy import to avoid circular dependency with package initialization
    from ... import __version__

    click.echo(f"DynamoDB Optima version {__version__}")
