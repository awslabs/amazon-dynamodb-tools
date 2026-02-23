"""
Database view management for DMetrics.

Provides functionality to create, update, and manage database views
with version control and dependency tracking.
"""

import logging
from typing import Dict, List, Optional

from .connection import DatabaseManager
from .views import get_capacity_view_definitions

logger = logging.getLogger(__name__)


class ViewManager:
    """Manages database views with versioning and dependency tracking."""

    def __init__(self, db_manager: Optional[DatabaseManager] = None):
        self.db_manager = db_manager or DatabaseManager()
        self.view_registry = {
            "capacity": get_capacity_view_definitions,
            # Add more view categories as they're developed
            # "latency": get_latency_view_definitions,
            # "cost_optimization": get_cost_optimization_view_definitions,
        }

    def create_all_views(
        self, categories: Optional[List[str]] = None
    ) -> Dict[str, bool]:
        """
        Create all views or specific categories.

        Args:
            categories: List of view categories to create (None = all)

        Returns:
            Dictionary mapping view names to success status
        """
        results = {}

        # Determine which categories to process
        target_categories = categories or list(self.view_registry.keys())

        for category in target_categories:
            if category not in self.view_registry:
                logger.warning(f"Unknown view category: {category}")
                continue

            logger.info(f"Creating {category} views...")
            category_views = self.view_registry[category]()

            for view_name, view_sql in category_views.items():
                try:
                    self.db_manager.execute_query(view_sql)
                    results[view_name] = True
                    logger.info(f"✅ Created view: {view_name}")
                except Exception as e:
                    results[view_name] = False
                    logger.error(f"❌ Failed to create view {view_name}: {e}")

        return results

    def drop_view(self, view_name: str, if_exists: bool = True) -> bool:
        """
        Drop a specific view.

        Args:
            view_name: Name of the view to drop
            if_exists: Use IF EXISTS clause

        Returns:
            True if successful, False otherwise
        """
        try:
            if_exists_clause = "IF EXISTS" if if_exists else ""
            sql = f"DROP VIEW {if_exists_clause} {view_name}"
            self.db_manager.execute_query(sql)
            logger.info(f"✅ Dropped view: {view_name}")
            return True
        except Exception as e:
            logger.error(f"❌ Failed to drop view {view_name}: {e}")
            return False

    def recreate_view(self, view_name: str, category: str) -> bool:
        """
        Recreate a specific view (drop and create).

        Args:
            view_name: Name of the view to recreate
            category: Category the view belongs to

        Returns:
            True if successful, False otherwise
        """
        if category not in self.view_registry:
            logger.error(f"Unknown view category: {category}")
            return False

        # Get view definition
        category_views = self.view_registry[category]()
        if view_name not in category_views:
            logger.error(f"View {view_name} not found in category {category}")
            return False

        # Drop and recreate
        self.drop_view(view_name, if_exists=True)

        try:
            view_sql = category_views[view_name]
            self.db_manager.execute_query(view_sql)
            logger.info(f"✅ Recreated view: {view_name}")
            return True
        except Exception as e:
            logger.error(f"❌ Failed to recreate view {view_name}: {e}")
            return False

    def list_views(self) -> List[Dict[str, str]]:
        """
        List all views in the database.

        Returns:
            List of dictionaries with view information
        """
        try:
            # DuckDB-specific query to list views
            sql = """
            SELECT
                table_name as view_name,
                table_type,
                table_schema
            FROM information_schema.tables
            WHERE table_type = 'VIEW'
            ORDER BY table_name
            """

            results = self.db_manager.execute_query(sql)
            return [
                {
                    "view_name": row["view_name"],
                    "type": row["table_type"],
                    "schema": row["table_schema"],
                }
                for row in results
            ]
        except Exception as e:
            logger.error(f"Failed to list views: {e}")
            return []

    def validate_view_dependencies(self) -> Dict[str, List[str]]:
        """
        Validate that all view dependencies exist.

        Returns:
            Dictionary mapping view names to missing dependencies
        """
        missing_deps = {}

        # Check if base tables exist
        required_tables = ["metrics", "table_metadata", "gsi_metadata"]

        for table in required_tables:
            try:
                self.db_manager.execute_query(f"SELECT 1 FROM {table} LIMIT 1")
            except Exception:
                # Table doesn't exist or is empty
                for category_name, category_func in self.view_registry.items():
                    views = category_func()
                    for view_name in views.keys():
                        if view_name not in missing_deps:
                            missing_deps[view_name] = []
                        missing_deps[view_name].append(table)

        return missing_deps

    def get_view_info(self) -> Dict[str, Dict[str, str]]:
        """
        Get information about all available views.

        Returns:
            Dictionary with view information organized by category
        """
        info = {}

        for category_name, category_func in self.view_registry.items():
            views = category_func()
            info[category_name] = {
                "count": len(views),
                "views": list(views.keys()),
                "description": f"{category_name.title()} analysis views",
            }

        return info


def create_views_cli_command():
    """CLI command to create database views."""
    import click

    @click.command()
    @click.option(
        "--category", multiple=True, help="Specific view categories to create"
    )
    @click.option("--recreate", is_flag=True, help="Recreate existing views")
    @click.option(
        "--list-only", is_flag=True, help="List available views without creating"
    )
    def create_views(category, recreate, list_only):
        """Create or manage database views for analysis."""

        view_manager = ViewManager()

        if list_only:
            info = view_manager.get_view_info()
            click.echo("\n📊 Available View Categories:\n")

            for cat_name, cat_info in info.items():
                click.echo(f"🔹 {cat_name.upper()}")
                click.echo(f"   Views: {cat_info['count']}")
                click.echo(f"   Names: {', '.join(cat_info['views'])}")
                click.echo(f"   Description: {cat_info['description']}")
                click.echo()
            return

        # Validate dependencies
        missing_deps = view_manager.validate_view_dependencies()
        if missing_deps:
            click.echo("⚠️  Missing dependencies detected:")
            for view_name, deps in missing_deps.items():
                click.echo(f"   {view_name}: missing {', '.join(deps)}")
            click.echo("\nRun data collection first to populate base tables.")
            return

        # Create views
        categories = list(category) if category else None
        results = view_manager.create_all_views(categories)

        # Report results
        successful = sum(1 for success in results.values() if success)
        total = len(results)

        click.echo(f"\n📊 View Creation Results: {successful}/{total} successful\n")

        for view_name, success in results.items():
            status = "✅" if success else "❌"
            click.echo(f"{status} {view_name}")

        if successful == total:
            click.echo(f"\n🎉 All views created successfully!")
            click.echo("You can now use these views in Superset dashboards.")
        else:
            click.echo(f"\n⚠️  Some views failed to create. Check logs for details.")

    return create_views


# For integration with existing CLI
def get_view_management_commands():
    """Get view management CLI commands for integration."""
    return {"create-views": create_views_cli_command()}
