"""
Path management for DynamoDB Optima.

Centralizes all file system paths with support for custom project root.
This enables multi-organization deployments with isolated data directories.
"""
from pathlib import Path
from typing import Optional

# Default project root (parent of src directory)
# From paths.py location: src/dynamodb_optima/paths.py -> up 2 levels to project root
DEFAULT_PROJECT_ROOT = Path(__file__).parent.parent.parent

# Global override set by CLI --project-root option
_project_root_override: Optional[Path] = None


def set_project_root(path: str) -> None:
    """
    Set custom project root directory.
    
    Called by CLI before any other initialization to ensure all paths
    use the specified root directory.
    
    Args:
        path: Path to project root directory
    """
    global _project_root_override
    _project_root_override = Path(path).resolve()


def get_project_root() -> Path:
    """
    Get active project root directory.
    
    Returns override if set via CLI, otherwise returns default project root.
    
    Returns:
        Path to active project root directory
    """
    return _project_root_override or DEFAULT_PROJECT_ROOT


def get_data_dir() -> Path:
    """
    Get data directory for database and application files.
    
    Returns:
        Path: <project_root>/data
    """
    return get_project_root() / "data"


def get_logs_dir() -> Path:
    """
    Get logs directory for application logs.
    
    Returns:
        Path: <project_root>/logs
    """
    return get_project_root() / "logs"


def get_checkpoints_dir() -> Path:
    """
    Get checkpoints directory for resumable operations.
    
    Returns:
        Path: <project_root>/checkpoints
    """
    return get_project_root() / "checkpoints"


def get_database_path() -> str:
    """
    Get default database file path.
    
    Returns:
        str: <project_root>/data/metrics_collector.db
    """
    return str(get_data_dir() / "metrics_collector.db")
