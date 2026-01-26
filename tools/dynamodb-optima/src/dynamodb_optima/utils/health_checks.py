"""
Health check and monitoring utilities for DynamoDB Optima operations.

Provides system health monitoring, operation status checks, and performance
monitoring capabilities.
"""

import logging
import os
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Dict, Optional, Tuple

import click


class HealthStatus(Enum):
    """Health status levels."""

    HEALTHY = "healthy"
    WARNING = "warning"
    CRITICAL = "critical"
    UNKNOWN = "unknown"


@dataclass
class HealthCheck:
    """Individual health check result."""

    name: str
    status: HealthStatus
    message: str
    details: Dict[str, Any] = field(default_factory=dict)
    timestamp: datetime = field(default_factory=datetime.now)
    duration_ms: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "name": self.name,
            "status": self.status.value,
            "message": self.message,
            "details": self.details,
            "timestamp": self.timestamp.isoformat(),
            "duration_ms": self.duration_ms,
        }


@dataclass
class SystemMetrics:
    """System performance metrics."""

    cpu_percent: float
    memory_percent: float
    memory_available_gb: float
    disk_usage_percent: float
    disk_free_gb: float
    load_average: Tuple[float, float, float]
    uptime_seconds: float

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "cpu_percent": self.cpu_percent,
            "memory_percent": self.memory_percent,
            "memory_available_gb": self.memory_available_gb,
            "disk_usage_percent": self.disk_usage_percent,
            "disk_free_gb": self.disk_free_gb,
            "load_average": list(self.load_average),
            "uptime_seconds": self.uptime_seconds,
        }


@dataclass
class OperationHealth:
    """Health status of operations."""

    running_operations: int
    paused_operations: int
    failed_operations: int
    completed_operations: int
    total_operations: int
    oldest_running_operation: Optional[datetime] = None

    @property
    def success_rate(self) -> float:
        """Calculate success rate."""
        if self.total_operations == 0:
            return 100.0
        return (self.completed_operations / self.total_operations) * 100

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "running_operations": self.running_operations,
            "paused_operations": self.paused_operations,
            "failed_operations": self.failed_operations,
            "completed_operations": self.completed_operations,
            "total_operations": self.total_operations,
            "success_rate": self.success_rate,
            "oldest_running_operation": (
                self.oldest_running_operation.isoformat()
                if self.oldest_running_operation
                else None
            ),
        }


class HealthMonitor:
    """
    Comprehensive health monitoring system for DynamoDB Optima operations.
    """

    def __init__(self, logger: Optional[logging.Logger] = None):
        """Initialize health monitor."""
        self.logger = logger or logging.getLogger(__name__)
        self.start_time = datetime.now()

    def check_system_resources(self) -> HealthCheck:
        """Check system resource availability."""
        start_time = time.time()

        try:
            # Try to import psutil for system metrics
            try:
                import psutil

                cpu_percent = psutil.cpu_percent(interval=1)
                memory = psutil.virtual_memory()
                disk = psutil.disk_usage(".")
            except ImportError:
                # Fallback to basic system info without psutil
                return HealthCheck(
                    name="system_resources",
                    status=HealthStatus.WARNING,
                    message="System monitoring unavailable (psutil not installed)",
                    details={"error": "psutil module not available"},
                    duration_ms=(time.time() - start_time) * 1000,
                )

            # Determine status based on thresholds
            status = HealthStatus.HEALTHY
            issues = []

            if cpu_percent > 90:
                status = HealthStatus.CRITICAL
                issues.append(f"CPU usage critical: {cpu_percent:.1f}%")
            elif cpu_percent > 75:
                status = HealthStatus.WARNING
                issues.append(f"CPU usage high: {cpu_percent:.1f}%")

            if memory.percent > 90:
                status = HealthStatus.CRITICAL
                issues.append(f"Memory usage critical: {memory.percent:.1f}%")
            elif memory.percent > 80:
                status = HealthStatus.WARNING
                issues.append(f"Memory usage high: {memory.percent:.1f}%")

            if disk.percent > 95:
                status = HealthStatus.CRITICAL
                issues.append(f"Disk usage critical: {disk.percent:.1f}%")
            elif disk.percent > 85:
                status = HealthStatus.WARNING
                issues.append(f"Disk usage high: {disk.percent:.1f}%")

            # Build message
            if issues:
                message = "; ".join(issues)
            else:
                message = f"System resources healthy (CPU: {cpu_percent:.1f}%, Memory: {memory.percent:.1f}%, Disk: {disk.percent:.1f}%)"

            details = {
                "cpu_percent": cpu_percent,
                "memory_percent": memory.percent,
                "memory_available_gb": memory.available / (1024**3),
                "disk_percent": disk.percent,
                "disk_free_gb": disk.free / (1024**3),
            }

            duration_ms = (time.time() - start_time) * 1000

            return HealthCheck(
                name="system_resources",
                status=status,
                message=message,
                details=details,
                duration_ms=duration_ms,
            )

        except Exception as e:
            duration_ms = (time.time() - start_time) * 1000
            return HealthCheck(
                name="system_resources",
                status=HealthStatus.CRITICAL,
                message=f"Failed to check system resources: {e}",
                details={"error": str(e)},
                duration_ms=duration_ms,
            )

    def check_database_health(self) -> HealthCheck:
        """Check database connectivity and health."""
        start_time = time.time()

        try:
            from ..database.connection import DatabaseManager

            db_manager = DatabaseManager()

            # Test database connection
            with db_manager.get_connection() as conn:
                # Simple query to test connectivity
                result = conn.execute("SELECT 1 as test").fetchone()

                if result and result[0] == 1:
                    # Check database file size and location
                    db_path = getattr(db_manager, "database_path", "./data/metrics_collector.db")
                    if os.path.exists(db_path):
                        db_size_mb = os.path.getsize(db_path) / (1024 * 1024)

                        # Check for basic tables
                        tables_result = conn.execute(
                            "SELECT name FROM sqlite_master WHERE type='table'"
                        ).fetchall()
                        table_count = len(tables_result)

                        details = {
                            "database_path": str(db_path),
                            "database_size_mb": db_size_mb,
                            "table_count": table_count,
                            "tables": [row[0] for row in tables_result],
                        }

                        message = f"Database healthy ({table_count} tables, {db_size_mb:.1f} MB)"
                        status = HealthStatus.HEALTHY
                    else:
                        message = "Database file not found"
                        status = HealthStatus.CRITICAL
                        details = {"database_path": str(db_path)}
                else:
                    message = "Database connection test failed"
                    status = HealthStatus.CRITICAL
                    details = {}

            duration_ms = (time.time() - start_time) * 1000

            return HealthCheck(
                name="database",
                status=status,
                message=message,
                details=details,
                duration_ms=duration_ms,
            )

        except Exception as e:
            duration_ms = (time.time() - start_time) * 1000
            return HealthCheck(
                name="database",
                status=HealthStatus.CRITICAL,
                message=f"Database health check failed: {e}",
                details={"error": str(e)},
                duration_ms=duration_ms,
            )

    def check_aws_connectivity(self) -> HealthCheck:
        """Check AWS service connectivity."""
        start_time = time.time()

        try:
            from ..aws.client import aws_client_manager

            # Test AWS credentials
            session = aws_client_manager.get_session()
            sts = session.client("sts")
            identity = sts.get_caller_identity()

            # Test basic service access
            regions_tested = []
            service_status = {}

            # Test a few key regions
            test_regions = ["us-east-1", "us-west-2", "eu-west-1"]

            for region in test_regions[:2]:  # Test first 2 regions to keep it fast
                try:
                    # Test DynamoDB
                    dynamodb = session.client("dynamodb", region_name=region)
                    dynamodb.list_tables(Limit=1)
                    service_status[f"dynamodb_{region}"] = True

                    # Test CloudWatch
                    cloudwatch = session.client("cloudwatch", region_name=region)
                    cloudwatch.list_metrics(MaxRecords=1)
                    service_status[f"cloudwatch_{region}"] = True

                    regions_tested.append(region)

                except Exception as region_error:
                    service_status[f"error_{region}"] = str(region_error)

            # Determine overall status
            successful_tests = sum(1 for v in service_status.values() if v is True)
            total_tests = len(test_regions) * 2  # 2 services per region

            if successful_tests == total_tests:
                status = HealthStatus.HEALTHY
                message = (
                    f"AWS connectivity healthy (tested {len(regions_tested)} regions)"
                )
            elif successful_tests > 0:
                status = HealthStatus.WARNING
                message = f"Partial AWS connectivity ({successful_tests}/{total_tests} services)"
            else:
                status = HealthStatus.CRITICAL
                message = "AWS connectivity failed"

            details = {
                "account_id": identity.get("Account"),
                "user_arn": identity.get("Arn"),
                "regions_tested": regions_tested,
                "service_status": service_status,
            }

            duration_ms = (time.time() - start_time) * 1000

            return HealthCheck(
                name="aws_connectivity",
                status=status,
                message=message,
                details=details,
                duration_ms=duration_ms,
            )

        except Exception as e:
            duration_ms = (time.time() - start_time) * 1000
            return HealthCheck(
                name="aws_connectivity",
                status=HealthStatus.CRITICAL,
                message=f"AWS connectivity check failed: {e}",
                details={"error": str(e)},
                duration_ms=duration_ms,
            )

    def check_operation_health(self) -> HealthCheck:
        """Check health of running operations."""
        start_time = time.time()

        try:
            from ..core.state import StateManager

            state_manager = StateManager()
            checkpoints = state_manager.list_checkpoints_with_details()

            # Categorize operations
            running = [c for c in checkpoints if c.get("status") == "RUNNING"]
            paused = [c for c in checkpoints if c.get("status") == "PAUSED"]
            failed = [c for c in checkpoints if c.get("status") == "FAILED"]
            completed = [c for c in checkpoints if c.get("status") == "COMPLETED"]

            # Find oldest running operation
            oldest_running = None
            if running:
                oldest_running = min(
                    (c.get("start_time") for c in running if c.get("start_time")),
                    default=None,
                )

            # Determine status
            status = HealthStatus.HEALTHY
            issues = []

            # Check for stuck operations (running > 24 hours)
            if oldest_running:
                if isinstance(oldest_running, str):
                    try:
                        oldest_running = datetime.fromisoformat(
                            oldest_running.replace("Z", "+00:00")
                        )
                    except ValueError:
                        oldest_running = datetime.fromisoformat(oldest_running)

                age = datetime.now() - oldest_running.replace(tzinfo=None)
                if age > timedelta(hours=24):
                    status = HealthStatus.WARNING
                    issues.append(
                        f"Long-running operation detected ({age.days}d {age.seconds//3600}h)"
                    )

            # Check failure rate
            total_ops = len(checkpoints)
            if total_ops > 0:
                failure_rate = len(failed) / total_ops * 100
                if failure_rate > 50:
                    status = HealthStatus.CRITICAL
                    issues.append(f"High failure rate: {failure_rate:.1f}%")
                elif failure_rate > 20:
                    status = HealthStatus.WARNING
                    issues.append(f"Elevated failure rate: {failure_rate:.1f}%")

            # Build message
            if issues:
                message = "; ".join(issues)
            else:
                message = f"Operations healthy ({len(running)} running, {len(completed)} completed)"

            details = {
                "running_operations": len(running),
                "paused_operations": len(paused),
                "failed_operations": len(failed),
                "completed_operations": len(completed),
                "total_operations": total_ops,
                "oldest_running_operation": (
                    oldest_running.isoformat() if oldest_running else None
                ),
                "failure_rate": len(failed) / total_ops * 100 if total_ops > 0 else 0,
            }

            duration_ms = (time.time() - start_time) * 1000

            return HealthCheck(
                name="operations",
                status=status,
                message=message,
                details=details,
                duration_ms=duration_ms,
            )

        except Exception as e:
            duration_ms = (time.time() - start_time) * 1000
            return HealthCheck(
                name="operations",
                status=HealthStatus.CRITICAL,
                message=f"Operation health check failed: {e}",
                details={"error": str(e)},
                duration_ms=duration_ms,
            )

    def run_all_checks(self) -> Dict[str, HealthCheck]:
        """Run all health checks and return results."""
        checks = {}

        # Run each check
        check_functions = [
            self.check_system_resources,
            self.check_database_health,
            self.check_aws_connectivity,
            self.check_operation_health,
        ]

        for check_func in check_functions:
            try:
                check_result = check_func()
                checks[check_result.name] = check_result
            except Exception as e:
                # Create a failed check result
                checks[check_func.__name__] = HealthCheck(
                    name=check_func.__name__,
                    status=HealthStatus.CRITICAL,
                    message=f"Health check failed: {e}",
                    details={"error": str(e)},
                )

        return checks

    def get_system_metrics(self) -> SystemMetrics:
        """Get current system performance metrics."""
        try:
            # Try to import psutil for detailed metrics
            try:
                import psutil

                cpu_percent = psutil.cpu_percent(
                    interval=0.1
                )  # Shorter interval for responsiveness
                memory = psutil.virtual_memory()
                disk = psutil.disk_usage(".")
                load_avg = (
                    os.getloadavg() if hasattr(os, "getloadavg") else (0.0, 0.0, 0.0)
                )
                uptime = (datetime.now() - self.start_time).total_seconds()

                return SystemMetrics(
                    cpu_percent=cpu_percent,
                    memory_percent=memory.percent,
                    memory_available_gb=memory.available / (1024**3),
                    disk_usage_percent=disk.percent,
                    disk_free_gb=disk.free / (1024**3),
                    load_average=load_avg,
                    uptime_seconds=uptime,
                )
            except ImportError:
                # Fallback metrics without psutil
                uptime = (datetime.now() - self.start_time).total_seconds()
                return SystemMetrics(
                    cpu_percent=0.0,
                    memory_percent=0.0,
                    memory_available_gb=0.0,
                    disk_usage_percent=0.0,
                    disk_free_gb=0.0,
                    load_average=(0.0, 0.0, 0.0),
                    uptime_seconds=uptime,
                )
        except Exception as e:
            self.logger.error(f"Failed to get system metrics: {e}")
            # Return default metrics on error
            return SystemMetrics(
                cpu_percent=0.0,
                memory_percent=0.0,
                memory_available_gb=0.0,
                disk_usage_percent=0.0,
                disk_free_gb=0.0,
                load_average=(0.0, 0.0, 0.0),
                uptime_seconds=0.0,
            )

    def format_health_report(self, checks: Dict[str, HealthCheck]) -> str:
        """Format health check results for CLI display."""
        lines = []

        # Overall status
        overall_status = HealthStatus.HEALTHY
        critical_count = sum(
            1 for c in checks.values() if c.status == HealthStatus.CRITICAL
        )
        warning_count = sum(
            1 for c in checks.values() if c.status == HealthStatus.WARNING
        )

        if critical_count > 0:
            overall_status = HealthStatus.CRITICAL
        elif warning_count > 0:
            overall_status = HealthStatus.WARNING

        # Status icons
        status_icons = {
            HealthStatus.HEALTHY: "✅",
            HealthStatus.WARNING: "⚠️",
            HealthStatus.CRITICAL: "❌",
            HealthStatus.UNKNOWN: "❓",
        }

        # Header
        icon = status_icons[overall_status]
        lines.append(f"{icon} System Health Status: {overall_status.value.upper()}")
        lines.append("")

        # Individual checks
        for check in checks.values():
            check_icon = status_icons[check.status]
            lines.append(
                f"{check_icon} {check.name.replace('_', ' ').title()}: {check.message}"
            )

            # Show key details for failed checks
            if (
                check.status in [HealthStatus.CRITICAL, HealthStatus.WARNING]
                and check.details
            ):
                for key, value in check.details.items():
                    if key != "error" and isinstance(value, (int, float, str)):
                        lines.append(f"   {key}: {value}")

        # System metrics summary
        try:
            metrics = self.get_system_metrics()
            lines.append("")
            lines.append("📊 System Metrics:")
            lines.append(f"   CPU: {metrics.cpu_percent:.1f}%")
            lines.append(
                f"   Memory: {metrics.memory_percent:.1f}% ({metrics.memory_available_gb:.1f} GB available)"
            )
            lines.append(
                f"   Disk: {metrics.disk_usage_percent:.1f}% ({metrics.disk_free_gb:.1f} GB free)"
            )
            lines.append(f"   Uptime: {self._format_uptime(metrics.uptime_seconds)}")
        except Exception:
            pass  # Skip metrics on error

        return "\n".join(lines)

    def _format_uptime(self, seconds: float) -> str:
        """Format uptime in human-readable format."""
        if seconds < 60:
            return f"{int(seconds)}s"
        elif seconds < 3600:
            minutes = int(seconds // 60)
            secs = int(seconds % 60)
            return f"{minutes}m {secs}s"
        elif seconds < 86400:
            hours = int(seconds // 3600)
            minutes = int((seconds % 3600) // 60)
            return f"{hours}h {minutes}m"
        else:
            days = int(seconds // 86400)
            hours = int((seconds % 86400) // 3600)
            return f"{days}d {hours}h"


def create_health_monitor(logger: Optional[logging.Logger] = None) -> HealthMonitor:
    """Create a configured health monitor."""
    return HealthMonitor(logger)


def run_health_checks(
    logger: Optional[logging.Logger] = None,
) -> Dict[str, HealthCheck]:
    """Run all health checks and return results."""
    monitor = create_health_monitor(logger)
    return monitor.run_all_checks()


def display_health_status(logger: Optional[logging.Logger] = None) -> None:
    """Display comprehensive health status to CLI."""
    monitor = create_health_monitor(logger)
    checks = monitor.run_all_checks()
    report = monitor.format_health_report(checks)
    click.echo(report)
