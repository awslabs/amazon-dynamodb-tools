"""
Health check and monitoring utilities for DynamoDB Optima operations.

Provides system health monitoring, operation status checks, and performance
monitoring capabilities.
"""

import asyncio
import logging
import os
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple

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
        """Check AWS service connectivity - unified multi-account approach."""
        start_time = time.time()

        try:
            from ..aws.client import aws_client_manager
            from ..aws.organizations import OrganizationsManager
            from ..config import get_settings
            from ..database.connection import get_database_manager

            # Test AWS credentials first (STS)
            session = aws_client_manager.get_session()
            try:
                sts = session.client("sts")
                identity = sts.get_caller_identity()
            except Exception as sts_error:
                error_msg = self._parse_aws_error(sts_error)
                duration_ms = (time.time() - start_time) * 1000
                return HealthCheck(
                    name="aws_connectivity",
                    status=HealthStatus.CRITICAL,
                    message=f"AWS connectivity check failed: {error_msg}",
                    details={"error": str(sts_error), "error_type": type(sts_error).__name__},
                    duration_ms=duration_ms,
                )

            # Get accounts to test - either from database or create synthetic single account
            accounts = self._get_account_info()
            
            if not accounts:
                # No aws_accounts table or empty - create synthetic account from current identity
                self.logger.info("No accounts in database - using current account from STS")
                
                # Get regions from table_metadata (any account)
                db_manager = get_database_manager()
                with db_manager.get_connection_context() as conn:
                    result = conn.execute("""
                        SELECT DISTINCT region 
                        FROM table_metadata 
                        WHERE region IS NOT NULL
                        ORDER BY region
                    """).fetchall()
                    regions = [row[0] for row in result] if result else ["us-east-1"]
                
                accounts = [{
                    "account_id": identity.get("Account"),
                    "account_name": "Current Account",
                    "is_management_account": True
                }]
                
                # Manually set regions for this synthetic account
                self.logger.info(f"Testing current account {identity.get('Account')} in {len(regions)} regions")
            else:
                self.logger.info(f"Testing {len(accounts)} accounts from database")
            
            # Unified testing logic for all accounts
            org_manager = OrganizationsManager()
            settings = get_settings()
            account_results = []
            
            for account in accounts:
                account_id = account["account_id"]
                account_name = account["account_name"]
                is_management = account["is_management_account"]
                
                # Get regions for this account
                regions = self._get_account_regions(account_id)
                
                if not regions:
                    self.logger.warning(f"No regions found for account {account_id}, skipping")
                    continue
                
                # Get credentials (None for management account)
                credentials = None
                role_status = None
                
                if not is_management:
                    try:
                        credentials = asyncio.run(
                            org_manager.get_account_credentials(
                                account_id=account_id,
                                role_name=settings.organizations_role_name
                            )
                        )
                        role_status = "success"
                        self.logger.info(
                            f"Successfully assumed role in account",
                            account_id=account_id,
                            account_name=account_name,
                            role=settings.organizations_role_name
                        )
                    except Exception as e:
                        role_status = "failed"
                        error_msg = self._parse_aws_error(e)
                        self.logger.error(
                            f"Failed to assume role in account",
                            account_id=account_id,
                            account_name=account_name,
                            role=settings.organizations_role_name,
                            error=error_msg
                        )
                        # Add failed result for this account
                        account_results.append({
                            "account_id": account_id,
                            "account_name": account_name,
                            "is_management": is_management,
                            "role_status": role_status,
                            "role_error": error_msg,
                            "regions": regions,
                            "results": [],
                            "successful": 0,
                            "total": 0,
                            "status": "failed"
                        })
                        continue
                
                # Test services for this account
                result = asyncio.run(
                    self._test_account_services(
                        account_id=account_id,
                        account_name=account_name,
                        is_management=is_management,
                        regions=regions,
                        credentials=credentials
                    )
                )
                result["role_status"] = role_status
                account_results.append(result)
            
            # Test optional services (once, using management account credentials)
            optional_results = []
            
            # Test S3 (global - for CUR data)
            s3_result = self._test_aws_service(
                session, "s3", None,
                lambda client: client.list_buckets(),
                "S3 bucket access (for CUR data)"
            )
            optional_results.append(s3_result)
            
            # Test Organizations (global - for multi-account)
            org_result = self._test_aws_service(
                session, "organizations", None,
                lambda client: client.describe_organization(),
                "Organizations API (multi-account)"
            )
            optional_results.append(org_result)
            
            # Test Pricing API (us-east-1 only - for cost calculations)
            pricing_result = self._test_aws_service(
                session, "pricing", "us-east-1",
                lambda client: client.get_products(ServiceCode='AmazonDynamoDB', MaxResults=1),
                "Pricing API (cost calculations)"
            )
            optional_results.append(pricing_result)
            
            # Aggregate results (core + optional)
            core_successful = sum(r["successful"] for r in account_results)
            core_total = sum(r["total"] for r in account_results)
            optional_successful = sum(1 for r in optional_results if r["success"])
            optional_total = len(optional_results)
            
            total_successful = core_successful + optional_successful
            total_tests = core_total + optional_total
            failed_accounts = [r for r in account_results if r["status"] == "failed"]
            
            # Determine overall status
            account_count = len(accounts)
            if total_successful == total_tests:
                status = HealthStatus.HEALTHY
                message = f"AWS connectivity healthy (all {total_tests} services across {account_count} account{'s' if account_count > 1 else ''})"
            elif total_successful > 0:
                status = HealthStatus.WARNING
                message = f"Partial AWS connectivity ({total_successful}/{total_tests} services across {account_count} account{'s' if account_count > 1 else ''})"
            else:
                status = HealthStatus.CRITICAL
                message = f"AWS connectivity failed (0/{total_tests} services across {account_count} account{'s' if account_count > 1 else ''})"
            
            # Build detailed output
            details = {
                "mode": "multi-account" if account_count > 1 else "single-account",
                "management_account_id": identity.get("Account"),
                "management_user_arn": identity.get("Arn"),
                "total_accounts": account_count,
                "accounts_tested": len(account_results),
                "failed_accounts": len(failed_accounts),
                "account_results": account_results,
                "optional_services": {
                    "successful": optional_successful,
                    "total": optional_total,
                    "results": optional_results
                },
                "core_successful": core_successful,
                "core_total": core_total,
                "total_successful": total_successful,
                "total_tests": total_tests
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
                details={"error": str(e), "error_type": type(e).__name__},
                duration_ms=duration_ms,
            )

    def _test_aws_service(
        self, session, service_name: str, region: Optional[str], 
        test_func, description: str
    ) -> Dict[str, Any]:
        """Test a single AWS service."""
        start_time = time.time()
        
        try:
            if region:
                client = session.client(service_name, region_name=region)
                location = f"{service_name} ({region})"
            else:
                client = session.client(service_name)
                location = f"{service_name} (global)"
            
            # Execute the test
            test_func(client)
            
            duration_ms = (time.time() - start_time) * 1000
            return {
                "service": service_name,
                "region": region,
                "location": location,
                "description": description,
                "success": True,
                "duration_ms": duration_ms,
                "error": None
            }
            
        except Exception as e:
            duration_ms = (time.time() - start_time) * 1000
            error_msg = self._parse_aws_error(e)
            
            return {
                "service": service_name,
                "region": region,
                "location": location if region else f"{service_name} (global)",
                "description": description,
                "success": False,
                "duration_ms": duration_ms,
                "error": error_msg,
                "error_type": type(e).__name__,
                "raw_error": str(e)
            }

    def _is_multi_account_mode(self) -> bool:
        """Check if database has multiple accounts."""
        try:
            from ..database.connection import get_database_manager
            
            db_manager = get_database_manager()
            with db_manager.get_connection_context() as conn:
                result = conn.execute("""
                    SELECT COUNT(DISTINCT account_id) as account_count 
                    FROM aws_accounts
                """).fetchone()
                
                if result and result[0] > 1:
                    self.logger.debug(f"Multi-account mode detected: {result[0]} accounts")
                    return True
                    
                return False
                
        except Exception as e:
            # If table doesn't exist or query fails, assume single account mode
            self.logger.debug(f"Multi-account check failed (assuming single account): {e}")
            return False
    
    def _get_account_info(self) -> List[Dict[str, Any]]:
        """Get all accounts from aws_accounts table with metadata."""
        try:
            from ..database.connection import get_database_manager
            
            db_manager = get_database_manager()
            with db_manager.get_connection_context() as conn:
                result = conn.execute("""
                    SELECT 
                        account_id,
                        account_name,
                        is_management_account
                    FROM aws_accounts
                    ORDER BY is_management_account DESC, account_name
                """).fetchall()
                
                accounts = []
                for row in result:
                    accounts.append({
                        "account_id": row[0],
                        "account_name": row[1],
                        "is_management_account": bool(row[2])
                    })
                
                self.logger.debug(f"Retrieved {len(accounts)} accounts from database")
                return accounts
                
        except Exception as e:
            self.logger.error(f"Failed to get account info: {e}")
            return []
    
    def _get_account_regions(self, account_id: str) -> List[str]:
        """Get regions for a specific account from table_metadata."""
        try:
            from ..database.connection import get_database_manager
            
            db_manager = get_database_manager()
            with db_manager.get_connection_context() as conn:
                result = conn.execute("""
                    SELECT DISTINCT region 
                    FROM table_metadata 
                    WHERE account_id = ? AND region IS NOT NULL
                    ORDER BY region
                """, (account_id,)).fetchall()
                
                regions = [row[0] for row in result]
                self.logger.debug(f"Account {account_id}: {len(regions)} regions - {regions}")
                return regions
                
        except Exception as e:
            self.logger.error(f"Failed to get regions for account {account_id}: {e}")
            return []
    
    async def _test_account_services(
        self,
        account_id: str,
        account_name: str,
        is_management: bool,
        regions: List[str],
        credentials: Optional[Dict[str, str]]
    ) -> Dict[str, Any]:
        """Test core services for a single account across its regions."""
        import aioboto3
        
        # Create session with appropriate credentials
        if credentials:
            session = aioboto3.Session(
                aws_access_key_id=credentials["aws_access_key_id"],
                aws_secret_access_key=credentials["aws_secret_access_key"],
                aws_session_token=credentials["aws_session_token"]
            )
        else:
            session = aioboto3.Session()
        
        core_results = []
        
        # Test DynamoDB in each region
        for region in regions:
            async with session.client("dynamodb", region_name=region) as client:
                try:
                    await client.list_tables(Limit=1)
                    core_results.append({
                        "service": "dynamodb",
                        "region": region,
                        "success": True,
                        "error": None
                    })
                    self.logger.debug(
                        f"✓ Health check passed",
                        account_id=account_id,
                        account_name=account_name,
                        service="dynamodb",
                        region=region,
                        api_call="list_tables"
                    )
                except Exception as e:
                    error_msg = self._parse_aws_error(e)
                    core_results.append({
                        "service": "dynamodb",
                        "region": region,
                        "success": False,
                        "error": error_msg
                    })
                    # Log failed API call with full details for programmatic review
                    self.logger.error(
                        f"✗ Health check failed",
                        account_id=account_id,
                        account_name=account_name,
                        service="dynamodb",
                        region=region,
                        api_call="list_tables",
                        error=error_msg,
                        error_type=type(e).__name__,
                        raw_error=str(e)
                    )
        
        # Test CloudWatch in each region
        for region in regions:
            async with session.client("cloudwatch", region_name=region) as client:
                try:
                    await client.list_metrics(Namespace='AWS/DynamoDB')
                    core_results.append({
                        "service": "cloudwatch",
                        "region": region,
                        "success": True,
                        "error": None
                    })
                    self.logger.debug(
                        f"✓ Health check passed",
                        account_id=account_id,
                        account_name=account_name,
                        service="cloudwatch",
                        region=region,
                        api_call="list_metrics"
                    )
                except Exception as e:
                    error_msg = self._parse_aws_error(e)
                    core_results.append({
                        "service": "cloudwatch",
                        "region": region,
                        "success": False,
                        "error": error_msg
                    })
                    # Log failed API call with full details for programmatic review
                    self.logger.error(
                        f"✗ Health check failed",
                        account_id=account_id,
                        account_name=account_name,
                        service="cloudwatch",
                        region=region,
                        api_call="list_metrics",
                        error=error_msg,
                        error_type=type(e).__name__,
                        raw_error=str(e)
                    )
        
        # Calculate success metrics
        successful = sum(1 for r in core_results if r["success"])
        total = len(core_results)
        
        return {
            "account_id": account_id,
            "account_name": account_name,
            "is_management": is_management,
            "regions": regions,
            "results": core_results,
            "successful": successful,
            "total": total,
            "status": "success" if successful == total else "partial" if successful > 0 else "failed"
        }
    
    def _parse_aws_error(self, error: Exception) -> str:
        """Parse AWS error to provide helpful message."""
        error_str = str(error)
        error_type = type(error).__name__
        
        # Common AWS error patterns
        if "AccessDenied" in error_str or "AccessDeniedException" in error_type:
            # Try to extract the missing permission
            if "not authorized to perform:" in error_str:
                parts = error_str.split("not authorized to perform:")
                if len(parts) > 1:
                    permission = parts[1].split()[0].strip()
                    return f"Access denied - Missing IAM permission: {permission}"
            return "Access denied - Check IAM permissions"
        
        elif "InvalidClientTokenId" in error_str:
            return "Invalid AWS credentials - Check AWS_ACCESS_KEY_ID"
        
        elif "SignatureDoesNotMatch" in error_str:
            return "AWS credential signature error - Check AWS_SECRET_ACCESS_KEY"
        
        elif "ExpiredToken" in error_str:
            return "AWS credentials expired - Refresh your session"
        
        elif "not subscribed" in error_str.lower():
            return "Service not enabled for this account"
        
        elif "AWSOrganizationsNotInUseException" in error_type:
            return "Account not in an AWS Organization"
        
        elif "AccountNotFoundException" in error_type:
            return "AWS account not found"
        
        elif "Parameter validation failed" in error_str or "ParamValidationError" in error_type:
            # Extract the actual parameter error
            if ":" in error_str:
                # Get everything after the first colon for parameter errors
                parts = error_str.split(":", 1)
                if len(parts) > 1:
                    return f"Invalid parameters: {parts[1].strip()[:80]}"
            return "Invalid API parameters"
        
        else:
            # Return first line of error for brevity
            first_line = error_str.split('\n')[0]
            return first_line[:120] if len(first_line) > 120 else first_line


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
            # self.check_operation_health,  # TODO: Disabled - checkpoints not working, table is empty
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

            # Special formatting for AWS connectivity check
            if check.name == "aws_connectivity" and check.details:
                # Show management account info
                if "management_account_id" in check.details:
                    lines.append(f"   management_account_id: {check.details['management_account_id']}")
                if "management_user_arn" in check.details:
                    lines.append(f"   management_user_arn: {check.details['management_user_arn']}")
                
                # Multi-account format - show per-account results
                if "account_results" in check.details:
                    lines.append("")
                    
                    for account_result in check.details["account_results"]:
                        account_id = account_result["account_id"]
                        account_name = account_result["account_name"]
                        is_management = account_result.get("is_management", False)
                        
                        # Account header
                        if is_management:
                            lines.append(f"   Account: {account_name} ({account_id}) [Management]")
                        else:
                            lines.append(f"   Account: {account_name} ({account_id})")
                        
                        # Show role assumption status for member accounts
                        role_status = account_result.get("role_status")
                        if role_status:
                            if role_status == "success":
                                lines.append(f"      Role: OrganizationAccountAccessRole ✅")
                            elif role_status == "failed":
                                role_error = account_result.get("role_error", "Unknown error")
                                lines.append(f"      Role: OrganizationAccountAccessRole ❌")
                                lines.append(f"         Error: {role_error}")
                                lines.append("")
                                continue  # Skip service testing if role assumption failed
                        
                        # Group results by service name
                        services_by_name = {}
                        for result in account_result.get("results", []):
                            service = result["service"]
                            if service not in services_by_name:
                                services_by_name[service] = []
                            services_by_name[service].append(result)
                        
                        # Format each service with its regions
                        for service_name, results in services_by_name.items():
                            region_statuses = []
                            errors = []
                            for result in results:
                                icon = "✅" if result["success"] else "❌"
                                region = result["region"]
                                region_statuses.append(f"{icon} {region}")
                                if not result["success"] and result.get("error"):
                                    errors.append(f"         {region}: {result['error']}")
                            
                            lines.append(f"      {service_name}: {', '.join(region_statuses)}")
                            # Show errors below the service line if any
                            for error in errors:
                                lines.append(error)
                        
                        lines.append("")  # Blank line between accounts
                
                # Show optional services
                if "optional_services" in check.details:
                    optional = check.details["optional_services"]
                    lines.append(f"   Optional Services ({optional['successful']}/{optional['total']}):")
                    for result in optional.get("results", []):
                        result_icon = "✅" if result["success"] else "❌"
                        lines.append(f"      {result_icon} {result['location']}: {result['description']}")
                        if not result["success"] and result.get("error"):
                            lines.append(f"         {result['error']}")
            
            # Show key details for other failed checks
            elif (
                check.status in [HealthStatus.CRITICAL, HealthStatus.WARNING]
                and check.details
            ):
                for key, value in check.details.items():
                    if key not in ["error", "core_services", "optional_services", "regions_tested"] and isinstance(value, (int, float, str)):
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
