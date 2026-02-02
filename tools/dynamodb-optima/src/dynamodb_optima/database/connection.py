"""
DuckDB database connection management.

Provides connection pooling, schema initialization, and database utilities
for the DMetrics analytical database with comprehensive error handling,
performance monitoring, and transaction management.
"""

import os
import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import duckdb

from ..config import get_settings
from ..logging import get_logger

logger = get_logger("dynamodb_optima.database")

# Database schema version for migrations
SCHEMA_VERSION = "1.0.0"


@dataclass
class DatabasePerformanceMetrics:
    """Performance metrics for database operations."""

    # Connection pool metrics
    total_connections_created: int = 0
    active_connections: int = 0
    pool_hits: int = 0
    pool_misses: int = 0

    # Query performance metrics
    total_queries: int = 0
    total_query_time: float = 0.0
    slow_queries: int = 0  # Queries > 1 second
    failed_queries: int = 0

    # Batch operation metrics (using staging tables, not transactions)
    batch_operations: int = 0
    batch_records_processed: int = 0
    batch_failures: int = 0
    staging_table_operations: int = 0

    # Timing metrics
    last_reset: datetime = field(default_factory=datetime.now)

    def reset(self) -> None:
        """Reset all metrics."""
        self.__init__()

    def get_average_query_time(self) -> float:
        """Get average query execution time."""
        return (
            self.total_query_time / self.total_queries
            if self.total_queries > 0
            else 0.0
        )

    def get_pool_hit_ratio(self) -> float:
        """Get connection pool hit ratio."""
        total_requests = self.pool_hits + self.pool_misses
        return self.pool_hits / total_requests if total_requests > 0 else 0.0

    def get_query_failure_rate(self) -> float:
        """Get query failure rate."""
        return (
            self.failed_queries / self.total_queries if self.total_queries > 0 else 0.0
        )


@dataclass
class DatabaseHealthStatus:
    """Database health status information."""

    is_healthy: bool
    schema_version: Optional[str]
    connection_pool_status: str
    performance_metrics: DatabasePerformanceMetrics
    last_check: datetime
    issues: List[str] = field(default_factory=list)
    recommendations: List[str] = field(default_factory=list)


class DatabaseError(Exception):
    """Base exception for database operations."""

    pass


class SchemaError(DatabaseError):
    """Exception for schema-related errors."""

    pass


class ConnectionPoolError(DatabaseError):
    """Exception for connection pool errors."""

    pass


class DatabaseManager:
    """
    Manages DuckDB database connections, schema, and connection pooling with
    comprehensive error handling, performance monitoring, and optimization.
    """

    def __init__(
        self, database_url: Optional[str] = None, max_connections: Optional[int] = None
    ):
        """Initialize database manager with enhanced connection pooling and monitoring."""
        self.settings = get_settings()
        # Use resolved URL that respects --project-root CLI option
        self.database_url = database_url or self.settings.database_url
        self.max_connections = max_connections or self.settings.database_pool_size

        # Connection pool management
        self._connections: List[duckdb.DuckDBPyConnection] = []
        self._available_connections: List[duckdb.DuckDBPyConnection] = []
        self._connection_usage: Dict[int, datetime] = {}  # Track connection usage
        self._lock = threading.RLock()
        self._schema_initialized = False
        self._schema_validated = False

        # Performance monitoring
        self._performance_metrics = DatabasePerformanceMetrics()
        self._health_check_interval = timedelta(minutes=5)
        self._last_health_check = datetime.now()

        # Extract database path from URL
        self.db_path = self._parse_database_url(self.database_url)

        # Validate database path and permissions
        self._validate_database_path()

        logger.info(
            f"DatabaseManager initialized with enhanced monitoring",
            database_path=self.db_path,
            max_connections=self.max_connections,
            pool_size=self.settings.database_pool_size,
        )

    def _parse_database_url(self, url: str) -> str:
        """Parse database URL to extract file path with validation."""
        try:
            if url.startswith("duckdb://"):
                return url.replace("duckdb://", "")
            elif url.startswith("file://"):
                return url.replace("file://", "")
            else:
                return url
        except Exception as e:
            logger.error(f"Failed to parse database URL: {url}", error=str(e))
            raise DatabaseError(f"Invalid database URL format: {url}")

    def _validate_database_path(self) -> None:
        """Validate database path and permissions."""
        try:
            if self.db_path == ":memory:":
                logger.info("Using in-memory database")
                return

            db_path = Path(self.db_path)

            # Check if parent directory exists or can be created
            if not db_path.parent.exists():
                try:
                    db_path.parent.mkdir(parents=True, exist_ok=True)
                    logger.info(f"Created database directory: {db_path.parent}")
                except PermissionError as e:
                    raise DatabaseError(
                        f"Cannot create database directory {db_path.parent}: {e}"
                    )

            # Check write permissions
            if db_path.exists():
                if not os.access(db_path, os.W_OK):
                    raise DatabaseError(
                        f"No write permission for database file: {db_path}"
                    )
            else:
                # Check if we can create the file
                try:
                    db_path.touch()
                    db_path.unlink()  # Remove test file
                except PermissionError as e:
                    raise DatabaseError(f"Cannot create database file {db_path}: {e}")

            logger.debug(f"Database path validation successful: {self.db_path}")

        except Exception as e:
            if isinstance(e, DatabaseError):
                raise
            logger.error(f"Database path validation failed: {e}")
            raise DatabaseError(f"Database path validation failed: {e}")

    def _create_connection(self) -> duckdb.DuckDBPyConnection:
        """Create a new database connection with enhanced configuration and error handling."""
        start_time = time.time()

        try:
            # Ensure database directory exists (already validated in __init__)
            db_path = Path(self.db_path)
            if db_path != Path(":memory:"):
                db_path.parent.mkdir(parents=True, exist_ok=True)

            connection = duckdb.connect(str(db_path))

            # Configure connection for optimal analytical performance
            self._configure_connection_performance(connection)

            # Update performance metrics
            with self._lock:
                self._performance_metrics.total_connections_created += 1
                self._performance_metrics.active_connections += 1

            connection_time = time.time() - start_time
            logger.debug(
                f"Created new DuckDB connection",
                database_path=self.db_path,
                connection_time_ms=round(connection_time * 1000, 2),
                total_connections=self._performance_metrics.total_connections_created,
            )

            return connection

        except Exception as e:
            connection_time = time.time() - start_time
            logger.error(
                f"Failed to create database connection",
                database_path=self.db_path,
                connection_time_ms=round(connection_time * 1000, 2),
                error=str(e),
            )
            raise ConnectionPoolError(
                f"Failed to create connection to {self.db_path}: {e}"
            )

    def _configure_connection_performance(
        self, connection: duckdb.DuckDBPyConnection
    ) -> None:
        """Configure connection for optimal performance based on workload patterns."""
        try:
            # Memory configuration based on available system memory
            memory_limit = self._calculate_optimal_memory_limit()
            connection.execute(f"SET memory_limit='{memory_limit}'")

            # Thread configuration based on CPU cores
            thread_count = self._calculate_optimal_thread_count()
            connection.execute(f"SET threads={thread_count}")

            # Performance optimizations for analytical workloads
            connection.execute("SET enable_progress_bar=false")
            connection.execute("SET enable_object_cache=true")
            connection.execute("SET enable_http_metadata_cache=true")
            connection.execute(
                "SET preserve_insertion_order=false"
            )  # Better for analytics

            # Optimize for batch operations
            connection.execute("SET checkpoint_threshold='1GB'")
            connection.execute("SET wal_autocheckpoint=10000")

            logger.debug(
                f"Connection configured for optimal performance",
                memory_limit=memory_limit,
                threads=thread_count,
            )

        except Exception as e:
            logger.warning(f"Failed to configure connection performance: {e}")
            # Continue with default settings rather than failing

    def _calculate_optimal_memory_limit(self) -> str:
        """Calculate optimal memory limit based on available system memory."""
        try:
            # Lazy import - only needed conditionally
            import psutil

            available_memory_gb = psutil.virtual_memory().available / (1024**3)

            # Use 25% of available memory, with min 1GB and max 8GB
            optimal_memory_gb = max(1, min(8, available_memory_gb * 0.25))
            return f"{int(optimal_memory_gb)}GB"  # Use integer to avoid parsing issues

        except ImportError:
            logger.debug("psutil not available, using default memory limit")
            return "2GB"
        except Exception as e:
            logger.warning(f"Failed to calculate optimal memory limit: {e}")
            return "2GB"

    def _calculate_optimal_thread_count(self) -> int:
        """Calculate optimal thread count based on CPU cores."""
        try:
            # Lazy import - only needed conditionally
            import os

            cpu_count = os.cpu_count() or 4

            # Use 50% of CPU cores, with min 2 and max 8
            optimal_threads = max(2, min(8, cpu_count // 2))
            return optimal_threads

        except Exception as e:
            logger.warning(f"Failed to calculate optimal thread count: {e}")
            return 4

    def get_connection(self) -> duckdb.DuckDBPyConnection:
        """Get database connection from pool with enhanced monitoring and optimization."""
        start_time = time.time()

        with self._lock:
            # Initialize and validate schema on first connection
            if not self._schema_initialized:
                self._initialize_schema_once()

            # Perform periodic health checks
            self._perform_health_check_if_needed()

            # Try to get available connection from pool
            if self._available_connections:
                connection = self._available_connections.pop()
                connection_id = id(connection)
                self._connection_usage[connection_id] = datetime.now()

                # Update performance metrics
                self._performance_metrics.pool_hits += 1

                acquisition_time = time.time() - start_time
                logger.debug(
                    "Reused connection from pool",
                    connection_id=connection_id,
                    acquisition_time_ms=round(acquisition_time * 1000, 2),
                    pool_size=len(self._available_connections),
                )
                return connection

            # Create new connection if under limit
            if len(self._connections) < self.max_connections:
                connection = self._create_connection()
                self._connections.append(connection)
                connection_id = id(connection)
                self._connection_usage[connection_id] = datetime.now()

                # Update performance metrics
                self._performance_metrics.pool_misses += 1

                acquisition_time = time.time() - start_time
                logger.debug(
                    "Created new connection for pool",
                    connection_id=connection_id,
                    acquisition_time_ms=round(acquisition_time * 1000, 2),
                    pool_utilization=f"{len(self._connections)}/{self.max_connections}",
                )
                return connection

            # Pool is full, create temporary connection with warning
            self._performance_metrics.pool_misses += 1
            acquisition_time = time.time() - start_time

            logger.warning(
                "Connection pool exhausted, creating temporary connection",
                pool_size=self.max_connections,
                acquisition_time_ms=round(acquisition_time * 1000, 2),
                recommendation="Consider increasing database_pool_size in configuration",
            )

            return self._create_connection()

    def return_connection(self, connection: duckdb.DuckDBPyConnection) -> None:
        """Return connection to pool with usage tracking and cleanup."""
        with self._lock:
            connection_id = id(connection)

            # Calculate connection usage time
            usage_start = self._connection_usage.get(connection_id)
            usage_time = (
                (datetime.now() - usage_start).total_seconds() if usage_start else 0
            )

            if (
                connection in self._connections
                and connection not in self._available_connections
            ):
                self._available_connections.append(connection)

                logger.debug(
                    "Returned connection to pool",
                    connection_id=connection_id,
                    usage_time_ms=round(usage_time * 1000, 2),
                    available_connections=len(self._available_connections),
                )
            else:
                # Close temporary connection
                try:
                    connection.close()
                    self._performance_metrics.active_connections -= 1

                    logger.debug(
                        "Closed temporary connection",
                        connection_id=connection_id,
                        usage_time_ms=round(usage_time * 1000, 2),
                    )
                except Exception as e:
                    logger.warning(
                        f"Error closing temporary connection",
                        connection_id=connection_id,
                        error=str(e),
                    )

            # Clean up usage tracking
            self._connection_usage.pop(connection_id, None)

    @contextmanager
    def get_connection_context(self):
        """Enhanced context manager for database connections with error handling."""
        connection = None
        start_time = time.time()

        try:
            connection = self.get_connection()
            yield connection

        except Exception as e:
            # Log connection context errors
            context_time = time.time() - start_time
            logger.error(
                f"Error in database connection context",
                context_time_ms=round(context_time * 1000, 2),
                error=str(e),
            )
            raise

        finally:
            if connection is not None:
                try:
                    self.return_connection(connection)
                except Exception as e:
                    logger.error(f"Error returning connection to pool: {e}")

    def _perform_health_check_if_needed(self) -> None:
        """Perform periodic health checks on the database and connection pool."""
        now = datetime.now()

        if now - self._last_health_check < self._health_check_interval:
            return

        try:
            # Check database connectivity
            with self.get_connection_context() as conn:
                conn.execute("SELECT 1").fetchone()

            # Check for stale connections (unused for > 30 minutes)
            stale_threshold = now - timedelta(minutes=30)
            stale_connections = [
                conn_id
                for conn_id, usage_time in self._connection_usage.items()
                if usage_time < stale_threshold
            ]

            if stale_connections:
                logger.info(
                    f"Found {len(stale_connections)} stale connections",
                    recommendation="Consider reducing database_pool_size if consistently unused",
                )

            self._last_health_check = now

        except Exception as e:
            logger.warning(f"Database health check failed: {e}")

    def get_health_status(self) -> DatabaseHealthStatus:
        """Get comprehensive database health status."""
        try:
            # Test database connectivity
            is_healthy = True
            issues = []
            recommendations = []

            try:
                with self.get_connection_context() as conn:
                    conn.execute("SELECT 1").fetchone()
            except Exception as e:
                is_healthy = False
                issues.append(f"Database connectivity failed: {e}")

            # Check schema version
            schema_version = self.get_schema_version()
            if not schema_version:
                issues.append("Schema version not found")
            elif schema_version != SCHEMA_VERSION:
                issues.append(
                    f"Schema version mismatch: {schema_version} != {SCHEMA_VERSION}"
                )

            # Analyze connection pool performance
            pool_hit_ratio = self._performance_metrics.get_pool_hit_ratio()
            if pool_hit_ratio < 0.8:  # Less than 80% hit ratio
                recommendations.append(
                    f"Low connection pool hit ratio ({pool_hit_ratio:.1%}). "
                    "Consider increasing database_pool_size."
                )

            # Analyze query performance
            avg_query_time = self._performance_metrics.get_average_query_time()
            if avg_query_time > 1.0:  # Average query time > 1 second
                recommendations.append(
                    f"High average query time ({avg_query_time:.2f}s). "
                    "Consider optimizing queries or increasing memory_limit."
                )

            # Check failure rates
            query_failure_rate = self._performance_metrics.get_query_failure_rate()
            if query_failure_rate > 0.05:  # More than 5% failure rate
                issues.append(f"High query failure rate: {query_failure_rate:.1%}")

            # Determine connection pool status
            pool_utilization = len(self._connections) / self.max_connections
            if pool_utilization > 0.9:
                pool_status = "HIGH_UTILIZATION"
                recommendations.append(
                    "Connection pool near capacity. Consider increasing pool size."
                )
            elif pool_utilization > 0.7:
                pool_status = "MODERATE_UTILIZATION"
            else:
                pool_status = "NORMAL"

            return DatabaseHealthStatus(
                is_healthy=is_healthy and len(issues) == 0,
                schema_version=schema_version,
                connection_pool_status=pool_status,
                performance_metrics=self._performance_metrics,
                last_check=datetime.now(),
                issues=issues,
                recommendations=recommendations,
            )

        except Exception as e:
            logger.error(f"Failed to get database health status: {e}")
            return DatabaseHealthStatus(
                is_healthy=False,
                schema_version=None,
                connection_pool_status="ERROR",
                performance_metrics=self._performance_metrics,
                last_check=datetime.now(),
                issues=[f"Health check failed: {e}"],
                recommendations=["Check database configuration and connectivity"],
            )

    def close_all_connections(self) -> None:
        """Close all connections in the pool."""
        with self._lock:
            all_connections = self._connections + self._available_connections
            for connection in all_connections:
                try:
                    connection.close()
                except Exception as e:
                    logger.warning(f"Error closing connection: {e}")

            self._connections.clear()
            self._available_connections.clear()
            logger.info("Closed all database connections")

    def _initialize_schema_once(self) -> None:
        """Initialize schema only once (thread-safe) with validation."""
        if self._schema_initialized:
            return

        try:
            connection = self._create_connection()

            # Create schema
            self._create_schema(connection)

            # Validate schema after creation
            self._validate_schema(connection)

            self._schema_initialized = True
            self._schema_validated = True
            connection.close()

            logger.info(
                "Database schema initialized and validated successfully",
                schema_version=SCHEMA_VERSION,
            )

            # Create analysis views for dashboards
            self._create_analysis_views(connection)

        except Exception as e:
            logger.error(f"Failed to initialize schema: {e}")
            raise SchemaError(f"Schema initialization failed: {e}")

    def _validate_schema(self, connection: duckdb.DuckDBPyConnection) -> None:
        """Validate database schema integrity."""
        try:
            # Check required tables exist (comprehensive schema from schema.py)
            required_tables = [
                "table_metadata",
                "gsi_metadata",
                "aws_accounts",
                "pricing_data",
                "metrics",
                "capacity_mode_recommendations",
                "table_class_recommendations",
                "utilization_recommendations",
                "collection_state",
                "checkpoints",
                "cur_metadata",
                "cur_data",
                "schema_version",
            ]

            for table in required_tables:
                try:
                    connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
                except Exception as e:
                    raise SchemaError(
                        f"Required table '{table}' not found or invalid: {e}"
                    )

            # Check required views exist
            required_views = [
                "normalized_metrics",
                "metric_identifiers",
                "daily_utilization",
            ]

            for view in required_views:
                try:
                    connection.execute(
                        f"SELECT COUNT(*) FROM {view} LIMIT 1"
                    ).fetchone()
                except Exception as e:
                    raise SchemaError(
                        f"Required view '{view}' not found or invalid: {e}"
                    )

            # Validate schema version
            try:
                result = connection.execute(
                    "SELECT version FROM schema_version ORDER BY applied_at DESC LIMIT 1"
                ).fetchone()

                if not result or result[0] != SCHEMA_VERSION:
                    raise SchemaError(
                        f"Schema version mismatch. Expected: {SCHEMA_VERSION}, "
                        f"Found: {result[0] if result else 'None'}"
                    )
            except Exception as e:
                if "schema_version" not in str(e):  # Don't double-report table missing
                    raise SchemaError(f"Schema version validation failed: {e}")

            # Test basic operations on key tables
            try:
                # Test metrics table structure
                connection.execute(
                    """
                    SELECT table_name, resource_name, metric_name, timestamp, value
                    FROM metrics LIMIT 1
                """
                ).fetchone()

                # Test normalized_metrics view
                connection.execute(
                    """
                    SELECT normalized_value, normalized_unit
                    FROM normalized_metrics LIMIT 1
                """
                ).fetchone()
                
                # Test pricing_data table structure (use 'region_code' which is the PRIMARY column)
                connection.execute(
                    """
                    SELECT region_code, price_per_unit, product_family
                    FROM pricing_data LIMIT 1
                """
                ).fetchone()

            except Exception as e:
                raise SchemaError(f"Schema structure validation failed: {e}")

            logger.debug("Database schema validation completed successfully")

        except SchemaError:
            raise
        except Exception as e:
            raise SchemaError(f"Unexpected error during schema validation: {e}")

    def validate_schema_at_startup(self) -> bool:
        """Validate schema at application startup."""
        try:
            if not self._schema_validated:
                with self.get_connection_context() as conn:
                    self._validate_schema(conn)
                    self._schema_validated = True

            logger.info("Startup schema validation completed successfully")
            return True

        except Exception as e:
            logger.error(f"Startup schema validation failed: {e}")
            return False

    def _create_schema(self, connection: duckdb.DuckDBPyConnection) -> None:
        """Create database schema with tables, indexes, and views."""
        
        # Import and initialize extended schema - SINGLE SOURCE OF TRUTH
        # This creates all tables and indexes
        from .schema import initialize_database
        initialize_database(connection)

        # Create normalized metrics view for utilization calculations
        normalized_metrics_view_sql = """
        CREATE OR REPLACE VIEW normalized_metrics AS
        SELECT
            *,
            CASE
                WHEN metric_name IN (
                    'ConsumedReadCapacityUnits',
                    'ConsumedWriteCapacityUnits'
                ) AND statistic = 'Sum'
                THEN value / period_seconds
                ELSE value
            END as normalized_value,

            CASE
                WHEN metric_name IN (
                    'ConsumedReadCapacityUnits',
                    'ConsumedWriteCapacityUnits'
                ) AND statistic = 'Sum'
                THEN 'Count/Second'
                ELSE unit
            END as normalized_unit
        FROM metrics
        """
        connection.execute(normalized_metrics_view_sql)

        # Create metric identifiers view for easy querying
        metric_identifiers_view_sql = """
        CREATE OR REPLACE VIEW metric_identifiers AS
        SELECT
            *,
            CASE
                WHEN operation IS NULL THEN
                    CONCAT(metric_name, ':', statistic, ':', period_seconds)
                WHEN operation_type IS NULL THEN
                    CONCAT(metric_name, ':', statistic, ':', period_seconds,
                           ':', operation)
                ELSE
                    CONCAT(metric_name, ':', statistic, ':', period_seconds,
                           ':', operation, ':', operation_type)
            END as metric_id
        FROM metrics
        """
        connection.execute(metric_identifiers_view_sql)

        # Create daily utilization materialized view for performance
        daily_utilization_view_sql = """
        CREATE OR REPLACE VIEW daily_utilization AS
        SELECT
            resource_name,
            resource_type,
            DATE(timestamp) as date,

            -- Use normalized values for consumed capacity (converted to units/second)
            AVG(CASE WHEN metric_name = 'ConsumedReadCapacityUnits'
                     AND statistic = 'Sum' AND period_seconds = 300
                     THEN value / period_seconds END) as avg_consumed_read_rate,
            AVG(CASE WHEN metric_name = 'ProvisionedReadCapacityUnits'
                     AND statistic = 'Average' AND period_seconds = 3600
                     THEN value END) as avg_provisioned_read_rate,
            AVG(CASE WHEN metric_name = 'ConsumedWriteCapacityUnits'
                     AND statistic = 'Sum' AND period_seconds = 300
                     THEN value / period_seconds END) as avg_consumed_write_rate,
            AVG(CASE WHEN metric_name = 'ProvisionedWriteCapacityUnits'
                     AND statistic = 'Average' AND period_seconds = 3600
                     THEN value END) as avg_provisioned_write_rate,

            -- Calculate utilization percentages
            CASE WHEN AVG(CASE WHEN metric_name = 'ProvisionedReadCapacityUnits'
                               AND statistic = 'Average' THEN value END) > 0
                 THEN (AVG(CASE WHEN metric_name = 'ConsumedReadCapacityUnits'
                                AND statistic = 'Sum' AND period_seconds = 300
                                THEN value / period_seconds END) /
                       AVG(CASE WHEN metric_name = 'ProvisionedReadCapacityUnits'
                                AND statistic = 'Average' THEN value END)) * 100
                 ELSE NULL END as read_utilization_percent,

            CASE WHEN AVG(CASE WHEN metric_name = 'ProvisionedWriteCapacityUnits'
                               AND statistic = 'Average' THEN value END) > 0
                 THEN (AVG(CASE WHEN metric_name = 'ConsumedWriteCapacityUnits'
                                AND statistic = 'Sum' AND period_seconds = 300
                                THEN value / period_seconds END) /
                       AVG(CASE WHEN metric_name = 'ProvisionedWriteCapacityUnits'
                                AND statistic = 'Average' THEN value END)) * 100
                 ELSE NULL END as write_utilization_percent
        FROM metrics
        WHERE timestamp >= CURRENT_DATE - INTERVAL '90 days'
        GROUP BY resource_name, resource_type, DATE(timestamp)
        """
        connection.execute(daily_utilization_view_sql)

        # Create schema version table and insert current version
        schema_version_sql = """
        CREATE TABLE IF NOT EXISTS schema_version (
            version VARCHAR PRIMARY KEY,
            applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
        connection.execute(schema_version_sql)

        # Insert current schema version
        connection.execute(
            """INSERT INTO schema_version (version) VALUES (?)
               ON CONFLICT (version) DO UPDATE SET
                   applied_at = now()""",
            [SCHEMA_VERSION],
        )

        logger.info(f"Database schema created successfully (version {SCHEMA_VERSION})")

    def _create_analysis_views(self, connection: duckdb.DuckDBPyConnection) -> None:
        """Create analysis views for dashboards and reporting."""
        try:
            from .view_manager import ViewManager

            # Create view manager with existing connection
            view_manager = ViewManager(self)

            # Create capacity views (essential for cost optimization)
            results = view_manager.create_all_views(categories=["capacity"])

            successful_views = [name for name, success in results.items() if success]
            failed_views = [name for name, success in results.items() if not success]

            if successful_views:
                logger.info(
                    f"Created {len(successful_views)} analysis views",
                    successful_views=successful_views,
                )

            if failed_views:
                logger.warning(
                    f"Failed to create {len(failed_views)} views",
                    failed_views=failed_views,
                )

        except Exception as e:
            logger.warning(f"Failed to create analysis views: {e}")
            # Don't fail database initialization if views fail

    def get_schema_version(self) -> Optional[str]:
        """Get current schema version."""
        try:
            with self.get_connection_context() as conn:
                result = conn.execute(
                    "SELECT version FROM schema_version "
                    "ORDER BY applied_at DESC LIMIT 1"
                ).fetchone()
                return result[0] if result else None
        except Exception as e:
            logger.warning(f"Could not retrieve schema version: {e}")
            return None

    def execute_query(
        self, query: str, parameters: Optional[List[Any]] = None
    ) -> List[Dict[str, Any]]:
        """Execute query with enhanced error handling and performance monitoring."""
        start_time = time.time()
        query_hash = hash(query)

        with self.get_connection_context() as conn:
            try:
                # Execute query with parameters
                if parameters:
                    result = conn.execute(query, parameters)
                else:
                    result = conn.execute(query)

                # Get column names
                columns = (
                    [desc[0] for desc in result.description]
                    if result.description
                    else []
                )

                # Convert rows to dictionaries
                rows = result.fetchall()
                results = [dict(zip(columns, row)) for row in rows]

                # Update performance metrics
                execution_time = time.time() - start_time
                with self._lock:
                    self._performance_metrics.total_queries += 1
                    self._performance_metrics.total_query_time += execution_time

                    if execution_time > 1.0:  # Slow query threshold
                        self._performance_metrics.slow_queries += 1

                # Log slow queries for optimization
                if execution_time > 1.0:
                    logger.warning(
                        "Slow query detected",
                        execution_time_ms=round(execution_time * 1000, 2),
                        query_hash=query_hash,
                        result_count=len(results),
                        query_preview=(
                            query[:100] + "..." if len(query) > 100 else query
                        ),
                    )
                else:
                    logger.debug(
                        "Query executed successfully",
                        execution_time_ms=round(execution_time * 1000, 2),
                        result_count=len(results),
                    )

                return results

            except Exception as e:
                # Update failure metrics
                execution_time = time.time() - start_time
                with self._lock:
                    self._performance_metrics.failed_queries += 1

                # Enhanced error logging
                logger.error(
                    "Query execution failed",
                    execution_time_ms=round(execution_time * 1000, 2),
                    query_hash=query_hash,
                    error=str(e),
                    query_preview=query[:200] + "..." if len(query) > 200 else query,
                )

                if parameters:
                    logger.error(f"Query parameters: {parameters}")

                # Provide specific error context
                if "syntax error" in str(e).lower():
                    raise DatabaseError(f"SQL syntax error: {e}")
                elif "no such table" in str(e).lower():
                    raise DatabaseError(f"Table not found: {e}")
                elif "no such column" in str(e).lower():
                    raise DatabaseError(f"Column not found: {e}")
                else:
                    raise DatabaseError(f"Query execution failed: {e}")

    def execute_insert(self, table: str, data: Dict[str, Any]) -> None:
        """Execute insert statement."""
        columns = list(data.keys())
        placeholders = ["?" for _ in columns]
        values = list(data.values())

        query = (
            f"INSERT INTO {table} ({', '.join(columns)}) "
            f"VALUES ({', '.join(placeholders)})"
        )

        with self.get_connection_context() as conn:
            try:
                conn.execute(query, values)
                logger.debug(f"Inserted record into {table}")
            except Exception as e:
                logger.error(f"Insert failed for table {table}: {e}")
                raise DatabaseError(f"Insert failed: {e}")

    def execute_batch_insert(self, table: str, data: List[Dict[str, Any]]) -> None:
        """Execute optimized bulk insert using DuckDB's staging table approach."""
        if not data:
            return

        # Lazy import - only needed conditionally
        import json

        # Lazy import - only needed conditionally
        import os

        # Lazy import - only needed conditionally
        import tempfile

        start_time = time.time()
        record_count = len(data)

        with self.get_connection_context() as conn:
            try:
                # Update batch metrics
                with self._lock:
                    self._performance_metrics.batch_operations += 1
                    self._performance_metrics.batch_records_processed += record_count

                # Create temporary JSON file for bulk loading
                with tempfile.NamedTemporaryFile(
                    mode="w", suffix=".json", delete=False
                ) as temp_file:
                    # Write data as NDJSON (newline-delimited JSON) for DuckDB
                    for row in data:
                        # Convert datetime to string for JSON serialization
                        json_row = {}
                        for key, value in row.items():
                            if hasattr(value, "isoformat"):  # datetime object
                                json_row[key] = value.isoformat()
                            elif isinstance(value, dict):  # dimensions
                                json_row[key] = json.dumps(value)
                            else:
                                json_row[key] = value
                        temp_file.write(json.dumps(json_row) + "\n")

                    temp_file_path = temp_file.name

                try:
                    # Use DuckDB's optimized bulk insert from JSON - direct approach
                    columns = list(data[0].keys())
                    column_casts = self._build_column_casts(columns)

                    # Add created_at column with default value for metrics table
                    if table == "metrics":
                        column_casts.append("CURRENT_TIMESTAMP as created_at")

                    query = f"""
                        INSERT INTO {table}
                        SELECT {', '.join(column_casts)}
                        FROM read_ndjson_auto(?)
                    """

                    conn.execute(query, [temp_file_path])

                    execution_time = time.time() - start_time
                    logger.debug(
                        f"Bulk inserted records using DuckDB OLAP optimization",
                        table=table,
                        record_count=record_count,
                        execution_time_ms=round(execution_time * 1000, 2),
                        throughput_records_per_sec=round(
                            record_count / execution_time, 2
                        ),
                    )

                finally:
                    # Clean up temporary file
                    try:
                        os.unlink(temp_file_path)
                    except Exception:
                        pass

            except Exception as e:
                # Update failure metrics
                with self._lock:
                    self._performance_metrics.batch_failures += 1

                execution_time = time.time() - start_time
                logger.error(
                    f"Bulk insert failed",
                    table=table,
                    record_count=record_count,
                    execution_time_ms=round(execution_time * 1000, 2),
                    error=str(e),
                )
                raise DatabaseError(f"Bulk insert failed for table {table}: {e}")

    def _build_column_casts(self, columns: List[str]) -> List[str]:
        """Build column casting expressions for proper data types."""
        column_casts = []
        for col in columns:
            if col == "timestamp":
                column_casts.append(f"CAST({col} AS TIMESTAMP) as {col}")
            elif col in [
                "value",
                "provisioned_read_capacity",
                "provisioned_write_capacity",
                "current_cost",
                "recommended_cost",
                "savings_potential",
                "confidence_score",
                "completion_percentage",
            ]:
                column_casts.append(f"CAST({col} AS DOUBLE) as {col}")
            elif col in ["period_seconds", "analysis_period_days"]:
                column_casts.append(f"CAST({col} AS INTEGER) as {col}")
            else:
                column_casts.append(col)
        return column_casts

    def execute_batch_upsert_metrics(self, data: List[Dict[str, Any]]) -> None:
        """Execute optimized bulk upsert for metrics using DuckDB's staging table approach."""
        if not data:
            return

        # Lazy import - only needed conditionally
        import json

        # Lazy import - only needed conditionally
        import os

        # Lazy import - only needed conditionally
        import tempfile

        start_time = time.time()
        record_count = len(data)

        with self.get_connection_context() as conn:
            try:
                # Update batch metrics
                with self._lock:
                    self._performance_metrics.batch_operations += 1
                    self._performance_metrics.batch_records_processed += record_count

                # Create temporary JSON file for bulk loading
                with tempfile.NamedTemporaryFile(
                    mode="w", suffix=".json", delete=False
                ) as temp_file:
                    # Write data as NDJSON (newline-delimited JSON) for DuckDB
                    for row in data:
                        # Convert datetime to string for JSON serialization
                        json_row = {}
                        for key, value in row.items():
                            if hasattr(value, "isoformat"):  # datetime object
                                json_row[key] = value.isoformat()
                            elif isinstance(value, dict):  # dimensions
                                json_row[key] = json.dumps(value)
                            else:
                                json_row[key] = value
                        temp_file.write(json.dumps(json_row) + "\n")

                    temp_file_path = temp_file.name

                try:
                    # Create staging table and load data from JSON file - DuckDB's optimal approach
                    staging_table_name = f"staging_metrics_{int(time.time() * 1000000)}"  # Use microseconds for uniqueness

                    conn.execute(
                        f"""
                        CREATE TEMPORARY TABLE {staging_table_name} AS
                        SELECT
                            account_id,
                            table_name,
                            resource_name,
                            resource_type,
                            metric_name,
                            operation,
                            operation_type,
                            statistic,
                            period_seconds,
                            CAST(timestamp AS TIMESTAMP) as timestamp,
                            CAST(value AS DOUBLE) as value,
                            unit,
                            region,
                            dimensions,
                            CURRENT_TIMESTAMP as created_at
                        FROM read_ndjson_auto(?)
                    """,
                        [temp_file_path],
                    )

                    # Perform bulk merge operation using DuckDB's efficient staging approach
                    conn.execute(
                        f"""
                        INSERT INTO metrics
                        SELECT * FROM {staging_table_name}
                        ON CONFLICT (account_id, resource_name, metric_name, timestamp, statistic, period_seconds)
                        DO UPDATE SET
                            value = EXCLUDED.value,
                            unit = EXCLUDED.unit,
                            dimensions = EXCLUDED.dimensions,
                            operation = EXCLUDED.operation,
                            operation_type = EXCLUDED.operation_type,
                            resource_type = EXCLUDED.resource_type,
                            table_name = EXCLUDED.table_name,
                            region = EXCLUDED.region
                    """
                    )

                    # Clean up staging table - DuckDB handles this efficiently
                    conn.execute(f"DROP TABLE {staging_table_name}")

                    execution_time = time.time() - start_time
                    logger.debug(
                        f"Bulk upserted metrics using DuckDB staging table approach",
                        record_count=record_count,
                        execution_time_ms=round(execution_time * 1000, 2),
                        throughput_records_per_sec=round(
                            record_count / execution_time, 2
                        ),
                    )

                finally:
                    # Clean up temporary file
                    try:
                        os.unlink(temp_file_path)
                    except Exception:
                        pass

            except Exception as e:
                # Update failure metrics
                with self._lock:
                    self._performance_metrics.batch_failures += 1

                execution_time = time.time() - start_time
                logger.error(
                    f"Bulk upsert failed for metrics",
                    record_count=record_count,
                    execution_time_ms=round(execution_time * 1000, 2),
                    error=str(e),
                )
                raise DatabaseError(f"Bulk upsert failed: {e}")

    def get_table_info(self, table_name: str) -> Dict[str, Any]:
        """Get information about a table."""
        query = f"DESCRIBE {table_name}"
        try:
            return self.execute_query(query)
        except Exception as e:
            logger.error(f"Failed to get table info for {table_name}: {e}")
            raise DatabaseError(f"Failed to get table info: {e}")

    def vacuum_database(self) -> None:
        """Optimize database by running VACUUM."""
        with self.get_connection_context() as conn:
            try:
                conn.execute("VACUUM")
                logger.info("Database vacuum completed")
            except Exception as e:
                logger.error(f"Database vacuum failed: {e}")
                raise DatabaseError(f"Vacuum failed: {e}")

    def get_database_stats(self) -> Dict[str, Any]:
        """Get comprehensive database statistics with performance metrics."""
        stats = {}

        try:
            # Get table row counts and sizes
            tables = [
                "metrics",
                "table_metadata",
                "gsi_metadata",
                "cost_analyses",
                "operation_states",
            ]

            total_rows = 0
            for table in tables:
                try:
                    result = self.execute_query(
                        f"SELECT COUNT(*) as count FROM {table}"
                    )
                    count = result[0]["count"] if result else 0
                    stats[f"{table}_count"] = count
                    total_rows += count
                except Exception:
                    stats[f"{table}_count"] = 0

            stats["total_rows"] = total_rows

            # Get database file size if not in-memory
            if self.db_path != ":memory:":
                db_file = Path(self.db_path)
                if db_file.exists():
                    file_size = db_file.stat().st_size
                    stats["database_size_bytes"] = file_size
                    stats["database_size_mb"] = round(file_size / (1024 * 1024), 2)
                else:
                    stats["database_size_bytes"] = 0
                    stats["database_size_mb"] = 0

            # Get schema version
            stats["schema_version"] = self.get_schema_version()

            # Add performance metrics
            stats["performance_metrics"] = {
                "total_connections_created": self._performance_metrics.total_connections_created,
                "active_connections": self._performance_metrics.active_connections,
                "pool_hit_ratio": self._performance_metrics.get_pool_hit_ratio(),
                "total_queries": self._performance_metrics.total_queries,
                "average_query_time_ms": round(
                    self._performance_metrics.get_average_query_time() * 1000, 2
                ),
                "slow_queries": self._performance_metrics.slow_queries,
                "query_failure_rate": self._performance_metrics.get_query_failure_rate(),
                "batch_operations": self._performance_metrics.batch_operations,
                "batch_records_processed": self._performance_metrics.batch_records_processed,
                "batch_failures": self._performance_metrics.batch_failures,
            }

            # Add connection pool status
            stats["connection_pool"] = {
                "max_connections": self.max_connections,
                "active_connections": len(self._connections),
                "available_connections": len(self._available_connections),
                "utilization_percent": round(
                    (len(self._connections) / self.max_connections) * 100, 1
                ),
            }

            return stats

        except Exception as e:
            logger.error(f"Failed to get database stats: {e}")
            return {"error": str(e)}

    def get_performance_recommendations(self) -> List[str]:
        """Get performance optimization recommendations based on current metrics."""
        recommendations = []

        try:
            # Analyze connection pool performance
            pool_hit_ratio = self._performance_metrics.get_pool_hit_ratio()
            if pool_hit_ratio < 0.8:
                recommendations.append(
                    f"Low connection pool hit ratio ({pool_hit_ratio:.1%}). "
                    f"Consider increasing database_pool_size from {self.max_connections} to "
                    f"{min(self.max_connections * 2, 50)}."
                )

            # Analyze query performance
            avg_query_time = self._performance_metrics.get_average_query_time()
            if avg_query_time > 1.0:
                recommendations.append(
                    f"High average query time ({avg_query_time:.2f}s). "
                    "Consider adding indexes, optimizing queries, or increasing memory_limit."
                )

            slow_query_ratio = (
                self._performance_metrics.slow_queries
                / self._performance_metrics.total_queries
                if self._performance_metrics.total_queries > 0
                else 0
            )
            if slow_query_ratio > 0.1:  # More than 10% slow queries
                recommendations.append(
                    f"High slow query ratio ({slow_query_ratio:.1%}). "
                    "Review query patterns and consider database optimization."
                )

            # Analyze failure rates
            query_failure_rate = self._performance_metrics.get_query_failure_rate()
            if query_failure_rate > 0.05:  # More than 5% failure rate
                recommendations.append(
                    f"High query failure rate ({query_failure_rate:.1%}). "
                    "Check for schema issues or data quality problems."
                )

            batch_failure_rate = (
                self._performance_metrics.batch_failures
                / self._performance_metrics.batch_operations
                if self._performance_metrics.batch_operations > 0
                else 0
            )
            if batch_failure_rate > 0.02:  # More than 2% batch failure rate
                recommendations.append(
                    f"High batch operation failure rate ({batch_failure_rate:.1%}). "
                    "Review batch data quality and transaction handling."
                )

            # Analyze database size and suggest maintenance
            stats = self.get_database_stats()
            if (
                "database_size_mb" in stats and stats["database_size_mb"] > 1000
            ):  # > 1GB
                recommendations.append(
                    f"Large database size ({stats['database_size_mb']:.1f}MB). "
                    "Consider running VACUUM or implementing data archival."
                )

            # Connection pool utilization
            pool_utilization = len(self._connections) / self.max_connections
            if pool_utilization > 0.9:
                recommendations.append(
                    f"High connection pool utilization ({pool_utilization:.1%}). "
                    f"Consider increasing database_pool_size from {self.max_connections}."
                )

            if not recommendations:
                recommendations.append(
                    "Database performance is optimal. No recommendations at this time."
                )

            return recommendations

        except Exception as e:
            logger.error(f"Failed to generate performance recommendations: {e}")
            return [f"Error generating recommendations: {e}"]

    def reset_performance_metrics(self) -> None:
        """Reset performance metrics for fresh monitoring period."""
        with self._lock:
            self._performance_metrics.reset()
        logger.info("Performance metrics reset successfully")

    def __enter__(self):
        """Context manager entry."""
        return self.get_connection()

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.return_connection(self.__enter__())


# Global database manager instance
_db_manager: Optional[DatabaseManager] = None


def get_database_manager() -> DatabaseManager:
    """Get database manager instance with lazy initialization."""
    global _db_manager

    if _db_manager is None:
        settings = get_settings()
        _db_manager = DatabaseManager(
            database_url=settings.database_url,
            max_connections=settings.database_pool_size,
        )

        # Validate schema at startup
        if not _db_manager.validate_schema_at_startup():
            logger.error("Database schema validation failed at startup")
            raise SchemaError("Database schema validation failed")

    return _db_manager


def get_database() -> duckdb.DuckDBPyConnection:
    """Get database connection for dependency injection."""
    return get_database_manager().get_connection()


def get_connection() -> duckdb.DuckDBPyConnection:
    """Get database connection (alias for get_database for backwards compatibility)."""
    return get_database_manager().get_connection()


def initialize_database() -> bool:
    """Initialize database and validate schema at application startup."""
    try:
        db_manager = get_database_manager()
        health_status = db_manager.get_health_status()

        if not health_status.is_healthy:
            logger.error(
                "Database health check failed at startup",
                issues=health_status.issues,
                recommendations=health_status.recommendations,
            )
            return False

        logger.info(
            "Database initialized successfully",
            schema_version=health_status.schema_version,
            pool_status=health_status.connection_pool_status,
        )

        # Log performance recommendations if any
        recommendations = db_manager.get_performance_recommendations()
        if (
            recommendations
            and recommendations[0]
            != "Database performance is optimal. No recommendations at this time."
        ):
            logger.info(
                "Database performance recommendations", recommendations=recommendations
            )

        return True

    except Exception as e:
        logger.error(f"Database initialization failed: {e}")
        return False


def shutdown_database() -> None:
    """Shutdown database connections gracefully."""
    global _db_manager

    if _db_manager is not None:
        try:
            _db_manager.close_all_connections()
            logger.info("Database connections closed successfully")
        except Exception as e:
            logger.error(f"Error during database shutdown: {e}")
        finally:
            _db_manager = None
