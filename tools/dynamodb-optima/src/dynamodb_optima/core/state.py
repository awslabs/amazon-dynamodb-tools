"""
State management system for resumable operations.

Provides checkpoint and resume capabilities for long-running operations
like discovery, collection, and analysis with persistent storage.
"""

import gzip
import json
import logging
import pickle
import threading
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

logger = logging.getLogger(__name__)

# State schema version for backward compatibility
STATE_SCHEMA_VERSION = "1.1.0"

# Previous schema versions for migration support
SUPPORTED_SCHEMA_VERSIONS = ["1.0.0", "1.1.0"]


@dataclass
class TableMetadata:
    """Metadata for discovered DynamoDB tables."""

    table_name: str
    region: str
    billing_mode: str
    provisioned_read_capacity: Optional[int] = None
    provisioned_write_capacity: Optional[int] = None
    gsi_count: int = 0
    status: str = "ACTIVE"
    last_updated: Optional[datetime] = None


@dataclass
class GSIMetadata:
    """Metadata for Global Secondary Indexes."""

    table_name: str
    gsi_name: str
    region: str
    provisioned_read_capacity: Optional[int] = None
    provisioned_write_capacity: Optional[int] = None
    projection_type: str = "ALL"
    last_updated: Optional[datetime] = None


@dataclass
class CollectionState:
    """Detailed state for metrics collection operations."""

    # Discovery phase state
    regions_to_discover: List[str] = field(default_factory=list)
    regions_completed: List[str] = field(default_factory=list)
    tables_discovered: Dict[str, List[TableMetadata]] = field(
        default_factory=dict
    )  # region -> tables
    gsis_discovered: Dict[str, List[GSIMetadata]] = field(
        default_factory=dict
    )  # table -> GSIs

    # Collection phase state
    resources_to_collect: List[str] = field(
        default_factory=list
    )  # table names and GSI names
    time_periods_to_collect: List[Tuple[datetime, datetime]] = field(
        default_factory=list
    )
    completed_collections: Set[Tuple[str, str, datetime]] = field(
        default_factory=set
    )  # (resource, metric, timestamp)
    completed_resources: Set[str] = field(
        default_factory=set
    )  # Set of "region:resource_name" strings for completed resources
    failed_collections: List[Dict[str, Any]] = field(default_factory=list)

    # Progress tracking
    total_operations: int = 0
    completed_operations: int = 0
    completed_batches: int = 0  # Track completed API batches for better progress
    start_time: Optional[datetime] = None
    last_checkpoint_time: Optional[datetime] = None
    estimated_completion: Optional[datetime] = None


@dataclass
class OperationState:
    """State tracking for resumable operations."""

    operation_id: str
    operation_type: str  # 'DISCOVERY', 'COLLECTION', 'ANALYSIS'
    status: str  # 'RUNNING', 'PAUSED', 'COMPLETED', 'FAILED'
    start_time: datetime
    last_checkpoint_time: datetime
    completion_percentage: float
    estimated_completion: Optional[datetime]
    error_message: Optional[str]
    created_by: str
    region: Optional[str] = None

    # Schema versioning
    schema_version: str = STATE_SCHEMA_VERSION

    # Detailed state data
    collection_state: Optional[CollectionState] = None

    # Custom state data for different operation types
    custom_data: Dict[str, Any] = field(default_factory=dict)


class StateValidationError(Exception):
    """Raised when state validation fails."""

    pass


class StateMigrationError(Exception):
    """Raised when state migration fails."""

    pass


class StateConsistencyError(Exception):
    """Raised when state consistency checks fail."""

    pass


class StateManager:
    """Manages persistent state for resumable operations with thread safety."""

    def __init__(
        self,
        enable_compression: bool = True,
        auto_cleanup_enabled: bool = True,
        max_checkpoint_age_days: int = 7,
    ):
        """Initialize state manager with checkpoint directory."""
        # Import here to avoid circular dependency
        from ..paths import get_checkpoints_dir
        
        # Always use centralized path management (respects --project-root)
        self.checkpoint_dir = get_checkpoints_dir()
        self.checkpoint_dir.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()  # Reentrant lock for nested operations
        self.enable_compression = enable_compression
        self.auto_cleanup_enabled = auto_cleanup_enabled
        self.max_checkpoint_age_days = max_checkpoint_age_days

        # Track active operations to prevent bypassing
        self._active_operations: Set[str] = set()

        logger.info(
            f"StateManager initialized with checkpoint directory: {self.checkpoint_dir}, "
            f"compression: {enable_compression}, auto_cleanup: {auto_cleanup_enabled}"
        )

        # Perform initial cleanup if enabled
        if self.auto_cleanup_enabled:
            try:
                self.cleanup_old_checkpoints(max_age_days=self.max_checkpoint_age_days)
            except Exception as e:
                logger.warning(f"Initial checkpoint cleanup failed: {e}")

    def _get_checkpoint_path(self, operation_id: str) -> Path:
        """Get the file path for a checkpoint."""
        return self.checkpoint_dir / f"{operation_id}.checkpoint"

    def _get_timestamp_checkpoint_name(
        self, operation_type: str, timestamp: Optional[datetime] = None
    ) -> str:
        """Generate human-readable checkpoint name with ISO timestamp."""
        if timestamp is None:
            timestamp = datetime.now()

        # Format: discovery_2025-09-08T22-15-34
        iso_timestamp = timestamp.strftime("%Y-%m-%dT%H-%M-%S")
        return f"{operation_type.lower()}_{iso_timestamp}"

    def _validate_state(self, state: OperationState) -> None:
        """Validate state object for consistency."""
        if not state.operation_id:
            raise StateValidationError("Operation ID cannot be empty")

        if state.operation_type not in ["DISCOVERY", "COLLECTION", "ANALYSIS"]:
            raise StateValidationError(
                f"Invalid operation type: {state.operation_type}"
            )

        if state.status not in ["RUNNING", "PAUSED", "COMPLETED", "FAILED"]:
            raise StateValidationError(f"Invalid status: {state.status}")

        if state.completion_percentage < 0 or state.completion_percentage > 100:
            raise StateValidationError(
                f"Invalid completion percentage: {state.completion_percentage}"
            )

        # Validate required timestamps
        if not state.start_time:
            raise StateValidationError("Start time cannot be None")

        if not state.last_checkpoint_time:
            raise StateValidationError("Last checkpoint time cannot be None")

        # Validate schema version compatibility
        if hasattr(state, "schema_version"):
            if state.schema_version not in SUPPORTED_SCHEMA_VERSIONS:
                raise StateValidationError(
                    f"Unsupported schema version: {state.schema_version}. "
                    f"Supported versions: {SUPPORTED_SCHEMA_VERSIONS}"
                )
        else:
            # Legacy state without schema version
            logger.warning("State missing schema version, assuming 1.0.0")
            state.schema_version = "1.0.0"

    def _migrate_state(self, state: OperationState) -> OperationState:
        """Migrate state from older schema versions to current version."""
        if not hasattr(state, "schema_version"):
            state.schema_version = "1.0.0"

        original_version = state.schema_version

        try:
            # Migration from 1.0.0 to 1.1.0
            if state.schema_version == "1.0.0":
                logger.info(f"Migrating state {state.operation_id} from 1.0.0 to 1.1.0")

                # Add new fields introduced in 1.1.0
                if not hasattr(state, "custom_data"):
                    state.custom_data = {}

                # Ensure collection_state has new fields
                if state.collection_state:
                    if not hasattr(state.collection_state, "completed_batches"):
                        state.collection_state.completed_batches = 0

                    if not hasattr(state.collection_state, "completed_resources"):
                        state.collection_state.completed_resources = set()

                state.schema_version = "1.1.0"

            # Future migrations would go here
            # elif state.schema_version == "1.1.0":
            #     # Migration to 1.2.0
            #     pass

            if state.schema_version != original_version:
                logger.info(
                    f"Successfully migrated state {state.operation_id} "
                    f"from {original_version} to {state.schema_version}"
                )

            return state

        except Exception as e:
            raise StateMigrationError(
                f"Failed to migrate state {state.operation_id} "
                f"from {original_version} to {STATE_SCHEMA_VERSION}: {e}"
            )

    def _compress_state(self, state: OperationState) -> bytes:
        """Compress state data for storage."""
        if not self.enable_compression:
            return pickle.dumps(state, protocol=pickle.HIGHEST_PROTOCOL)

        # Use gzip compression for large state objects
        pickled_data = pickle.dumps(state, protocol=pickle.HIGHEST_PROTOCOL)
        return gzip.compress(pickled_data, compresslevel=6)

    def _decompress_state(self, data: bytes) -> OperationState:
        """Decompress state data from storage."""
        try:
            # Try gzip decompression first
            if self.enable_compression:
                try:
                    decompressed_data = gzip.decompress(data)
                    return pickle.loads(decompressed_data)
                except (gzip.BadGzipFile, OSError):
                    # Fall back to uncompressed pickle
                    pass

            # Try direct pickle loading
            return pickle.loads(data)

        except Exception as e:
            raise StateValidationError(f"Failed to decompress state data: {e}")

    def _ensure_operation_consistency(self, operation_id: str) -> None:
        """Ensure operation is being managed consistently through StateManager."""
        if operation_id not in self._active_operations:
            logger.debug(f"Registering operation {operation_id} with StateManager")
            self._active_operations.add(operation_id)

    def _validate_state_consistency(self, state: OperationState) -> None:
        """Perform comprehensive state consistency checks."""
        # Check for logical inconsistencies
        if state.collection_state:
            cs = state.collection_state

            # Validate completion counts
            if cs.completed_operations > cs.total_operations:
                raise StateConsistencyError(
                    f"Completed operations ({cs.completed_operations}) "
                    f"exceeds total operations ({cs.total_operations})"
                )

            # Validate region consistency
            if len(cs.regions_completed) > len(cs.regions_to_discover):
                logger.warning(
                    f"More regions completed ({len(cs.regions_completed)}) "
                    f"than regions to discover ({len(cs.regions_to_discover)})"
                )

            # Validate time consistency
            if cs.start_time and state.last_checkpoint_time:
                if cs.start_time > state.last_checkpoint_time:
                    raise StateConsistencyError(
                        "Start time cannot be after last checkpoint time"
                    )

        # Validate completion percentage consistency
        if state.collection_state and state.collection_state.total_operations > 0:
            expected_percentage = (
                state.collection_state.completed_operations
                / state.collection_state.total_operations
                * 100
            )
            if abs(state.completion_percentage - expected_percentage) > 5.0:
                logger.warning(
                    f"Completion percentage mismatch: "
                    f"reported {state.completion_percentage}%, "
                    f"calculated {expected_percentage}%"
                )

    def save_checkpoint(
        self, state: OperationState, filename: Optional[str] = None
    ) -> None:
        """Save operation state to persistent storage with thread safety."""
        with self._lock:
            try:
                # Ensure consistent operation management
                self._ensure_operation_consistency(state.operation_id)

                # Validate state before saving
                self._validate_state(state)
                self._validate_state_consistency(state)

                # Migrate state if needed
                state = self._migrate_state(state)

                # Update checkpoint timestamp
                state.last_checkpoint_time = datetime.now()

                # Determine file path
                if filename:
                    checkpoint_path = self.checkpoint_dir / filename
                else:
                    checkpoint_path = self._get_checkpoint_path(state.operation_id)

                # Create temporary file for atomic write
                temp_path = checkpoint_path.with_suffix(".tmp")

                # Compress and save state
                compressed_data = self._compress_state(state)
                with open(temp_path, "wb") as f:
                    f.write(compressed_data)

                # Atomic rename
                temp_path.rename(checkpoint_path)

                logger.debug(
                    f"Checkpoint saved for operation {state.operation_id} "
                    f"at {checkpoint_path} (compressed: {self.enable_compression})"
                )

                # Perform automatic cleanup if enabled
                if self.auto_cleanup_enabled:
                    try:
                        self.cleanup_old_checkpoints(
                            max_age_days=self.max_checkpoint_age_days
                        )
                    except Exception as cleanup_error:
                        logger.warning(f"Auto-cleanup failed: {cleanup_error}")

            except Exception as e:
                logger.error(
                    f"Failed to save checkpoint for operation {state.operation_id}: {e}"
                )
                raise

    def load_checkpoint(self, operation_id: str) -> Optional[OperationState]:
        """Load operation state from persistent storage with validation and recovery."""
        with self._lock:
            checkpoint_path = self._get_checkpoint_path(operation_id)

            if not checkpoint_path.exists():
                logger.warning(f"Checkpoint not found for operation {operation_id}")
                return None

            try:
                # Ensure consistent operation management
                self._ensure_operation_consistency(operation_id)

                with open(checkpoint_path, "rb") as f:
                    data = f.read()

                # Decompress and load state
                state = self._decompress_state(data)

                # Migrate state if needed
                state = self._migrate_state(state)

                # Validate loaded state
                self._validate_state(state)
                self._validate_state_consistency(state)

                logger.info(
                    f"Checkpoint loaded for operation {operation_id} "
                    f"(schema version: {state.schema_version})"
                )
                return state

            except (
                StateMigrationError,
                StateValidationError,
                StateConsistencyError,
            ) as e:
                logger.error(
                    f"State validation/migration failed for operation {operation_id}: {e}"
                )
                # Move problematic file to backup
                backup_path = checkpoint_path.with_suffix(
                    f".error_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
                )
                checkpoint_path.rename(backup_path)
                logger.info(f"Problematic checkpoint moved to {backup_path}")
                return None

            except Exception as e:
                logger.debug(
                    f"Failed to load checkpoint for operation {operation_id}: {e}"
                )
                # Move corrupted file to backup
                backup_path = checkpoint_path.with_suffix(".corrupted")
                try:
                    checkpoint_path.rename(backup_path)
                    logger.debug(f"Corrupted checkpoint moved to {backup_path}")
                except Exception as rename_error:
                    logger.warning(
                        f"Failed to move corrupted checkpoint: {rename_error}"
                    )
                return None

    def list_checkpoints(self) -> List[str]:
        """List available checkpoint operation IDs."""
        with self._lock:
            try:
                checkpoints = []
                for checkpoint_file in self.checkpoint_dir.glob("*.checkpoint"):
                    operation_id = checkpoint_file.stem
                    checkpoints.append(operation_id)

                logger.debug(f"Found {len(checkpoints)} checkpoints")
                return sorted(checkpoints)

            except Exception as e:
                logger.error(f"Failed to list checkpoints: {e}")
                return []

    def list_checkpoints_with_details(self) -> List[Dict[str, Any]]:
        """List available checkpoints with detailed information."""
        with self._lock:
            try:
                checkpoints = []
                for checkpoint_file in self.checkpoint_dir.glob("*.checkpoint"):
                    operation_id = checkpoint_file.stem

                    # Get file stats
                    stat = checkpoint_file.stat()

                    # Try to load basic state info
                    try:
                        state = self.load_checkpoint(operation_id)
                        if state:
                            checkpoints.append(
                                {
                                    "operation_id": operation_id,
                                    "operation_type": state.operation_type,
                                    "status": state.status,
                                    "completion_percentage": (
                                        state.completion_percentage
                                    ),
                                    "start_time": state.start_time,
                                    "last_checkpoint_time": state.last_checkpoint_time,
                                    "estimated_completion": state.estimated_completion,
                                    "file_size": stat.st_size,
                                    "created_time": datetime.fromtimestamp(
                                        stat.st_ctime
                                    ),
                                    "modified_time": datetime.fromtimestamp(
                                        stat.st_mtime
                                    ),
                                }
                            )
                    except Exception as e:
                        # Include corrupted checkpoints with error info
                        checkpoints.append(
                            {
                                "operation_id": operation_id,
                                "operation_type": "UNKNOWN",
                                "status": "CORRUPTED",
                                "completion_percentage": 0.0,
                                "error": str(e),
                                "file_size": stat.st_size,
                                "created_time": datetime.fromtimestamp(stat.st_ctime),
                                "modified_time": datetime.fromtimestamp(stat.st_mtime),
                            }
                        )

                # Sort by creation time (newest first)
                checkpoints.sort(
                    key=lambda x: x.get("created_time", datetime.min), reverse=True
                )
                return checkpoints

            except Exception as e:
                logger.error(f"Failed to list checkpoints with details: {e}")
                return []

    def get_latest_checkpoint(
        self, operation_type: Optional[str] = None
    ) -> Optional[str]:
        """Get the most recent checkpoint, optionally filtered by operation type."""
        checkpoints = self.list_checkpoints_with_details()

        if operation_type:
            checkpoints = [
                c for c in checkpoints if c.get("operation_type") == operation_type
            ]

        # Filter out corrupted checkpoints
        valid_checkpoints = [c for c in checkpoints if c.get("status") != "CORRUPTED"]

        if valid_checkpoints:
            return valid_checkpoints[0]["operation_id"]

        return None

    def delete_checkpoint(self, operation_id: str) -> bool:
        """Delete a checkpoint file."""
        with self._lock:
            checkpoint_path = self._get_checkpoint_path(operation_id)

            if checkpoint_path.exists():
                try:
                    # Unregister operation
                    self.unregister_operation(operation_id)

                    checkpoint_path.unlink()
                    logger.info(f"Checkpoint deleted for operation {operation_id}")
                    return True
                except Exception as e:
                    logger.error(
                        f"Failed to delete checkpoint for operation {operation_id}: {e}"
                    )
                    return False

            return False

    def cleanup_old_checkpoints(
        self, max_age_days: int = 7, cleanup_backups: bool = True
    ) -> int:
        """Clean up old checkpoint files and backup files."""
        with self._lock:
            cutoff_time = datetime.now() - timedelta(days=max_age_days)
            cleaned_count = 0

            try:
                # Clean up main checkpoint files
                for checkpoint_file in self.checkpoint_dir.glob("*.checkpoint"):
                    file_mtime = datetime.fromtimestamp(checkpoint_file.stat().st_mtime)

                    if file_mtime < cutoff_time:
                        try:
                            # Check if operation is still running
                            operation_id = checkpoint_file.stem
                            state = self.load_checkpoint(operation_id)
                            if state and state.status in ["RUNNING", "PAUSED"]:
                                logger.debug(
                                    f"Skipping cleanup of active operation {operation_id}"
                                )
                                continue

                            # Unregister operation before cleanup
                            self.unregister_operation(operation_id)

                            checkpoint_file.unlink()
                            cleaned_count += 1
                            logger.debug(
                                f"Cleaned up old checkpoint: {checkpoint_file.name}"
                            )

                        except Exception as e:
                            logger.warning(
                                f"Failed to clean up checkpoint {checkpoint_file.name}: {e}"
                            )

                # Clean up backup files if requested
                if cleanup_backups:
                    backup_patterns = ["*.corrupted", "*.error_*", "*.tmp"]
                    for pattern in backup_patterns:
                        for backup_file in self.checkpoint_dir.glob(pattern):
                            try:
                                file_mtime = datetime.fromtimestamp(
                                    backup_file.stat().st_mtime
                                )
                                if file_mtime < cutoff_time:
                                    backup_file.unlink()
                                    cleaned_count += 1
                                    logger.debug(
                                        f"Cleaned up backup file: {backup_file.name}"
                                    )
                            except Exception as e:
                                logger.warning(
                                    f"Failed to clean up backup {backup_file.name}: {e}"
                                )

                logger.info(f"Cleaned up {cleaned_count} old files")
                return cleaned_count

            except Exception as e:
                logger.error(f"Failed to cleanup old checkpoints: {e}")
                return 0

    def export_state(
        self, operation_id: str, export_path: str, compress: bool = True
    ) -> bool:
        """Export state to a file for debugging or migration."""
        with self._lock:
            try:
                state = self.load_checkpoint(operation_id)
                if not state:
                    return False

                export_file = Path(export_path)
                export_file.parent.mkdir(parents=True, exist_ok=True)

                # Use compression for export
                if compress:
                    compressed_data = self._compress_state(state)
                    with open(export_file, "wb") as f:
                        f.write(compressed_data)
                else:
                    with open(export_file, "wb") as f:
                        pickle.dump(state, f, protocol=pickle.HIGHEST_PROTOCOL)

                logger.info(
                    f"State exported for operation {operation_id} to {export_path} "
                    f"(compressed: {compress})"
                )
                return True

            except Exception as e:
                logger.error(
                    f"Failed to export state for operation {operation_id}: {e}"
                )
                return False

    def import_state(
        self, import_path: str, new_operation_id: Optional[str] = None
    ) -> Optional[str]:
        """Import state from a file for debugging or migration."""
        with self._lock:
            try:
                # Lazy import - only needed for file operations
                import_file = Path(import_path)
                if not import_file.exists():
                    logger.error(f"Import file not found: {import_path}")
                    return None

                with open(import_file, "rb") as f:
                    data = f.read()

                # Try to decompress and load state
                try:
                    state = self._decompress_state(data)
                except Exception:
                    # Fall back to direct pickle loading
                    state = pickle.loads(data)

                # Migrate state if needed
                state = self._migrate_state(state)

                # Assign new operation ID if requested
                if new_operation_id:
                    # Unregister old operation if it exists
                    if state.operation_id in self._active_operations:
                        self.unregister_operation(state.operation_id)
                    state.operation_id = new_operation_id
                else:
                    # Generate new ID to avoid conflicts
                    if state.operation_id in self._active_operations:
                        self.unregister_operation(state.operation_id)
                    state.operation_id = str(uuid.uuid4())

                # Reset status and timestamps for imported state
                state.status = "PAUSED"
                state.last_checkpoint_time = datetime.now()

                self.save_checkpoint(state)

                logger.info(f"State imported with operation ID {state.operation_id}")
                return state.operation_id

            except Exception as e:
                logger.error(f"Failed to import state from {import_path}: {e}")
                return None

    def get_checkpoint_info(self, operation_id: str) -> Optional[Dict[str, Any]]:
        """Get basic information about a checkpoint without loading full state."""
        checkpoint_path = self._get_checkpoint_path(operation_id)

        if not checkpoint_path.exists():
            return None

        try:
            stat = checkpoint_path.stat()
            return {
                "operation_id": operation_id,
                "file_size": stat.st_size,
                "created_time": datetime.fromtimestamp(stat.st_ctime),
                "modified_time": datetime.fromtimestamp(stat.st_mtime),
                "file_path": str(checkpoint_path),
            }
        except Exception as e:
            logger.error(f"Failed to get checkpoint info for {operation_id}: {e}")
            return None

    def create_operation_state(
        self,
        operation_type: str,
        created_by: str = "system",
        region: Optional[str] = None,
        operation_id: Optional[str] = None,
        use_timestamp_id: bool = True,
    ) -> OperationState:
        """Create a new operation state with default values."""
        if not operation_id:
            if use_timestamp_id:
                operation_id = self._get_timestamp_checkpoint_name(operation_type)
            else:
                operation_id = str(uuid.uuid4())

        # Ensure consistent operation management
        self._ensure_operation_consistency(operation_id)

        now = datetime.now()

        return OperationState(
            operation_id=operation_id,
            operation_type=operation_type,
            status="RUNNING",
            start_time=now,
            last_checkpoint_time=now,
            completion_percentage=0.0,
            estimated_completion=None,
            error_message=None,
            created_by=created_by,
            region=region,
            collection_state=CollectionState(start_time=now),
            schema_version=STATE_SCHEMA_VERSION,
        )

    def register_operation(self, operation_id: str) -> None:
        """Explicitly register an operation with StateManager to prevent bypassing."""
        with self._lock:
            self._ensure_operation_consistency(operation_id)
            logger.debug(f"Operation {operation_id} registered with StateManager")

    def unregister_operation(self, operation_id: str) -> None:
        """Unregister an operation when it's completed or cancelled."""
        with self._lock:
            self._active_operations.discard(operation_id)
            logger.debug(f"Operation {operation_id} unregistered from StateManager")

    def get_active_operations(self) -> List[str]:
        """Get list of currently active operations."""
        with self._lock:
            return list(self._active_operations)

    def validate_operation_consistency(self, operation_id: str) -> bool:
        """Check if an operation is being managed consistently through StateManager."""
        with self._lock:
            return operation_id in self._active_operations

    def recover_state(self, operation_id: str) -> Optional[OperationState]:
        """Attempt to recover a corrupted or problematic state."""
        with self._lock:
            checkpoint_path = self._get_checkpoint_path(operation_id)

            # Look for backup files
            backup_patterns = [
                f"{operation_id}.corrupted",
                f"{operation_id}.error_*",
            ]

            for pattern in backup_patterns:
                backup_files = list(self.checkpoint_dir.glob(pattern))
                if backup_files:
                    # Try to recover from the most recent backup
                    backup_file = max(backup_files, key=lambda p: p.stat().st_mtime)
                    logger.info(f"Attempting recovery from {backup_file}")

                    try:
                        with open(backup_file, "rb") as f:
                            data = f.read()

                        # Try different recovery strategies
                        state = None

                        # Strategy 1: Try decompression and migration
                        try:
                            state = self._decompress_state(data)
                            state = self._migrate_state(state)
                        except Exception as e:
                            logger.debug(f"Recovery strategy 1 failed: {e}")

                        # Strategy 2: Try direct pickle loading (legacy format)
                        if state is None:
                            try:
                                state = pickle.loads(data)
                                # Add missing fields for legacy states
                                if not hasattr(state, "schema_version"):
                                    state.schema_version = "1.0.0"
                                state = self._migrate_state(state)
                            except Exception as e:
                                logger.debug(f"Recovery strategy 2 failed: {e}")

                        if state:
                            # Reset status for recovered state
                            state.status = "PAUSED"
                            state.error_message = "Recovered from backup"
                            state.last_checkpoint_time = datetime.now()

                            # Save recovered state
                            self.save_checkpoint(state)
                            logger.info(
                                f"Successfully recovered state for {operation_id}"
                            )
                            return state

                    except Exception as e:
                        logger.warning(f"Failed to recover from {backup_file}: {e}")

            logger.error(f"Could not recover state for operation {operation_id}")
            return None

    def compress_large_states(self, size_threshold_mb: float = 10.0) -> int:
        """Compress large checkpoint files to save disk space."""
        with self._lock:
            compressed_count = 0
            size_threshold_bytes = size_threshold_mb * 1024 * 1024

            try:
                for checkpoint_file in self.checkpoint_dir.glob("*.checkpoint"):
                    if checkpoint_file.stat().st_size > size_threshold_bytes:
                        try:
                            # Load and re-save with compression
                            operation_id = checkpoint_file.stem
                            state = self.load_checkpoint(operation_id)
                            if state:
                                # Force compression on
                                old_compression = self.enable_compression
                                self.enable_compression = True
                                self.save_checkpoint(state)
                                self.enable_compression = old_compression
                                compressed_count += 1
                                logger.debug(
                                    f"Compressed checkpoint: {checkpoint_file.name}"
                                )

                        except Exception as e:
                            logger.warning(
                                f"Failed to compress {checkpoint_file.name}: {e}"
                            )

                logger.info(f"Compressed {compressed_count} large checkpoint files")
                return compressed_count

            except Exception as e:
                logger.error(f"Failed to compress large states: {e}")
                return 0

    def export_state_json(self, operation_id: str, export_path: str) -> bool:
        """Export state to JSON format for debugging and analysis."""
        with self._lock:
            try:
                state = self.load_checkpoint(operation_id)
                if not state:
                    return False

                export_file = Path(export_path)
                export_file.parent.mkdir(parents=True, exist_ok=True)

                # Convert state to dictionary for JSON serialization
                state_dict = asdict(state)

                # Handle datetime objects
                def datetime_handler(obj):
                    if isinstance(obj, datetime):
                        return obj.isoformat()
                    elif isinstance(obj, set):
                        return list(obj)
                    raise TypeError(
                        f"Object of type {type(obj)} is not JSON serializable"
                    )

                with open(export_file, "w") as f:
                    json.dump(state_dict, f, indent=2, default=datetime_handler)

                logger.info(f"State exported to JSON: {export_path}")
                return True

            except Exception as e:
                logger.error(f"Failed to export state to JSON: {e}")
                return False

    def get_state_statistics(self) -> Dict[str, Any]:
        """Get statistics about stored states for monitoring and debugging."""
        with self._lock:
            try:
                stats = {
                    "total_checkpoints": 0,
                    "by_status": {},
                    "by_operation_type": {},
                    "total_size_mb": 0.0,
                    "oldest_checkpoint": None,
                    "newest_checkpoint": None,
                    "active_operations": len(self._active_operations),
                    "corrupted_files": 0,
                }

                checkpoint_files = list(self.checkpoint_dir.glob("*.checkpoint"))
                stats["total_checkpoints"] = len(checkpoint_files)

                # Count corrupted and error files
                stats["corrupted_files"] = len(
                    list(self.checkpoint_dir.glob("*.corrupted"))
                    + list(self.checkpoint_dir.glob("*.error_*"))
                )

                total_size = 0
                oldest_time = None
                newest_time = None

                for checkpoint_file in checkpoint_files:
                    file_stat = checkpoint_file.stat()
                    total_size += file_stat.st_size

                    file_time = datetime.fromtimestamp(file_stat.st_mtime)
                    if oldest_time is None or file_time < oldest_time:
                        oldest_time = file_time
                    if newest_time is None or file_time > newest_time:
                        newest_time = file_time

                    # Try to get state info for statistics
                    try:
                        state = self.load_checkpoint(checkpoint_file.stem)
                        if state:
                            stats["by_status"][state.status] = (
                                stats["by_status"].get(state.status, 0) + 1
                            )
                            stats["by_operation_type"][state.operation_type] = (
                                stats["by_operation_type"].get(state.operation_type, 0)
                                + 1
                            )
                    except Exception:
                        # Skip problematic checkpoints for statistics
                        pass

                stats["total_size_mb"] = total_size / (1024 * 1024)
                stats["oldest_checkpoint"] = (
                    oldest_time.isoformat() if oldest_time else None
                )
                stats["newest_checkpoint"] = (
                    newest_time.isoformat() if newest_time else None
                )

                return stats

            except Exception as e:
                logger.error(f"Failed to get state statistics: {e}")
                return {"error": str(e)}


class StateManagerMixin:
    """Mixin class to ensure consistent StateManager usage in other classes."""

    def __init__(self, *args, state_manager: Optional[StateManager] = None, **kwargs):
        """Initialize with StateManager dependency injection."""
        super().__init__(*args, **kwargs)
        self.state_manager = state_manager or StateManager()
        self._operation_id: Optional[str] = None

    def _ensure_state_manager_usage(self, operation_id: str) -> None:
        """Ensure operation is properly registered with StateManager."""
        if not self.state_manager.validate_operation_consistency(operation_id):
            logger.warning(
                f"Operation {operation_id} not registered with StateManager. "
                "This may indicate bypassing of state management."
            )
            self.state_manager.register_operation(operation_id)

    def _create_operation(self, operation_type: str, **kwargs) -> OperationState:
        """Create a new operation with proper StateManager integration."""
        state = self.state_manager.create_operation_state(operation_type, **kwargs)
        self._operation_id = state.operation_id
        return state

    def _save_state(self, state: OperationState) -> None:
        """Save state with consistency checks."""
        self._ensure_state_manager_usage(state.operation_id)
        self.state_manager.save_checkpoint(state)

    def _load_state(self, operation_id: str) -> Optional[OperationState]:
        """Load state with consistency checks."""
        # When loading a checkpoint from disk, automatically register it
        # (it may not be in _active_operations after process restart)
        state = self.state_manager.load_checkpoint(operation_id)
        if state:
            # Successfully loaded from disk, ensure it's registered
            self.state_manager.register_operation(operation_id)
        return state

    def _cleanup_operation(self, operation_id: str) -> None:
        """Clean up operation when completed or cancelled."""
        self.state_manager.unregister_operation(operation_id)
        if self._operation_id == operation_id:
            self._operation_id = None


def ensure_state_manager_consistency(func):
    """Decorator to ensure StateManager consistency in operation methods."""

    def wrapper(self, *args, **kwargs):
        if hasattr(self, "state_manager") and hasattr(self, "_operation_id"):
            if self._operation_id:
                self._ensure_state_manager_usage(self._operation_id)
        return func(self, *args, **kwargs)

    return wrapper
