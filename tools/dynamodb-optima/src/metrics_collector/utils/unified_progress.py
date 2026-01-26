"""
Unified progress tracking system for DMetrics operations.

Provides consistent progress tracking patterns, real-time updates, ETA calculations,
and performance metrics across all long-running operations.
"""

import os
import sys
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Callable, Dict, List, Optional

import click


class OperationStatus(Enum):
    """Status of an operation."""

    INITIALIZING = "initializing"
    RUNNING = "running"
    PAUSED = "paused"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass
class PerformanceMetrics:
    """Performance metrics for operations."""

    items_per_second: float = 0.0
    bytes_processed: int = 0
    api_calls_made: int = 0
    errors_encountered: int = 0
    retry_count: int = 0
    memory_usage_mb: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "items_per_second": self.items_per_second,
            "bytes_processed": self.bytes_processed,
            "api_calls_made": self.api_calls_made,
            "errors_encountered": self.errors_encountered,
            "retry_count": self.retry_count,
            "memory_usage_mb": self.memory_usage_mb,
        }


@dataclass
class ProgressState:
    """Complete state of a progress tracking operation."""

    operation_id: str
    operation_type: str
    status: OperationStatus
    current: int = 0
    total: int = 0
    start_time: datetime = field(default_factory=datetime.now)
    last_update: datetime = field(default_factory=datetime.now)
    estimated_completion: Optional[datetime] = None
    current_item: str = ""
    stage: str = ""
    error_message: str = ""
    performance_metrics: PerformanceMetrics = field(default_factory=PerformanceMetrics)

    @property
    def completion_percentage(self) -> float:
        """Calculate completion percentage."""
        if self.total <= 0:
            return 0.0
        return min((self.current / self.total) * 100, 100.0)

    @property
    def elapsed_time(self) -> timedelta:
        """Calculate elapsed time."""
        return datetime.now() - self.start_time

    @property
    def eta(self) -> Optional[timedelta]:
        """Calculate estimated time to completion."""
        if self.current <= 0 or self.total <= 0 or self.current >= self.total:
            return None

        elapsed = self.elapsed_time.total_seconds()
        if elapsed <= 0:
            return None

        rate = self.current / elapsed
        if rate <= 0:
            return None

        remaining_items = self.total - self.current
        eta_seconds = remaining_items / rate
        return timedelta(seconds=int(eta_seconds))


class UnifiedProgressTracker:
    """
    Unified progress tracker with real-time updates, performance metrics,
    and consistent patterns across all operations.
    """

    def __init__(
        self,
        operation_id: str,
        operation_type: str,
        total: int,
        description: str = "Processing",
        show_performance: bool = True,
        update_callback: Optional[Callable[[ProgressState], None]] = None,
        terminal_width: Optional[int] = None,
    ):
        """
        Initialize unified progress tracker.

        Args:
            operation_id: Unique identifier for the operation
            operation_type: Type of operation (DISCOVERY, COLLECTION, etc.)
            total: Total number of items to process
            description: Description of the operation
            show_performance: Whether to show performance metrics
            update_callback: Optional callback for progress updates
            terminal_width: Terminal width (auto-detected if None)
        """
        self.state = ProgressState(
            operation_id=operation_id,
            operation_type=operation_type,
            status=OperationStatus.INITIALIZING,
            total=total,
        )
        self.description = description
        self.show_performance = show_performance
        self.update_callback = update_callback
        self.terminal_width = terminal_width or self._get_terminal_width()

        # Rendering control
        self._render_lock = threading.RLock()
        self._last_render_time = 0
        self._render_interval = 0.1  # Minimum time between renders

        # Performance tracking
        self._performance_start_time = time.time()
        self._last_performance_update = self._performance_start_time

        # Real-time update thread
        self._update_thread = None
        self._stop_updates = threading.Event()
        self._start_real_time_updates()

    def _get_terminal_width(self) -> int:
        """Get terminal width with fallback."""
        try:
            return os.get_terminal_size().columns
        except (OSError, AttributeError):
            return 120  # Fallback width

    def _start_real_time_updates(self):
        """Start real-time update thread for live progress display."""

        def update_worker():
            while not self._stop_updates.is_set():
                if self.state.status == OperationStatus.RUNNING:
                    self._update_performance_metrics()
                    self._render()
                time.sleep(0.5)  # Update every 500ms

        self._update_thread = threading.Thread(target=update_worker, daemon=True)
        self._update_thread.start()

    def _update_performance_metrics(self):
        """Update performance metrics."""
        now = time.time()
        elapsed = now - self._performance_start_time

        if elapsed > 0 and self.state.current > 0:
            self.state.performance_metrics.items_per_second = (
                self.state.current / elapsed
            )

        # Update memory usage if available
        try:
            # Lazy import - only needed conditionally
            import psutil

            process = psutil.Process()
            self.state.performance_metrics.memory_usage_mb = (
                process.memory_info().rss / 1024 / 1024
            )
        except (ImportError, Exception):
            pass  # psutil not available or other error

    def start(self):
        """Start the operation."""
        self.state.status = OperationStatus.RUNNING
        self.state.start_time = datetime.now()
        self._performance_start_time = time.time()
        self._render()

        if self.update_callback:
            self.update_callback(self.state)

    def update(
        self,
        increment: int = 1,
        current_item: str = "",
        stage: str = "",
        api_calls: int = 0,
        bytes_processed: int = 0,
        errors: int = 0,
    ):
        """
        Update progress with optional performance metrics.

        Args:
            increment: Number of items completed
            current_item: Current item being processed
            stage: Current stage of operation
            api_calls: Number of API calls made
            bytes_processed: Bytes processed
            errors: Number of errors encountered
        """
        with self._render_lock:
            self.state.current = min(self.state.current + increment, self.state.total)
            self.state.last_update = datetime.now()

            if current_item:
                self.state.current_item = current_item
            if stage:
                self.state.stage = stage

            # Update performance metrics
            if api_calls > 0:
                self.state.performance_metrics.api_calls_made += api_calls
            if bytes_processed > 0:
                self.state.performance_metrics.bytes_processed += bytes_processed
            if errors > 0:
                self.state.performance_metrics.errors_encountered += errors

            # Update ETA
            eta = self.state.eta
            if eta:
                self.state.estimated_completion = datetime.now() + eta

            # Render if enough time has passed
            now = time.time()
            if now - self._last_render_time >= self._render_interval:
                self._render()
                self._last_render_time = now

            if self.update_callback:
                self.update_callback(self.state)

    def set_progress(self, current: int, current_item: str = "", stage: str = ""):
        """Set absolute progress value."""
        with self._render_lock:
            self.state.current = min(max(current, 0), self.state.total)
            self.state.last_update = datetime.now()

            if current_item:
                self.state.current_item = current_item
            if stage:
                self.state.stage = stage

            self._render()

            if self.update_callback:
                self.update_callback(self.state)

    def pause(self, reason: str = ""):
        """Pause the operation."""
        self.state.status = OperationStatus.PAUSED
        if reason:
            self.state.error_message = reason
        self._render()

        if self.update_callback:
            self.update_callback(self.state)

    def resume(self):
        """Resume the operation."""
        self.state.status = OperationStatus.RUNNING
        self.state.error_message = ""
        self._render()

        if self.update_callback:
            self.update_callback(self.state)

    def complete(self, final_message: str = ""):
        """Mark operation as complete."""
        self.state.status = OperationStatus.COMPLETED
        self.state.current = self.state.total
        self.state.last_update = datetime.now()

        if final_message:
            self.state.current_item = final_message

        self._stop_updates.set()
        self._render_final()

        if self.update_callback:
            self.update_callback(self.state)

    def fail(self, error_message: str):
        """Mark operation as failed."""
        self.state.status = OperationStatus.FAILED
        self.state.error_message = error_message
        self.state.last_update = datetime.now()

        self._stop_updates.set()
        self._render_final()

        if self.update_callback:
            self.update_callback(self.state)

    def cancel(self, reason: str = ""):
        """Cancel the operation."""
        self.state.status = OperationStatus.CANCELLED
        if reason:
            self.state.error_message = reason
        self.state.last_update = datetime.now()

        self._stop_updates.set()
        self._render_final()

        if self.update_callback:
            self.update_callback(self.state)

    def _render(self):
        """Render progress bar to terminal."""
        if self.state.total <= 0:
            return

        # Build progress bar
        bar_width = min(40, self.terminal_width // 3)
        filled_width = int((self.state.current / self.state.total) * bar_width)
        bar = "█" * filled_width + "░" * (bar_width - filled_width)

        # Build status line components
        components = []

        # Status indicator
        status_icons = {
            OperationStatus.INITIALIZING: "🔄",
            OperationStatus.RUNNING: "⚡",
            OperationStatus.PAUSED: "⏸️",
            OperationStatus.COMPLETED: "✅",
            OperationStatus.FAILED: "❌",
            OperationStatus.CANCELLED: "🛑",
        }
        icon = status_icons.get(self.state.status, "🔄")

        # Main description with stage
        desc_parts = [self.description]
        if self.state.stage:
            desc_parts.append(f"({self.state.stage})")
        components.append(f"{icon} {' '.join(desc_parts)}")

        # Progress info
        components.append(f"[{bar}]")
        components.append(f"{self.state.completion_percentage:5.1f}%")
        components.append(f"({self.state.current:,}/{self.state.total:,})")

        # ETA
        if self.state.eta and self.state.status == OperationStatus.RUNNING:
            eta_str = self._format_duration(self.state.eta.total_seconds())
            components.append(f"ETA: {eta_str}")

        # Performance metrics
        if (
            self.show_performance
            and self.state.performance_metrics.items_per_second > 0
        ):
            rate = self.state.performance_metrics.items_per_second
            if rate >= 1:
                components.append(f"{rate:.1f}/s")
            else:
                components.append(f"{rate:.2f}/s")

        # Current item (truncated if needed)
        if self.state.current_item:
            available_width = self.terminal_width - sum(len(c) for c in components) - 10
            if available_width > 20:
                item_display = self.state.current_item
                if len(item_display) > available_width:
                    item_display = item_display[: available_width - 3] + "..."
                components.append(f"- {item_display}")

        # Build final line
        status_line = " ".join(components)

        # Truncate and pad to clear previous content
        if len(status_line) > self.terminal_width - 5:
            status_line = status_line[: self.terminal_width - 8] + "..."

        padded_line = status_line.ljust(self.terminal_width - 1)

        # Write to terminal
        print(f"\r{padded_line}", end="", flush=True)

    def _render_final(self):
        """Render final status message."""
        elapsed = self.state.elapsed_time
        elapsed_str = self._format_duration(elapsed.total_seconds())

        # Status-specific messages
        if self.state.status == OperationStatus.COMPLETED:
            icon = "✅"
            status_msg = "completed"
        elif self.state.status == OperationStatus.FAILED:
            icon = "❌"
            status_msg = f"failed: {self.state.error_message}"
        elif self.state.status == OperationStatus.CANCELLED:
            icon = "🛑"
            status_msg = f"cancelled: {self.state.error_message}"
        else:
            icon = "⏸️"
            status_msg = "paused"

        # Performance summary
        perf_parts = []
        if self.show_performance:
            metrics = self.state.performance_metrics
            if metrics.items_per_second > 0:
                perf_parts.append(f"{metrics.items_per_second:.1f} items/s")
            if metrics.api_calls_made > 0:
                perf_parts.append(f"{metrics.api_calls_made:,} API calls")
            if metrics.errors_encountered > 0:
                perf_parts.append(f"{metrics.errors_encountered} errors")

        # Build final message
        final_parts = [
            f"{icon} {self.description} {status_msg}",
            f"({self.state.current:,}/{self.state.total:,} items, {elapsed_str})",
        ]

        if perf_parts:
            final_parts.append(f"[{', '.join(perf_parts)}]")

        final_message = " ".join(final_parts)

        # Clear line and print final message
        clear_line = " " * (self.terminal_width - 1)
        print(f"\r{clear_line}\r{final_message}")
        sys.stdout.flush()

    def _format_duration(self, seconds: float) -> str:
        """Format duration in seconds to human-readable string."""
        if seconds < 60:
            return f"{int(seconds)}s"
        elif seconds < 3600:
            minutes = int(seconds // 60)
            secs = int(seconds % 60)
            return f"{minutes}m {secs}s"
        else:
            hours = int(seconds // 3600)
            minutes = int((seconds % 3600) // 60)
            return f"{hours}h {minutes}m"

    def get_status_summary(self) -> Dict[str, Any]:
        """Get comprehensive status summary."""
        return {
            "operation_id": self.state.operation_id,
            "operation_type": self.state.operation_type,
            "status": self.state.status.value,
            "progress": {
                "current": self.state.current,
                "total": self.state.total,
                "percentage": self.state.completion_percentage,
                "current_item": self.state.current_item,
                "stage": self.state.stage,
            },
            "timing": {
                "start_time": self.state.start_time.isoformat(),
                "last_update": self.state.last_update.isoformat(),
                "elapsed_seconds": self.state.elapsed_time.total_seconds(),
                "estimated_completion": (
                    self.state.estimated_completion.isoformat()
                    if self.state.estimated_completion
                    else None
                ),
                "eta_seconds": (
                    self.state.eta.total_seconds() if self.state.eta else None
                ),
            },
            "performance": self.state.performance_metrics.to_dict(),
            "error_message": self.state.error_message,
        }

    def __enter__(self):
        """Context manager entry."""
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        if exc_type is not None:
            self.fail(f"{exc_type.__name__}: {exc_val}")
        else:
            self.complete()

        # Clean up update thread
        self._stop_updates.set()
        if self._update_thread and self._update_thread.is_alive():
            self._update_thread.join(timeout=1.0)


class MultiStageProgressTracker:
    """
    Multi-stage progress tracker for complex operations with multiple phases.
    """

    def __init__(
        self,
        operation_id: str,
        operation_type: str,
        stages: List[tuple[str, int]],
        description: str = "Processing",
        update_callback: Optional[Callable[[ProgressState], None]] = None,
    ):
        """
        Initialize multi-stage progress tracker.

        Args:
            operation_id: Unique identifier for the operation
            operation_type: Type of operation
            stages: List of (stage_name, stage_total) tuples
            description: Overall operation description
            update_callback: Optional callback for progress updates
        """
        self.stages = stages
        self.current_stage_index = 0
        self.stage_progress = 0

        total_items = sum(stage[1] for stage in stages)

        self.tracker = UnifiedProgressTracker(
            operation_id=operation_id,
            operation_type=operation_type,
            total=total_items,
            description=description,
            update_callback=update_callback,
        )

        # Set initial stage
        if self.stages:
            self.tracker.state.stage = (
                f"Stage 1/{len(self.stages)}: {self.stages[0][0]}"
            )

    def start(self):
        """Start the multi-stage operation."""
        self.tracker.start()

    def update_stage(self, increment: int = 1, current_item: str = "", **kwargs):
        """Update progress within current stage."""
        if self.current_stage_index >= len(self.stages):
            return

        stage_name, stage_total = self.stages[self.current_stage_index]
        self.stage_progress = min(self.stage_progress + increment, stage_total)

        # Update overall progress
        completed_items = sum(
            self.stages[i][1] for i in range(self.current_stage_index)
        )
        completed_items += self.stage_progress

        # Update stage info
        stage_info = (
            f"Stage {self.current_stage_index + 1}/{len(self.stages)}: {stage_name}"
        )

        self.tracker.set_progress(
            current=completed_items, current_item=current_item, stage=stage_info
        )

        # Check if stage is complete
        if (
            self.stage_progress >= stage_total
            and self.current_stage_index < len(self.stages) - 1
        ):
            self.next_stage()

    def next_stage(self):
        """Move to the next stage."""
        if self.current_stage_index < len(self.stages) - 1:
            # Complete current stage first
            stage_name, stage_total = self.stages[self.current_stage_index]
            remaining = stage_total - self.stage_progress

            # Update overall progress to complete current stage
            completed_items = sum(
                self.stages[i][1] for i in range(self.current_stage_index)
            )
            completed_items += stage_total  # Complete current stage

            self.current_stage_index += 1
            self.stage_progress = 0

            stage_name, _ = self.stages[self.current_stage_index]
            stage_info = (
                f"Stage {self.current_stage_index + 1}/{len(self.stages)}: {stage_name}"
            )

            self.tracker.set_progress(current=completed_items, stage=stage_info)

    def complete(self, final_message: str = ""):
        """Complete all stages."""
        self.tracker.complete(final_message)

    def fail(self, error_message: str):
        """Fail the operation."""
        self.tracker.fail(error_message)

    def cancel(self, reason: str = ""):
        """Cancel the operation."""
        self.tracker.cancel(reason)

    def get_status_summary(self) -> Dict[str, Any]:
        """Get comprehensive status summary."""
        summary = self.tracker.get_status_summary()
        summary["stages"] = {
            "current_stage": (
                self.current_stage_index + 1
                if self.current_stage_index < len(self.stages)
                else len(self.stages)
            ),
            "total_stages": len(self.stages),
            "stage_progress": self.stage_progress,
            "stage_total": (
                self.stages[self.current_stage_index][1]
                if self.current_stage_index < len(self.stages)
                else 0
            ),
            "stages_list": [
                {"name": name, "total": total} for name, total in self.stages
            ],
        }
        return summary

    def __enter__(self):
        """Context manager entry."""
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        if exc_type is not None:
            self.fail(f"{exc_type.__name__}: {exc_val}")
        else:
            self.complete()
