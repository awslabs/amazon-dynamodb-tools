"""
Progress tracking utilities for long-running operations.

Provides progress bars, ETA calculations, and status displays for CLI
operations.
"""

import os
import sys
import time
from datetime import datetime, timedelta
from typing import Optional

import click

# No need for logging context manager anymore since console logging is disabled


def get_terminal_width() -> int:
    """Get the current terminal width, with a fallback."""
    try:
        return os.get_terminal_size().columns
    except (OSError, AttributeError):
        return 80  # Fallback to 80 columns


def truncate_line(line: str, max_width: int, padding: int = 20) -> str:
    """Truncate a line to fit within terminal width, accounting for padding."""
    # Ensure we have enough space to clear the line
    effective_width = max_width - 5  # Leave some margin for safety

    if len(line) <= effective_width:
        # Pad to effective width to ensure previous content is cleared
        return line + " " * (effective_width - len(line))

    # Calculate how much we need to truncate
    available_width = effective_width - 3  # 3 for "..."
    if available_width <= 0:
        return line[:effective_width]

    # Truncate and add ellipsis, then pad to effective width
    truncated = line[:available_width] + "..."
    return truncated + " " * (effective_width - len(truncated))


class ActivityIndicator:
    """
    Activity indicator for operations with unknown total (like discovery).

    Shows a spinner and real-time status updates without requiring a known total.
    """

    def __init__(self, description: str = "Processing"):
        """Initialize activity indicator."""
        self.description = description
        self.start_time = time.time()
        self.last_update = self.start_time
        self.spinner_chars = ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"]
        self.spinner_index = 0
        self.status = ""

    def update(self, status: str = "") -> None:
        """Update the activity indicator with new status."""
        self.status = status
        self.last_update = time.time()
        self._render()

    def _render(self) -> None:
        """Render the activity indicator."""
        # Update spinner
        spinner = self.spinner_chars[self.spinner_index]
        self.spinner_index = (self.spinner_index + 1) % len(self.spinner_chars)

        # Calculate elapsed time
        elapsed = time.time() - self.start_time
        elapsed_str = format_duration(elapsed)

        # Build status line
        parts = [f"{spinner} {self.description}"]
        if self.status:
            parts.append(f"- {self.status}")
        parts.append(f"({elapsed_str})")

        # Write to terminal with padding to clear previous content
        status_line = " ".join(parts)
        # Truncate line to fit terminal width and add padding
        terminal_width = get_terminal_width()
        truncated_line = truncate_line(status_line, terminal_width)
        print(f"\r{truncated_line}", end="", flush=True)

    def finish(self, final_status: str = "Complete") -> None:
        """Finish the activity indicator."""
        elapsed = time.time() - self.start_time
        elapsed_str = format_duration(elapsed)

        # Clear the current line and write final status with checkmark
        final_line = f"✅ {self.description} - {final_status} ({elapsed_str})"
        # Truncate line to fit terminal width and add padding, then newline
        terminal_width = get_terminal_width()
        truncated_line = truncate_line(final_line, terminal_width)
        print(f"\r{truncated_line}")
        sys.stdout.flush()


class ProgressTracker:
    """
    Enhanced progress tracker with visual progress bars and ETA calculations.

    Provides both programmatic progress tracking and visual CLI output.
    """

    def __init__(
        self,
        total: int,
        description: str = "Processing",
        show_eta: bool = True,
        show_percentage: bool = True,
        show_count: bool = True,
        width: int = 40,
    ):
        """
        Initialize progress tracker.

        Args:
            total: Total number of items to process
            description: Description of the operation
            show_eta: Whether to show estimated time remaining
            show_percentage: Whether to show percentage completion
            show_count: Whether to show current/total count
            width: Width of the progress bar in characters
        """
        self.total = total
        self.description = description
        self.show_eta = show_eta
        self.show_percentage = show_percentage
        self.show_count = show_count
        self.width = width

        self.current = 0
        self.start_time = time.time()
        self.last_update = self.start_time
        self.current_item = ""  # Additional info to show at the end
        self._render_lock = False  # Simple lock to prevent concurrent renders

    def update(
        self,
        increment: int = 1,
        description: Optional[str] = None,
        current_item: Optional[str] = None,
    ) -> None:
        """
        Update progress by the specified increment.

        Args:
            increment: Number of items completed
            description: Optional updated description
            current_item: Optional current item info to show at the end
        """
        self.current = min(self.current + increment, self.total)
        self.last_update = time.time()

        if description:
            self.description = description

        if current_item:
            self.current_item = current_item

        self._render()

    def set_progress(
        self,
        current: int,
        description: Optional[str] = None,
        current_item: Optional[str] = None,
    ) -> None:
        """
        Set absolute progress value.

        Args:
            current: Current number of completed items
            description: Optional updated description
            current_item: Optional current item info to show at the end
        """
        self.current = min(max(current, 0), self.total)
        self.last_update = time.time()

        if description:
            self.description = description

        if current_item:
            self.current_item = current_item

        self._render()

    def finish(self, description: Optional[str] = None) -> None:
        """Mark progress as complete."""
        self.current = self.total
        if description:
            self.description = description
        self._render()

    def _render(self) -> None:
        """Render the progress bar to the terminal."""
        if self.total == 0:
            return

        # Prevent concurrent renders (simple lock)
        if self._render_lock:
            return
        self._render_lock = True

        try:
            # Calculate percentage
            percentage = (self.current / self.total) * 100

            # Calculate ETA
            elapsed = time.time() - self.start_time
            if self.current > 0 and elapsed > 0:
                rate = self.current / elapsed
                remaining = self.total - self.current
                eta_seconds = remaining / rate if rate > 0 else 0
                eta = timedelta(seconds=int(eta_seconds))
            else:
                eta = None

            # Build progress bar
            filled_width = int((self.current / self.total) * self.width)
            bar = "█" * filled_width + "░" * (self.width - filled_width)

            # Build status line
            parts = [f"{self.description}"]

            if self.show_count:
                parts.append(f"({self.current}/{self.total})")

            parts.append(f"[{bar}]")

            if self.show_percentage:
                parts.append(f"{percentage:5.1f}%")

            if self.show_eta and eta is not None and self.current < self.total:
                if eta.total_seconds() < 60:
                    eta_str = f"{int(eta.total_seconds())}s"
                elif eta.total_seconds() < 3600:
                    minutes = int(eta.total_seconds() // 60)
                    seconds = int(eta.total_seconds() % 60)
                    eta_str = f"{minutes}m {seconds}s"
                else:
                    hours = int(eta.total_seconds() // 3600)
                    minutes = int((eta.total_seconds() % 3600) // 60)
                    eta_str = f"{hours}h {minutes}m"
                parts.append(f"ETA: {eta_str}")

            # Add current item info at the end if available
            if self.current_item:
                parts.append(f"- {self.current_item}")

            # Write to terminal - use carriage return to overwrite current line
            status_line = " ".join(parts)

            # Ensure we clear the previous line by padding with spaces
            terminal_width = get_terminal_width()
            if len(status_line) > terminal_width - 10:
                status_line = status_line[: terminal_width - 13] + "..."

            # Pad with spaces to clear previous content
            padded_line = status_line.ljust(terminal_width - 1)

            # Use print with end='' and flush=True for atomic output
            print(f"\r{padded_line}", end="", flush=True)

        finally:
            self._render_lock = False


class MultiStageProgress:
    """
    Progress tracker for multi-stage operations.

    Tracks progress across multiple stages with overall completion
    percentage.
    """

    def __init__(self, stages: list[tuple[str, int]], description: str = "Processing"):
        """
        Initialize multi-stage progress tracker.

        Args:
            stages: List of (stage_name, stage_total) tuples
            description: Overall operation description
        """
        self.stages = stages
        self.description = description
        self.current_stage = 0
        self.stage_progress = 0

        self.total_items = sum(stage[1] for stage in stages)
        self.completed_items = 0

        self.start_time = time.time()

    def update_stage(
        self, increment: int = 1, description: Optional[str] = None
    ) -> None:
        """Update progress within the current stage."""
        if self.current_stage >= len(self.stages):
            return

        stage_name, stage_total = self.stages[self.current_stage]
        self.stage_progress = min(self.stage_progress + increment, stage_total)
        self.completed_items += increment

        # Check if stage is complete
        if (
            self.stage_progress >= stage_total
            and self.current_stage < len(self.stages) - 1
        ):
            self.current_stage += 1
            self.stage_progress = 0

        self._render(description)

    def next_stage(self, description: Optional[str] = None) -> None:
        """Move to the next stage."""
        if self.current_stage < len(self.stages):
            # Complete current stage
            stage_name, stage_total = self.stages[self.current_stage]
            remaining = stage_total - self.stage_progress
            self.completed_items += remaining

            self.current_stage += 1
            self.stage_progress = 0

        self._render(description)

    def finish(self, description: Optional[str] = None) -> None:
        """Mark all stages as complete."""
        self.current_stage = len(self.stages)
        self.completed_items = self.total_items
        self._render(description)

    def _render(self, description: Optional[str] = None) -> None:
        """Render the multi-stage progress bar."""
        if self.current_stage >= len(self.stages):
            # All stages complete
            overall_percentage = 100.0
            stage_info = "Complete"
        else:
            # Calculate overall percentage
            overall_percentage = (self.completed_items / self.total_items) * 100

            # Current stage info
            stage_name, stage_total = self.stages[self.current_stage]
            stage_percentage = (
                (self.stage_progress / stage_total) * 100 if stage_total > 0 else 0
            )
            stage_info = (
                f"Stage {self.current_stage + 1}/{len(self.stages)}: "
                f"{stage_name} ({stage_percentage:.1f}%)"
            )

        # Calculate ETA
        elapsed = time.time() - self.start_time
        if self.completed_items > 0 and elapsed > 0:
            rate = self.completed_items / elapsed
            remaining = self.total_items - self.completed_items
            eta_seconds = remaining / rate if rate > 0 else 0
            eta = timedelta(seconds=int(eta_seconds))
        else:
            eta = None

        # Build status line
        parts = [f"{description or self.description}"]
        parts.append(f"- {stage_info}")
        parts.append(f"- Overall: {overall_percentage:.1f}%")

        if eta is not None and self.completed_items < self.total_items:
            if eta.total_seconds() < 60:
                eta_str = f"{int(eta.total_seconds())}s"
            elif eta.total_seconds() < 3600:
                minutes = int(eta.total_seconds() // 60)
                seconds = int(eta.total_seconds() % 60)
                eta_str = f"{minutes}m {seconds}s"
            else:
                hours = int(eta.total_seconds() // 3600)
                minutes = int((eta.total_seconds() % 3600) // 60)
                eta_str = f"{hours}h {minutes}m"
            parts.append(f"- ETA: {eta_str}")

        # Write to terminal with padding to clear previous content
        status_line = " ".join(parts)
        # Truncate line to fit terminal width and add padding
        terminal_width = get_terminal_width()
        truncated_line = truncate_line(status_line, terminal_width)
        print(f"\r{truncated_line}", end="", flush=True)


def format_duration(seconds: float) -> str:
    """Format duration in seconds to human-readable string."""
    if seconds < 60:
        return f"{seconds:.1f}s"
    elif seconds < 3600:
        minutes = int(seconds // 60)
        secs = int(seconds % 60)
        return f"{minutes}m {secs}s"
    else:
        hours = int(seconds // 3600)
        minutes = int((seconds % 3600) // 60)
        return f"{hours}h {minutes}m"


def format_eta(start_time: datetime, current: int, total: int) -> Optional[str]:
    """Calculate and format ETA based on current progress."""
    if current <= 0 or total <= 0:
        return None

    elapsed = (datetime.now() - start_time).total_seconds()
    if elapsed <= 0:
        return None

    rate = current / elapsed
    remaining = total - current

    if rate <= 0:
        return None

    eta_seconds = remaining / rate
    return format_duration(eta_seconds)
