"""
Simple, bulletproof progress bar implementation with dynamic elements.
No dependencies, no complex logic, just works.
"""

import sys
import threading
import time
from typing import Optional


class SimpleProgress:
    """Ultra-simple progress bar with dynamic elements for long waits."""

    def __init__(self, total: int, description: str = "Progress"):
        self.total = total
        self.current = 0
        self.description = description
        self.start_time = time.time()
        self.last_render = 0
        self._spinner_active = False
        self._spinner_thread = None
        self._spinner_stop = threading.Event()

    def update(self, current: int, status: Optional[str] = None):
        """Update progress - only renders if enough time has passed."""
        self.current = current

        # Stop any active spinner when progress updates
        self._stop_spinner()

        # Only update every 0.1 seconds to avoid spam
        now = time.time()
        if now - self.last_render < 0.1 and current < self.total:
            return

        self.last_render = now
        self._render(status)

    def update_with_spinner_fallback(
        self, current: int, status: Optional[str] = None, spinner_delay: float = 3.0
    ):
        """Update progress, but show spinner if next update takes too long."""
        self.update(current, status)

        # Start a timer to show spinner if next update is delayed
        def delayed_spinner():
            time.sleep(spinner_delay)
            if time.time() - self.last_render > spinner_delay:
                # If we haven't had an update in spinner_delay seconds, show spinner
                if not self._spinner_active and self.current < self.total:
                    self.show_spinner(
                        f"Processing batch {self.current + 1}/{self.total}"
                    )

        # Start timer in background thread
        import threading

        timer_thread = threading.Thread(target=delayed_spinner, daemon=True)
        timer_thread.start()

    def show_spinner(self, message: str = "Processing..."):
        """Show a spinner for long unmeasurable operations."""
        if self._spinner_active:
            return

        self._spinner_active = True
        self._spinner_stop.clear()

        def spinner_worker():
            spinner_chars = ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"]
            i = 0
            start_time = time.time()

            while not self._spinner_stop.is_set():
                elapsed = time.time() - start_time
                if elapsed > 60:
                    duration_str = f"{int(elapsed // 60)}m {int(elapsed % 60)}s"
                else:
                    duration_str = f"{elapsed:.0f}s"

                spinner = spinner_chars[i % len(spinner_chars)]
                line = f"{spinner} {message} ({duration_str})"
                print(f"\r{line}", end="", flush=True)

                i += 1
                time.sleep(0.1)

        self._spinner_thread = threading.Thread(target=spinner_worker, daemon=True)
        self._spinner_thread.start()

    def _stop_spinner(self):
        """Stop the spinner if it's running."""
        if self._spinner_active:
            self._spinner_stop.set()
            if self._spinner_thread and self._spinner_thread.is_alive():
                self._spinner_thread.join(timeout=0.2)
            self._spinner_active = False
            # Clear the spinner line
            print("\r", end="", flush=True)

    def _render(self, status: Optional[str] = None):
        """Render the progress bar."""
        if self.total == 0:
            return

        # Calculate percentage
        pct = (self.current / self.total) * 100

        # Create progress bar (40 chars wide)
        filled = int((self.current / self.total) * 40)
        bar = "█" * filled + "░" * (40 - filled)

        # Calculate ETA
        elapsed = time.time() - self.start_time
        if self.current > 0:
            eta_seconds = (elapsed / self.current) * (self.total - self.current)
            if eta_seconds > 60:
                eta = f"{int(eta_seconds // 60)}m {int(eta_seconds % 60)}s"
            else:
                eta = f"{int(eta_seconds)}s"
        else:
            eta = "calculating..."

        # Build the line
        if status:
            line = (
                f"{status} ({self.current}/{self.total}) [{bar}] {pct:5.1f}% ETA: {eta}"
            )
        else:
            line = (
                f"{self.description} ({self.current}/{self.total}) "
                f"[{bar}] {pct:5.1f}% ETA: {eta}"
            )

        # Use print for consistent output
        print(f"\r{line}", end="", flush=True)

    def finish(self, message: Optional[str] = None):
        """Finish the progress bar."""
        # Stop any active spinner
        self._stop_spinner()

        self.current = self.total
        elapsed = time.time() - self.start_time

        if elapsed > 60:
            duration = f"{int(elapsed // 60)}m {int(elapsed % 60)}s"
        else:
            duration = f"{elapsed:.1f}s"

        if message:
            final_msg = f"{message} ({duration})"
        else:
            final_msg = f"{self.description} completed ({duration})"

        # Clear the line and print final message
        print(f"\r{final_msg}")


async def run_with_status_display(
    message: str, completion_message: str, async_operation
):
    """
    Run an async operation with a status display that works reliably.

    Based on the successful pattern used by AWS credentials validation and resource loading.
    Uses a robust try/finally approach to ensure the spinner always stops.

    Args:
        message: The message to show during the operation
        completion_message: The message to show when completed
        async_operation: The async function/coroutine to execute

    Returns:
        The result of the async operation
    """
    status_display = DynamicStatusDisplay()

    try:
        status_display.start(message)
        result = await async_operation
    finally:
        # Ensure the spinner stops no matter what happens
        status_display.stop()

    print(completion_message)
    return result


class DynamicStatusDisplay:
    """Simple, reliable status display that works with async operations."""

    def __init__(self, min_display_time: float = 0.5):
        self._active = False
        self._thread = None
        self._stop_event = threading.Event()
        self._current_message = ""
        self._min_display_time = min_display_time
        self._start_time = None

    def start(self, message: str):
        """Start showing dynamic status."""
        if self._active:
            self.stop()

        self._current_message = message
        self._active = True
        self._stop_event.clear()
        self._start_time = time.time()

        def status_worker():
            # Simple spinner characters
            spinners = ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"]
            spinner_idx = 0
            start_time = time.time()

            while not self._stop_event.is_set():
                elapsed = time.time() - start_time
                if elapsed > 60:
                    duration_str = f" ({int(elapsed // 60)}m {int(elapsed % 60)}s)"
                else:
                    duration_str = f" ({elapsed:.0f}s)"

                spinner_char = spinners[spinner_idx % len(spinners)]
                line = f"{spinner_char} {self._current_message}{duration_str}"

                # Clear the entire line and write new content using ANSI escape
                print(f"\033[2K\r{line}", end="", flush=True)

                spinner_idx += 1
                time.sleep(0.2)

        self._thread = threading.Thread(target=status_worker, daemon=True)
        self._thread.start()

    def update_message(self, message: str):
        """Update the status message."""
        self._current_message = message

    def stop(self):
        """Stop the dynamic status display."""
        if self._active:
            # Ensure minimum display time for better UX
            if self._start_time:
                elapsed = time.time() - self._start_time
                if elapsed < self._min_display_time:
                    time.sleep(self._min_display_time - elapsed)

            self._stop_event.set()
            if self._thread and self._thread.is_alive():
                self._thread.join(timeout=0.5)
            self._active = False

            # More aggressive line clearing to handle interference
            # Use ANSI escape sequences for better control
            print(
                "\033[2K\r", end="", flush=True
            )  # Clear entire line and return to start
