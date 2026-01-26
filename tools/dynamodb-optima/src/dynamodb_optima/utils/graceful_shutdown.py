"""
Enhanced graceful shutdown handling for DMetrics operations.

Provides comprehensive shutdown management with state preservation,
cleanup procedures, and user guidance.
"""

import logging
import signal
import sys
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional

import click


@dataclass
class ShutdownHandler:
    """Configuration for shutdown handling."""

    operation_id: str
    operation_type: str
    cleanup_callback: Optional[Callable[[], None]] = None
    save_state_callback: Optional[Callable[[], bool]] = None
    timeout_seconds: int = 30
    priority: int = 1  # 1 = high priority, 2 = medium, 3 = low

    def __post_init__(self):
        self.registered_at = datetime.now()


class GracefulShutdownManager:
    """
    Enhanced graceful shutdown manager with comprehensive state preservation
    and cleanup procedures.
    """

    def __init__(self, logger: Optional[logging.Logger] = None):
        """Initialize graceful shutdown manager."""
        self.logger = logger or logging.getLogger(__name__)
        self._handlers: Dict[str, ShutdownHandler] = {}
        self._shutdown_initiated = False
        self._shutdown_lock = threading.RLock()
        self._original_handlers = {}

        # Register signal handlers
        self._register_signal_handlers()

    def _register_signal_handlers(self):
        """Register system signal handlers."""
        # Store original handlers for restoration
        self._original_handlers["SIGINT"] = signal.signal(
            signal.SIGINT, self._signal_handler
        )

        if hasattr(signal, "SIGTERM"):
            self._original_handlers["SIGTERM"] = signal.signal(
                signal.SIGTERM, self._signal_handler
            )

        if hasattr(signal, "SIGHUP"):
            self._original_handlers["SIGHUP"] = signal.signal(
                signal.SIGHUP, self._signal_handler
            )

    def _signal_handler(self, signum: int, frame):
        """Handle shutdown signals."""
        signal_names = {
            signal.SIGINT: "SIGINT (Ctrl+C)",
            signal.SIGTERM: "SIGTERM",
        }

        if hasattr(signal, "SIGHUP"):
            signal_names[signal.SIGHUP] = "SIGHUP"

        signal_name = signal_names.get(signum, f"Signal {signum}")

        self.logger.info(f"Received {signal_name}, initiating graceful shutdown...")
        click.echo(f"\n🛑 Shutdown signal received ({signal_name})")

        self.initiate_shutdown()

    def register_operation(
        self,
        operation_id: str,
        operation_type: str,
        cleanup_callback: Optional[Callable[[], None]] = None,
        save_state_callback: Optional[Callable[[], bool]] = None,
        timeout_seconds: int = 30,
        priority: int = 1,
    ) -> None:
        """
        Register an operation for graceful shutdown handling.

        Args:
            operation_id: Unique identifier for the operation
            operation_type: Type of operation (DISCOVERY, COLLECTION, etc.)
            cleanup_callback: Optional cleanup function to call on shutdown
            save_state_callback: Optional state saving function
            timeout_seconds: Maximum time to wait for operation cleanup
            priority: Priority level (1=high, 2=medium, 3=low)
        """
        with self._shutdown_lock:
            if self._shutdown_initiated:
                self.logger.warning(
                    f"Cannot register operation {operation_id} - shutdown already initiated"
                )
                return

            handler = ShutdownHandler(
                operation_id=operation_id,
                operation_type=operation_type,
                cleanup_callback=cleanup_callback,
                save_state_callback=save_state_callback,
                timeout_seconds=timeout_seconds,
                priority=priority,
            )

            self._handlers[operation_id] = handler
            self.logger.debug(
                f"Registered operation {operation_id} for graceful shutdown"
            )

    def unregister_operation(self, operation_id: str) -> None:
        """Unregister an operation from shutdown handling."""
        with self._shutdown_lock:
            if operation_id in self._handlers:
                del self._handlers[operation_id]
                self.logger.debug(
                    f"Unregistered operation {operation_id} from graceful shutdown"
                )

    def initiate_shutdown(self) -> None:
        """Initiate graceful shutdown process."""
        with self._shutdown_lock:
            if self._shutdown_initiated:
                return

            self._shutdown_initiated = True

        try:
            self._perform_shutdown()
        except Exception as e:
            self.logger.error(f"Error during graceful shutdown: {e}")
            click.echo(f"⚠️  Error during shutdown: {e}")
        finally:
            self._cleanup_and_exit()

    def _perform_shutdown(self) -> None:
        """Perform the actual shutdown process."""
        if not self._handlers:
            click.echo("💾 No active operations to save")
            return

        click.echo(f"💾 Saving state for {len(self._handlers)} active operations...")

        # Sort handlers by priority (high priority first)
        sorted_handlers = sorted(
            self._handlers.values(), key=lambda h: (h.priority, h.registered_at)
        )

        saved_count = 0
        failed_count = 0

        for handler in sorted_handlers:
            try:
                click.echo(
                    f"   💾 Saving {handler.operation_type} operation: {handler.operation_id}"
                )

                # Save state if callback provided
                if handler.save_state_callback:
                    start_time = time.time()
                    success = False

                    try:
                        # Run with timeout
                        success = self._run_with_timeout(
                            handler.save_state_callback, handler.timeout_seconds
                        )
                    except TimeoutError:
                        self.logger.warning(
                            f"State saving timeout for {handler.operation_id}"
                        )
                        click.echo(
                            f"      ⚠️  Timeout saving state (>{handler.timeout_seconds}s)"
                        )
                    except Exception as e:
                        self.logger.error(
                            f"State saving failed for {handler.operation_id}: {e}"
                        )
                        click.echo(f"      ❌ Failed to save state: {e}")

                    duration = time.time() - start_time

                    if success:
                        click.echo(f"      ✅ State saved ({duration:.1f}s)")
                        saved_count += 1
                    else:
                        failed_count += 1
                else:
                    # No state callback, just mark as handled
                    click.echo(f"      ℹ️  No state to save")
                    saved_count += 1

                # Run cleanup if provided
                if handler.cleanup_callback:
                    try:
                        self._run_with_timeout(
                            handler.cleanup_callback, handler.timeout_seconds
                        )
                        click.echo(f"      🧹 Cleanup completed")
                    except Exception as e:
                        self.logger.warning(
                            f"Cleanup failed for {handler.operation_id}: {e}"
                        )
                        click.echo(f"      ⚠️  Cleanup warning: {e}")

            except Exception as e:
                self.logger.error(
                    f"Error handling shutdown for {handler.operation_id}: {e}"
                )
                click.echo(f"      ❌ Error: {e}")
                failed_count += 1

        # Summary
        if saved_count > 0:
            click.echo(f"✅ Successfully saved {saved_count} operations")

        if failed_count > 0:
            click.echo(f"⚠️  {failed_count} operations had issues during shutdown")

        # Provide recovery guidance
        if saved_count > 0:
            click.echo("💡 Use 'dynamodb_optima resume --latest' to continue operations")

    def _run_with_timeout(self, func: Callable, timeout_seconds: int) -> Any:
        """Run a function with timeout."""
        result = [None]
        exception = [None]

        def target():
            try:
                result[0] = func()
            except Exception as e:
                exception[0] = e

        thread = threading.Thread(target=target, daemon=True)
        thread.start()
        thread.join(timeout=timeout_seconds)

        if thread.is_alive():
            raise TimeoutError(f"Function timed out after {timeout_seconds} seconds")

        if exception[0]:
            raise exception[0]

        return result[0]

    def _cleanup_and_exit(self) -> None:
        """Perform final cleanup and exit."""
        try:
            # Restore original signal handlers
            for sig_name, original_handler in self._original_handlers.items():
                if original_handler is not None:
                    signal.signal(getattr(signal, sig_name), original_handler)

            click.echo("👋 Goodbye!")

        except Exception as e:
            self.logger.error(f"Error during final cleanup: {e}")

        finally:
            sys.exit(0)

    def is_shutdown_initiated(self) -> bool:
        """Check if shutdown has been initiated."""
        return self._shutdown_initiated

    def get_registered_operations(self) -> List[Dict[str, Any]]:
        """Get list of registered operations."""
        with self._shutdown_lock:
            return [
                {
                    "operation_id": handler.operation_id,
                    "operation_type": handler.operation_type,
                    "priority": handler.priority,
                    "registered_at": handler.registered_at.isoformat(),
                    "has_state_callback": handler.save_state_callback is not None,
                    "has_cleanup_callback": handler.cleanup_callback is not None,
                    "timeout_seconds": handler.timeout_seconds,
                }
                for handler in self._handlers.values()
            ]


# Global shutdown manager instance
_shutdown_manager: Optional[GracefulShutdownManager] = None


def get_shutdown_manager(
    logger: Optional[logging.Logger] = None,
) -> GracefulShutdownManager:
    """Get or create the global shutdown manager."""
    global _shutdown_manager

    if _shutdown_manager is None:
        _shutdown_manager = GracefulShutdownManager(logger)

    return _shutdown_manager


def register_operation_for_shutdown(
    operation_id: str,
    operation_type: str,
    cleanup_callback: Optional[Callable[[], None]] = None,
    save_state_callback: Optional[Callable[[], bool]] = None,
    timeout_seconds: int = 30,
    priority: int = 1,
    logger: Optional[logging.Logger] = None,
) -> None:
    """
    Register an operation for graceful shutdown handling.

    This is a convenience function that uses the global shutdown manager.
    """
    manager = get_shutdown_manager(logger)
    manager.register_operation(
        operation_id=operation_id,
        operation_type=operation_type,
        cleanup_callback=cleanup_callback,
        save_state_callback=save_state_callback,
        timeout_seconds=timeout_seconds,
        priority=priority,
    )


def unregister_operation_from_shutdown(
    operation_id: str, logger: Optional[logging.Logger] = None
) -> None:
    """
    Unregister an operation from graceful shutdown handling.

    This is a convenience function that uses the global shutdown manager.
    """
    manager = get_shutdown_manager(logger)
    manager.unregister_operation(operation_id)


class ShutdownContext:
    """
    Context manager for automatic operation registration and cleanup.
    """

    def __init__(
        self,
        operation_id: str,
        operation_type: str,
        cleanup_callback: Optional[Callable[[], None]] = None,
        save_state_callback: Optional[Callable[[], bool]] = None,
        timeout_seconds: int = 30,
        priority: int = 1,
        logger: Optional[logging.Logger] = None,
    ):
        """Initialize shutdown context."""
        self.operation_id = operation_id
        self.operation_type = operation_type
        self.cleanup_callback = cleanup_callback
        self.save_state_callback = save_state_callback
        self.timeout_seconds = timeout_seconds
        self.priority = priority
        self.logger = logger

    def __enter__(self):
        """Enter context and register operation."""
        register_operation_for_shutdown(
            operation_id=self.operation_id,
            operation_type=self.operation_type,
            cleanup_callback=self.cleanup_callback,
            save_state_callback=self.save_state_callback,
            timeout_seconds=self.timeout_seconds,
            priority=self.priority,
            logger=self.logger,
        )
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit context and unregister operation."""
        unregister_operation_from_shutdown(self.operation_id, self.logger)


def with_graceful_shutdown(
    operation_type: str,
    cleanup_callback: Optional[Callable[[], None]] = None,
    save_state_callback: Optional[Callable[[], bool]] = None,
    timeout_seconds: int = 30,
    priority: int = 1,
    logger: Optional[logging.Logger] = None,
):
    """
    Decorator for adding graceful shutdown handling to functions.

    Args:
        operation_type: Type of operation
        cleanup_callback: Optional cleanup function
        save_state_callback: Optional state saving function
        timeout_seconds: Maximum time for shutdown operations
        priority: Priority level for shutdown order
        logger: Optional logger instance
    """

    def decorator(func: Callable):
        def wrapper(*args, **kwargs):
            # Generate operation ID
            import uuid

            operation_id = f"{operation_type.lower()}_{str(uuid.uuid4())[:8]}"

            with ShutdownContext(
                operation_id=operation_id,
                operation_type=operation_type,
                cleanup_callback=cleanup_callback,
                save_state_callback=save_state_callback,
                timeout_seconds=timeout_seconds,
                priority=priority,
                logger=logger,
            ):
                return func(*args, **kwargs)

        return wrapper

    return decorator
