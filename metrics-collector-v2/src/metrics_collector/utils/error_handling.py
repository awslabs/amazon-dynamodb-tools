"""
Comprehensive error handling and recovery guidance system.

Provides consistent error messaging, recovery guidance, and error categorization
across all DMetrics operations.
"""

import logging
import sys
import traceback
from dataclasses import dataclass
from enum import Enum
from typing import Any, Callable, Dict, List, Optional

import click


class ErrorCategory(Enum):
    """Categories of errors for consistent handling."""

    AUTHENTICATION = "authentication"
    NETWORK = "network"
    API_QUOTA = "api_quota"
    DATABASE = "database"
    CONFIGURATION = "configuration"
    VALIDATION = "validation"
    TIMEOUT = "timeout"
    PERMISSION = "permission"
    RESOURCE_NOT_FOUND = "resource_not_found"
    SYSTEM = "system"
    USER_INPUT = "user_input"
    UNKNOWN = "unknown"


@dataclass
class ErrorContext:
    """Context information for errors."""

    operation_id: Optional[str] = None
    operation_type: Optional[str] = None
    resource_name: Optional[str] = None
    region: Optional[str] = None
    stage: Optional[str] = None
    additional_info: Dict[str, Any] = None

    def __post_init__(self):
        if self.additional_info is None:
            self.additional_info = {}


@dataclass
class RecoveryAction:
    """Suggested recovery action for an error."""

    action: str
    description: str
    command: Optional[str] = None
    priority: int = 1  # 1 = high, 2 = medium, 3 = low

    def format_for_display(self) -> str:
        """Format recovery action for CLI display."""
        parts = [f"💡 {self.action}"]
        if self.description:
            parts.append(f"   {self.description}")
        if self.command:
            parts.append(f"   Command: {self.command}")
        return "\n".join(parts)


@dataclass
class ErrorInfo:
    """Complete error information with recovery guidance."""

    category: ErrorCategory
    title: str
    message: str
    technical_details: str
    recovery_actions: List[RecoveryAction]
    context: ErrorContext
    is_recoverable: bool = True
    should_retry: bool = False
    retry_delay_seconds: int = 0

    def format_for_display(self, show_technical: bool = False) -> str:
        """Format error for CLI display."""
        lines = []

        # Error header with category
        category_icons = {
            ErrorCategory.AUTHENTICATION: "🔐",
            ErrorCategory.NETWORK: "🌐",
            ErrorCategory.API_QUOTA: "⚡",
            ErrorCategory.DATABASE: "💾",
            ErrorCategory.CONFIGURATION: "⚙️",
            ErrorCategory.VALIDATION: "✅",
            ErrorCategory.TIMEOUT: "⏱️",
            ErrorCategory.PERMISSION: "🚫",
            ErrorCategory.RESOURCE_NOT_FOUND: "🔍",
            ErrorCategory.SYSTEM: "🖥️",
            ErrorCategory.USER_INPUT: "👤",
            ErrorCategory.UNKNOWN: "❓",
        }

        icon = category_icons.get(self.category, "❌")
        lines.append(f"{icon} {self.title}")

        # Main message
        if self.message:
            lines.append(f"   {self.message}")

        # Context information
        if self.context.operation_id:
            lines.append(f"   Operation: {self.context.operation_id}")
        if self.context.resource_name:
            lines.append(f"   Resource: {self.context.resource_name}")
        if self.context.region:
            lines.append(f"   Region: {self.context.region}")
        if self.context.stage:
            lines.append(f"   Stage: {self.context.stage}")

        # Technical details (if requested)
        if show_technical and self.technical_details:
            lines.append(f"   Technical: {self.technical_details}")

        # Recovery actions
        if self.recovery_actions:
            lines.append("")
            lines.append("🔧 Suggested Actions:")
            for action in sorted(self.recovery_actions, key=lambda x: x.priority):
                lines.append(action.format_for_display())

        # Retry information
        if self.should_retry:
            retry_msg = "🔄 This error may be temporary and can be retried"
            if self.retry_delay_seconds > 0:
                retry_msg += f" (wait {self.retry_delay_seconds}s)"
            lines.append("")
            lines.append(retry_msg)

        return "\n".join(lines)


class ErrorHandler:
    """
    Comprehensive error handler with categorization and recovery guidance.
    """

    def __init__(self, logger: Optional[logging.Logger] = None):
        """Initialize error handler."""
        self.logger = logger or logging.getLogger(__name__)
        self._error_patterns = self._build_error_patterns()
        self._recovery_actions = self._build_recovery_actions()

    def _build_error_patterns(self) -> Dict[ErrorCategory, List[str]]:
        """Build patterns for error categorization."""
        return {
            ErrorCategory.AUTHENTICATION: [
                "credential",
                "access denied",
                "unauthorized",
                "invalid token",
                "expired token",
                "authentication failed",
                "aws credentials",
                "no credentials",
                "unable to locate credentials",
            ],
            ErrorCategory.NETWORK: [
                "connection",
                "network",
                "timeout",
                "unreachable",
                "dns",
                "connection refused",
                "connection reset",
                "no route to host",
                "network is unreachable",
            ],
            ErrorCategory.API_QUOTA: [
                "throttl",
                "rate limit",
                "quota",
                "too many requests",
                "request limit exceeded",
                "api limit",
                "service limit",
            ],
            ErrorCategory.DATABASE: [
                "database",
                "sqlite",
                "duckdb",
                "db lock",
                "database lock",
                "database is locked",
                "disk full",
                "no space left",
            ],
            ErrorCategory.CONFIGURATION: [
                "config",
                "setting",
                "parameter",
                "invalid configuration",
                "missing configuration",
                "configuration error",
            ],
            ErrorCategory.VALIDATION: [
                "validation",
                "invalid",
                "malformed",
                "format error",
                "schema error",
                "type error",
                "value error",
                "invalid parameter",
            ],
            ErrorCategory.TIMEOUT: [
                "timeout",
                "timed out",
                "deadline exceeded",
                "operation timeout",
            ],
            ErrorCategory.PERMISSION: [
                "permission",
                "access denied",
                "forbidden",
                "not authorized",
                "insufficient permissions",
                "access is denied",
            ],
            ErrorCategory.RESOURCE_NOT_FOUND: [
                "not found",
                "does not exist",
                "no such",
                "resource not found",
                "table not found",
                "region not found",
            ],
            ErrorCategory.SYSTEM: [
                "system error",
                "internal error",
                "unexpected error",
                "memory error",
                "disk error",
                "io error",
            ],
            ErrorCategory.USER_INPUT: [
                "invalid input",
                "invalid argument",
                "missing argument",
                "invalid option",
                "invalid parameter",
            ],
        }

    def _build_recovery_actions(self) -> Dict[ErrorCategory, List[RecoveryAction]]:
        """Build recovery actions for each error category."""
        return {
            ErrorCategory.AUTHENTICATION: [
                RecoveryAction(
                    "Check AWS credentials",
                    "Verify your AWS credentials are configured correctly",
                    "dmetrics aws-config --validate",
                    priority=1,
                ),
                RecoveryAction(
                    "Refresh credentials",
                    "If using temporary credentials, refresh them",
                    priority=2,
                ),
                RecoveryAction(
                    "Check IAM permissions",
                    "Ensure your IAM user/role has DynamoDB and CloudWatch permissions",
                    priority=2,
                ),
            ],
            ErrorCategory.NETWORK: [
                RecoveryAction(
                    "Check internet connection",
                    "Verify you have a stable internet connection",
                    priority=1,
                ),
                RecoveryAction(
                    "Try different region",
                    "Some AWS regions may be experiencing issues",
                    priority=2,
                ),
                RecoveryAction(
                    "Check firewall/proxy",
                    "Ensure AWS endpoints are accessible through your network",
                    priority=3,
                ),
            ],
            ErrorCategory.API_QUOTA: [
                RecoveryAction(
                    "Wait and retry",
                    "API throttling is temporary - the system will retry automatically",
                    priority=1,
                ),
                RecoveryAction(
                    "Reduce concurrency",
                    "Consider reducing the number of concurrent operations",
                    priority=2,
                ),
                RecoveryAction(
                    "Check service limits",
                    "Review your AWS service limits in the AWS console",
                    priority=3,
                ),
            ],
            ErrorCategory.DATABASE: [
                RecoveryAction(
                    "Check disk space",
                    "Ensure you have sufficient disk space for the database",
                    "df -h .",
                    priority=1,
                ),
                RecoveryAction(
                    "Check permissions",
                    "Verify write permissions to the database directory",
                    "ls -la data/",
                    priority=1,
                ),
                RecoveryAction(
                    "Restart operation",
                    "Database locks are usually temporary",
                    priority=2,
                ),
            ],
            ErrorCategory.CONFIGURATION: [
                RecoveryAction(
                    "Check configuration",
                    "Review your configuration settings",
                    priority=1,
                ),
                RecoveryAction(
                    "Reset to defaults",
                    "Consider resetting to default configuration",
                    priority=2,
                ),
                RecoveryAction(
                    "Check environment variables",
                    "Verify all required environment variables are set",
                    priority=2,
                ),
            ],
            ErrorCategory.VALIDATION: [
                RecoveryAction(
                    "Check input format",
                    "Verify your input follows the expected format",
                    priority=1,
                ),
                RecoveryAction(
                    "Review documentation",
                    "Check the documentation for correct usage",
                    priority=2,
                ),
            ],
            ErrorCategory.TIMEOUT: [
                RecoveryAction(
                    "Retry operation",
                    "Timeouts are often temporary - try again",
                    priority=1,
                ),
                RecoveryAction(
                    "Check network stability",
                    "Ensure you have a stable network connection",
                    priority=2,
                ),
                RecoveryAction(
                    "Reduce batch size",
                    "Consider processing smaller batches",
                    priority=3,
                ),
            ],
            ErrorCategory.PERMISSION: [
                RecoveryAction(
                    "Check IAM permissions",
                    "Verify your AWS IAM permissions include required actions",
                    priority=1,
                ),
                RecoveryAction(
                    "Check resource policies",
                    "Some resources may have additional access policies",
                    priority=2,
                ),
                RecoveryAction(
                    "Contact administrator",
                    "You may need additional permissions from your AWS administrator",
                    priority=3,
                ),
            ],
            ErrorCategory.RESOURCE_NOT_FOUND: [
                RecoveryAction(
                    "Verify resource exists",
                    "Check that the resource exists in the specified region",
                    priority=1,
                ),
                RecoveryAction(
                    "Check region",
                    "Ensure you're looking in the correct AWS region",
                    priority=1,
                ),
                RecoveryAction(
                    "Run discovery",
                    "Update your resource discovery to find current resources",
                    "dmetrics discover",
                    priority=2,
                ),
            ],
            ErrorCategory.SYSTEM: [
                RecoveryAction(
                    "Restart operation", "System errors are often temporary", priority=1
                ),
                RecoveryAction(
                    "Check system resources",
                    "Verify sufficient memory and disk space",
                    priority=2,
                ),
                RecoveryAction(
                    "Check logs",
                    "Review detailed logs for more information",
                    priority=3,
                ),
            ],
            ErrorCategory.USER_INPUT: [
                RecoveryAction(
                    "Check command syntax",
                    "Verify you're using the correct command syntax",
                    "dmetrics --help",
                    priority=1,
                ),
                RecoveryAction(
                    "Review examples",
                    "Check the documentation for usage examples",
                    priority=2,
                ),
            ],
        }

    def categorize_error(
        self, error: Exception, context: Optional[ErrorContext] = None
    ) -> ErrorCategory:
        """Categorize an error based on its message and type."""
        error_str = str(error).lower()
        error_type = type(error).__name__.lower()

        # Check error message against patterns (prioritize more specific patterns)
        # Check validation patterns first (more specific)
        for pattern in self._error_patterns[ErrorCategory.VALIDATION]:
            if pattern in error_str or pattern in error_type:
                return ErrorCategory.VALIDATION

        # Check database patterns before network (database connection vs network connection)
        for pattern in self._error_patterns[ErrorCategory.DATABASE]:
            if pattern in error_str or pattern in error_type:
                return ErrorCategory.DATABASE

        # Check other patterns
        for category, patterns in self._error_patterns.items():
            if category in [ErrorCategory.VALIDATION, ErrorCategory.DATABASE]:
                continue  # Already checked above
            for pattern in patterns:
                if pattern in error_str or pattern in error_type:
                    return category

        # Check specific exception types
        if "timeout" in error_type or "timeout" in error_str:
            return ErrorCategory.TIMEOUT
        elif "permission" in error_type or "access" in error_type:
            return ErrorCategory.PERMISSION
        elif "network" in error_type or "connection" in error_type:
            return ErrorCategory.NETWORK
        elif "validation" in error_type or "value" in error_type:
            return ErrorCategory.VALIDATION

        return ErrorCategory.UNKNOWN

    def create_error_info(
        self,
        error: Exception,
        context: Optional[ErrorContext] = None,
        custom_message: Optional[str] = None,
        custom_actions: Optional[List[RecoveryAction]] = None,
    ) -> ErrorInfo:
        """Create comprehensive error information."""
        if context is None:
            context = ErrorContext()

        category = self.categorize_error(error, context)

        # Build title and message
        title = f"{category.value.replace('_', ' ').title()} Error"
        message = custom_message or str(error)

        # Get technical details
        technical_details = f"{type(error).__name__}: {error}"

        # Get recovery actions
        recovery_actions = custom_actions or self._recovery_actions.get(category, [])

        # Determine if error is recoverable and should retry
        is_recoverable = category not in [
            ErrorCategory.CONFIGURATION,
            ErrorCategory.USER_INPUT,
        ]
        should_retry = category in [
            ErrorCategory.NETWORK,
            ErrorCategory.API_QUOTA,
            ErrorCategory.TIMEOUT,
            ErrorCategory.SYSTEM,
        ]

        # Set retry delay based on category
        retry_delay = 0
        if category == ErrorCategory.API_QUOTA:
            retry_delay = 60  # Wait 1 minute for quota issues
        elif category == ErrorCategory.NETWORK:
            retry_delay = 30  # Wait 30 seconds for network issues
        elif category == ErrorCategory.TIMEOUT:
            retry_delay = 15  # Wait 15 seconds for timeouts

        return ErrorInfo(
            category=category,
            title=title,
            message=message,
            technical_details=technical_details,
            recovery_actions=recovery_actions,
            context=context,
            is_recoverable=is_recoverable,
            should_retry=should_retry,
            retry_delay_seconds=retry_delay,
        )

    def handle_error(
        self,
        error: Exception,
        context: Optional[ErrorContext] = None,
        show_technical: bool = False,
        exit_on_error: bool = True,
    ) -> ErrorInfo:
        """
        Handle an error with comprehensive logging and user guidance.

        Args:
            error: The exception that occurred
            context: Additional context about the error
            show_technical: Whether to show technical details
            exit_on_error: Whether to exit the program on error

        Returns:
            ErrorInfo object with complete error details
        """
        error_info = self.create_error_info(error, context)

        # Log the error
        self.logger.error(
            f"Error in {context.operation_type if context else 'unknown'} operation",
            extra={
                "error_category": error_info.category.value,
                "operation_id": context.operation_id if context else None,
                "resource_name": context.resource_name if context else None,
                "region": context.region if context else None,
                "error_type": type(error).__name__,
                "error_message": str(error),
                "traceback": traceback.format_exc(),
            },
        )

        # Display error to user
        click.echo(error_info.format_for_display(show_technical), err=True)

        # Exit if requested
        if exit_on_error and not error_info.is_recoverable:
            sys.exit(1)

        return error_info

    def handle_operation_error(
        self,
        error: Exception,
        operation_id: str,
        operation_type: str,
        resource_name: Optional[str] = None,
        region: Optional[str] = None,
        stage: Optional[str] = None,
        show_technical: bool = False,
    ) -> ErrorInfo:
        """Handle an error that occurred during an operation."""
        context = ErrorContext(
            operation_id=operation_id,
            operation_type=operation_type,
            resource_name=resource_name,
            region=region,
            stage=stage,
        )

        return self.handle_error(error, context, show_technical, exit_on_error=False)


def create_error_handler(logger: Optional[logging.Logger] = None) -> ErrorHandler:
    """Create a configured error handler."""
    return ErrorHandler(logger)


def handle_cli_error(
    error: Exception,
    operation_type: str = "CLI",
    show_technical: bool = False,
    logger: Optional[logging.Logger] = None,
) -> None:
    """
    Convenience function for handling CLI errors.

    Args:
        error: The exception that occurred
        operation_type: Type of CLI operation
        show_technical: Whether to show technical details
        logger: Optional logger instance
    """
    handler = create_error_handler(logger)
    context = ErrorContext(operation_type=operation_type)
    handler.handle_error(error, context, show_technical, exit_on_error=True)


def with_error_handling(
    operation_type: str,
    logger: Optional[logging.Logger] = None,
    show_technical: bool = False,
):
    """
    Decorator for adding comprehensive error handling to functions.

    Args:
        operation_type: Type of operation for context
        logger: Optional logger instance
        show_technical: Whether to show technical details
    """

    def decorator(func: Callable):
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                handle_cli_error(e, operation_type, show_technical, logger)

        return wrapper

    return decorator
