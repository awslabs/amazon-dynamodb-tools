"""
Timestamp alignment utilities for optimal CloudWatch performance.

Provides intelligent timestamp alignment to CloudWatch-optimized boundaries
for different metric collection periods, improving data consistency and API efficiency.
"""

import math
from datetime import datetime, timedelta
from typing import Tuple

from ..logging import get_logger

logger = get_logger("dmetrics.utils.timestamp_alignment")


class TimestampAligner:
    """
    Handles timestamp alignment for optimal CloudWatch API performance.

    CloudWatch performs better when timestamps are aligned to natural boundaries:
    - 1-hour periods: align to hour boundaries (00:00, 01:00, 02:00)
    - 5-minute periods: align to 5-minute intervals (00:00, 00:05, 00:10)
    - 1-minute periods: align to minute boundaries (00:00, 00:01, 00:02)
    """

    @staticmethod
    def align_to_hour_boundary(timestamp: datetime) -> datetime:
        """
        Align timestamp to the nearest hour boundary (floor).

        Args:
            timestamp: Input timestamp to align

        Returns:
            Timestamp aligned to hour boundary (minutes and seconds set to 0)

        Example:
            2025-01-15 14:23:45 -> 2025-01-15 14:00:00
        """
        aligned = timestamp.replace(minute=0, second=0, microsecond=0)

        logger.debug(
            "Aligned timestamp to hour boundary",
            original=timestamp.isoformat(),
            aligned=aligned.isoformat(),
            adjustment_minutes=(timestamp - aligned).total_seconds() / 60,
        )

        return aligned

    @staticmethod
    def align_to_five_minute_boundary(timestamp: datetime) -> datetime:
        """
        Align timestamp to the nearest 5-minute boundary (floor).

        Args:
            timestamp: Input timestamp to align

        Returns:
            Timestamp aligned to 5-minute boundary

        Example:
            2025-01-15 14:23:45 -> 2025-01-15 14:20:00
            2025-01-15 14:07:30 -> 2025-01-15 14:05:00
        """
        # Calculate minutes aligned to 5-minute intervals
        aligned_minutes = (timestamp.minute // 5) * 5
        aligned = timestamp.replace(minute=aligned_minutes, second=0, microsecond=0)

        logger.debug(
            "Aligned timestamp to 5-minute boundary",
            original=timestamp.isoformat(),
            aligned=aligned.isoformat(),
            adjustment_seconds=(timestamp - aligned).total_seconds(),
        )

        return aligned

    @staticmethod
    def align_to_minute_boundary(timestamp: datetime) -> datetime:
        """
        Align timestamp to the nearest minute boundary (floor).

        Args:
            timestamp: Input timestamp to align

        Returns:
            Timestamp aligned to minute boundary (seconds set to 0)

        Example:
            2025-01-15 14:23:45 -> 2025-01-15 14:23:00
        """
        aligned = timestamp.replace(second=0, microsecond=0)

        logger.debug(
            "Aligned timestamp to minute boundary",
            original=timestamp.isoformat(),
            aligned=aligned.isoformat(),
            adjustment_seconds=(timestamp - aligned).total_seconds(),
        )

        return aligned

    @classmethod
    def align_timestamp_for_period(
        cls, timestamp: datetime, period_seconds: int
    ) -> datetime:
        """
        Align timestamp based on the CloudWatch period for optimal performance.

        Args:
            timestamp: Input timestamp to align
            period_seconds: CloudWatch period in seconds (60, 300, 3600, etc.)

        Returns:
            Optimally aligned timestamp for the given period

        Alignment rules:
        - 3600s (1 hour): Align to hour boundaries
        - 300s (5 minutes): Align to 5-minute boundaries
        - 60s (1 minute): Align to minute boundaries
        - Other periods: Align to minute boundaries (conservative)
        """
        original_timestamp = timestamp

        if period_seconds >= 3600:  # 1 hour or longer
            aligned = cls.align_to_hour_boundary(timestamp)
            alignment_type = "hour"
        elif period_seconds >= 300:  # 5 minutes to 1 hour
            aligned = cls.align_to_five_minute_boundary(timestamp)
            alignment_type = "5-minute"
        elif period_seconds >= 60:  # 1 minute to 5 minutes
            aligned = cls.align_to_minute_boundary(timestamp)
            alignment_type = "minute"
        else:
            # For sub-minute periods, align to minute boundary (conservative)
            aligned = cls.align_to_minute_boundary(timestamp)
            alignment_type = "minute (conservative)"

        adjustment_seconds = (original_timestamp - aligned).total_seconds()

        logger.info(
            "Timestamp aligned for CloudWatch optimization",
            period_seconds=period_seconds,
            alignment_type=alignment_type,
            original=original_timestamp.isoformat(),
            aligned=aligned.isoformat(),
            adjustment_seconds=adjustment_seconds,
        )

        return aligned

    @classmethod
    def calculate_optimal_time_window(
        cls, start_time: datetime, end_time: datetime, period_seconds: int
    ) -> Tuple[datetime, datetime]:
        """
        Calculate optimal time window with aligned boundaries to avoid partial periods.

        Args:
            start_time: Desired start time
            end_time: Desired end time
            period_seconds: CloudWatch period in seconds

        Returns:
            Tuple of (aligned_start_time, aligned_end_time) optimized for CloudWatch

        The function ensures:
        - Start time is aligned to appropriate boundary (floor)
        - End time is aligned to appropriate boundary (ceil)
        - Time window covers complete periods only
        - No partial periods that could cause data inconsistency
        """
        original_start = start_time
        original_end = end_time

        # Align start time (floor - go back to include the period)
        aligned_start = cls.align_timestamp_for_period(start_time, period_seconds)

        # For end time, we want to align forward to ensure we capture complete periods
        if period_seconds >= 3600:  # 1 hour or longer
            # Align to next hour boundary if not already aligned
            if (
                end_time.minute != 0
                or end_time.second != 0
                or end_time.microsecond != 0
            ):
                aligned_end = end_time.replace(
                    minute=0, second=0, microsecond=0
                ) + timedelta(hours=1)
            else:
                aligned_end = end_time
        elif period_seconds >= 300:  # 5 minutes to 1 hour
            # Align to next 5-minute boundary if not already aligned
            current_minutes = end_time.minute
            aligned_minutes = math.ceil(current_minutes / 5) * 5
            if aligned_minutes >= 60:
                aligned_end = end_time.replace(
                    minute=0, second=0, microsecond=0
                ) + timedelta(hours=1)
            elif (
                aligned_minutes != current_minutes
                or end_time.second != 0
                or end_time.microsecond != 0
            ):
                aligned_end = end_time.replace(
                    minute=aligned_minutes, second=0, microsecond=0
                )
            else:
                aligned_end = end_time
        else:  # 1 minute or shorter
            # Align to next minute boundary if not already aligned
            if end_time.second != 0 or end_time.microsecond != 0:
                aligned_end = end_time.replace(second=0, microsecond=0) + timedelta(
                    minutes=1
                )
            else:
                aligned_end = end_time

        # Calculate adjustments for logging
        start_adjustment = (original_start - aligned_start).total_seconds()
        end_adjustment = (aligned_end - original_end).total_seconds()
        window_duration = (aligned_end - aligned_start).total_seconds()

        # Calculate number of complete periods in the window
        complete_periods = int(window_duration / period_seconds)

        logger.info(
            "Calculated optimal time window for CloudWatch collection",
            period_seconds=period_seconds,
            original_start=original_start.isoformat(),
            original_end=original_end.isoformat(),
            aligned_start=aligned_start.isoformat(),
            aligned_end=aligned_end.isoformat(),
            start_adjustment_seconds=start_adjustment,
            end_adjustment_seconds=end_adjustment,
            window_duration_hours=window_duration / 3600,
            complete_periods=complete_periods,
            efficiency_gain="Aligned boundaries improve CloudWatch API performance",
        )

        return aligned_start, aligned_end

    @classmethod
    def validate_timestamp_alignment(
        cls, timestamp: datetime, period_seconds: int
    ) -> bool:
        """
        Validate that a timestamp is properly aligned for the given period.

        Args:
            timestamp: Timestamp to validate
            period_seconds: CloudWatch period in seconds

        Returns:
            True if timestamp is optimally aligned, False otherwise
        """
        aligned_timestamp = cls.align_timestamp_for_period(timestamp, period_seconds)
        is_aligned = timestamp == aligned_timestamp

        if not is_aligned:
            adjustment_seconds = (timestamp - aligned_timestamp).total_seconds()
            logger.warning(
                "Timestamp not optimally aligned for CloudWatch period",
                timestamp=timestamp.isoformat(),
                period_seconds=period_seconds,
                expected_alignment=aligned_timestamp.isoformat(),
                adjustment_needed_seconds=adjustment_seconds,
            )
        else:
            logger.debug(
                "Timestamp validation passed - optimal alignment confirmed",
                timestamp=timestamp.isoformat(),
                period_seconds=period_seconds,
            )

        return is_aligned

    @classmethod
    def get_alignment_recommendation(cls, period_seconds: int) -> str:
        """
        Get human-readable alignment recommendation for a given period.

        Args:
            period_seconds: CloudWatch period in seconds

        Returns:
            String describing the recommended alignment strategy
        """
        if period_seconds >= 3600:
            return f"Align to hour boundaries (00:00, 01:00, 02:00) for {period_seconds}s periods"
        elif period_seconds >= 300:
            return f"Align to 5-minute intervals (00:00, 00:05, 00:10) for {period_seconds}s periods"
        elif period_seconds >= 60:
            return f"Align to minute boundaries (00:00, 00:01, 00:02) for {period_seconds}s periods"
        else:
            return f"Align to minute boundaries (conservative) for {period_seconds}s periods"


def align_collection_timestamps(
    start_time: datetime, end_time: datetime, period_seconds: int
) -> Tuple[datetime, datetime]:
    """
    Convenience function to align collection timestamps for optimal CloudWatch performance.

    Args:
        start_time: Desired collection start time
        end_time: Desired collection end time
        period_seconds: CloudWatch metric period in seconds

    Returns:
        Tuple of (aligned_start, aligned_end) optimized for CloudWatch API calls
    """
    return TimestampAligner.calculate_optimal_time_window(
        start_time, end_time, period_seconds
    )


def validate_cloudwatch_timestamps(
    start_time: datetime, end_time: datetime, period_seconds: int
) -> bool:
    """
    Validate that timestamps are optimally aligned for CloudWatch collection.

    Args:
        start_time: Collection start time
        end_time: Collection end time
        period_seconds: CloudWatch metric period in seconds

    Returns:
        True if both timestamps are optimally aligned, False otherwise
    """
    aligner = TimestampAligner()
    start_valid = aligner.validate_timestamp_alignment(start_time, period_seconds)
    end_valid = aligner.validate_timestamp_alignment(end_time, period_seconds)

    return start_valid and end_valid
