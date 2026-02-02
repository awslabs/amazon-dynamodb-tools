"""
DynamoDB Autoscaling Simulation.

Simulates how DynamoDB's autoscaling would provision read and write capacity
based on consumed capacity metrics. This allows cost comparison between
On-Demand and Provisioned capacity modes.

Autoscaling Rules (per AWS documentation):
- Scale-Out: When consumption > target utilization for 2 consecutive minutes
- Scale-In: When consumption < (target - 20%) for 15 consecutive minutes
- First 4 scale-ins per day can happen anytime
- After 4 scale-ins, only 1 scale-in per hour is allowed
"""

from dataclasses import dataclass
from datetime import datetime
from typing import List, Tuple
import logging

logger = logging.getLogger(__name__)


@dataclass
class MetricDataPoint:
    """
    Represents a single CloudWatch metric data point.
    
    Attributes:
        metric_name: Name of the metric (e.g., 'ConsumedReadCapacityUnits')
        timestamp: Time of measurement
        table_name: DynamoDB table or index name
        consumed_units: Actual consumed capacity units
        units_per_second: Consumed units per second (consumed_units / 60)
        provisioned_units: Simulated provisioned capacity (calculated by simulation)
    """
    metric_name: str
    timestamp: datetime
    table_name: str
    consumed_units: float
    units_per_second: float
    provisioned_units: float = 0.0
    
    def to_list(self) -> List:
        """Convert to list format for compatibility."""
        return [
            self.metric_name,
            self.timestamp,
            self.table_name,
            self.consumed_units,
            self.units_per_second,
            self.provisioned_units
        ]


class AutoscalingSimulator:
    """
    Simulates DynamoDB autoscaling behavior.
    
    This simulator replicates how AWS DynamoDB autoscaling would provision
    capacity based on actual consumption patterns, allowing for accurate
    cost comparison between On-Demand and Provisioned modes.
    """
    
    def __init__(
        self,
        target_utilization: float = 0.7,
        scale_in_threshold_multiplier: float = 1.2,
        min_capacity: int = 1,
        max_capacity: int = 40000
    ):
        """
        Initialize autoscaling simulator.
        
        Args:
            target_utilization: Target utilization percentage (default: 0.7 = 70%)
            scale_in_threshold_multiplier: Multiplier for scale-in threshold (default: 1.2 = 20%)
            min_capacity: Minimum provisioned capacity units
            max_capacity: Maximum provisioned capacity units
        """
        self.target_utilization = target_utilization
        self.scale_in_threshold = scale_in_threshold_multiplier
        self.min_capacity = min_capacity
        self.max_capacity = max_capacity
    
    def _calculate_provisioned_capacity(
        self,
        consumed: float,
        current_provisioned: float
    ) -> float:
        """
        Calculate required provisioned capacity based on consumption.
        
        Args:
            consumed: Consumed capacity units per second
            current_provisioned: Current provisioned capacity
        
        Returns:
            Calculated provisioned capacity respecting min/max bounds
        """
        required = (consumed / self.target_utilization) * 100
        return min(max(required, current_provisioned, self.min_capacity), self.max_capacity)
    
    def _should_scale_out(
        self,
        last_2_consumed: List[float]
    ) -> Tuple[bool, float]:
        """
        Determine if scale-out is needed based on last 2 minutes.
        
        Scale-out occurs when consumption exceeds target utilization
        for 2 consecutive minutes.
        
        Args:
            last_2_consumed: List of last 2 consumed capacity values
        
        Returns:
            Tuple of (should_scale, new_capacity)
        """
        if len(last_2_consumed) < 2:
            return False, 0.0
        
        # Use minimum of last 2 to be conservative, max for actual provisioning
        min_consumed = min(last_2_consumed)
        max_consumed = max(last_2_consumed)
        
        required_capacity = (min_consumed / self.target_utilization) * 100
        
        if required_capacity > self.max_capacity:
            # Would need to scale out but already at max
            return True, max_consumed / self.target_utilization * 100
        
        return True, required_capacity
    
    def _should_scale_in(
        self,
        last_15_consumed: List[float],
        last_15_provisioned: List[float],
        current_provisioned: float,
        scale_in_count: int,
        last_60_provisioned: List[float] = None
    ) -> Tuple[bool, float]:
        """
        Determine if scale-in is needed based on last 15 minutes.
        
        Scale-in occurs when consumption is below (target - 20%) for 15 consecutive minutes.
        First 4 scale-ins can happen anytime, then limited to 1 per hour.
        
        Args:
            last_15_consumed: List of last 15 consumed capacity values
            last_15_provisioned: List of last 15 provisioned capacity values
            current_provisioned: Current provisioned capacity
            scale_in_count: Number of scale-ins so far today
            last_60_provisioned: List of last 60 provisioned values (for hourly limit)
        
        Returns:
            Tuple of (should_scale, new_capacity)
        """
        if len(last_15_consumed) < 15:
            return False, current_provisioned
        
        max_consumed_15min = max(last_15_consumed)
        
        # Check if provisioned capacity has been decreasing (already scaling in)
        has_decreased = any(
            x > y for x, y in zip(last_15_provisioned, last_15_provisioned[1:])
        )
        
        if has_decreased:
            # Already in a scale-in operation
            return False, current_provisioned
        
        # Calculate new capacity if we were to scale in
        new_capacity = max(
            (max_consumed_15min / self.target_utilization) * 100,
            self.min_capacity
        )
        new_capacity = min(new_capacity, current_provisioned)
        
        # Check if scale-in threshold is met (20% headroom)
        if current_provisioned <= (new_capacity * self.scale_in_threshold):
            # Not enough headroom for scale-in
            return False, current_provisioned
        
        # First 4 scale-ins can happen anytime
        if scale_in_count < 4:
            return True, new_capacity
        
        # After 4 scale-ins, check if we've scaled in the last hour
        if last_60_provisioned and len(last_60_provisioned) >= 60:
            has_scaled_in_last_hour = any(
                x > y for x, y in zip(last_60_provisioned, last_60_provisioned[1:])
            )
            if not has_scaled_in_last_hour:
                return True, new_capacity
        
        return False, current_provisioned
    
    def simulate(
        self,
        read_metrics: List[MetricDataPoint],
        write_metrics: List[MetricDataPoint]
    ) -> Tuple[List[MetricDataPoint], List[MetricDataPoint]]:
        """
        Simulate autoscaling for read and write capacity.
        
        Args:
            read_metrics: List of read capacity data points (sorted by timestamp)
            write_metrics: List of write capacity data points (sorted by timestamp)
        
        Returns:
            Tuple of (simulated_read_metrics, simulated_write_metrics)
        """
        if not read_metrics or not write_metrics:
            logger.warning("Empty metrics provided to simulator")
            return [], []
        
        # Use the shorter list to ensure we have matching pairs
        num_points = min(len(read_metrics), len(write_metrics))
        
        simulated_read = []
        simulated_write = []
        
        # Initialize with first point
        prev_read = read_metrics[0]
        prev_write = write_metrics[0]
        
        # Set initial provisioned capacity
        prev_read.provisioned_units = self._calculate_provisioned_capacity(
            prev_read.units_per_second, 0
        )
        prev_write.provisioned_units = self._calculate_provisioned_capacity(
            prev_write.units_per_second, 0
        )
        
        simulated_read.append(prev_read)
        simulated_write.append(prev_write)
        
        scale_in_count = 0
        last_scale_change = "read"
        
        for i in range(1, num_points):
            current_read = read_metrics[i]
            current_write = write_metrics[i]
            
            # Reset scale-in count at midnight
            if current_read.timestamp.hour == 0 and current_read.timestamp.minute == 0:
                scale_in_count = 0
            
            # Start with previous provisioned capacity
            current_read.provisioned_units = prev_read.provisioned_units
            current_write.provisioned_units = prev_write.provisioned_units
            
            # Handle first 2 points (need at least 2 for scale-out logic)
            if i <= 2:
                simulated_read.append(current_read)
                simulated_write.append(prev_write)
                prev_read = current_read
                prev_write = current_write
                continue
            
            # Get last 2 minutes for scale-out
            last_2_read = [m.units_per_second for m in read_metrics[i-2:i]]
            last_2_write = [m.units_per_second for m in write_metrics[i-2:i]]
            
            # Scale-out logic (based on last 2 minutes)
            should_scale_read, new_read_capacity = self._should_scale_out(last_2_read)
            if should_scale_read:
                current_read.provisioned_units = min(
                    max(new_read_capacity, prev_read.provisioned_units),
                    self.max_capacity
                )
            
            should_scale_write, new_write_capacity = self._should_scale_out(last_2_write)
            if should_scale_write:
                current_write.provisioned_units = min(
                    max(new_write_capacity, prev_write.provisioned_units),
                    self.max_capacity
                )
            
            # Handle first 14 points (need at least 15 for scale-in logic)
            if i <= 14:
                simulated_read.append(current_read)
                simulated_write.append(current_write)
                prev_read = current_read
                prev_write = current_write
                continue
            
            # Get last 15 minutes for scale-in
            last_15_read_consumed = [m.units_per_second for m in read_metrics[i-15:i]]
            last_15_read_provisioned = [m.provisioned_units for m in simulated_read[i-15:i]]
            last_15_write_consumed = [m.units_per_second for m in write_metrics[i-15:i]]
            last_15_write_provisioned = [m.provisioned_units for m in simulated_write[i-15:i]]
            
            # Get last 60 minutes if available (for hourly scale-in limit)
            last_60_read = None
            last_60_write = None
            if i >= 60:
                last_60_read = [m.provisioned_units for m in simulated_read[i-60:i]]
                last_60_write = [m.provisioned_units for m in simulated_write[i-60:i]]
            
            # Scale-in logic for read
            should_scale_in_read, new_read_capacity = self._should_scale_in(
                last_15_read_consumed,
                last_15_read_provisioned,
                current_read.provisioned_units,
                scale_in_count,
                last_60_read
            )
            
            # Scale-in logic for write
            should_scale_in_write, new_write_capacity = self._should_scale_in(
                last_15_write_consumed,
                last_15_write_provisioned,
                current_write.provisioned_units,
                scale_in_count,
                last_60_write
            )
            
            # Apply scale-in with alternating priority if both want to scale
            if should_scale_in_read and should_scale_in_write:
                if last_scale_change == "write":
                    current_read.provisioned_units = new_read_capacity
                    scale_in_count += 1
                    last_scale_change = "read"
                else:
                    current_write.provisioned_units = new_write_capacity
                    scale_in_count += 1
                    last_scale_change = "write"
            elif should_scale_in_read:
                current_read.provisioned_units = new_read_capacity
                if prev_read.provisioned_units > current_read.provisioned_units:
                    scale_in_count += 1
            elif should_scale_in_write:
                current_write.provisioned_units = new_write_capacity
                if prev_write.provisioned_units > current_write.provisioned_units:
                    scale_in_count += 1
            
            simulated_read.append(current_read)
            simulated_write.append(current_write)
            prev_read = current_read
            prev_write = current_write
        
        logger.info(
            f"Autoscaling simulation complete: {num_points} data points, "
            f"{scale_in_count} scale-in operations"
        )
        
        return simulated_read, simulated_write
