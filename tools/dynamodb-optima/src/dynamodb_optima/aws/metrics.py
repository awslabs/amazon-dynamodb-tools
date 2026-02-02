"""
CloudWatch metric configurations for AWS services.

Defines which metrics to collect for DynamoDB and other services,
including statistics, periods, and dimensions.
"""

from dataclasses import dataclass, field
from typing import List, Optional


@dataclass
class MetricConfiguration:
    """Configuration for a CloudWatch metric to collect."""
    
    metric_name: str
    statistics: List[str] = field(default_factory=lambda: ["Average", "Sum", "Maximum"])
    periods: List[int] = field(default_factory=lambda: [60, 300])  # seconds
    operation: Optional[str] = None  # For operation-specific metrics
    operation_type: Optional[str] = None  # For batch operation metrics


def get_service_metrics(service: str = "dynamodb", comprehensive: bool = False) -> List[MetricConfiguration]:
    """
    Get metric configurations for a specific AWS service.
    
    Args:
        service: Service name ('dynamodb', 'documentdb', etc.)
        comprehensive: If True, return comprehensive metrics; if False, return basic metrics
        
    Returns:
        List of metric configurations for the service
    """
    if service == "dynamodb":
        return get_dynamodb_metrics(comprehensive)
    else:
        raise ValueError(f"Unsupported service: {service}")


def get_dynamodb_metrics(comprehensive: bool = False) -> List[MetricConfiguration]:
    """
    Get DynamoDB metric configurations.
    
    Args:
        comprehensive: If True, return all metrics; if False, return essential metrics only
        
    Returns:
        List of DynamoDB metric configurations
    """
    # Essential metrics (always collected)
    essential_metrics = [
        # Capacity metrics
        MetricConfiguration(
            metric_name="ConsumedReadCapacityUnits",
            statistics=["Sum", "Average", "Maximum"],
            periods=[60, 300],
        ),
        MetricConfiguration(
            metric_name="ConsumedWriteCapacityUnits",
            statistics=["Sum", "Average", "Maximum"],
            periods=[60, 300],
        ),
        MetricConfiguration(
            metric_name="ProvisionedReadCapacityUnits",
            statistics=["Average"],
            periods=[300],
        ),
        MetricConfiguration(
            metric_name="ProvisionedWriteCapacityUnits",
            statistics=["Average"],
            periods=[300],
        ),
        
        # Throttling metrics
        MetricConfiguration(
            metric_name="ReadThrottleEvents",
            statistics=["Sum"],
            periods=[60, 300],
        ),
        MetricConfiguration(
            metric_name="WriteThrottleEvents",
            statistics=["Sum"],
            periods=[60, 300],
        ),
        
        # Request metrics
        MetricConfiguration(
            metric_name="UserErrors",
            statistics=["Sum"],
            periods=[60, 300],
        ),
        MetricConfiguration(
            metric_name="SystemErrors",
            statistics=["Sum"],
            periods=[60, 300],
        ),
    ]
    
    if not comprehensive:
        return essential_metrics
    
    # Additional comprehensive metrics
    comprehensive_metrics = essential_metrics + [
        # Latency metrics
        MetricConfiguration(
            metric_name="SuccessfulRequestLatency",
            statistics=["Average", "Maximum"],
            periods=[60, 300],
            operation="GetItem",
        ),
        MetricConfiguration(
            metric_name="SuccessfulRequestLatency",
            statistics=["Average", "Maximum"],
            periods=[60, 300],
            operation="PutItem",
        ),
        MetricConfiguration(
            metric_name="SuccessfulRequestLatency",
            statistics=["Average", "Maximum"],
            periods=[60, 300],
            operation="Query",
        ),
        MetricConfiguration(
            metric_name="SuccessfulRequestLatency",
            statistics=["Average", "Maximum"],
            periods=[60, 300],
            operation="Scan",
        ),
        
        # Conditional check failures
        MetricConfiguration(
            metric_name="ConditionalCheckFailedRequests",
            statistics=["Sum"],
            periods=[60, 300],
        ),
        
        # Transaction metrics
        MetricConfiguration(
            metric_name="TransactionConflict",
            statistics=["Sum"],
            periods=[60, 300],
        ),
        
        # Batch operation metrics
        MetricConfiguration(
            metric_name="SuccessfulRequestLatency",
            statistics=["Average", "Maximum"],
            periods=[60, 300],
            operation="BatchGetItem",
        ),
        MetricConfiguration(
            metric_name="SuccessfulRequestLatency",
            statistics=["Average", "Maximum"],
            periods=[60, 300],
            operation="BatchWriteItem",
        ),
    ]
    
    return comprehensive_metrics
