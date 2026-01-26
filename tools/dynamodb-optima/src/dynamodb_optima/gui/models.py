"""
Data models for GUI components.

Defines dataclasses for recommendations, filters, and summary statistics.
"""

from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass
class RecommendationFilter:
    """Filters for recommendation queries."""

    recommendation_type: Optional[str] = None  # 'capacity', 'table_class', 'utilization'
    min_savings: float = 0.0
    status_filter: Optional[str] = None  # 'pending', 'accepted', 'rejected', 'implemented', None for all
    region_filter: Optional[str] = None
    table_filter: Optional[str] = None
    account_filter: Optional[str] = None


@dataclass
class CapacityRecommendation:
    """Capacity mode recommendation for display."""

    recommendation_id: str
    table_name: str
    region: str
    account_id: str
    current_billing_mode: str
    recommended_billing_mode: str
    analysis_days: int
    current_monthly_cost_usd: float
    projected_monthly_cost_usd: float
    monthly_savings_usd: float
    annual_savings_usd: float
    savings_percentage: float
    confidence_score: float
    risk_level: str
    recommendation_reason: str
    created_at: datetime
    status: str
    # Optional fields
    avg_provisioned_rcu: Optional[int] = None
    avg_provisioned_wcu: Optional[int] = None
    avg_read_utilization: Optional[float] = None
    avg_write_utilization: Optional[float] = None


@dataclass
class TableClassRecommendation:
    """Table class recommendation for display."""

    recommendation_id: str
    table_name: str
    region: str
    account_id: str
    current_table_class: str
    recommended_table_class: str
    analysis_months: int
    current_monthly_storage_cost_usd: float
    current_monthly_throughput_cost_usd: float
    current_monthly_total_cost_usd: float
    projected_monthly_storage_cost_usd: float
    projected_monthly_throughput_cost_usd: float
    projected_monthly_total_cost_usd: float
    monthly_savings_usd: float
    annual_savings_usd: float
    savings_percentage: float
    avg_table_size_gb: float
    storage_to_throughput_ratio: float
    breakeven_ratio: float
    is_above_breakeven: bool
    confidence_score: float
    recommendation_reason: str
    created_at: datetime
    status: str


@dataclass
class UtilizationRecommendation:
    """Utilization recommendation for display."""

    recommendation_id: str
    table_name: str
    region: str
    account_id: str
    resource_type: str  # 'table', 'gsi'
    resource_name: str
    analysis_days: int
    current_provisioned_rcu: int
    current_provisioned_wcu: int
    recommended_provisioned_rcu: int
    recommended_provisioned_wcu: int
    current_monthly_cost_usd: float
    projected_monthly_cost_usd: float
    monthly_savings_usd: float
    annual_savings_usd: float
    savings_percentage: float
    avg_read_utilization: float
    avg_write_utilization: float
    max_read_utilization: float
    max_write_utilization: float
    confidence_score: float
    risk_level: str
    recommendation_reason: str
    created_at: datetime
    status: str


@dataclass
class SummaryStats:
    """Summary statistics for dashboard."""

    total_tables: int
    total_recommendations: int
    optimized_count: int
    not_optimized_count: int
    total_monthly_savings: float
    total_annual_savings: float
    capacity_savings: float
    table_class_savings: float
    utilization_savings: float
    capacity_count: int
    table_class_count: int
    utilization_count: int
