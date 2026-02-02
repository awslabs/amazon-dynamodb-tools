"""
Database query functions for GUI.

Handles querying recommendations from DuckDB with filtering support.
"""

from typing import List, Optional
from datetime import datetime

from .models import (
    CapacityRecommendation,
    RecommendationFilter,
    SummaryStats,
    TableClassRecommendation,
    UtilizationRecommendation,
)


def get_capacity_recommendations(
    connection, filters: RecommendationFilter
) -> List[CapacityRecommendation]:
    """Query capacity mode recommendations from database."""
    query = """
        SELECT 
            recommendation_id,
            table_name,
            region,
            account_id,
            current_billing_mode,
            recommended_billing_mode,
            analysis_days,
            current_monthly_cost_usd,
            projected_monthly_cost_usd,
            monthly_savings_usd,
            annual_savings_usd,
            savings_percentage,
            confidence_score,
            risk_level,
            recommendation_reason,
            created_at,
            status,
            avg_provisioned_rcu,
            avg_provisioned_wcu,
            avg_read_utilization,
            avg_write_utilization
        FROM capacity_mode_recommendations
        WHERE monthly_savings_usd >= ?
    """

    params = [filters.min_savings]

    if filters.region_filter:
        query += " AND region = ?"
        params.append(filters.region_filter)

    if filters.table_filter:
        query += " AND regexp_matches(table_name, ?)"
        params.append(filters.table_filter)

    if filters.status_filter:
        query += " AND status = ?"
        params.append(filters.status_filter)

    if filters.account_filter:
        query += " AND account_id = ?"
        params.append(filters.account_filter)

    query += " ORDER BY monthly_savings_usd DESC"

    try:
        results = connection.execute(query, params).fetchall()
    except Exception as e:
        # Handle invalid regex pattern
        if filters.table_filter and ("regex" in str(e).lower() or "invalid" in str(e).lower()):
            raise ValueError(f"Invalid regex pattern '{filters.table_filter}': {str(e)}")
        raise

    recommendations = []
    for row in results:
        recommendations.append(
            CapacityRecommendation(
                recommendation_id=row[0],
                table_name=row[1],
                region=row[2],
                account_id=row[3],
                current_billing_mode=row[4],
                recommended_billing_mode=row[5],
                analysis_days=row[6],
                current_monthly_cost_usd=float(row[7]) if row[7] else 0.0,
                projected_monthly_cost_usd=float(row[8]) if row[8] else 0.0,
                monthly_savings_usd=float(row[9]) if row[9] else 0.0,
                annual_savings_usd=float(row[10]) if row[10] else 0.0,
                savings_percentage=float(row[11]) if row[11] else 0.0,
                confidence_score=float(row[12]) if row[12] else 0.0,
                risk_level=row[13] or "unknown",
                recommendation_reason=row[14] or "",
                created_at=row[15],
                status=row[16] or "pending",
                avg_provisioned_rcu=row[17],
                avg_provisioned_wcu=row[18],
                avg_read_utilization=float(row[19]) if row[19] else None,
                avg_write_utilization=float(row[20]) if row[20] else None,
            )
        )

    return recommendations


def get_table_class_recommendations(
    connection, filters: RecommendationFilter
) -> List[TableClassRecommendation]:
    """Query table class recommendations from database."""
    query = """
        SELECT 
            recommendation_id,
            table_name,
            region,
            account_id,
            current_table_class,
            recommended_table_class,
            analysis_months,
            current_monthly_storage_cost_usd,
            current_monthly_throughput_cost_usd,
            current_monthly_total_cost_usd,
            projected_monthly_storage_cost_usd,
            projected_monthly_throughput_cost_usd,
            projected_monthly_total_cost_usd,
            monthly_savings_usd,
            annual_savings_usd,
            savings_percentage,
            avg_table_size_gb,
            storage_to_throughput_ratio,
            breakeven_ratio,
            is_above_breakeven,
            confidence_score,
            recommendation_reason,
            created_at,
            status
        FROM table_class_recommendations
        WHERE monthly_savings_usd >= ?
    """

    params = [filters.min_savings]

    if filters.region_filter:
        query += " AND region = ?"
        params.append(filters.region_filter)

    if filters.table_filter:
        query += " AND regexp_matches(table_name, ?)"
        params.append(filters.table_filter)

    if filters.status_filter:
        query += " AND status = ?"
        params.append(filters.status_filter)

    if filters.account_filter:
        query += " AND account_id = ?"
        params.append(filters.account_filter)

    query += " ORDER BY monthly_savings_usd DESC"

    try:
        results = connection.execute(query, params).fetchall()
    except Exception as e:
        # Handle invalid regex pattern
        if filters.table_filter and ("regex" in str(e).lower() or "invalid" in str(e).lower()):
            raise ValueError(f"Invalid regex pattern '{filters.table_filter}': {str(e)}")
        raise

    recommendations = []
    for row in results:
        recommendations.append(
            TableClassRecommendation(
                recommendation_id=row[0],
                table_name=row[1],
                region=row[2],
                account_id=row[3],
                current_table_class=row[4],
                recommended_table_class=row[5],
                analysis_months=row[6],
                current_monthly_storage_cost_usd=float(row[7]) if row[7] else 0.0,
                current_monthly_throughput_cost_usd=float(row[8]) if row[8] else 0.0,
                current_monthly_total_cost_usd=float(row[9]) if row[9] else 0.0,
                projected_monthly_storage_cost_usd=float(row[10]) if row[10] else 0.0,
                projected_monthly_throughput_cost_usd=float(row[11]) if row[11] else 0.0,
                projected_monthly_total_cost_usd=float(row[12]) if row[12] else 0.0,
                monthly_savings_usd=float(row[13]) if row[13] else 0.0,
                annual_savings_usd=float(row[14]) if row[14] else 0.0,
                savings_percentage=float(row[15]) if row[15] else 0.0,
                avg_table_size_gb=float(row[16]) if row[16] else 0.0,
                storage_to_throughput_ratio=float(row[17]) if row[17] else 0.0,
                breakeven_ratio=float(row[18]) if row[18] else 0.0,
                is_above_breakeven=bool(row[19]) if row[19] is not None else False,
                confidence_score=float(row[20]) if row[20] else 0.0,
                recommendation_reason=row[21] or "",
                created_at=row[22],
                status=row[23] or "pending",
            )
        )

    return recommendations


def get_utilization_recommendations(
    connection, filters: RecommendationFilter
) -> List[UtilizationRecommendation]:
    """Query utilization recommendations from database."""
    query = """
        SELECT 
            recommendation_id,
            table_name,
            region,
            account_id,
            resource_type,
            resource_name,
            analysis_days,
            current_provisioned_rcu,
            current_provisioned_wcu,
            recommended_provisioned_rcu,
            recommended_provisioned_wcu,
            current_monthly_cost_usd,
            projected_monthly_cost_usd,
            monthly_savings_usd,
            annual_savings_usd,
            savings_percentage,
            avg_read_utilization,
            avg_write_utilization,
            max_read_utilization,
            max_write_utilization,
            confidence_score,
            risk_level,
            recommendation_reason,
            created_at,
            status
        FROM utilization_recommendations
        WHERE monthly_savings_usd >= ?
    """

    params = [filters.min_savings]

    if filters.region_filter:
        query += " AND region = ?"
        params.append(filters.region_filter)

    if filters.table_filter:
        query += " AND regexp_matches(table_name, ?)"
        params.append(filters.table_filter)

    if filters.status_filter:
        query += " AND status = ?"
        params.append(filters.status_filter)

    if filters.account_filter:
        query += " AND account_id = ?"
        params.append(filters.account_filter)

    query += " ORDER BY monthly_savings_usd DESC"

    try:
        results = connection.execute(query, params).fetchall()
    except Exception as e:
        # Handle invalid regex pattern
        if filters.table_filter and ("regex" in str(e).lower() or "invalid" in str(e).lower()):
            raise ValueError(f"Invalid regex pattern '{filters.table_filter}': {str(e)}")
        raise

    recommendations = []
    for row in results:
        recommendations.append(
            UtilizationRecommendation(
                recommendation_id=row[0],
                table_name=row[1],
                region=row[2],
                account_id=row[3],
                resource_type=row[4],
                resource_name=row[5],
                analysis_days=row[6],
                current_provisioned_rcu=row[7] or 0,
                current_provisioned_wcu=row[8] or 0,
                recommended_provisioned_rcu=row[9] or 0,
                recommended_provisioned_wcu=row[10] or 0,
                current_monthly_cost_usd=float(row[11]) if row[11] else 0.0,
                projected_monthly_cost_usd=float(row[12]) if row[12] else 0.0,
                monthly_savings_usd=float(row[13]) if row[13] else 0.0,
                annual_savings_usd=float(row[14]) if row[14] else 0.0,
                savings_percentage=float(row[15]) if row[15] else 0.0,
                avg_read_utilization=float(row[16]) if row[16] else 0.0,
                avg_write_utilization=float(row[17]) if row[17] else 0.0,
                max_read_utilization=float(row[18]) if row[18] else 0.0,
                max_write_utilization=float(row[19]) if row[19] else 0.0,
                confidence_score=float(row[20]) if row[20] else 0.0,
                risk_level=row[21] or "unknown",
                recommendation_reason=row[22] or "",
                created_at=row[23],
                status=row[24] or "pending",
            )
        )

    return recommendations


def get_summary_stats(connection) -> SummaryStats:
    """Calculate summary statistics across all recommendations."""
    # Get capacity stats
    capacity_query = """
        SELECT 
            COUNT(*) as count,
            COALESCE(SUM(monthly_savings_usd), 0) as monthly_savings,
            COUNT(DISTINCT table_name) as tables
        FROM capacity_mode_recommendations
        WHERE current_billing_mode != recommended_billing_mode
    """
    capacity_result = connection.execute(capacity_query).fetchone()
    capacity_count = capacity_result[0]
    capacity_savings = float(capacity_result[1])

    # Get table class stats
    table_class_query = """
        SELECT 
            COUNT(*) as count,
            COALESCE(SUM(monthly_savings_usd), 0) as monthly_savings,
            COUNT(DISTINCT table_name) as tables
        FROM table_class_recommendations
        WHERE current_table_class != recommended_table_class
    """
    table_class_result = connection.execute(table_class_query).fetchone()
    table_class_count = table_class_result[0]
    table_class_savings = float(table_class_result[1])

    # Get utilization stats
    utilization_query = """
        SELECT 
            COUNT(*) as count,
            COALESCE(SUM(monthly_savings_usd), 0) as monthly_savings,
            COUNT(DISTINCT table_name) as tables
        FROM utilization_recommendations
    """
    utilization_result = connection.execute(utilization_query).fetchone()
    utilization_count = utilization_result[0]
    utilization_savings = float(utilization_result[1])

    # Get unique table count across all recommendations
    unique_tables_query = """
        SELECT COUNT(DISTINCT table_name) FROM (
            SELECT DISTINCT table_name FROM capacity_mode_recommendations
            UNION
            SELECT DISTINCT table_name FROM table_class_recommendations
            UNION
            SELECT DISTINCT table_name FROM utilization_recommendations
        )
    """
    total_tables = connection.execute(unique_tables_query).fetchone()[0]

    total_recommendations = capacity_count + table_class_count + utilization_count
    total_monthly_savings = capacity_savings + table_class_savings + utilization_savings
    total_annual_savings = total_monthly_savings * 12

    # Count optimized vs not optimized
    optimized_query = """
        SELECT COUNT(*) FROM (
            SELECT table_name FROM capacity_mode_recommendations 
            WHERE current_billing_mode = recommended_billing_mode
            UNION ALL
            SELECT table_name FROM table_class_recommendations 
            WHERE current_table_class = recommended_table_class
        )
    """
    optimized_count = connection.execute(optimized_query).fetchone()[0]

    not_optimized_count = total_recommendations - optimized_count

    return SummaryStats(
        total_tables=total_tables,
        total_recommendations=total_recommendations,
        optimized_count=optimized_count,
        not_optimized_count=not_optimized_count,
        total_monthly_savings=total_monthly_savings,
        total_annual_savings=total_annual_savings,
        capacity_savings=capacity_savings,
        table_class_savings=table_class_savings,
        utilization_savings=utilization_savings,
        capacity_count=capacity_count,
        table_class_count=table_class_count,
        utilization_count=utilization_count,
    )


def get_available_regions(connection) -> List[str]:
    """Get list of regions with recommendations."""
    query = """
        SELECT DISTINCT region FROM (
            SELECT region FROM capacity_mode_recommendations
            UNION
            SELECT region FROM table_class_recommendations
            UNION
            SELECT region FROM utilization_recommendations
        )
        ORDER BY region
    """
    results = connection.execute(query).fetchall()
    return [row[0] for row in results]


def get_available_tables(connection, region: Optional[str] = None) -> List[str]:
    """Get list of tables with recommendations."""
    if region:
        query = """
            SELECT DISTINCT table_name FROM (
                SELECT table_name FROM capacity_mode_recommendations WHERE region = ?
                UNION
                SELECT table_name FROM table_class_recommendations WHERE region = ?
                UNION
                SELECT table_name FROM utilization_recommendations WHERE region = ?
            )
            ORDER BY table_name
        """
        params = [region, region, region]
    else:
        query = """
            SELECT DISTINCT table_name FROM (
                SELECT table_name FROM capacity_mode_recommendations
                UNION
                SELECT table_name FROM table_class_recommendations
                UNION
                SELECT table_name FROM utilization_recommendations
            )
            ORDER BY table_name
        """
        params = []

    results = connection.execute(query, params).fetchall()
    return [row[0] for row in results]


def get_available_accounts(connection) -> List[str]:
    """Get list of account IDs with recommendations."""
    query = """
        SELECT DISTINCT account_id FROM (
            SELECT account_id FROM capacity_mode_recommendations
            UNION
            SELECT account_id FROM table_class_recommendations
            UNION
            SELECT account_id FROM utilization_recommendations
        )
        ORDER BY account_id
    """
    results = connection.execute(query).fetchall()
    return [row[0] for row in results if row[0]]
