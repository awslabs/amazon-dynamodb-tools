"""
Capacity Mode Analysis for DynamoDB tables.

Analyzes whether tables should use On-Demand or Provisioned capacity mode,
with autoscaling simulation for accurate cost projections.
"""

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from decimal import Decimal
from functools import lru_cache

from ..config import get_settings
from ..logging import get_logger
from ..aws.pricing_collector import PricingCollector
from .autoscaling_sim import AutoscalingSimulator, MetricDataPoint

logger = get_logger(__name__)


@dataclass
class CapacityRecommendation:
    """Recommendation for capacity mode optimization."""
    
    account_id: str
    region: str
    table_name: str
    table_class: str  # STANDARD or STANDARD_INFREQUENT_ACCESS
    metric_type: str  # READ or WRITE
    
    # Current state
    current_mode: str  # ON_DEMAND or PROVISIONED
    current_min_capacity: Optional[int]
    current_max_capacity: Optional[int]
    current_target_utilization: Optional[float]
    autoscaling_enabled: bool
    
    # Cost analysis
    current_cost: float
    on_demand_cost: float
    provisioned_cost: float
    estimated_savings: float
    savings_percentage: float
    
    # Recommendation
    recommended_mode: str  # ON_DEMAND, PROVISIONED, or PROVISIONED_MODIFY
    recommended_min_capacity: Optional[int]
    recommended_max_capacity: Optional[int]
    recommended_target_utilization: Optional[float]
    
    # Analysis metadata
    analysis_days: int
    confidence_score: float
    optimization_status: str  # OPTIMIZED or NOT_OPTIMIZED
    note: Optional[str] = None
    analyzed_at: datetime = None
    
    def __post_init__(self):
        if self.analyzed_at is None:
            self.analyzed_at = datetime.now()


class CapacityModeAnalyzer:
    """Analyzes DynamoDB capacity modes and generates cost optimization recommendations."""
    
    def __init__(self, connection):
        """
        Initialize capacity mode analyzer.
        
        Args:
            connection: DuckDB database connection
        """
        self.connection = connection
        self.settings = get_settings()
        self.pricing_collector = PricingCollector()
    
    async def analyze_table(
        self,
        account_id: str,
        region: str,
        table_name: str,
        days: int = 14
    ) -> List[CapacityRecommendation]:
        """
        Analyze capacity mode for a specific table.
        
        Args:
            account_id: AWS account ID
            region: AWS region
            table_name: DynamoDB table name
            days: Number of days of metrics to analyze
        
        Returns:
            List of capacity recommendations (one per metric type)
        """
        # Reduced logging - only log at debug level for individual tables
        logger.debug(
            "Analyzing capacity mode",
            account_id=account_id,
            region=region,
            table_name=table_name,
            days=days
        )
        
        # Get table metadata
        table_info = self._get_table_info(account_id, region, table_name)
        if not table_info:
            logger.warning("Table not found in database", table_name=table_name)
            return []
        
        # Ensure pricing data is available
        await self.pricing_collector.collect_and_store_pricing(region, self.connection)
        
        # Get pricing data
        pricing = self._get_pricing(region)
        
        # Get CloudWatch metrics
        read_metrics, write_metrics = self._get_metrics(
            account_id, region, table_name, days
        )
        
        if not read_metrics or not write_metrics:
            logger.warning("Insufficient metrics for analysis", table_name=table_name)
            return []
        
        # Run autoscaling simulation
        simulator = AutoscalingSimulator(
            target_utilization=self.settings.autoscaling_target_utilization,
            min_capacity=self.settings.autoscaling_min_capacity,
            max_capacity=self.settings.autoscaling_max_capacity
        )
        
        simulated_read, simulated_write = simulator.simulate(read_metrics, write_metrics)
        
        # Calculate costs
        read_recommendation = self._generate_recommendation(
            account_id, region, table_name, table_info,
            "READ", read_metrics, simulated_read, pricing, days
        )
        
        write_recommendation = self._generate_recommendation(
            account_id, region, table_name, table_info,
            "WRITE", write_metrics, simulated_write, pricing, days
        )
        
        recommendations = [read_recommendation, write_recommendation]
        
        # Store recommendations in database
        self._store_recommendations(recommendations)
        
        logger.info(
            "Capacity analysis complete",
            table_name=table_name,
            read_savings=f"${read_recommendation.estimated_savings:.2f}",
            write_savings=f"${write_recommendation.estimated_savings:.2f}"
        )
        
        return recommendations
    
    def _get_table_info(
        self, account_id: str, region: str, table_name: str
    ) -> Optional[Dict]:
        """Get table metadata from database."""
        result = self.connection.execute(
            """
            SELECT 
                account_id, table_name, billing_mode,
                provisioned_read_capacity, provisioned_write_capacity
            FROM table_metadata
            WHERE region = ? AND table_name = ?
            """,
            (region, table_name)
        ).fetchone()
        
        if not result:
            return None
        
        return {
            "account_id": result[0],
            "table_name": result[1],
            "table_class": "STANDARD",  # Default to STANDARD (table_class not in schema)
            "billing_mode": result[2],
            "provisioned_read_capacity": result[3] if result[3] else 0,
            "provisioned_write_capacity": result[4] if result[4] else 0,
            "table_size_bytes": 0,  # Not available in current schema
            "item_count": 0  # Not available in current schema
        }
    
    @lru_cache(maxsize=32)
    def _get_pricing(self, region: str) -> Dict[str, float]:
        """
        Get pricing data for the region (cached).
        
        Filters out free tier and returns actual paid prices.
        """
        # Query for non-free-tier pricing (where begin_range != "0" or price > 0 and end_range = "Inf")
        results = self.connection.execute(
            """
            SELECT usage_type, operation, price_per_unit
            FROM pricing_data
            WHERE region = ?
              AND price_per_unit > 0
              AND end_range = 'Inf'
            """,
            (region,)
        ).fetchall()
        
        # Map usage_type and operation to logical pricing keys
        pricing = {}
        
        for usage_type, operation, price in results:
            if not usage_type:
                continue
                
            # On-Demand pricing (PayPerRequestThroughput)
            if operation == "PayPerRequestThroughput":
                if "ReadRequestUnits" in usage_type and "IA" not in usage_type and "Repl" not in usage_type:
                    pricing["on_demand_read"] = float(price)
                elif "WriteRequestUnits" in usage_type and "IA" not in usage_type and "Repl" not in usage_type:
                    pricing["on_demand_write"] = float(price)
                elif "IA-ReadRequestUnits" in usage_type:
                    pricing["on_demand_read_ia"] = float(price)
                elif "IA-WriteRequestUnits" in usage_type:
                    pricing["on_demand_write_ia"] = float(price)
            
            # Provisioned pricing (CommittedThroughput)
            elif operation == "CommittedThroughput":
                if "ReadCapacityUnit-Hrs" in usage_type and "IA" not in usage_type and "Repl" not in usage_type:
                    pricing["provisioned_read"] = float(price)
                elif "WriteCapacityUnit-Hrs" in usage_type and "IA" not in usage_type and "Repl" not in usage_type:
                    pricing["provisioned_write"] = float(price)
                elif "IA-ReadCapacityUnit-Hrs" in usage_type:
                    pricing["provisioned_read_ia"] = float(price)
                elif "IA-WriteCapacityUnit-Hrs" in usage_type:
                    pricing["provisioned_write_ia"] = float(price)
        
        logger.debug(f"Loaded pricing for {region}", pricing_keys=list(pricing.keys()))
        return pricing
    
    def _get_metrics(
        self,
        account_id: str,
        region: str,
        table_name: str,
        days: int
    ) -> Tuple[List[MetricDataPoint], List[MetricDataPoint]]:
        """Get CloudWatch metrics from database."""
        cutoff_date = datetime.now() - timedelta(days=days)
        
        # Get read metrics from metrics table (resource_name for both tables and GSIs)
        read_results = self.connection.execute(
            """
            SELECT timestamp, value
            FROM metrics
            WHERE region = ? AND resource_name = ?
                AND metric_name = 'ConsumedReadCapacityUnits'
                AND statistic = 'Sum'
                AND timestamp >= ?
            ORDER BY timestamp ASC
            """,
            (region, table_name, cutoff_date)
        ).fetchall()
        
        # Get write metrics
        write_results = self.connection.execute(
            """
            SELECT timestamp, value
            FROM metrics
            WHERE region = ? AND resource_name = ?
                AND metric_name = 'ConsumedWriteCapacityUnits'
                AND statistic = 'Sum'
                AND timestamp >= ?
            ORDER BY timestamp ASC
            """,
            (region, table_name, cutoff_date)
        ).fetchall()
        
        read_metrics = [
            MetricDataPoint(
                metric_name="ConsumedReadCapacityUnits",
                timestamp=row[0],
                table_name=table_name,
                consumed_units=float(row[1]),
                units_per_second=float(row[1]) / 60.0
            )
            for row in read_results
        ]
        
        write_metrics = [
            MetricDataPoint(
                metric_name="ConsumedWriteCapacityUnits",
                timestamp=row[0],
                table_name=table_name,
                consumed_units=float(row[1]),
                units_per_second=float(row[1]) / 60.0
            )
            for row in write_results
        ]
        
        return read_metrics, write_metrics
    
    def _calculate_costs(
        self,
        metrics: List[MetricDataPoint],
        simulated: List[MetricDataPoint],
        pricing: Dict[str, Decimal],
        is_read: bool,
        table_class: str,
        current_provisioned_capacity: Optional[int] = None,
        analysis_days: int = 14
    ) -> Tuple[float, float, float]:
        """
        Calculate projected costs for different capacity modes.
        
        Note: These are CALCULATED costs based on pricing and capacity, not actual AWS bills.
        Actual costs from CUR data may differ due to discounts, reserved capacity, etc.
        
        Returns:
            Tuple of (on_demand_cost, current_provisioned_cost, optimal_provisioned_cost)
        """
        # Determine pricing keys based on metric type and table class
        if table_class == "STANDARD_INFREQUENT_ACCESS":
            on_demand_key = "on_demand_read" if is_read else "on_demand_write"
            provisioned_key = "provisioned_read_ia" if is_read else "provisioned_write_ia"
        else:
            on_demand_key = "on_demand_read" if is_read else "on_demand_write"
            provisioned_key = "provisioned_read" if is_read else "provisioned_write"
        
        on_demand_price = float(pricing.get(on_demand_key, 0))
        provisioned_price = float(pricing.get(provisioned_key, 0))
        
        # Calculate On-Demand cost (based on consumption in analysis period, scaled to monthly)
        total_consumed_in_period = sum(m.consumed_units for m in metrics)
        # Scale to monthly: (consumed in period / analysis days) * 30.4 days
        monthly_consumed = (total_consumed_in_period / analysis_days) * 30.4
        on_demand_cost = monthly_consumed * on_demand_price
        
        # Calculate cost for CURRENT provisioned capacity (fixed capacity, not autoscaled)
        # IMPORTANT: Calculate MONTHLY costs (730 hours) not analysis period costs
        if current_provisioned_capacity and current_provisioned_capacity > 0:
            # Current fixed provisioned capacity cost: capacity * 730 hours * price_per_hour
            monthly_hours = 730  # Standard month hours for cost projections
            current_provisioned_cost = current_provisioned_capacity * monthly_hours * provisioned_price
        else:
            current_provisioned_cost = 0.0
        
        # Calculate OPTIMAL provisioned cost (based on autoscaling simulation)
        # Scale from analysis period to monthly
        total_provisioned_hours_in_period = sum(m.provisioned_units for m in simulated) / 60.0  # Convert minutes to hours
        # Scale to monthly: (hours in period / analysis hours) * monthly hours
        analysis_hours = analysis_days * 24
        monthly_hours = 730
        optimal_provisioned_cost = (total_provisioned_hours_in_period / analysis_hours) * monthly_hours * provisioned_price
        
        return on_demand_cost, current_provisioned_cost, optimal_provisioned_cost
    
    def _generate_recommendation(
        self,
        account_id: str,
        region: str,
        table_name: str,
        table_info: Dict,
        metric_type: str,
        metrics: List[MetricDataPoint],
        simulated: List[MetricDataPoint],
        pricing: Dict[str, Decimal],
        days: int
    ) -> CapacityRecommendation:
        """Generate a capacity recommendation."""
        is_read = (metric_type == "READ")
        table_class = table_info["table_class"]
        current_mode = "PROVISIONED" if table_info["billing_mode"] == "PROVISIONED" else "ON_DEMAND"
        
        # Get current provisioned capacity
        current_provisioned_capacity = (
            table_info["provisioned_read_capacity"] if is_read 
            else table_info["provisioned_write_capacity"]
        )
        
        # Calculate costs (returns 3 values: on-demand, current provisioned, optimal provisioned)
        on_demand_cost, current_provisioned_cost, optimal_provisioned_cost = self._calculate_costs(
            metrics, simulated, pricing, is_read, table_class,
            current_provisioned_capacity=current_provisioned_capacity,
            analysis_days=days
        )
        
        # Determine current cost based on billing mode
        current_cost = current_provisioned_cost if current_mode == "PROVISIONED" else on_demand_cost
        
        # Determine recommendation
        overprovision_threshold = 0.15  # 15% overprovision threshold
        
        if current_mode == "PROVISIONED":
            # Compare: current fixed provisioned vs on-demand vs optimal autoscaled provisioned
            if optimal_provisioned_cost < on_demand_cost and optimal_provisioned_cost < current_provisioned_cost:
                # Optimal provisioned with autoscaling is best
                if (current_provisioned_cost - optimal_provisioned_cost) / current_provisioned_cost > overprovision_threshold:
                    recommended_mode = "PROVISIONED_MODIFY"
                    recommended_cost = optimal_provisioned_cost
                else:
                    recommended_mode = "PROVISIONED"
                    recommended_cost = current_provisioned_cost
            elif on_demand_cost < current_provisioned_cost:
                # On-demand is better than current fixed provisioned
                recommended_mode = "ON_DEMAND"
                recommended_cost = on_demand_cost
            else:
                # Current fixed provisioned is already optimal
                recommended_mode = "PROVISIONED"
                recommended_cost = current_provisioned_cost
        else:
            # Currently ON_DEMAND
            if optimal_provisioned_cost < on_demand_cost:
                recommended_mode = "PROVISIONED"
                recommended_cost = optimal_provisioned_cost
            else:
                recommended_mode = "ON_DEMAND"
                recommended_cost = on_demand_cost
        
        # Calculate savings
        estimated_savings = max(0, current_cost - recommended_cost)
        savings_percentage = (estimated_savings / current_cost * 100) if current_cost > 0 else 0
        
        # Determine optimization status
        optimization_status = "OPTIMIZED" if recommended_mode == current_mode else "NOT_OPTIMIZED"
        
        # Calculate confidence score (based on data completeness and variability)
        expected_points = days * 24 * 60  # One per minute
        actual_points = len(metrics)
        data_completeness = min(actual_points / expected_points, 1.0) if expected_points > 0 else 0
        confidence_score = data_completeness * 0.8 + 0.2  # Base confidence 20%, up to 100%
        
        return CapacityRecommendation(
            account_id=account_id,
            region=region,
            table_name=table_name,
            table_class=table_class,
            metric_type=metric_type,
            current_mode=current_mode,
            current_min_capacity=current_provisioned_capacity if current_mode == "PROVISIONED" else None,
            current_max_capacity=current_provisioned_capacity if current_mode == "PROVISIONED" else None,
            current_target_utilization=self.settings.autoscaling_target_utilization if current_mode == "PROVISIONED" else None,
            autoscaling_enabled=(current_mode == "PROVISIONED"),
            current_cost=current_cost,
            on_demand_cost=on_demand_cost,
            provisioned_cost=optimal_provisioned_cost,  # Store optimal provisioned cost for comparison
            estimated_savings=estimated_savings,
            savings_percentage=savings_percentage,
            recommended_mode=recommended_mode,
            recommended_min_capacity=self.settings.autoscaling_min_capacity if "PROVISIONED" in recommended_mode else None,
            recommended_max_capacity=self.settings.autoscaling_max_capacity if "PROVISIONED" in recommended_mode else None,
            recommended_target_utilization=self.settings.autoscaling_target_utilization if "PROVISIONED" in recommended_mode else None,
            analysis_days=days,
            confidence_score=confidence_score,
            optimization_status=optimization_status,
            note=f"Current capacity: {current_provisioned_capacity} units. Costs are calculated based on pricing, not CUR actuals."
        )
    
    def _store_recommendations(self, recommendations: List[CapacityRecommendation]) -> None:
        """Store recommendations in the database (aggregating READ/WRITE into single record)."""
        import uuid
        from datetime import datetime
        
        # Group recommendations by table (should be 2: READ and WRITE)
        if len(recommendations) != 2:
            logger.warning(f"Expected 2 recommendations (READ/WRITE), got {len(recommendations)}")
            return
        
        # Aggregate READ and WRITE recommendations
        read_rec = next((r for r in recommendations if r.metric_type == "READ"), None)
        write_rec = next((r for r in recommendations if r.metric_type == "WRITE"), None)
        
        if not read_rec or not write_rec:
            logger.warning("Missing READ or WRITE recommendation")
            return
        
        # Generate recommendation ID
        rec_id = str(uuid.uuid4())
        
        # Calculate totals
        total_current_cost = read_rec.current_cost + write_rec.current_cost
        total_savings = read_rec.estimated_savings + write_rec.estimated_savings
        savings_pct = (total_savings / total_current_cost * 100) if total_current_cost > 0 else 0
        
        # Determine overall billing mode recommendation
        if read_rec.current_mode == "ON_DEMAND" or write_rec.current_mode == "ON_DEMAND":
            current_billing_mode = "PAY_PER_REQUEST"
        else:
            current_billing_mode = "PROVISIONED"
        
        if read_rec.recommended_mode == "ON_DEMAND" or write_rec.recommended_mode == "ON_DEMAND":
            recommended_billing_mode = "PAY_PER_REQUEST"
        else:
            recommended_billing_mode = "PROVISIONED"
        
        # Insert aggregated recommendation
        self.connection.execute(
            """
            INSERT OR REPLACE INTO capacity_mode_recommendations (
                recommendation_id, account_id, region, table_name,
                current_billing_mode, recommended_billing_mode,
                analysis_start_date, analysis_end_date, analysis_days,
                current_monthly_cost_usd, current_read_cost_usd, current_write_cost_usd,
                projected_monthly_cost_usd, projected_read_cost_usd, projected_write_cost_usd,
                monthly_savings_usd, annual_savings_usd, savings_percentage,
                avg_provisioned_rcu, avg_provisioned_wcu,
                max_provisioned_rcu, max_provisioned_wcu,
                min_provisioned_rcu, min_provisioned_wcu,
                avg_read_utilization, avg_write_utilization,
                peak_read_utilization, peak_write_utilization,
                confidence_score, risk_level, recommendation_reason,
                created_at, status
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                rec_id,
                read_rec.account_id,
                read_rec.region,
                read_rec.table_name,
                current_billing_mode,
                recommended_billing_mode,
                datetime.now() - timedelta(days=read_rec.analysis_days),  # analysis_start_date
                datetime.now(),  # analysis_end_date
                read_rec.analysis_days,
                total_current_cost,  # current_monthly_cost_usd
                read_rec.current_cost,  # current_read_cost_usd
                write_rec.current_cost,  # current_write_cost_usd
                total_current_cost - total_savings,  # projected_monthly_cost_usd
                read_rec.current_cost - read_rec.estimated_savings,  # projected_read_cost_usd
                write_rec.current_cost - write_rec.estimated_savings,  # projected_write_cost_usd
                total_savings,  # monthly_savings_usd
                total_savings * 12,  # annual_savings_usd
                savings_pct,  # savings_percentage
                read_rec.recommended_min_capacity,  # avg_provisioned_rcu
                write_rec.recommended_min_capacity,  # avg_provisioned_wcu
                read_rec.recommended_max_capacity,  # max_provisioned_rcu
                write_rec.recommended_max_capacity,  # max_provisioned_wcu
                read_rec.recommended_min_capacity,  # min_provisioned_rcu
                write_rec.recommended_min_capacity,  # min_provisioned_wcu
                None,  # avg_read_utilization (not calculated yet)
                None,  # avg_write_utilization (not calculated yet)
                None,  # peak_read_utilization (not calculated yet)
                None,  # peak_write_utilization (not calculated yet)
                (read_rec.confidence_score + write_rec.confidence_score) / 2,  # confidence_score
                "low",  # risk_level (default)
                f"Analysis: {read_rec.note}",  # recommendation_reason
                datetime.now(),  # created_at
                "pending"  # status
            )
        )
        
        self.connection.commit()
        logger.debug(f"Stored aggregated capacity recommendation for {read_rec.table_name}")
    
    async def analyze_all_tables(self, days: int = 14) -> List[CapacityRecommendation]:
        """
        Analyze capacity mode for all discovered tables.
        
        Args:
            days: Number of days of metrics to analyze
        
        Returns:
            List of all capacity recommendations
        """
        # Clear existing recommendations once at the start
        self.connection.execute("DELETE FROM capacity_mode_recommendations")
        self.connection.commit()
        
        # Get all tables from database with account_id
        tables = self.connection.execute(
            """
            SELECT DISTINCT account_id, region, table_name
            FROM table_metadata
            ORDER BY account_id, region, table_name
            """
        ).fetchall()
        
        logger.info(f"Analyzing capacity mode for {len(tables)} tables")
        
        all_recommendations = []
        
        for account_id, region, table_name in tables:
            try:
                recommendations = await self.analyze_table(
                    account_id, region, table_name, days
                )
                all_recommendations.extend(recommendations)
            except Exception as e:
                logger.error(
                    f"Failed to analyze table {table_name}",
                    error=str(e),
                    region=region,
                    account_id=account_id
                )
        
        logger.info(
            f"Capacity analysis complete: {len(all_recommendations)} recommendations generated"
        )
        
        return all_recommendations
