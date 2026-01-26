"""
Utilization analysis for DynamoDB tables.

Identifies underutilized provisioned capacity and generates recommendations
to reduce costs by adjusting capacity or switching to On-Demand mode.
"""

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from functools import lru_cache

from ..logging import get_logger
from ..aws.pricing_collector import PricingCollector

logger = get_logger(__name__)


@dataclass
class UtilizationRecommendation:
    """Recommendation based on underutilized provisioned capacity."""
    
    account_id: str
    region: str
    table_name: str
    resource_name: str  # table_name or table_name#gsi_name
    resource_type: str  # 'TABLE' or 'GSI'
    
    # Utilization metrics
    read_utilization_pct: float
    write_utilization_pct: float
    avg_utilization_pct: float
    
    # Current capacity
    provisioned_read_capacity: int
    provisioned_write_capacity: int
    
    # Average consumption
    avg_consumed_read_capacity: float
    avg_consumed_write_capacity: float
    
    # Recommendations
    recommendation_type: str  # 'REDUCE_CAPACITY', 'SWITCH_TO_ON_DEMAND', 'OK'
    recommended_read_capacity: Optional[int]
    recommended_write_capacity: Optional[int]
    potential_monthly_savings: float
    
    # Analysis metadata
    analysis_days: int
    data_points: int
    confidence_score: float
    rationale: str
    created_at: datetime


class UtilizationAnalyzer:
    """
    Analyzer for utilization-based recommendations.
    
    Identifies underutilized provisioned capacity and suggests optimizations.
    """
    
    def __init__(self, connection):
        """
        Initialize utilization analyzer.
        
        Args:
            connection: DuckDB database connection
        """
        self.connection = connection
        self.pricing_collector = PricingCollector()
    
    def analyze_all_tables(
        self,
        days: int = 14,
        utilization_threshold: float = 45.0,
        min_savings: float = 10.0
    ) -> List[UtilizationRecommendation]:
        """
        Analyze utilization for all tables with provisioned capacity.
        
        Args:
            days: Number of days of metrics to analyze
            utilization_threshold: Flag resources below this utilization %
            min_savings: Minimum monthly savings to recommend
        
        Returns:
            List of utilization recommendations
        """
        logger.info(
            "Analyzing utilization for all tables",
            days=days,
            threshold=utilization_threshold
        )
        
        # Get all provisioned tables from metadata
        provisioned_resources = self._get_provisioned_resources()
        
        logger.debug(f"Found {len(provisioned_resources)} provisioned resources")
        
        recommendations = []
        
        for resource in provisioned_resources:
            try:
                rec = self._analyze_resource(
                    resource,
                    days,
                    utilization_threshold,
                    min_savings
                )
                
                if rec and rec.recommendation_type != 'OK':
                    recommendations.append(rec)
                    
            except Exception as e:
                logger.error(
                    f"Failed to analyze resource {resource['resource_name']}",
                    error=str(e)
                )
        
        logger.info(
            f"Utilization analysis complete: {len(recommendations)} recommendations"
        )
        
        # Store recommendations in database
        if recommendations:
            self._store_recommendations(recommendations)
        
        return recommendations
    
    def analyze_table(
        self,
        region: str,
        table_name: str,
        days: int = 14,
        utilization_threshold: float = 45.0,
        min_savings: float = 10.0
    ) -> List[UtilizationRecommendation]:
        """
        Analyze utilization for a specific table.
        
        Args:
            region: AWS region
            table_name: DynamoDB table name
            days: Number of days to analyze
            utilization_threshold: Flag if below this %
            min_savings: Minimum monthly savings
        
        Returns:
            List of recommendations (table + GSIs)
        """
        logger.info(
            "Analyzing utilization for table",
            table=table_name,
            region=region
        )
        
        # Get table and GSI info
        resources = self._get_table_resources(region, table_name)
        
        recommendations = []
        
        for resource in resources:
            try:
                rec = self._analyze_resource(
                    resource,
                    days,
                    utilization_threshold,
                    min_savings
                )
                
                if rec and rec.recommendation_type != 'OK':
                    recommendations.append(rec)
                    
            except Exception as e:
                logger.error(
                    f"Failed to analyze resource {resource['resource_name']}",
                    error=str(e)
                )
        
        return recommendations
    
    def _get_provisioned_resources(self) -> List[dict]:
        """Get all provisioned tables and GSIs from database."""
        # Get tables with provisioned capacity
        tables = self.connection.execute(
            """
            SELECT 
                account_id,
                region,
                table_name,
                table_name as resource_name,
                'TABLE' as resource_type,
                provisioned_read_capacity,
                provisioned_write_capacity
            FROM table_metadata
            WHERE billing_mode = 'PROVISIONED'
                AND provisioned_read_capacity > 0
                AND provisioned_write_capacity > 0
            """
        ).fetchall()
        
        # Get GSIs with provisioned capacity
        gsis = self.connection.execute(
            """
            SELECT 
                account_id,
                region,
                table_name,
                resource_name,
                'GSI' as resource_type,
                provisioned_read_capacity,
                provisioned_write_capacity
            FROM gsi_metadata
            WHERE provisioned_read_capacity > 0
                AND provisioned_write_capacity > 0
            """
        ).fetchall()
        
        # Convert to list of dicts
        resources = []
        
        for row in tables:
            resources.append({
                'account_id': row[0],
                'region': row[1],
                'table_name': row[2],
                'resource_name': row[3],
                'resource_type': row[4],
                'provisioned_read_capacity': row[5],
                'provisioned_write_capacity': row[6]
            })
        
        for row in gsis:
            resources.append({
                'account_id': row[0],
                'region': row[1],
                'table_name': row[2],
                'resource_name': row[3],
                'resource_type': row[4],
                'provisioned_read_capacity': row[5],
                'provisioned_write_capacity': row[6]
            })
        
        return resources
    
    def _get_table_resources(
        self, region: str, table_name: str
    ) -> List[dict]:
        """Get table and GSI resources for a specific table."""
        # Get table
        table = self.connection.execute(
            """
            SELECT 
                account_id,
                region,
                table_name,
                table_name as resource_name,
                'TABLE' as resource_type,
                provisioned_read_capacity,
                provisioned_write_capacity
            FROM table_metadata
            WHERE region = ? AND table_name = ?
                AND billing_mode = 'PROVISIONED'
            """,
            (region, table_name)
        ).fetchone()
        
        # Get GSIs for this table
        gsis = self.connection.execute(
            """
            SELECT 
                account_id,
                region,
                table_name,
                resource_name,
                'GSI' as resource_type,
                provisioned_read_capacity,
                provisioned_write_capacity
            FROM gsi_metadata
            WHERE region = ? AND table_name = ?
                AND provisioned_read_capacity > 0
            """,
            (region, table_name)
        ).fetchall()
        
        resources = []
        
        if table:
            resources.append({
                'account_id': table[0],
                'region': table[1],
                'table_name': table[2],
                'resource_name': table[3],
                'resource_type': table[4],
                'provisioned_read_capacity': table[5],
                'provisioned_write_capacity': table[6]
            })
        
        for row in gsis:
            resources.append({
                'account_id': row[0],
                'region': row[1],
                'table_name': row[2],
                'resource_name': row[3],
                'resource_type': row[4],
                'provisioned_read_capacity': row[5],
                'provisioned_write_capacity': row[6]
            })
        
        return resources
    
    @lru_cache(maxsize=32)
    def _get_pricing(self, region: str) -> Dict[str, float]:
        """
        Get pricing data for the region (cached). Raises exception if pricing missing.
        
        Filters out free tier and returns actual paid prices.
        """
        # Query for non-free-tier pricing
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
            
            # Provisioned pricing (CommittedThroughput)
            elif operation == "CommittedThroughput":
                if "ReadCapacityUnit-Hrs" in usage_type and "IA" not in usage_type and "Repl" not in usage_type:
                    pricing["provisioned_read"] = float(price)
                elif "WriteCapacityUnit-Hrs" in usage_type and "IA" not in usage_type and "Repl" not in usage_type:
                    pricing["provisioned_write"] = float(price)
        
        # STRICT VALIDATION - no defaults!
        required_keys = ['provisioned_read', 'provisioned_write', 'on_demand_read', 'on_demand_write']
        missing = [k for k in required_keys if k not in pricing]
        if missing:
            raise ValueError(
                f"Missing required pricing data for {region}: {missing}. "
                "Run pricing collection first with: dynamodb-optima collect-pricing"
            )
        
        logger.debug(f"Loaded pricing for {region}", pricing_keys=list(pricing.keys()))
        return pricing
    
    def _analyze_resource(
        self,
        resource: dict,
        days: int,
        utilization_threshold: float,
        min_savings: float
    ) -> Optional[UtilizationRecommendation]:
        """Analyze a single resource for utilization."""
        # Get consumed capacity metrics
        read_consumed, write_consumed = self._get_consumed_metrics(
            resource['region'],
            resource['resource_name'],
            resource['resource_type'],
            days
        )
        
        if not read_consumed or not write_consumed:
            logger.debug(
                f"Insufficient metrics for {resource['resource_name']}"
            )
            return None
        
        # Calculate average consumption (per second)
        avg_read_consumed = sum(read_consumed) / len(read_consumed)
        avg_write_consumed = sum(write_consumed) / len(write_consumed)
        
        # Calculate utilization percentages
        prov_read = resource['provisioned_read_capacity']
        prov_write = resource['provisioned_write_capacity']
        
        read_util = (avg_read_consumed / prov_read * 100) if prov_read > 0 else 0
        write_util = (avg_write_consumed / prov_write * 100) if prov_write > 0 else 0
        avg_util = (read_util + write_util) / 2
        
        # Get pricing data for the region
        pricing = self._get_pricing(resource['region'])
        
        # Determine recommendation
        recommendation_type, recommended_read, recommended_write, savings = (
            self._generate_recommendation(
                prov_read,
                prov_write,
                avg_read_consumed,
                avg_write_consumed,
                read_util,
                write_util,
                utilization_threshold,
                pricing
            )
        )
        
        # Skip if savings below threshold
        if savings < min_savings:
            recommendation_type = 'OK'
        
        # Calculate confidence score
        data_completeness = min(len(read_consumed) / (days * 24 * 60), 1.0)
        confidence = data_completeness * 0.8 + 0.2
        
        # Build rationale
        rationale = self._build_rationale(
            recommendation_type,
            read_util,
            write_util,
            prov_read,
            prov_write,
            avg_read_consumed,
            avg_write_consumed
        )
        
        return UtilizationRecommendation(
            account_id=resource.get('account_id', 'default'),
            region=resource['region'],
            table_name=resource['table_name'],
            resource_name=resource['resource_name'],
            resource_type=resource['resource_type'],
            read_utilization_pct=read_util,
            write_utilization_pct=write_util,
            avg_utilization_pct=avg_util,
            provisioned_read_capacity=prov_read,
            provisioned_write_capacity=prov_write,
            avg_consumed_read_capacity=avg_read_consumed,
            avg_consumed_write_capacity=avg_write_consumed,
            recommendation_type=recommendation_type,
            recommended_read_capacity=recommended_read,
            recommended_write_capacity=recommended_write,
            potential_monthly_savings=savings,
            analysis_days=days,
            data_points=len(read_consumed),
            confidence_score=confidence,
            rationale=rationale,
            created_at=datetime.now()
        )
    
    def _get_consumed_metrics(
        self,
        region: str,
        resource_name: str,
        resource_type: str,
        days: int
    ) -> Tuple[List[float], List[float]]:
        """Get consumed capacity metrics from database."""
        cutoff = datetime.now() - timedelta(days=days)
        
        # Get read consumed capacity (per second, converted from Sum/period)
        read_results = self.connection.execute(
            """
            SELECT value
            FROM metrics
            WHERE region = ?
                AND resource_name = ?
                AND metric_name = 'ConsumedReadCapacityUnits'
                AND statistic = 'Sum'
                AND timestamp >= ?
            ORDER BY timestamp
            """,
            (region, resource_name, cutoff)
        ).fetchall()
        
        # Get write consumed capacity
        write_results = self.connection.execute(
            """
            SELECT value
            FROM metrics
            WHERE region = ?
                AND resource_name = ?
                AND metric_name = 'ConsumedWriteCapacityUnits'
                AND statistic = 'Sum'
                AND timestamp >= ?
            ORDER BY timestamp
            """,
            (region, resource_name, cutoff)
        ).fetchall()
        
        # Convert Sum to per-second by dividing by 60 (1-minute period)
        read_consumed = [row[0] / 60.0 for row in read_results]
        write_consumed = [row[0] / 60.0 for row in write_results]
        
        return read_consumed, write_consumed
    
    def _generate_recommendation(
        self,
        prov_read: int,
        prov_write: int,
        avg_read: float,
        avg_write: float,
        read_util: float,
        write_util: float,
        threshold: float,
        pricing: Dict[str, float]
    ) -> Tuple[str, Optional[int], Optional[int], float]:
        """
        Generate recommendation based on utilization using real pricing data.
        
        Returns:
            Tuple of (recommendation_type, recommended_read, recommended_write, savings)
        """
        # If both below threshold significantly, consider On-Demand
        if read_util < threshold and write_util < threshold:
            if read_util < 30 and write_util < 30:
                # Very low utilization - switch to On-Demand
                # Calculate current provisioned cost using real pricing
                current_cost_read = prov_read * pricing['provisioned_read'] * 730  # hours per month
                current_cost_write = prov_write * pricing['provisioned_write'] * 730
                
                # Calculate On-Demand cost using real pricing
                # On-Demand pricing is per million requests
                # avg_read/write is in units per second, so convert to monthly requests
                seconds_per_month = 2.628e6  # 30.4 days * 24 hours * 3600 seconds
                on_demand_cost_read = (avg_read * seconds_per_month) * pricing['on_demand_read']
                on_demand_cost_write = (avg_write * seconds_per_month) * pricing['on_demand_write']
                
                savings = max(0, (current_cost_read + current_cost_write) - 
                                (on_demand_cost_read + on_demand_cost_write))
                
                return ('SWITCH_TO_ON_DEMAND', None, None, savings)
            
            else:
                # Moderate underutilization - reduce capacity
                # Recommend average consumption + 20% buffer
                recommended_read = max(5, int(avg_read * 1.2))
                recommended_write = max(5, int(avg_write * 1.2))
                
                # Calculate savings using real pricing
                read_savings = (prov_read - recommended_read) * pricing['provisioned_read'] * 730
                write_savings = (prov_write - recommended_write) * pricing['provisioned_write'] * 730
                savings = max(0, read_savings + write_savings)
                
                return ('REDUCE_CAPACITY', recommended_read, recommended_write, savings)
        
        # Utilization is acceptable
        return ('OK', None, None, 0.0)
    
    def _build_rationale(
        self,
        rec_type: str,
        read_util: float,
        write_util: float,
        prov_read: int,
        prov_write: int,
        avg_read: float,
        avg_write: float
    ) -> str:
        """Build human-readable rationale for recommendation."""
        if rec_type == 'SWITCH_TO_ON_DEMAND':
            return (
                f"Very low utilization (Read: {read_util:.1f}%, Write: {write_util:.1f}%). "
                f"Provisioned capacity ({prov_read} RCU, {prov_write} WCU) significantly "
                f"exceeds average consumption ({avg_read:.1f} RCU/s, {avg_write:.1f} WCU/s). "
                "Switch to On-Demand mode for cost optimization."
            )
        
        elif rec_type == 'REDUCE_CAPACITY':
            return (
                f"Low utilization (Read: {read_util:.1f}%, Write: {write_util:.1f}%). "
                f"Current capacity ({prov_read} RCU, {prov_write} WCU) can be reduced "
                f"based on average consumption ({avg_read:.1f} RCU/s, {avg_write:.1f} WCU/s)."
            )
        
        else:
            return (
                f"Utilization is acceptable (Read: {read_util:.1f}%, Write: {write_util:.1f}%)."
            )
    
    def _store_recommendations(self, recommendations: List[UtilizationRecommendation]) -> None:
        """Store utilization recommendations in the database."""
        import uuid
        
        # Clear existing recommendations before inserting new ones
        self.connection.execute("DELETE FROM utilization_recommendations")
        
        for rec in recommendations:
            # Generate recommendation ID
            rec_id = str(uuid.uuid4())
            
            # Insert recommendation
            self.connection.execute(
                """
                INSERT OR REPLACE INTO utilization_recommendations (
                    recommendation_id, account_id, region, table_name,
                    resource_name, resource_type,
                    current_provisioned_rcu, current_provisioned_wcu,
                    recommended_provisioned_rcu, recommended_provisioned_wcu,
                    monthly_savings_usd, annual_savings_usd,
                    avg_read_utilization, avg_write_utilization,
                    analysis_days, confidence_score,
                    recommendation_reason, created_at, status
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    rec_id,
                    rec.account_id,
                    rec.region,
                    rec.table_name,
                    rec.resource_name,
                    rec.resource_type,
                    rec.provisioned_read_capacity,
                    rec.provisioned_write_capacity,
                    rec.recommended_read_capacity,
                    rec.recommended_write_capacity,
                    rec.potential_monthly_savings,
                    rec.potential_monthly_savings * 12,  # annual savings
                    rec.read_utilization_pct,
                    rec.write_utilization_pct,
                    rec.analysis_days,
                    rec.confidence_score,
                    rec.rationale,
                    rec.created_at,
                    "pending"
                )
            )
        
        self.connection.commit()
        logger.debug(f"Stored {len(recommendations)} utilization recommendations")
