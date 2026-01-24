"""
Table class analysis for DynamoDB Standard ↔ Standard-IA recommendations.

Analyzes CUR data to determine if tables should switch storage classes based on
storage-to-throughput cost ratios.
"""

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import List, Optional, Dict, Any
from decimal import Decimal

from ..logging import get_logger
from ..database.connection import get_connection

logger = get_logger(__name__)


@dataclass
class TableClassRecommendation:
    """Recommendation for table class optimization."""
    
    table_name: str
    account_id: str
    region: str
    payer_account_id: str
    
    current_class: str  # 'STANDARD' or 'STANDARD_IA'
    recommended_class: str  # 'STANDARD' or 'STANDARD_IA'
    
    # Cost analysis
    current_monthly_storage_cost: Decimal
    current_monthly_throughput_cost: Decimal
    current_monthly_total_cost: Decimal
    
    projected_monthly_storage_cost: Decimal
    projected_monthly_throughput_cost: Decimal
    projected_monthly_total_cost: Decimal
    
    potential_monthly_savings: Decimal
    potential_annual_savings: Decimal
    
    # Analysis metadata
    analysis_start_date: datetime
    analysis_end_date: datetime
    analysis_days: int
    uses_reserved_capacity: bool
    
    # Rationale
    storage_to_throughput_ratio: Decimal
    breakeven_ratio: Decimal
    recommendation_reason: str


class TableClassAnalyzer:
    """
    Analyze table class and generate Standard ↔ Standard-IA recommendations.
    
    Based on table_class_optimizer logic, adapted to use DuckDB cur_data table.
    """
    
    # Breakeven ratios from AWS pricing
    # Standard → Standard-IA: storage_cost > (0.25/0.6) * throughput_cost
    STANDARD_TO_IA_RATIO = Decimal('0.25') / Decimal('0.6')  # ~0.4167
    
    # Standard-IA → Standard: storage_cost < (0.2/1.5) * throughput_cost  
    IA_TO_STANDARD_RATIO = Decimal('0.2') / Decimal('1.5')  # ~0.1333
    
    def __init__(self, min_monthly_savings: Decimal = Decimal('50.0')):
        """
        Initialize table class analyzer.
        
        Args:
            min_monthly_savings: Minimum monthly savings to recommend (default: $50)
        """
        self.min_monthly_savings = min_monthly_savings
        self.conn = get_connection()
    
    def analyze_tables(
        self,
        months: int = 3,
        account_ids: Optional[List[str]] = None,
        regions: Optional[List[str]] = None,
        table_names: Optional[List[str]] = None
    ) -> List[TableClassRecommendation]:
        """
        Analyze tables and generate table class recommendations.
        
        Args:
            months: Number of months to analyze (default: 3)
            account_ids: Filter by account IDs (None = all accounts)
            regions: Filter by regions (None = all regions)
            table_names: Filter by table names (None = all tables)
        
        Returns:
            List of TableClassRecommendation objects
        """
        logger.info(
            "Starting table class analysis",
            months=months,
            min_savings=float(self.min_monthly_savings)
        )
        
        # Calculate date range
        end_date = datetime.now()
        start_date = end_date - timedelta(days=months * 30)
        
        # Build and execute aggregation query
        aggregated_data = self._aggregate_cur_data(
            start_date=start_date,
            end_date=end_date,
            account_ids=account_ids,
            regions=regions,
            table_names=table_names
        )
        
        # Generate recommendations
        recommendations = []
        for row in aggregated_data:
            rec = self._generate_recommendation(row, start_date, end_date)
            if rec and rec.potential_monthly_savings >= self.min_monthly_savings:
                recommendations.append(rec)
        
        logger.info(
            "Table class analysis complete",
            total_tables=len(aggregated_data),
            recommendations=len(recommendations),
            total_potential_savings=sum(r.potential_monthly_savings for r in recommendations)
        )
        
        return recommendations
    
    def _aggregate_cur_data(
        self,
        start_date: datetime,
        end_date: datetime,
        account_ids: Optional[List[str]],
        regions: Optional[List[str]],
        table_names: Optional[List[str]]
    ) -> List[Dict[str, Any]]:
        """
        Aggregate CUR data by table for analysis.
        
        Adapted from table_class_optimizer/DDB_TableClassReco.sql
        """
        # Build WHERE clause filters
        filters = [
            "resource_name IS NOT NULL",
            "usage_month >= ?",
            "usage_month <= ?"
        ]
        params = [start_date.date(), end_date.date()]
        
        if account_ids:
            filters.append("account_id IN ({})".format(','.join('?' * len(account_ids))))
            params.extend(account_ids)
        
        if regions:
            filters.append("region IN ({})".format(','.join('?' * len(regions))))
            params.extend(regions)
        
        if table_names:
            filters.append("resource_name IN ({})".format(','.join('?' * len(table_names))))
            params.extend(table_names)
        
        where_clause = " AND ".join(filters)
        
        # Aggregate query matching original SQL logic
        query = f"""
        WITH aggregated AS (
            SELECT
                account_id,
                region,
                resource_name AS table_name,
                MIN(usage_start_date) AS usage_start_date,
                MAX(usage_end_date) AS usage_end_date,
                
                -- Check for reserved capacity (pricing not in CUR, so check operation type)
                MAX(CASE WHEN operation LIKE '%Commit%' THEN 1 ELSE 0 END) AS uses_reservations,
                
                -- Standard throughput cost (RCU/WCU, not IA)
                SUM(CASE 
                    WHEN (usage_type LIKE '%RequestUnits%' OR usage_type LIKE '%CapacityUnit-Hrs%')
                         AND usage_type NOT LIKE '%IA%'
                    THEN net_unblended_cost
                    ELSE 0
                END) AS actual_throughput_cost,
                
                -- Standard storage cost (not IA)
                SUM(CASE 
                    WHEN usage_type LIKE '%TimedStorage-ByteHrs%'
                         AND usage_type NOT LIKE '%IA%'
                    THEN net_unblended_cost
                    ELSE 0
                END) AS actual_storage_cost,
                
                -- Standard-IA throughput cost
                SUM(CASE 
                    WHEN (usage_type LIKE '%RequestUnits%' OR usage_type LIKE '%CapacityUnit-Hrs%')
                         AND usage_type LIKE '%IA%'
                    THEN net_unblended_cost
                    ELSE 0
                END) AS actual_throughput_cost_ia,
                
                -- Standard-IA storage cost
                SUM(CASE 
                    WHEN usage_type LIKE '%TimedStorage-ByteHrs%'
                         AND usage_type LIKE '%IA%'
                    THEN net_unblended_cost
                    ELSE 0
                END) AS actual_storage_cost_ia
                
            FROM cur_data
            WHERE {where_clause}
            GROUP BY account_id, region, resource_name
        )
        SELECT
            account_id,
            region,
            table_name,
            usage_start_date,
            usage_end_date,
            uses_reservations,
            actual_throughput_cost,
            actual_storage_cost,
            actual_throughput_cost_ia,
            actual_storage_cost_ia,
            GREATEST(DATE_DIFF('day', usage_start_date, usage_end_date), 1) AS active_days
        FROM aggregated
        WHERE uses_reservations = 0  -- Exclude tables with reserved capacity
        """
        
        result = self.conn.execute(query, params).fetchall()
        
        # Convert to list of dicts
        columns = [
            'account_id', 'region', 'table_name', 'usage_start_date', 'usage_end_date',
            'uses_reservations', 'actual_throughput_cost', 'actual_storage_cost',
            'actual_throughput_cost_ia', 'actual_storage_cost_ia', 'active_days'
        ]
        
        return [dict(zip(columns, row)) for row in result]
    
    def _generate_recommendation(
        self,
        row: Dict[str, Any],
        start_date: datetime,
        end_date: datetime
    ) -> Optional[TableClassRecommendation]:
        """
        Generate recommendation for a single table.
        
        Applies breakeven formulas from original SQL.
        """
        # Extract values
        account_id = row['account_id']
        region = row['region']
        table_name = row['table_name']
        active_days = row['active_days']
        
        # Convert costs to monthly
        days_to_month = Decimal('30.416')  # Average days per month
        month_multiplier = days_to_month / Decimal(str(active_days))
        
        std_throughput = Decimal(str(row['actual_throughput_cost'])) * month_multiplier
        std_storage = Decimal(str(row['actual_storage_cost'])) * month_multiplier
        ia_throughput = Decimal(str(row['actual_throughput_cost_ia'])) * month_multiplier
        ia_storage = Decimal(str(row['actual_storage_cost_ia'])) * month_multiplier
        
        # Determine current class and calculate recommendation
        if std_throughput > 0 or std_storage > 0:
            # Currently using Standard
            current_class = 'STANDARD'
            current_monthly_storage = std_storage
            current_monthly_throughput = std_throughput
            
            # Check if should move to Standard-IA
            # Formula: storage_cost > (0.25/0.6) * throughput_cost
            # Special case: If throughput is zero or negligible, IA is almost always better
            if std_throughput > Decimal('0.01'):
                # Calculate ratio for tables with meaningful throughput
                ratio = std_storage / std_throughput
                should_recommend_ia = ratio > self.STANDARD_TO_IA_RATIO
                reason_suffix = f"Storage-to-throughput ratio ({ratio:.3f}) exceeds Standard→IA breakeven ({self.STANDARD_TO_IA_RATIO:.3f})"
            else:
                # For tables with negligible/zero throughput (On-Demand tables)
                # IA is almost always better due to 60% storage savings
                ratio = Decimal('999.99')  # Infinite ratio
                should_recommend_ia = std_storage > Decimal('1.0')  # Only if storage cost > $1/month
                reason_suffix = "Table has storage costs but negligible throughput (On-Demand). IA provides 60% storage savings"
            
            if should_recommend_ia:
                # Calculate projected costs if moved to IA
                # IA storage is 40% of Standard (0.1 vs 0.25 per GB-month)
                # IA throughput is 2.5x Standard (1.5625 vs 0.625 per million)
                projected_storage = std_storage * Decimal('0.4')  # 0.1 / 0.25
                projected_throughput = std_throughput * Decimal('2.5')  # 1.5625 / 0.625
                
                savings = (std_storage + std_throughput) - (projected_storage + projected_throughput)
                
                if savings > 0:
                    return TableClassRecommendation(
                        table_name=table_name,
                        account_id=account_id,
                        region=region,
                        payer_account_id=account_id,  # TODO: Get from CUR if available
                        current_class='STANDARD',
                        recommended_class='STANDARD_IA',
                        current_monthly_storage_cost=current_monthly_storage,
                        current_monthly_throughput_cost=current_monthly_throughput,
                        current_monthly_total_cost=current_monthly_storage + current_monthly_throughput,
                        projected_monthly_storage_cost=projected_storage,
                        projected_monthly_throughput_cost=projected_throughput,
                        projected_monthly_total_cost=projected_storage + projected_throughput,
                        potential_monthly_savings=savings,
                        potential_annual_savings=savings * 12,
                        analysis_start_date=start_date,
                        analysis_end_date=end_date,
                        analysis_days=active_days,
                        uses_reserved_capacity=False,
                        storage_to_throughput_ratio=ratio,
                        breakeven_ratio=self.STANDARD_TO_IA_RATIO,
                        recommendation_reason=f"{reason_suffix}. Move to Standard-IA to reduce storage costs."
                    )
        
        elif ia_throughput > 0 or ia_storage > 0:
            # Currently using Standard-IA
            current_class = 'STANDARD_IA'
            current_monthly_storage = ia_storage
            current_monthly_throughput = ia_throughput
            
            # Check if should move to Standard
            # Formula: storage_cost < (0.2/1.5) * throughput_cost
            if ia_throughput > 0:
                ratio = ia_storage / ia_throughput
                if ratio < self.IA_TO_STANDARD_RATIO:
                    # Calculate projected costs if moved to Standard
                    projected_storage = ia_storage * Decimal('2.5')  # 0.25 / 0.1
                    projected_throughput = ia_throughput * Decimal('0.4')  # 0.625 / 1.5625
                    
                    savings = (ia_storage + ia_throughput) - (projected_storage + projected_throughput)
                    
                    if savings > 0:
                        return TableClassRecommendation(
                            table_name=table_name,
                            account_id=account_id,
                            region=region,
                            payer_account_id=account_id,
                            current_class='STANDARD_IA',
                            recommended_class='STANDARD',
                            current_monthly_storage_cost=current_monthly_storage,
                            current_monthly_throughput_cost=current_monthly_throughput,
                            current_monthly_total_cost=current_monthly_storage + current_monthly_throughput,
                            projected_monthly_storage_cost=projected_storage,
                            projected_monthly_throughput_cost=projected_throughput,
                            projected_monthly_total_cost=projected_storage + projected_throughput,
                            potential_monthly_savings=savings,
                            potential_annual_savings=savings * 12,
                            analysis_start_date=start_date,
                            analysis_end_date=end_date,
                            analysis_days=active_days,
                            uses_reserved_capacity=False,
                            storage_to_throughput_ratio=ratio,
                            breakeven_ratio=self.IA_TO_STANDARD_RATIO,
                            recommendation_reason=(
                                f"Storage-to-throughput ratio ({ratio:.3f}) below "
                                f"IA→Standard breakeven ({self.IA_TO_STANDARD_RATIO:.3f}). "
                                f"Move to Standard to reduce throughput costs."
                            )
                        )
        
        return None  # No recommendation
    
    def save_recommendations(
        self,
        recommendations: List[TableClassRecommendation]
    ) -> None:
        """
        Save recommendations to database.
        
        Args:
            recommendations: List of recommendations to save
        """
        if not recommendations:
            logger.info("No recommendations to save")
            return
        
        # Clear existing recommendations
        self.conn.execute("DELETE FROM table_class_recommendations")
        
        # Insert new recommendations
        for rec in recommendations:
            self.conn.execute("""
                INSERT INTO table_class_recommendations (
                    recommendation_id,
                    account_id,
                    region,
                    table_name,
                    current_table_class,
                    recommended_table_class,
                    analysis_start_date,
                    analysis_end_date,
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
                    storage_to_throughput_ratio,
                    breakeven_ratio,
                    is_above_breakeven,
                    has_reserved_capacity,
                    recommendation_reason,
                    created_at,
                    status
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, 'pending')
            """, (
                f"{rec.account_id}:{rec.region}:{rec.table_name}",
                rec.account_id,
                rec.region,
                rec.table_name,
                rec.current_class,
                rec.recommended_class,
                rec.analysis_start_date,
                rec.analysis_end_date,
                rec.analysis_days // 30,
                float(rec.current_monthly_storage_cost),
                float(rec.current_monthly_throughput_cost),
                float(rec.current_monthly_total_cost),
                float(rec.projected_monthly_storage_cost),
                float(rec.projected_monthly_throughput_cost),
                float(rec.projected_monthly_total_cost),
                float(rec.potential_monthly_savings),
                float(rec.potential_annual_savings),
                float((rec.potential_monthly_savings / rec.current_monthly_total_cost * 100) if rec.current_monthly_total_cost > 0 else 0),
                float(rec.storage_to_throughput_ratio),
                float(rec.breakeven_ratio),
                rec.storage_to_throughput_ratio > rec.breakeven_ratio if rec.current_class == 'STANDARD' else rec.storage_to_throughput_ratio < rec.breakeven_ratio,
                rec.uses_reserved_capacity,
                rec.recommendation_reason
            ))
        
        self.conn.commit()
        logger.info(f"Saved {len(recommendations)} recommendations to database")
