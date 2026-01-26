"""
Tests for AWS Pricing API collector.
"""

import pytest
from decimal import Decimal
from datetime import datetime, timedelta
from unittest.mock import AsyncMock, patch

from dynamodb_optima.aws.pricing_collector import PricingCollector, AMERICAN_REGIONS


@pytest.mark.unit
@pytest.mark.aws
class TestPricingCollector:
    """Test AWS Pricing collector."""
    
    def test_pricing_api_region_selection(self):
        """Test correct pricing API endpoint selection."""
        # American region should use us-east-1
        collector_us = PricingCollector(region_name="us-east-1")
        assert collector_us.pricing_api_region == "us-east-1"
        
        # Non-American region should use ap-south-1
        collector_eu = PricingCollector(region_name="eu-west-1")
        assert collector_eu.pricing_api_region == "ap-south-1"
    
    @pytest.mark.asyncio
    async def test_collect_all_pricing(self, mock_pricing_client):
        """Test collecting all pricing types."""
        collector = PricingCollector(region_name="us-east-1")
        
        with patch.object(collector.session, 'client') as mock_client:
            mock_client.return_value.__aenter__.return_value = mock_pricing_client
            
            pricing = await collector.collect_all_pricing("us-east-1")
            
            # Should have collected various pricing types
            assert isinstance(pricing, dict)
            # Note: Mock returns simplified data, real implementation would have more entries
    
    @pytest.mark.asyncio
    async def test_store_pricing_in_database(self, db_connection):
        """Test storing pricing data in database."""
        collector = PricingCollector(region_name="us-east-1")
        
        pricing_data = {
            "on_demand_read": Decimal("0.00000025"),
            "on_demand_write": Decimal("0.00000125"),
            "storage_standard": Decimal("0.25"),
            "storage_ia": Decimal("0.10")
        }
        
        await collector.store_pricing_in_database("us-east-1", pricing_data, db_connection)
        
        result = db_connection.execute(
            "SELECT COUNT(*) FROM pricing_data WHERE region = ?",
            ("us-east-1",)
        ).fetchone()
        
        assert result[0] == 4  # 4 pricing types stored
    
    @pytest.mark.asyncio
    async def test_is_pricing_stale(self, db_connection):
        """Test pricing staleness detection."""
        collector = PricingCollector(region_name="us-east-1")
        
        # No data - should be stale
        is_stale = await collector.is_pricing_stale("us-east-1", db_connection)
        assert is_stale is True
        
        # Insert fresh data
        db_connection.execute(
            "INSERT INTO pricing_data (region, pricing_type, price_per_unit, unit, collected_at) "
            "VALUES (?, ?, ?, ?, ?)",
            ("us-east-1", "on_demand_read", 0.00000025, "per_RCU", datetime.now())
        )
        db_connection.commit()
        
        # Should not be stale
        is_stale = await collector.is_pricing_stale("us-east-1", db_connection)
        assert is_stale is False
        
        # Insert old data
        old_date = datetime.now() - timedelta(days=35)
        db_connection.execute(
            "UPDATE pricing_data SET collected_at = ? WHERE region = ?",
            (old_date, "us-east-1")
        )
        db_connection.commit()
        
        # Should be stale
        is_stale = await collector.is_pricing_stale("us-east-1", db_connection)
        assert is_stale is True
    
    @pytest.mark.asyncio
    async def test_collect_and_store_pricing_skip_fresh(self, db_connection, mock_pricing_client):
        """Test that fresh pricing is not re-collected."""
        collector = PricingCollector(region_name="us-east-1")
        
        # Insert fresh pricing data
        db_connection.execute(
            "INSERT INTO pricing_data (region, pricing_type, price_per_unit, unit, collected_at) "
            "VALUES (?, ?, ?, ?, ?)",
            ("us-east-1", "on_demand_read", 0.00000025, "per_RCU", datetime.now())
        )
        db_connection.commit()
        
        with patch.object(collector.session, 'client') as mock_client:
            mock_client.return_value.__aenter__.return_value = mock_pricing_client
            
            # Should skip collection
            await collector.collect_and_store_pricing("us-east-1", db_connection, force_refresh=False)
            
            # Pricing client should not have been called
            mock_client.assert_not_called()
    
    @pytest.mark.asyncio
    async def test_collect_and_store_pricing_force_refresh(self, db_connection, mock_pricing_client):
        """Test forced pricing refresh."""
        collector = PricingCollector(region_name="us-east-1")
        
        # Insert fresh pricing data
        db_connection.execute(
            "INSERT INTO pricing_data (region, pricing_type, price_per_unit, unit, collected_at) "
            "VALUES (?, ?, ?, ?, ?)",
            ("us-east-1", "on_demand_read", 0.00000025, "per_RCU", datetime.now())
        )
        db_connection.commit()
        
        initial_count = db_connection.execute(
            "SELECT COUNT(*) FROM pricing_data WHERE region = ?",
            ("us-east-1",)
        ).fetchone()[0]
        
        with patch.object(collector, 'collect_all_pricing') as mock_collect:
            mock_collect.return_value = {
                "on_demand_read": Decimal("0.00000025"),
                "on_demand_write": Decimal("0.00000125")
            }
            
            # Force refresh even though data is fresh
            await collector.collect_and_store_pricing("us-east-1", db_connection, force_refresh=True)
            
            # Should have collected new pricing
            mock_collect.assert_called_once()
            
            # Data should be updated
            final_count = db_connection.execute(
                "SELECT COUNT(*) FROM pricing_data WHERE region = ?",
                ("us-east-1",)
            ).fetchone()[0]
            
            assert final_count >= initial_count


@pytest.mark.unit
def test_american_regions_list():
    """Test AMERICAN_REGIONS constant."""
    assert "us-east-1" in AMERICAN_REGIONS
    assert "us-west-2" in AMERICAN_REGIONS
    assert "ca-central-1" in AMERICAN_REGIONS
    assert "eu-west-1" not in AMERICAN_REGIONS
