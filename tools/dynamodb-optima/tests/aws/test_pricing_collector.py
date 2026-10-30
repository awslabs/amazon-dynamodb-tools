"""
Tests for AWS Pricing API collector.

Built on recorded `get-products` responses in tests/fixtures/pricing so the
parser is exercised against the real payload shape (OnDemand + Reserved terms,
free-tier dimensions, prefixed and unprefixed usage types).
"""

import json
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dynamodb_optima.aws.pricing_collector import PricingCollector, AMERICAN_REGIONS

FIXTURES = Path(__file__).resolve().parent.parent / "fixtures" / "pricing"


def load_products(region_code: str) -> list:
    """Recorded get-products PriceList items (already JSON-decoded) for a region."""
    return json.loads((FIXTURES / f"get_products_{region_code}.json").read_text())["PriceList"]


def product_for(region_code: str, usage_type: str) -> dict:
    return next(
        p for p in load_products(region_code)
        if p["product"]["attributes"].get("usagetype") == usage_type
    )


def mock_pricing_session(pages: list[list[dict]]) -> MagicMock:
    """A session whose pricing client returns `pages` from successive get_products calls."""
    client = MagicMock()
    responses = []
    for i, page in enumerate(pages):
        response = {"PriceList": [json.dumps(p) for p in page]}
        if i < len(pages) - 1:
            response["NextToken"] = f"token-{i + 1}"
        responses.append(response)
    client.get_products = AsyncMock(side_effect=responses)
    client.describe_services = AsyncMock(
        return_value={"Services": [{"AttributeNames": ["regionCode", "usagetype"]}]}
    )
    session = MagicMock()
    session.client.return_value.__aenter__ = AsyncMock(return_value=client)
    session.client.return_value.__aexit__ = AsyncMock(return_value=False)
    return session


@pytest.fixture
def collector():
    return PricingCollector()


def stored_count(conn, region_code: str) -> int:
    return conn.execute(
        "SELECT COUNT(*) FROM pricing_data WHERE region_code = ?", (region_code,)
    ).fetchone()[0]


@pytest.mark.unit
@pytest.mark.aws
class TestPricingCollector:
    """Test AWS Pricing collector."""

    def test_pricing_api_region_selection(self, collector):
        assert collector._get_pricing_api_region("us-west-2") == "us-east-1"
        assert collector._get_pricing_api_region("eu-west-1") == "ap-south-1"

    def test_parse_on_demand_dimensions(self, collector):
        """Every OnDemand price dimension becomes a record, free tier included."""
        product = product_for("us-west-2", "USW2-WriteCapacityUnit-Hrs")
        records = collector._parse_product(product, "us-west-2", [])
        on_demand = [r for r in records if r["term_type"] == "OnDemand"]

        assert sorted((r["begin_range"], r["end_range"], r["price_per_unit"]) for r in on_demand) == [
            ("0", "18600", 0.0),
            ("18600", "Inf", 0.00065),
        ]
        for r in on_demand:
            assert r["region_code"] == "us-west-2"
            assert r["usage_type"] == "USW2-WriteCapacityUnit-Hrs"
            assert r["operation"] == "CommittedThroughput"
            assert r["lease_contract_length"] is None

    @pytest.mark.asyncio
    async def test_collect_region_pricing_follows_pagination(self, collector):
        products = load_products("us-west-2")
        collector.session = mock_pricing_session([products[:10], products[10:]])

        records = await collector.collect_region_pricing("us-west-2", [])

        assert {r["usage_type"] for r in records} == {
            p["product"]["attributes"]["usagetype"] for p in products
        }

    @pytest.mark.asyncio
    async def test_store_pricing_replaces_region_rows(self, collector, db_connection):
        records = collector._parse_product(
            product_for("us-west-2", "USW2-ReadRequestUnits"), "us-west-2", []
        )
        await collector.store_pricing_in_database("us-west-2", records, db_connection)
        await collector.store_pricing_in_database("us-west-2", records, db_connection)

        assert stored_count(db_connection, "us-west-2") == len(records)

    @pytest.mark.asyncio
    async def test_is_pricing_stale(self, collector, db_connection):
        assert await collector.is_pricing_stale("us-west-2", db_connection) is True

        records = collector._parse_product(
            product_for("us-west-2", "USW2-ReadRequestUnits"), "us-west-2", []
        )
        await collector.store_pricing_in_database("us-west-2", records, db_connection)
        assert await collector.is_pricing_stale("us-west-2", db_connection) is False

        db_connection.execute(
            "UPDATE pricing_data SET collected_at = ? WHERE region_code = ?",
            (datetime.now() - timedelta(days=35), "us-west-2"),
        )
        assert await collector.is_pricing_stale("us-west-2", db_connection) is True

    @pytest.mark.asyncio
    async def test_collect_and_store_skips_fresh(self, collector, db_connection):
        records = collector._parse_product(
            product_for("us-west-2", "USW2-ReadRequestUnits"), "us-west-2", []
        )
        await collector.store_pricing_in_database("us-west-2", records, db_connection)
        collector.session = mock_pricing_session([load_products("us-west-2")])

        await collector.collect_and_store_pricing("us-west-2", db_connection)

        collector.session.client.assert_not_called()
        assert stored_count(db_connection, "us-west-2") == len(records)

    @pytest.mark.asyncio
    async def test_collect_and_store_force_refresh(self, collector, db_connection):
        records = collector._parse_product(
            product_for("us-west-2", "USW2-ReadRequestUnits"), "us-west-2", []
        )
        await collector.store_pricing_in_database("us-west-2", records, db_connection)
        collector.session = mock_pricing_session([load_products("us-west-2")])

        await collector.collect_and_store_pricing("us-west-2", db_connection, force_refresh=True)

        assert stored_count(db_connection, "us-west-2") > len(records)


@pytest.mark.unit
def test_american_regions_list():
    """Test AMERICAN_REGIONS constant."""
    assert "us-east-1" in AMERICAN_REGIONS
    assert "us-west-2" in AMERICAN_REGIONS
    assert "ca-central-1" in AMERICAN_REGIONS
    assert "eu-west-1" not in AMERICAN_REGIONS
