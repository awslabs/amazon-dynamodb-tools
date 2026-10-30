"""
Tests for the shared per-region price accessor (dynamodb_optima.pricing).
"""

import json
from datetime import datetime, timedelta
from pathlib import Path

import pytest

from dynamodb_optima.aws.pricing_collector import PricingCollector
from dynamodb_optima.pricing import (
    DEFAULT_REQUIRED,
    PRICE_KEYS,
    MissingPricingError,
    get_region_pricing,
)

FIXTURES = Path(__file__).resolve().parent / "fixtures" / "pricing"


async def store_fixture(conn, region_code: str) -> None:
    collector = PricingCollector()
    products = json.loads((FIXTURES / f"get_products_{region_code}.json").read_text())[
        "PriceList"
    ]
    records = [
        r for p in products for r in collector._parse_product(p, region_code, [])
    ]
    await collector.store_pricing_in_database(region_code, records, conn)


@pytest.mark.unit
class TestGetRegionPricing:

    @pytest.mark.asyncio
    async def test_prefixed_region_prices(self, db_connection):
        await store_fixture(db_connection, "us-west-2")

        pricing = get_region_pricing(db_connection, "us-west-2")

        assert pricing.region_code == "us-west-2"
        assert pricing.prices == pytest.approx(
            {
                "on_demand_read": 1.25e-07,
                "on_demand_write": 6.25e-07,
                "on_demand_read_ia": 1.55e-07,
                "on_demand_write_ia": 7.8e-07,
                "provisioned_read": 0.00013,
                "provisioned_write": 0.00065,
                "provisioned_read_ia": 0.00016,
                "provisioned_write_ia": 0.00081,
                "reserved_1yr_read_hourly": 0.000025,
                "reserved_1yr_read_upfront": 0.30,
                "reserved_1yr_write_hourly": 0.000128,
                "reserved_1yr_write_upfront": 1.50,
                "reserved_3yr_read_hourly": 0.000016,
                "reserved_3yr_read_upfront": 0.36,
                "reserved_3yr_write_hourly": 0.000081,
                "reserved_3yr_write_upfront": 1.80,
                "storage": 0.25,
                "storage_ia": 0.10,
                "replicated_write_on_demand": 6.25e-07,
                "replicated_write_on_demand_ia": 7.8e-07,
                "replicated_write_provisioned": 0.00065,
                "replicated_write_provisioned_ia": 0.00081,
                "export_full": 0.10,
                "export_incremental": 0.10,
                "import": 0.15,
                "restore": 0.15,
                "backup_storage": 0.10,
                "pitr_storage": 0.20,
                "kinesis_cdc": 1e-07,
                "streams_read": 2e-07,
            }
        )
        assert set(pricing.prices) == set(PRICE_KEYS)

    @pytest.mark.asyncio
    async def test_unprefixed_region_does_not_confuse_ia_and_standard(
        self, db_connection
    ):
        """us-east-1: unprefixed throughput usage types, prefixed export/PITR ones."""
        await store_fixture(db_connection, "us-east-1")

        pricing = get_region_pricing(db_connection, "us-east-1")

        assert pricing["on_demand_read"] == pytest.approx(1.25e-07)
        assert pricing["on_demand_read_ia"] == pytest.approx(1.55e-07)
        assert pricing["storage"] == pytest.approx(0.25)
        assert pricing["storage_ia"] == pytest.approx(0.10)
        assert pricing["export_full"] == pytest.approx(0.10)
        assert pricing["pitr_storage"] == pytest.approx(0.20)
        assert set(pricing.prices) == set(PRICE_KEYS)

    @pytest.mark.asyncio
    async def test_regional_prices_differ(self, db_connection):
        await store_fixture(db_connection, "eu-west-1")
        await store_fixture(db_connection, "us-west-2")

        eu = get_region_pricing(db_connection, "eu-west-1")

        assert eu["provisioned_write"] == pytest.approx(0.000735)
        assert eu["on_demand_write"] == pytest.approx(7.05e-07)

    @pytest.mark.asyncio
    async def test_free_tier_is_never_returned(self, db_connection):
        await store_fixture(db_connection, "us-west-2")

        pricing = get_region_pricing(db_connection, "us-west-2")

        assert all(price > 0 for price in pricing.prices.values())

    @pytest.mark.asyncio
    async def test_collected_at_is_latest_collection(self, db_connection):
        await store_fixture(db_connection, "us-west-2")
        old = datetime.now() - timedelta(days=10)
        db_connection.execute("UPDATE pricing_data SET collected_at = ?", (old,))

        pricing = get_region_pricing(db_connection, "us-west-2")

        assert abs(pricing.collected_at - old) < timedelta(seconds=1)

    def test_region_without_data_raises(self, db_connection):
        with pytest.raises(MissingPricingError, match="us-west-2"):
            get_region_pricing(db_connection, "us-west-2")

    @pytest.mark.asyncio
    async def test_missing_required_price_raises_naming_it(self, db_connection):
        await store_fixture(db_connection, "us-west-2")
        db_connection.execute(
            "DELETE FROM pricing_data WHERE usage_type = 'USW2-IA-WriteRequestUnits'"
        )

        with pytest.raises(MissingPricingError, match="on_demand_write_ia") as exc:
            get_region_pricing(db_connection, "us-west-2")
        assert exc.value.missing == ["on_demand_write_ia"]
        assert exc.value.region_code == "us-west-2"

    @pytest.mark.asyncio
    async def test_optional_prices_are_omitted_not_defaulted(self, db_connection):
        await store_fixture(db_connection, "us-west-2")
        db_connection.execute("DELETE FROM pricing_data WHERE term_type = 'Reserved'")

        pricing = get_region_pricing(db_connection, "us-west-2")

        assert not any(k.startswith("reserved_") for k in pricing.prices)
        with pytest.raises(MissingPricingError, match="reserved_1yr_write_hourly"):
            get_region_pricing(
                db_connection, "us-west-2", required=("reserved_1yr_write_hourly",)
            )

    @pytest.mark.asyncio
    async def test_legacy_rows_without_term_type_count_as_on_demand(
        self, db_connection
    ):
        await store_fixture(db_connection, "us-west-2")
        db_connection.execute("DELETE FROM pricing_data WHERE term_type = 'Reserved'")
        db_connection.execute("UPDATE pricing_data SET term_type = NULL")

        pricing = get_region_pricing(db_connection, "us-west-2")

        assert pricing["provisioned_write"] == pytest.approx(0.00065)

    def test_unknown_required_key_is_rejected(self, db_connection):
        with pytest.raises(ValueError, match="not_a_price"):
            get_region_pricing(db_connection, "us-west-2", required=("not_a_price",))

    def test_default_required_is_core_throughput_and_storage(self):
        assert set(DEFAULT_REQUIRED) == {
            "on_demand_read",
            "on_demand_write",
            "on_demand_read_ia",
            "on_demand_write_ia",
            "provisioned_read",
            "provisioned_write",
            "provisioned_read_ia",
            "provisioned_write_ia",
            "storage",
            "storage_ia",
        }
