"""
Regression tests for the analyzers' pricing readers.

Reserved Capacity rows share usage_type, operation and end_range='Inf' with the
On-Demand provisioned rows, so the readers must keep returning On-Demand prices
once Reserved rows are present in pricing_data.
"""

import json
import uuid
from datetime import datetime
from pathlib import Path

import pytest

from dynamodb_optima.analysis.capacity_mode import CapacityModeAnalyzer
from dynamodb_optima.analysis.utilization import UtilizationAnalyzer
from dynamodb_optima.aws.pricing_collector import PricingCollector

FIXTURES = Path(__file__).resolve().parent.parent / "fixtures" / "pricing"

# On-Demand list prices from the recorded fixtures, as each reader returns them.
STANDARD = (
    "on_demand_read",
    "on_demand_write",
    "provisioned_read",
    "provisioned_write",
)
EXPECTED = {
    "us-east-1": {
        "on_demand_read": 1.25e-07,
        "on_demand_write": 6.25e-07,
        "provisioned_read": 0.00013,
        "provisioned_write": 0.00065,
        "on_demand_read_ia": 1.55e-07,
        "on_demand_write_ia": 7.8e-07,
        "provisioned_read_ia": 0.00016,
        "provisioned_write_ia": 0.00081,
    },
    "us-west-2": {
        "on_demand_read": 1.25e-07,
        "on_demand_write": 6.25e-07,
        "provisioned_read": 0.00013,
        "provisioned_write": 0.00065,
        "on_demand_read_ia": 1.55e-07,
        "on_demand_write_ia": 7.8e-07,
        "provisioned_read_ia": 0.00016,
        "provisioned_write_ia": 0.00081,
    },
    "eu-west-1": {
        "on_demand_read": 1.415e-07,
        "on_demand_write": 7.05e-07,
        "provisioned_read": 0.000147,
        "provisioned_write": 0.000735,
        "on_demand_read_ia": 1.77e-07,
        "on_demand_write_ia": 8.85e-07,
        "provisioned_read_ia": 0.000184,
        "provisioned_write_ia": 0.000919,
    },
}


def expected_for(analyzer_cls, region_code: str) -> dict:
    """CapacityModeAnalyzer returns Standard + IA keys; Utilization only Standard."""
    prices = EXPECTED[region_code]
    if analyzer_cls is UtilizationAnalyzer:
        return {k: prices[k] for k in STANDARD}
    return prices


def load_products(region_code: str) -> list:
    return json.loads((FIXTURES / f"get_products_{region_code}.json").read_text())[
        "PriceList"
    ]


def reserved_records(region_code: str) -> list[dict]:
    """pricing_data rows for every Reserved price dimension in the fixture."""
    rows = []
    for product in load_products(region_code):
        attrs = product["product"]["attributes"]
        for term in product["terms"].get("Reserved", {}).values():
            term_attrs = term.get("termAttributes", {})
            for dim in term["priceDimensions"].values():
                rows.append(
                    {
                        "pricing_id": str(uuid.uuid4()),
                        "sku": product["product"]["sku"],
                        "region_code": region_code,
                        "location": attrs.get("location"),
                        "collected_at": datetime.now(),
                        "price_per_unit": float(dim["pricePerUnit"]["USD"]),
                        "unit": dim.get("unit"),
                        "currency": "USD",
                        "begin_range": dim.get("beginRange"),
                        "end_range": dim.get("endRange"),
                        "description": dim.get("description"),
                        "term_type": "Reserved",
                        "product_family": attrs.get("productFamily"),
                        "service_code": attrs.get("servicecode"),
                        "service_name": attrs.get("servicename"),
                        "location_type": attrs.get("locationType"),
                        "volume_type": attrs.get("volumeType"),
                        "usage_type": attrs.get("usagetype"),
                        "region": region_code,
                        "group_name": attrs.get("group"),
                        "group_description": attrs.get("groupDescription"),
                        "operation": attrs.get("operation"),
                        "lease_contract_length": term_attrs.get("LeaseContractLength"),
                        "purchase_option": term_attrs.get("PurchaseOption"),
                        "offering_class": term_attrs.get("OfferingClass"),
                    }
                )
    assert rows, "fixture should contain Reserved terms"
    return rows


async def store(conn, region_code: str, include_reserved: bool) -> None:
    collector = PricingCollector()
    records = []
    for product in load_products(region_code):
        records.extend(collector._parse_product(product, region_code, []))
    if include_reserved:
        # Reserved first, so a reader that doesn't filter on term_type picks
        # whichever row it sees last for the same key.
        records = (
            reserved_records(region_code) + records + reserved_records(region_code)
        )
    await collector.store_pricing_in_database(region_code, records, conn)


@pytest.mark.unit
@pytest.mark.parametrize("region_code", sorted(EXPECTED))
@pytest.mark.parametrize("analyzer_cls", [CapacityModeAnalyzer, UtilizationAnalyzer])
class TestPricingReaders:

    @pytest.mark.asyncio
    async def test_on_demand_prices(self, analyzer_cls, region_code, db_connection):
        await store(db_connection, region_code, include_reserved=False)

        assert analyzer_cls(db_connection)._get_pricing(region_code) == pytest.approx(
            expected_for(analyzer_cls, region_code)
        )

    @pytest.mark.asyncio
    async def test_reserved_rows_do_not_change_prices(
        self, analyzer_cls, region_code, db_connection
    ):
        await store(db_connection, region_code, include_reserved=True)

        assert analyzer_cls(db_connection)._get_pricing(region_code) == pytest.approx(
            expected_for(analyzer_cls, region_code)
        )


@pytest.mark.unit
class TestReadersWithoutPricing:
    """Behavior when a region has no pricing rows, kept as released."""

    def test_utilization_raises_value_error(self, db_connection):
        with pytest.raises(ValueError, match="us-west-2"):
            UtilizationAnalyzer(db_connection)._get_pricing("us-west-2")

    def test_capacity_mode_returns_empty(self, db_connection):
        assert CapacityModeAnalyzer(db_connection)._get_pricing("us-west-2") == {}

    @pytest.mark.asyncio
    async def test_capacity_mode_omits_missing_keys(self, db_connection):
        await store(db_connection, "us-west-2", include_reserved=False)
        db_connection.execute(
            "DELETE FROM pricing_data WHERE usage_type LIKE 'USW2-IA-%'"
        )

        assert CapacityModeAnalyzer(db_connection)._get_pricing("us-west-2") == (
            pytest.approx({k: EXPECTED["us-west-2"][k] for k in STANDARD})
        )
