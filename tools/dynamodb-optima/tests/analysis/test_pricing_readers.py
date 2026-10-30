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

# us-west-2 On-Demand list prices from the recorded fixture.
EXPECTED_ON_DEMAND = {
    "on_demand_read": 0.000000125,
    "on_demand_write": 0.000000625,
    "provisioned_read": 0.00013,
    "provisioned_write": 0.00065,
}


def load_products(region_code: str) -> list:
    return json.loads((FIXTURES / f"get_products_{region_code}.json").read_text())["PriceList"]


def reserved_records(region_code: str) -> list[dict]:
    """pricing_data rows for every Reserved price dimension in the fixture."""
    rows = []
    for product in load_products(region_code):
        attrs = product["product"]["attributes"]
        for term in product["terms"].get("Reserved", {}).values():
            term_attrs = term.get("termAttributes", {})
            for dim in term["priceDimensions"].values():
                rows.append({
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
                })
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
        records = reserved_records(region_code) + records + reserved_records(region_code)
    await collector.store_pricing_in_database(region_code, records, conn)


def subset(pricing: dict) -> dict:
    return {k: pricing[k] for k in EXPECTED_ON_DEMAND}


@pytest.mark.unit
@pytest.mark.parametrize("analyzer_cls", [CapacityModeAnalyzer, UtilizationAnalyzer])
class TestPricingReaders:

    @pytest.mark.asyncio
    async def test_on_demand_prices(self, analyzer_cls, db_connection):
        await store(db_connection, "us-west-2", include_reserved=False)

        assert subset(analyzer_cls(db_connection)._get_pricing("us-west-2")) == pytest.approx(
            EXPECTED_ON_DEMAND
        )

    @pytest.mark.asyncio
    async def test_reserved_rows_do_not_change_prices(self, analyzer_cls, db_connection):
        await store(db_connection, "us-west-2", include_reserved=True)

        assert subset(analyzer_cls(db_connection)._get_pricing("us-west-2")) == pytest.approx(
            EXPECTED_ON_DEMAND
        )
