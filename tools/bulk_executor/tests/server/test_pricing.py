"""Unit tests for the shared pricing utility.

Covers `python_modules/shared/pricing.py`:
- PricingUtility.get_capacity_pricing: parses the DynamoDB pricing API
  `PriceList`, maps product groups (DDB-ReadUnits/WriteUnits and their IA
  variants) to per-unit prices, skips zero-priced free-tier entries, and must
  not crash when a returned product is missing the `attributes.group` field
  (real API responses include products outside the four throughput groups).
- get_provisioned_capacity_pricing / get_on_demand_capacity_pricing: thin
  wrappers passing the correct productFamily filter value.

The pricing client is a boto3 client created in __init__; tests patch
`boto3.session.Session` in the module so no network call is made.
"""

import importlib.util
import json
import sys
from decimal import Decimal
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

# conftest.py stubs `python_modules.shared.pricing` with a bare Mock before
# collection (so pyspark-dependent verbs import cleanly). This suite tests the
# real parser, so load the actual module from disk — same importlib pattern
# conftest uses for shared.bulk_executor_error — without disturbing the global
# stub other suites rely on.
_spec = importlib.util.spec_from_file_location(
    "python_modules.shared.pricing_real",
    str(Path(__file__).resolve().parents[2]
        / "server/src/python_modules/shared/pricing.py"),
)
pricing_module = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(pricing_module)
PricingUtility = pricing_module.PricingUtility


def _entry(group, usd):
    """Build one PriceList JSON string with the given product group + USD price."""
    product = {"product": {"attributes": {"group": group}}}
    if group is None:
        # Product with no 'group' attribute at all — like a non-throughput
        # product the pricing API returns alongside the throughput ones.
        product = {"product": {"attributes": {"servicecode": "AmazonDynamoDB"}}}
    product["terms"] = {
        "OnDemand": {
            "offer1": {
                "priceDimensions": {
                    "dim1": {"pricePerUnit": {"USD": usd}}
                }
            }
        }
    }
    return json.dumps(product)


def _make_utility(price_list):
    """Construct a PricingUtility whose pricing client returns price_list."""
    with patch.object(pricing_module.boto3.session, "Session") as session_cls:
        client = MagicMock()
        client.get_products.return_value = {"PriceList": price_list}
        session_cls.return_value.client.return_value = client
        utility = PricingUtility()
    return utility, client


class TestGetCapacityPricing:
    def test_maps_read_and_write_groups_to_prices(self):
        utility, _ = _make_utility([
            _entry("DDB-ReadUnits", "0.00000025"),
            _entry("DDB-WriteUnits", "0.00000125"),
        ])

        result = utility.get_capacity_pricing("Provisioned IOPS", "us-east-1")

        assert result["std_rcu_pricing"] == Decimal("0.00000025")
        assert result["std_wcu_pricing"] == Decimal("0.00000125")

    def test_skips_zero_priced_free_tier_entries(self):
        utility, _ = _make_utility([
            _entry("DDB-ReadUnits", "0"),
            _entry("DDB-WriteUnits", "0.00000125"),
        ])

        result = utility.get_capacity_pricing("Provisioned IOPS", "us-east-1")

        assert "std_rcu_pricing" not in result
        assert result["std_wcu_pricing"] == Decimal("0.00000125")

    def test_product_missing_group_attribute_is_skipped_not_fatal(self):
        """A product with no `attributes.group` must not crash the parse.

        The real DynamoDB pricing API returns products outside the four
        throughput groups; indexing ['group'] blindly raises KeyError('group'),
        which on a live load surfaces as 'Error in writing to table: group'
        and kills the whole Glue job before any capacity warning can fire.
        """
        utility, _ = _make_utility([
            _entry(None, "0.00000099"),                 # no 'group' key
            _entry("DDB-WriteUnits", "0.00000125"),     # the one we care about
        ])

        result = utility.get_capacity_pricing("Provisioned IOPS", "us-east-1")

        assert result["std_wcu_pricing"] == Decimal("0.00000125")


class TestWrappers:
    def test_provisioned_wrapper_uses_provisioned_family(self):
        utility, client = _make_utility([])
        utility.get_provisioned_capacity_pricing("us-west-2")
        assert client.get_products.call_args.kwargs["Filters"][0]["Value"] == "Provisioned IOPS"

    def test_on_demand_wrapper_uses_pay_per_request_family(self):
        utility, client = _make_utility([])
        utility.get_on_demand_capacity_pricing("us-west-2")
        assert (
            client.get_products.call_args.kwargs["Filters"][0]["Value"]
            == "Amazon DynamoDB PayPerRequest Throughput"
        )
