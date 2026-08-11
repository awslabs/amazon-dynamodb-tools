"""Unit tests for the server-side pricing utility.

Covers `server/src/python_modules/shared/pricing.py`:

- get_capacity_pricing: buckets the four DDB-*Units groups into the pricing
  dict; skips zero-priced (free-tier) entries; and — the regression this file
  was created for (#267) — skips products that carry no 'group' attribute
  (e.g. the DynamoDB vector-search offerings AWS added to the PayPerRequest
  family) rather than raising KeyError on them.
- get_on_demand_capacity_pricing / get_provisioned_capacity_pricing: pass the
  right productFamily through to get_capacity_pricing.

conftest.py replaces `shared.pricing` with a Mock, so the real module is loaded
directly from its file path under a throwaway name, and its boto3 pricing client
is stubbed.
"""
import importlib.util
import json
from decimal import Decimal
from pathlib import Path
from unittest.mock import MagicMock

import pytest

_PRICING_PATH = (
    Path(__file__).resolve().parents[2]
    / "server/src/python_modules/shared/pricing.py"
)
_spec = importlib.util.spec_from_file_location("_real_pricing", _PRICING_PATH)
_pricing_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_pricing_mod)
PricingUtility = _pricing_mod.PricingUtility


def _entry(group=None, usagetype='x', price='0.0000001250', extra_attrs=None):
    """Build one PriceList JSON string in the AWS get_products shape.

    When group is None, the 'group' attribute is omitted entirely — mirroring
    the vector-search products that triggered #267.
    """
    attributes = {
        'regionCode': 'us-east-1',
        'servicecode': 'AmazonDynamoDB',
        'usagetype': usagetype,
        'location': 'US East (N. Virginia)',
        'servicename': 'Amazon DynamoDB',
    }
    if group is not None:
        attributes['group'] = group
    if extra_attrs:
        attributes.update(extra_attrs)
    product = {
        'product': {
            'productFamily': 'Amazon DynamoDB PayPerRequest Throughput',
            'attributes': attributes,
            'sku': 'SKU-' + usagetype,
        },
        'terms': {
            'OnDemand': {
                'offer1': {
                    'priceDimensions': {
                        'dim1': {'pricePerUnit': {'USD': price}}
                    }
                }
            }
        },
    }
    return json.dumps(product)


def _make_utility(price_list):
    util = PricingUtility.__new__(PricingUtility)  # skip real boto3 __init__
    util.pricing_client = MagicMock()
    util.pricing_client.get_products.return_value = {'PriceList': price_list}
    return util


class TestGetCapacityPricing:
    def test_buckets_the_four_unit_groups(self):
        util = _make_utility([
            _entry(group='DDB-ReadUnits', usagetype='ReadRequestUnits', price='0.0000001250'),
            _entry(group='DDB-WriteUnits', usagetype='WriteRequestUnits', price='0.0000006250'),
            _entry(group='DDB-ReadUnitsIA', usagetype='IA-ReadRequestUnits', price='0.0000000900'),
            _entry(group='DDB-WriteUnitsIA', usagetype='IA-WriteRequestUnits', price='0.0000004500'),
        ])
        result = util.get_capacity_pricing('Amazon DynamoDB PayPerRequest Throughput', 'us-east-1')
        assert result['std_rcu_pricing'] == Decimal('0.0000001250')
        assert result['std_wcu_pricing'] == Decimal('0.0000006250')
        assert result['ia_rcu_pricing'] == Decimal('0.0000000900')
        assert result['ia_wcu_pricing'] == Decimal('0.0000004500')

    def test_skips_products_without_group_attribute(self):
        # #267: vector-search products omit 'group'. Must be skipped, not crash,
        # and must not disturb the real capacity-unit prices.
        util = _make_utility([
            _entry(group='DDB-ReadUnits', usagetype='ReadRequestUnits', price='0.0000001250'),
            _entry(group=None, usagetype='USE1-VectorSearch', price='0.0020000000'),
            _entry(group=None, usagetype='USE1-VectorWriteRequest', price='0.5200000000'),
            _entry(group='DDB-WriteUnits', usagetype='WriteRequestUnits', price='0.0000006250'),
        ])
        result = util.get_capacity_pricing('Amazon DynamoDB PayPerRequest Throughput', 'us-east-1')
        # The two real products still priced; the vector products contributed nothing.
        assert result['std_rcu_pricing'] == Decimal('0.0000001250')
        assert result['std_wcu_pricing'] == Decimal('0.0000006250')
        assert 'ia_rcu_pricing' not in result
        assert 'ia_wcu_pricing' not in result

    def test_all_groupless_returns_empty_dict(self):
        util = _make_utility([
            _entry(group=None, usagetype='USE1-VectorSearch', price='0.0020000000'),
            _entry(group=None, usagetype='USE1-IA-VectorSearch', price='0.0025000000'),
        ])
        result = util.get_capacity_pricing('Amazon DynamoDB PayPerRequest Throughput', 'us-east-1')
        assert result == {}

    def test_zero_price_entry_is_skipped(self):
        # Free-tier initial entries are priced 0 and must not overwrite a real price.
        util = _make_utility([
            _entry(group='DDB-ReadUnits', usagetype='ReadRequestUnits', price='0'),
        ])
        result = util.get_capacity_pricing('Amazon DynamoDB PayPerRequest Throughput', 'us-east-1')
        assert 'std_rcu_pricing' not in result

    def test_unrecognized_group_is_ignored(self):
        # A product with a group we don't bucket (e.g. replicated write units) is
        # simply not stored, but must not raise.
        util = _make_utility([
            _entry(group='DDB-ReplicatedWriteUnits', usagetype='ReplWriteRequestUnits', price='0.0000006250'),
        ])
        result = util.get_capacity_pricing('Amazon DynamoDB PayPerRequest Throughput', 'us-east-1')
        assert result == {}


class TestPricingFamilyPassThrough:
    def test_on_demand_uses_payperrequest_family(self):
        util = _make_utility([])
        util.get_on_demand_capacity_pricing('us-east-1')
        family = util.pricing_client.get_products.call_args.kwargs['Filters'][0]['Value']
        assert family == 'Amazon DynamoDB PayPerRequest Throughput'

    def test_provisioned_uses_provisioned_family(self):
        util = _make_utility([])
        util.get_provisioned_capacity_pricing('us-east-1')
        family = util.pricing_client.get_products.call_args.kwargs['Filters'][0]['Value']
        assert family == 'Provisioned IOPS'
