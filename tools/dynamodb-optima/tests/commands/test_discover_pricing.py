"""
Tests for how `discover` collects pricing (the --pricing-terms option).
"""

from unittest.mock import AsyncMock, MagicMock

import pytest

from dynamodb_optima.aws.pricing_collector import ALL_TERMS, ON_DEMAND_TERMS
from dynamodb_optima.commands import discover as discover_module


@pytest.fixture
def fake_collector(monkeypatch):
    """Replace PricingCollector in the discover module; record constructor args."""
    created = []

    def factory(*args, **kwargs):
        instance = MagicMock()
        instance.collect_all_regions = AsyncMock()
        created.append(kwargs)
        return instance

    monkeypatch.setattr(discover_module, "PricingCollector", factory)
    monkeypatch.setattr(discover_module, "get_connection", lambda: MagicMock())
    return created


@pytest.mark.unit
class TestDiscoverPricingTerms:

    def test_option_is_declared(self):
        option = next(
            p for p in discover_module.discover.params if p.name == "pricing_terms"
        )
        assert option.default is None
        assert list(option.type.choices) == ["on-demand", "all"]

    def test_default_uses_settings(self, fake_collector):
        discover_module._collect_pricing(["us-west-2"], pricing_terms=None)

        assert fake_collector == [{"terms": None}]

    @pytest.mark.parametrize(
        "name, terms", [("on-demand", ON_DEMAND_TERMS), ("all", ALL_TERMS)]
    )
    def test_explicit_terms(self, fake_collector, name, terms):
        discover_module._collect_pricing(["us-west-2"], pricing_terms=name)

        assert fake_collector == [{"terms": terms}]
