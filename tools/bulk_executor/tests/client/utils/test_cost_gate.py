"""Unit tests for the cost gate feature (--XMaxEstimatedCostAllowed).

Covers `client/src/utils/__init__.py`:
- estimate_cost(): read-only table cost estimation, write cost estimation,
  copy (source+target) combined cost, missing table graceful skip, pricing
  API fallback to hardcoded defaults
- check_cost_gate(): param not set → pass-through, cost under threshold →
  proceed, cost over threshold → sys.exit with message
"""

import json
import math
from unittest.mock import MagicMock, patch

import pytest
from botocore.exceptions import ClientError

import utils as utils_module
from utils import check_cost_gate, estimate_cost


# --- Fixtures ---------------------------------------------------------------

@pytest.fixture
def mock_env_configs():
    env = MagicMock()
    env.aws_region = "us-east-1"
    return env


@pytest.fixture
def mock_table_response():
    return {
        'Table': {
            'ItemCount': 1000,
            'TableSizeBytes': 1_000_000,
            'BillingModeSummary': {'BillingMode': 'PAY_PER_REQUEST'},
            'TableClassSummary': {'TableClass': 'STANDARD'},
        }
    }


@pytest.fixture
def mock_pricing_response():
    """Pricing API response with standard on-demand prices."""
    def make_entry(group, price):
        return json.dumps({
            'product': {'attributes': {'group': group}},
            'terms': {'OnDemand': {
                'offer1': {
                    'priceDimensions': {
                        'dim1': {'pricePerUnit': {'USD': str(price)}}
                    }
                }
            }}
        })

    return {
        'PriceList': [
            make_entry('DDB-ReadUnits', 0.00000025),
            make_entry('DDB-WriteUnits', 0.00000125),
        ]
    }


# --- estimate_cost ----------------------------------------------------------

class TestEstimateCost:
    """Tests for estimate_cost function."""

    def test_returns_zero_when_no_tables(self, mock_env_configs):
        """No table/source/target in args → zero cost."""
        result = estimate_cost(mock_env_configs, {'verb': 'bootstrap'})
        assert result == 0.0

    @patch('utils.Clients')
    @patch('utils.boto3.client')
    def test_read_cost_on_demand_table(
        self, mock_boto3_client, mock_clients_cls,
        mock_env_configs, mock_table_response, mock_pricing_response
    ):
        """Estimates read cost for a single --table arg (on-demand billing)."""
        mock_ddb = MagicMock()
        mock_ddb.describe_table.return_value = mock_table_response
        mock_clients_cls.return_value.dynamodb_client = mock_ddb

        mock_pricing = MagicMock()
        mock_pricing.get_products.return_value = mock_pricing_response
        mock_boto3_client.return_value = mock_pricing

        result = estimate_cost(mock_env_configs, {'table': 'my-table'})

        size_bytes = 1_000_000
        read_units = math.ceil(size_bytes / 8096)
        expected = read_units * 0.00000025
        assert result == pytest.approx(expected)

    @patch('utils.Clients')
    @patch('utils.boto3.client')
    def test_read_cost_provisioned_table(
        self, mock_boto3_client, mock_clients_cls,
        mock_env_configs, mock_pricing_response
    ):
        """Provisioned tables get 1.5x discount on cost."""
        table_response = {
            'Table': {
                'ItemCount': 500,
                'TableSizeBytes': 500_000,
                'BillingModeSummary': {'BillingMode': 'PROVISIONED'},
                'TableClassSummary': {'TableClass': 'STANDARD'},
            }
        }
        mock_ddb = MagicMock()
        mock_ddb.describe_table.return_value = table_response
        mock_clients_cls.return_value.dynamodb_client = mock_ddb

        mock_pricing = MagicMock()
        mock_pricing.get_products.return_value = mock_pricing_response
        mock_boto3_client.return_value = mock_pricing

        result = estimate_cost(mock_env_configs, {'table': 'my-table'})

        read_units = math.ceil(500_000 / 8096)
        expected = (read_units * 0.00000025) / 1.5
        assert result == pytest.approx(expected)

    @patch('utils.Clients')
    @patch('utils.boto3.client')
    def test_write_cost_for_copy_target(
        self, mock_boto3_client, mock_clients_cls,
        mock_env_configs, mock_pricing_response
    ):
        """Copy target estimates write cost based on item count and size."""
        source_response = {
            'Table': {
                'ItemCount': 10000,
                'TableSizeBytes': 10_000_000,
                'BillingModeSummary': {'BillingMode': 'PAY_PER_REQUEST'},
                'TableClassSummary': {'TableClass': 'STANDARD'},
            }
        }
        target_response = {
            'Table': {
                'ItemCount': 10000,
                'TableSizeBytes': 10_000_000,
                'BillingModeSummary': {'BillingMode': 'PAY_PER_REQUEST'},
                'TableClassSummary': {'TableClass': 'STANDARD'},
            }
        }
        mock_ddb = MagicMock()
        mock_ddb.describe_table.side_effect = [source_response, target_response]
        mock_clients_cls.return_value.dynamodb_client = mock_ddb

        mock_pricing = MagicMock()
        mock_pricing.get_products.return_value = mock_pricing_response
        mock_boto3_client.return_value = mock_pricing

        result = estimate_cost(mock_env_configs, {'source': 'src-table', 'target': 'tgt-table'})

        # Read cost for source
        read_units = math.ceil(10_000_000 / 8096)
        read_cost = read_units * 0.00000025

        # Write cost for target
        avg_size = 10_000_000 / 10000
        avg_write_units = math.ceil(avg_size / 1024)
        write_units = 10000 * avg_write_units
        write_cost = write_units * 0.00000125

        expected = read_cost + write_cost
        assert result == pytest.approx(expected)

    @patch('utils.Clients')
    @patch('utils.boto3.client')
    def test_skips_missing_table(
        self, mock_boto3_client, mock_clients_cls, mock_env_configs
    ):
        """If describe_table raises ClientError, skip that table."""
        mock_ddb = MagicMock()
        mock_ddb.describe_table.side_effect = ClientError(
            {'Error': {'Code': 'ResourceNotFoundException', 'Message': 'not found'}},
            'DescribeTable'
        )
        mock_clients_cls.return_value.dynamodb_client = mock_ddb

        result = estimate_cost(mock_env_configs, {'table': 'missing-table'})
        assert result == 0.0

    @patch('utils.Clients')
    @patch('utils.boto3.client')
    def test_uses_fallback_pricing_on_api_error(
        self, mock_boto3_client, mock_clients_cls,
        mock_env_configs, mock_table_response
    ):
        """Falls back to hardcoded pricing when pricing API fails."""
        mock_ddb = MagicMock()
        mock_ddb.describe_table.return_value = mock_table_response
        mock_clients_cls.return_value.dynamodb_client = mock_ddb

        mock_pricing = MagicMock()
        mock_pricing.get_products.side_effect = Exception("pricing unavailable")
        mock_boto3_client.return_value = mock_pricing

        result = estimate_cost(mock_env_configs, {'table': 'my-table'})

        # Should still produce a cost using fallback pricing
        read_units = math.ceil(1_000_000 / 8096)
        expected = read_units * 0.00000025
        assert result == pytest.approx(expected)

    @patch('utils.Clients')
    @patch('utils.boto3.client')
    def test_read_cost_infrequent_access_uses_ia_pricing(
        self, mock_boto3_client, mock_clients_cls, mock_env_configs
    ):
        """STANDARD_INFREQUENT_ACCESS table reads use the IA read-unit price."""
        table_response = {
            'Table': {
                'ItemCount': 1000,
                'TableSizeBytes': 1_000_000,
                'BillingModeSummary': {'BillingMode': 'PAY_PER_REQUEST'},
                'TableClassSummary': {'TableClass': 'STANDARD_INFREQUENT_ACCESS'},
            }
        }
        mock_ddb = MagicMock()
        mock_ddb.describe_table.return_value = table_response
        mock_clients_cls.return_value.dynamodb_client = mock_ddb

        def make_entry(group, price):
            return json.dumps({
                'product': {'attributes': {'group': group}},
                'terms': {'OnDemand': {'offer1': {'priceDimensions': {
                    'dim1': {'pricePerUnit': {'USD': str(price)}}
                }}}}
            })

        # Include every pricing group so the DDB-*IA branches are exercised.
        mock_pricing = MagicMock()
        mock_pricing.get_products.return_value = {'PriceList': [
            make_entry('DDB-ReadUnits', 0.00000025),
            make_entry('DDB-WriteUnits', 0.00000125),
            make_entry('DDB-ReadUnitsIA', 0.00000031),
            make_entry('DDB-WriteUnitsIA', 0.00000156),
        ]}
        mock_boto3_client.return_value = mock_pricing

        result = estimate_cost(mock_env_configs, {'table': 'my-table'})

        read_units = math.ceil(1_000_000 / 8096)
        expected = read_units * 0.00000031  # IA read price, not STANDARD
        assert result == pytest.approx(expected)

    @patch('utils.Clients')
    @patch('utils.boto3.client')
    def test_write_cost_skips_target_with_zero_items(
        self, mock_boto3_client, mock_clients_cls,
        mock_env_configs, mock_pricing_response
    ):
        """A write target with zero items contributes no write cost (avoids div-by-zero)."""
        target_response = {
            'Table': {
                'ItemCount': 0,
                'TableSizeBytes': 0,
                'BillingModeSummary': {'BillingMode': 'PAY_PER_REQUEST'},
                'TableClassSummary': {'TableClass': 'STANDARD'},
            }
        }
        mock_ddb = MagicMock()
        mock_ddb.describe_table.return_value = target_response
        mock_clients_cls.return_value.dynamodb_client = mock_ddb

        mock_pricing = MagicMock()
        mock_pricing.get_products.return_value = mock_pricing_response
        mock_boto3_client.return_value = mock_pricing

        result = estimate_cost(mock_env_configs, {'target': 'empty-table'})
        assert result == 0.0

    @patch('utils.Clients')
    @patch('utils.boto3.client')
    def test_write_cost_provisioned_target_gets_discount(
        self, mock_boto3_client, mock_clients_cls,
        mock_env_configs, mock_pricing_response
    ):
        """Provisioned write targets get the same 1.5x discount as reads."""
        target_response = {
            'Table': {
                'ItemCount': 10000,
                'TableSizeBytes': 10_000_000,
                'BillingModeSummary': {'BillingMode': 'PROVISIONED'},
                'TableClassSummary': {'TableClass': 'STANDARD'},
            }
        }
        mock_ddb = MagicMock()
        mock_ddb.describe_table.return_value = target_response
        mock_clients_cls.return_value.dynamodb_client = mock_ddb

        mock_pricing = MagicMock()
        mock_pricing.get_products.return_value = mock_pricing_response
        mock_boto3_client.return_value = mock_pricing

        result = estimate_cost(mock_env_configs, {'target': 'tgt-table'})

        avg_size = 10_000_000 / 10000
        avg_write_units = math.ceil(avg_size / 1024)
        write_units = 10000 * avg_write_units
        expected = (write_units * 0.00000125) / 1.5
        assert result == pytest.approx(expected)


# --- check_cost_gate --------------------------------------------------------

class TestCheckCostGate:
    """Tests for check_cost_gate function."""

    def test_passes_when_param_not_set(self, mock_env_configs):
        """No --XMaxEstimatedCostAllowed → always proceed."""
        result = check_cost_gate(mock_env_configs, {'table': 'my-table'})
        assert result is True

    @patch('utils.estimate_cost')
    def test_passes_when_cost_under_threshold(self, mock_estimate, mock_env_configs):
        """Cost below threshold → proceed normally."""
        mock_estimate.return_value = 5.00
        result = check_cost_gate(
            mock_env_configs,
            {'table': 'my-table', 'XMaxEstimatedCostAllowed': 10.00}
        )
        assert result is True

    @patch('utils.estimate_cost')
    def test_exits_when_cost_exceeds_threshold(self, mock_estimate, mock_env_configs):
        """Cost above threshold → sys.exit with message."""
        mock_estimate.return_value = 15.50
        with pytest.raises(SystemExit) as exc_info:
            check_cost_gate(
                mock_env_configs,
                {'table': 'my-table', 'XMaxEstimatedCostAllowed': 10.00}
            )
        exit_message = str(exc_info.value)
        assert "$15.50" in exit_message
        assert "$10.00" in exit_message
        assert "exceeds" in exit_message

    @patch('utils.estimate_cost')
    def test_passes_when_cost_equals_threshold(self, mock_estimate, mock_env_configs):
        """Cost exactly equal to threshold → proceed (not strictly greater)."""
        mock_estimate.return_value = 10.00
        result = check_cost_gate(
            mock_env_configs,
            {'table': 'my-table', 'XMaxEstimatedCostAllowed': 10.00}
        )
        assert result is True
