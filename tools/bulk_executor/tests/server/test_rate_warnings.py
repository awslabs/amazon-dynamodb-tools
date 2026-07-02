"""Unit tests for rate validation warnings in get_dynamodb_throughput_configs.

Tests that warnings are logged when XMaxReadRate/XMaxWriteRate are:
- Too high: exceeds table's actual capacity (provisioned RCU/WCU or on-demand limit)
- Too low: below a reasonable fraction of table capacity (job will take unreasonably long)
"""

import importlib.util
import logging
import sys
from pathlib import Path
from unittest.mock import MagicMock

import botocore.exceptions
import pytest

# Load the real table_info module (same pattern as test_table_info.py)
sys.modules.pop('python_modules.shared.table_info', None)
sys.modules.pop('shared.table_info', None)

_TABLE_INFO_PATH = (
    Path(__file__).resolve().parents[2]
    / "server/src/python_modules/shared/table_info.py"
)
_spec = importlib.util.spec_from_file_location(
    "python_modules.shared.table_info", str(_TABLE_INFO_PATH)
)
table_info = importlib.util.module_from_spec(_spec)
sys.modules['python_modules.shared.table_info'] = table_info
_spec.loader.exec_module(table_info)


# --- Fixtures ---------------------------------------------------------------


@pytest.fixture
def boto3_mock(monkeypatch):
    mock = MagicMock()
    mock.Session.return_value.region_name = 'us-east-1'

    dynamodb_client = MagicMock()
    service_quotas_client = MagicMock()

    def client_factory(name, **kwargs):
        if name == 'dynamodb':
            return dynamodb_client
        if name == 'service-quotas':
            return service_quotas_client
        return MagicMock()

    mock.client.side_effect = client_factory
    mock.dynamodb_client = dynamodb_client
    mock.service_quotas_client = service_quotas_client

    monkeypatch.setattr(table_info, 'boto3', mock)
    return mock


@pytest.fixture
def provisioned_table():
    return {
        'Table': {
            'BillingModeSummary': {'BillingMode': 'PROVISIONED'},
            'ProvisionedThroughput': {
                'ReadCapacityUnits': 1000,
                'WriteCapacityUnits': 500,
            },
        }
    }


@pytest.fixture
def ondemand_table_with_limits():
    return {
        'Table': {
            'BillingModeSummary': {'BillingMode': 'PAY_PER_REQUEST'},
            'OnDemandThroughput': {
                'MaxReadRequestUnits': 25000,
                'MaxWriteRequestUnits': 15000,
            },
        }
    }


@pytest.fixture
def ondemand_table_no_limits():
    return {
        'Table': {
            'BillingModeSummary': {'BillingMode': 'PAY_PER_REQUEST'},
        }
    }


# --- Too-High Warnings: Rate exceeds table capacity -------------------------


class TestTooHighReadRateWarning:
    """Warn when XMaxReadRate exceeds the table's actual read capacity."""

    def test_provisioned_read_rate_above_capacity_warns(
        self, boto3_mock, provisioned_table, caplog
    ):
        """XMaxReadRate=2000 on a table with 1000 RCU → warning logged."""
        boto3_mock.dynamodb_client.describe_table.return_value = provisioned_table
        with caplog.at_level(logging.DEBUG):
            table_info.get_dynamodb_throughput_configs(
                args={'XMaxReadRate': '2000'}, table_name='t', modes=['read']
            )
        assert any(
            'exceeds' in msg.lower() and 'read' in msg.lower() and '1000' in msg
            for msg in caplog.messages
        ), f"Expected 'exceeds' warning about read capacity 1000, got: {caplog.messages}"

    def test_provisioned_read_rate_at_capacity_no_warning(
        self, boto3_mock, provisioned_table, caplog
    ):
        """XMaxReadRate=1000 on a table with 1000 RCU → no too-high warning."""
        boto3_mock.dynamodb_client.describe_table.return_value = provisioned_table
        with caplog.at_level(logging.DEBUG):
            table_info.get_dynamodb_throughput_configs(
                args={'XMaxReadRate': '1000'}, table_name='t', modes=['read']
            )
        assert not any(
            'exceeds' in msg.lower() and 'read' in msg.lower()
            for msg in caplog.messages
        )

    def test_ondemand_read_rate_above_table_limit_warns(
        self, boto3_mock, ondemand_table_with_limits, caplog
    ):
        """XMaxReadRate=30000 on on-demand table with 25000 limit → warning."""
        boto3_mock.dynamodb_client.describe_table.return_value = ondemand_table_with_limits
        with caplog.at_level(logging.DEBUG):
            table_info.get_dynamodb_throughput_configs(
                args={'XMaxReadRate': '30000'}, table_name='t', modes=['read']
            )
        assert any(
            'exceeds' in msg.lower() and 'read' in msg.lower() and '25000' in msg
            for msg in caplog.messages
        ), f"Expected 'exceeds' warning about read capacity 25000, got: {caplog.messages}"

    def test_ondemand_no_limit_read_rate_above_default_warns(
        self, boto3_mock, ondemand_table_no_limits, caplog
    ):
        """XMaxReadRate=50000 with no table/quota limit (default 40000) → warning."""
        boto3_mock.dynamodb_client.describe_table.return_value = ondemand_table_no_limits
        boto3_mock.service_quotas_client.get_service_quota.side_effect = (
            RuntimeError('unavailable')
        )
        with caplog.at_level(logging.DEBUG):
            table_info.get_dynamodb_throughput_configs(
                args={'XMaxReadRate': '50000'}, table_name='t', modes=['read']
            )
        assert any(
            'exceeds' in msg.lower() and 'read' in msg.lower() and '40000' in msg
            for msg in caplog.messages
        ), f"Expected 'exceeds' warning about read capacity 40000, got: {caplog.messages}"


class TestTooHighWriteRateWarning:
    """Warn when XMaxWriteRate exceeds the table's actual write capacity."""

    def test_provisioned_write_rate_above_capacity_warns(
        self, boto3_mock, provisioned_table, caplog
    ):
        """XMaxWriteRate=1000 on a table with 500 WCU → warning."""
        boto3_mock.dynamodb_client.describe_table.return_value = provisioned_table
        with caplog.at_level(logging.DEBUG):
            table_info.get_dynamodb_throughput_configs(
                args={'XMaxWriteRate': '1000'}, table_name='t', modes=['write']
            )
        assert any(
            'exceeds' in msg.lower() and 'write' in msg.lower() and '500' in msg
            for msg in caplog.messages
        ), f"Expected 'exceeds' warning about write capacity 500, got: {caplog.messages}"

    def test_provisioned_write_rate_at_capacity_no_warning(
        self, boto3_mock, provisioned_table, caplog
    ):
        """XMaxWriteRate=500 on a table with 500 WCU → no too-high warning."""
        boto3_mock.dynamodb_client.describe_table.return_value = provisioned_table
        with caplog.at_level(logging.DEBUG):
            table_info.get_dynamodb_throughput_configs(
                args={'XMaxWriteRate': '500'}, table_name='t', modes=['write']
            )
        assert not any(
            'exceeds' in msg.lower() and 'write' in msg.lower()
            for msg in caplog.messages
        )

    def test_ondemand_write_rate_above_table_limit_warns(
        self, boto3_mock, ondemand_table_with_limits, caplog
    ):
        """XMaxWriteRate=20000 on on-demand with 15000 limit → warning."""
        boto3_mock.dynamodb_client.describe_table.return_value = ondemand_table_with_limits
        with caplog.at_level(logging.DEBUG):
            table_info.get_dynamodb_throughput_configs(
                args={'XMaxWriteRate': '20000'}, table_name='t', modes=['write']
            )
        assert any(
            'exceeds' in msg.lower() and 'write' in msg.lower() and '15000' in msg
            for msg in caplog.messages
        ), f"Expected 'exceeds' warning about write capacity 15000, got: {caplog.messages}"


# --- Too-Low Warnings: Rate unreasonably low for table ----------------------


class TestTooLowReadRateWarning:
    """Warn when XMaxReadRate is unreasonably low relative to table capacity."""

    def test_read_rate_below_absolute_minimum_warns(
        self, boto3_mock, provisioned_table, caplog
    ):
        """XMaxReadRate=50 → below MIN_RECOMMENDED_READ_RATE (100)."""
        boto3_mock.dynamodb_client.describe_table.return_value = provisioned_table
        with caplog.at_level(logging.DEBUG):
            table_info.get_dynamodb_throughput_configs(
                args={'XMaxReadRate': '50'}, table_name='t', modes=['read']
            )
        assert any(
            'less than recommended' in msg.lower() or 'too low' in msg.lower()
            for msg in caplog.messages
        ), f"Expected low-rate warning, got: {caplog.messages}"

    def test_read_rate_far_below_capacity_warns(
        self, boto3_mock, caplog
    ):
        """XMaxReadRate=200 on a table with 40000 capacity → unreasonably low."""
        high_capacity_table = {
            'Table': {
                'BillingModeSummary': {'BillingMode': 'PROVISIONED'},
                'ProvisionedThroughput': {
                    'ReadCapacityUnits': 40000,
                    'WriteCapacityUnits': 20000,
                },
            }
        }
        boto3_mock.dynamodb_client.describe_table.return_value = high_capacity_table
        with caplog.at_level(logging.DEBUG):
            table_info.get_dynamodb_throughput_configs(
                args={'XMaxReadRate': '200'}, table_name='t', modes=['read']
            )
        assert any(
            'low' in msg.lower() and 'read' in msg.lower()
            for msg in caplog.messages
        ), f"Expected low-rate warning for 200 vs 40000 capacity, got: {caplog.messages}"


class TestTooLowWriteRateWarning:
    """Warn when XMaxWriteRate is unreasonably low relative to table capacity."""

    def test_write_rate_below_absolute_minimum_warns(
        self, boto3_mock, provisioned_table, caplog
    ):
        """XMaxWriteRate=50 → below MIN_RECOMMENDED_WRITE_RATE (100)."""
        boto3_mock.dynamodb_client.describe_table.return_value = provisioned_table
        with caplog.at_level(logging.DEBUG):
            table_info.get_dynamodb_throughput_configs(
                args={'XMaxWriteRate': '50'}, table_name='t', modes=['write']
            )
        assert any(
            'less than recommended' in msg.lower() or 'too low' in msg.lower()
            for msg in caplog.messages
        ), f"Expected low-rate warning, got: {caplog.messages}"

    def test_write_rate_far_below_capacity_warns(
        self, boto3_mock, caplog
    ):
        """XMaxWriteRate=100 on a table with 20000 WCU → unreasonably low."""
        high_capacity_table = {
            'Table': {
                'BillingModeSummary': {'BillingMode': 'PROVISIONED'},
                'ProvisionedThroughput': {
                    'ReadCapacityUnits': 40000,
                    'WriteCapacityUnits': 20000,
                },
            }
        }
        boto3_mock.dynamodb_client.describe_table.return_value = high_capacity_table
        with caplog.at_level(logging.DEBUG):
            table_info.get_dynamodb_throughput_configs(
                args={'XMaxWriteRate': '100'}, table_name='t', modes=['write']
            )
        assert any(
            'low' in msg.lower() and 'write' in msg.lower()
            for msg in caplog.messages
        ), f"Expected low-rate warning for 100 vs 20000 capacity, got: {caplog.messages}"


# --- No spurious warnings when rates are reasonable -------------------------


class TestNoSpuriousWarnings:
    """Rates within a reasonable band of table capacity produce no warnings."""

    def test_reasonable_read_rate_no_warnings(
        self, boto3_mock, provisioned_table, caplog
    ):
        """XMaxReadRate=800 on 1000 RCU table → no warnings."""
        boto3_mock.dynamodb_client.describe_table.return_value = provisioned_table
        with caplog.at_level(logging.DEBUG):
            table_info.get_dynamodb_throughput_configs(
                args={'XMaxReadRate': '800'}, table_name='t', modes=['read']
            )
        warning_msgs = [
            msg for msg in caplog.messages
            if 'exceeds' in msg.lower() or 'too low' in msg.lower()
            or ('low' in msg.lower() and 'read' in msg.lower() and 'less than recommended' not in msg.lower())
        ]
        assert warning_msgs == [], f"Unexpected warnings: {warning_msgs}"

    def test_reasonable_write_rate_no_warnings(
        self, boto3_mock, provisioned_table, caplog
    ):
        """XMaxWriteRate=400 on 500 WCU table → no warnings."""
        boto3_mock.dynamodb_client.describe_table.return_value = provisioned_table
        with caplog.at_level(logging.DEBUG):
            table_info.get_dynamodb_throughput_configs(
                args={'XMaxWriteRate': '400'}, table_name='t', modes=['write']
            )
        warning_msgs = [
            msg for msg in caplog.messages
            if 'exceeds' in msg.lower() or 'too low' in msg.lower()
            or ('low' in msg.lower() and 'write' in msg.lower() and 'less than recommended' not in msg.lower())
        ]
        assert warning_msgs == [], f"Unexpected warnings: {warning_msgs}"

    def test_auto_detected_rate_no_warnings(
        self, boto3_mock, provisioned_table, caplog
    ):
        """When no XMax is specified, auto-detected rate doesn't trigger warnings."""
        boto3_mock.dynamodb_client.describe_table.return_value = provisioned_table
        with caplog.at_level(logging.DEBUG):
            table_info.get_dynamodb_throughput_configs(
                args={}, table_name='t', modes=['read', 'write']
            )
        warning_msgs = [
            msg for msg in caplog.messages
            if 'exceeds' in msg.lower() or 'too low' in msg.lower()
        ]
        assert warning_msgs == [], f"Unexpected warnings: {warning_msgs}"
