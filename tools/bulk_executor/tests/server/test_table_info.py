"""Unit tests for shared/table_info.py.

Primary focus: get_dynamodb_throughput_configs. The percentile math in
this function is what the upcoming Glue connector version bump (issue
#145) replaces with direct WCU/RCU passthrough — every behavior pinned
down here is the baseline the version bump will be measured against.

Secondary coverage: get_quota_value, _parse_arn, _region_from_table_ref.
"""

import importlib.util
import sys
from pathlib import Path
from unittest.mock import MagicMock

import botocore.exceptions
import pytest

# The repo's tests/server/conftest.py mocks python_modules.shared.table_info
# as Mock() so that verb modules (find/sql/etc.) can import without pulling
# in the real table_info dependency chain. For these tests we want the real
# implementation, so we replace the mock entry with the real module loaded
# directly from disk. Relative imports inside table_info.py
# (`.logger`, `.pricing`) still resolve to the conftest mocks, which is
# fine — pricing is mocked per-test where needed and logger uses a real
# Python logger so caplog works.
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
    """Replace boto3 inside table_info with a dispatching mock.

    Exposes .dynamodb_client and .service_quotas_client so each test can
    wire describe_table / get_service_quota return values independently.
    """
    mock = MagicMock()
    mock.Session.return_value.region_name = 'us-east-1'

    dynamodb_client = MagicMock()
    service_quotas_client = MagicMock()
    autoscaling_client = MagicMock()

    def client_factory(name, **kwargs):
        if name == 'dynamodb':
            return dynamodb_client
        if name == 'service-quotas':
            return service_quotas_client
        if name == 'application-autoscaling':
            return autoscaling_client
        return MagicMock()

    mock.client.side_effect = client_factory
    mock.dynamodb_client = dynamodb_client
    mock.service_quotas_client = service_quotas_client
    mock.autoscaling_client = autoscaling_client

    monkeypatch.setattr(table_info, 'boto3', mock)
    return mock


@pytest.fixture
def provisioned_table():
    """describe_table response for a provisioned table.

    RCU=1000, WCU=500. Tests can mutate the dict before assigning to
    .return_value if they want different values.
    """
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
def ondemand_table():
    """describe_table response for an on-demand table with no table-level limits."""
    return {
        'Table': {
            'BillingModeSummary': {'BillingMode': 'PAY_PER_REQUEST'},
        }
    }


@pytest.fixture
def ondemand_table_with_limits():
    """describe_table response for an on-demand table with explicit limits."""
    return {
        'Table': {
            'BillingModeSummary': {'BillingMode': 'PAY_PER_REQUEST'},
            'OnDemandThroughput': {
                'MaxReadRequestUnits': 25000,
                'MaxWriteRequestUnits': 15000,
            },
        }
    }


# --- _parse_arn -------------------------------------------------------------


class TestParseArn:
    """Pure-logic ARN parser; no AWS calls."""

    def test_parses_six_part_dynamodb_arn(self):
        result = table_info._parse_arn(
            'arn:aws:dynamodb:us-east-1:123456789012:table/my-table'
        )
        assert result == {
            'partition': 'aws',
            'service': 'dynamodb',
            'region': 'us-east-1',
            'account': '123456789012',
            'resource': 'table/my-table',
        }

    def test_invalid_arn_raises_value_error(self):
        with pytest.raises(ValueError):
            table_info._parse_arn('not-an-arn')

    def test_resource_keeps_embedded_colons(self):
        # The split is bounded at maxsplit=5 so the resource segment can
        # contain colons (e.g. table/foo:stream:2024-01-01).
        result = table_info._parse_arn(
            'arn:aws:dynamodb:us-east-1:123456789012:table/foo:stream:2024-01-01'
        )
        assert result['resource'] == 'table/foo:stream:2024-01-01'

    def test_non_arn_prefix_raises(self):
        with pytest.raises(ValueError):
            table_info._parse_arn('xrn:aws:dynamodb:us-east-1:1:table/x')


# --- _region_from_table_ref -------------------------------------------------


class TestRegionFromTableRef:
    def test_plain_table_name_returns_none(self):
        assert table_info._region_from_table_ref('my-table') is None

    def test_empty_string_returns_none(self):
        assert table_info._region_from_table_ref('') is None

    def test_dynamodb_table_arn_returns_region(self):
        arn = 'arn:aws:dynamodb:eu-west-1:123456789012:table/my-table'
        assert table_info._region_from_table_ref(arn) == 'eu-west-1'

    def test_non_dynamodb_arn_returns_none(self):
        # arn:aws:s3:::my-bucket has 6 parts after split (empty region/account
        # are still parts) so _parse_arn succeeds, but service is 's3'
        arn = 'arn:aws:s3:::my-bucket'
        assert table_info._region_from_table_ref(arn) is None

    def test_dynamodb_non_table_resource_returns_none(self):
        arn = 'arn:aws:dynamodb:us-east-1:123456789012:backup/foo'
        assert table_info._region_from_table_ref(arn) is None


# --- get_quota_value --------------------------------------------------------


class TestGetQuotaValue:
    """Service-Quotas wrapper used by the on-demand quota fallback path."""

    def test_known_quota_returns_int(self, boto3_mock):
        boto3_mock.service_quotas_client.get_service_quota.return_value = {
            'Quota': {'Value': 80000}
        }
        assert (
            table_info.get_quota_value(
                'Table-level read throughput limit', 'us-east-1'
            )
            == 80000
        )

    def test_unknown_quota_name_returns_none(self, boto3_mock):
        assert (
            table_info.get_quota_value('NotARealQuota', 'us-east-1') is None
        )

    def test_no_such_resource_falls_back_to_aws_default(self, boto3_mock):
        sq = boto3_mock.service_quotas_client
        sq.get_service_quota.side_effect = botocore.exceptions.ClientError(
            {'Error': {'Code': 'NoSuchResourceException', 'Message': 'no'}},
            'GetServiceQuota',
        )
        sq.get_aws_default_service_quota.return_value = {
            'Quota': {'Value': 40000}
        }
        assert (
            table_info.get_quota_value(
                'Table-level write throughput limit', 'us-east-1'
            )
            == 40000
        )

    def test_other_client_error_returns_none(self, boto3_mock):
        boto3_mock.service_quotas_client.get_service_quota.side_effect = (
            botocore.exceptions.ClientError(
                {'Error': {'Code': 'AccessDeniedException', 'Message': 'denied'}},
                'GetServiceQuota',
            )
        )
        assert (
            table_info.get_quota_value(
                'Table-level read throughput limit', 'us-east-1'
            )
            is None
        )

    def test_unexpected_exception_returns_none(self, boto3_mock):
        boto3_mock.service_quotas_client.get_service_quota.side_effect = (
            RuntimeError('boom')
        )
        assert (
            table_info.get_quota_value(
                'Table-level read throughput limit', 'us-east-1'
            )
            is None
        )


# --- get_dynamodb_throughput_configs : provisioned, format=connector --------


class TestProvisionedConnectorFormat:
    """Provisioned table, format='connector'.

    Read path emits dynamodb.throughput.read = provisioned RCU and
    dynamodb.throughput.read.percent = '1.0'. Write path emits only
    dynamodb.throughput.write.percent computed as write_rate / WCU,
    capped at 1.5.
    """

    def test_read_uses_provisioned_rcu(self, boto3_mock, provisioned_table):
        boto3_mock.dynamodb_client.describe_table.return_value = provisioned_table
        opts = table_info.get_dynamodb_throughput_configs(
            args={}, table_name='t', modes=['read']
        )
        assert opts['dynamodb.throughput.read'] == '1000'
        assert opts['dynamodb.throughput.read.percent'] == '1.0'

    def test_write_percent_equals_rate_over_provisioned(
        self, boto3_mock, provisioned_table
    ):
        boto3_mock.dynamodb_client.describe_table.return_value = provisioned_table
        opts = table_info.get_dynamodb_throughput_configs(
            args={'XMaxWriteRate': '250'}, table_name='t', modes=['write']
        )
        # 250 / 500 = 0.5
        assert opts['dynamodb.throughput.write.percent'] == '0.5'

    def test_write_percent_capped_at_1_5(
        self, boto3_mock, provisioned_table
    ):
        boto3_mock.dynamodb_client.describe_table.return_value = provisioned_table
        opts = table_info.get_dynamodb_throughput_configs(
            args={'XMaxWriteRate': '100000'},
            table_name='t',
            modes=['write'],
        )
        # 100000 / 500 = 200; capped at 1.5
        assert opts['dynamodb.throughput.write.percent'] == '1.5'

    def test_read_xmax_overrides_provisioned(
        self, boto3_mock, provisioned_table
    ):
        boto3_mock.dynamodb_client.describe_table.return_value = provisioned_table
        opts = table_info.get_dynamodb_throughput_configs(
            args={'XMaxReadRate': '750'}, table_name='t', modes=['read']
        )
        assert opts['dynamodb.throughput.read'] == '750'
        assert opts['dynamodb.throughput.read.percent'] == '1.0'

    def test_combined_modes_returns_both_read_and_write_keys(
        self, boto3_mock, provisioned_table
    ):
        boto3_mock.dynamodb_client.describe_table.return_value = provisioned_table
        opts = table_info.get_dynamodb_throughput_configs(
            args={}, table_name='t', modes=['read', 'write']
        )
        assert 'dynamodb.throughput.read' in opts
        assert 'dynamodb.throughput.read.percent' in opts
        assert 'dynamodb.throughput.write.percent' in opts


# --- get_dynamodb_throughput_configs : on-demand, format=connector ----------


class TestOnDemandConnectorFormat:
    """On-demand resolution order:
        XMaxRate (explicit) >
        table-level OnDemandThroughput >
        account-level service quota >
        DEFAULT_ON_DEMAND_CAPACITY (40000)
    Write percent always uses 40000 as denominator on on-demand tables.
    """

    def test_read_uses_table_level_limit_when_present(
        self, boto3_mock, ondemand_table_with_limits
    ):
        boto3_mock.dynamodb_client.describe_table.return_value = (
            ondemand_table_with_limits
        )
        opts = table_info.get_dynamodb_throughput_configs(
            args={}, table_name='t', modes=['read']
        )
        assert opts['dynamodb.throughput.read'] == '25000'

    def test_read_uses_quota_when_no_table_limit(
        self, boto3_mock, ondemand_table
    ):
        boto3_mock.dynamodb_client.describe_table.return_value = ondemand_table
        boto3_mock.service_quotas_client.get_service_quota.return_value = {
            'Quota': {'Value': 80000}
        }
        opts = table_info.get_dynamodb_throughput_configs(
            args={}, table_name='t', modes=['read']
        )
        assert opts['dynamodb.throughput.read'] == '80000'

    def test_read_falls_back_to_default_40k(
        self, boto3_mock, ondemand_table
    ):
        boto3_mock.dynamodb_client.describe_table.return_value = ondemand_table
        # Service Quotas access denied → quota lookup returns None → default
        boto3_mock.service_quotas_client.get_service_quota.side_effect = (
            botocore.exceptions.ClientError(
                {'Error': {'Code': 'AccessDeniedException', 'Message': 'denied'}},
                'GetServiceQuota',
            )
        )
        opts = table_info.get_dynamodb_throughput_configs(
            args={}, table_name='t', modes=['read']
        )
        assert opts['dynamodb.throughput.read'] == '40000'

    def test_write_percent_uses_40k_denominator(
        self, boto3_mock, ondemand_table
    ):
        boto3_mock.dynamodb_client.describe_table.return_value = ondemand_table
        opts = table_info.get_dynamodb_throughput_configs(
            args={'XMaxWriteRate': '10000'},
            table_name='t',
            modes=['write'],
        )
        # 10000 / 40000 = 0.25
        assert opts['dynamodb.throughput.write.percent'] == '0.25'

    def test_write_xmax_overrides_table_level_limit(
        self, boto3_mock, ondemand_table_with_limits
    ):
        boto3_mock.dynamodb_client.describe_table.return_value = (
            ondemand_table_with_limits
        )
        opts = table_info.get_dynamodb_throughput_configs(
            args={'XMaxWriteRate': '20000'},
            table_name='t',
            modes=['write'],
        )
        # XMaxWriteRate=20000 set; denominator is still 40000 for on-demand
        # because no provisioned WCU is found. 20000/40000 = 0.5
        assert opts['dynamodb.throughput.write.percent'] == '0.5'


# --- get_dynamodb_throughput_configs : format=monitor -----------------------


class TestMonitorFormat:
    """format='monitor' returns aggregate_max_{read,write}_rate keys
    (used by the rate-limiter), not connector keys."""

    def test_returns_aggregate_max_read_rate(
        self, boto3_mock, provisioned_table
    ):
        boto3_mock.dynamodb_client.describe_table.return_value = provisioned_table
        result = table_info.get_dynamodb_throughput_configs(
            args={}, table_name='t', modes=['read'], format='monitor'
        )
        assert result == {'aggregate_max_read_rate': 1000}

    def test_returns_aggregate_max_write_rate_with_explicit_xmax(
        self, boto3_mock, provisioned_table
    ):
        boto3_mock.dynamodb_client.describe_table.return_value = provisioned_table
        result = table_info.get_dynamodb_throughput_configs(
            args={'XMaxWriteRate': '300'},
            table_name='t',
            modes=['write'],
            format='monitor',
        )
        assert result == {'aggregate_max_write_rate': 300}

    def test_returns_both_when_modes_includes_both(
        self, boto3_mock, provisioned_table
    ):
        boto3_mock.dynamodb_client.describe_table.return_value = provisioned_table
        result = table_info.get_dynamodb_throughput_configs(
            args={},
            table_name='t',
            modes=['read', 'write'],
            format='monitor',
        )
        assert result == {
            'aggregate_max_read_rate': 1000,
            'aggregate_max_write_rate': 500,
        }


# --- get_dynamodb_throughput_configs : misc -------------------------------


class TestUnknownFormat:
    def test_unknown_format_raises_value_error(
        self, boto3_mock, provisioned_table
    ):
        boto3_mock.dynamodb_client.describe_table.return_value = provisioned_table
        with pytest.raises(ValueError):
            table_info.get_dynamodb_throughput_configs(
                args={},
                table_name='t',
                modes=['read'],
                format='something-else',
            )


class TestBelowMinRecommended:
    """A rate below MIN_RECOMMENDED_READ_RATE/MIN_RECOMMENDED_WRITE_RATE
    triggers a warning log but is still returned (no skip, no exception).
    Pinning this prevents an over-eager refactor from raising or silently
    bumping the rate up to the floor."""

    def test_below_min_read_rate_still_returns_value(
        self, boto3_mock, provisioned_table
    ):
        opts = table_info.get_dynamodb_throughput_configs(
            args={'XMaxReadRate': '5'},
            table_name='t',
            modes=['read'],
        )
        assert opts['dynamodb.throughput.read'] == '5'

    def test_below_min_write_rate_still_returns_value(
        self, boto3_mock, provisioned_table
    ):
        boto3_mock.dynamodb_client.describe_table.return_value = provisioned_table
        opts = table_info.get_dynamodb_throughput_configs(
            args={'XMaxWriteRate': '10'},
            table_name='t',
            modes=['write'],
        )
        # Returns the percent — 10 / 500 = 0.02
        assert opts['dynamodb.throughput.write.percent'] == '0.02'


class TestDescribeTableFailure:
    """describe_table failure is non-fatal; the function continues with
    is_on_demand_table=False and an empty table_desc. Pinning this so a
    refactor doesn't tighten error handling and break the
    XMaxRate-only path that currently survives an AccessDenied."""

    def test_describe_failure_with_explicit_xmax_completes(self, boto3_mock):
        boto3_mock.dynamodb_client.describe_table.side_effect = RuntimeError(
            'no perms'
        )
        opts = table_info.get_dynamodb_throughput_configs(
            args={'XMaxReadRate': '500', 'XMaxWriteRate': '300'},
            table_name='t',
            modes=['read', 'write'],
        )
        assert opts['dynamodb.throughput.read'] == '500'
        assert opts['dynamodb.throughput.read.percent'] == '1.0'
        # No provisioned WCU found → denominator falls to DEFAULT (40000)
        # 300 / 40000 = 0.0075
        assert opts['dynamodb.throughput.write.percent'] == '0.0075'


# --- get_dynamodb_throughput_configs : additional branches -------------------


class TestThroughputConfigsOnDemandWriteBranches:
    """Cover on-demand write branches: table-specific limit, quota fallback,
    default fallback, and the 'no provisioned level found' provisioned branch."""

    def test_write_uses_table_specific_on_demand_limit(
        self, boto3_mock, ondemand_table_with_limits
    ):
        boto3_mock.dynamodb_client.describe_table.return_value = (
            ondemand_table_with_limits
        )
        opts = table_info.get_dynamodb_throughput_configs(
            args={}, table_name='t', modes=['write']
        )
        # table_write_limit=15000, denominator=40000 → 15000/40000 = 0.375
        assert opts['dynamodb.throughput.write.percent'] == '0.375'

    def test_write_uses_quota_when_no_table_limit(
        self, boto3_mock, ondemand_table
    ):
        boto3_mock.dynamodb_client.describe_table.return_value = ondemand_table
        boto3_mock.service_quotas_client.get_service_quota.return_value = {
            'Quota': {'Value': 60000}
        }
        opts = table_info.get_dynamodb_throughput_configs(
            args={}, table_name='t', modes=['write']
        )
        # quota_write_limit=60000, denominator=40000 → 60000/40000 = 1.5
        assert opts['dynamodb.throughput.write.percent'] == '1.5'

    def test_write_falls_back_to_default_40k(
        self, boto3_mock, ondemand_table
    ):
        boto3_mock.dynamodb_client.describe_table.return_value = ondemand_table
        boto3_mock.service_quotas_client.get_service_quota.side_effect = (
            RuntimeError('fail')
        )
        opts = table_info.get_dynamodb_throughput_configs(
            args={}, table_name='t', modes=['write']
        )
        # default=40000, denominator=40000 → 40000/40000 = 1.0
        assert opts['dynamodb.throughput.write.percent'] == '1.0'

    def test_provisioned_no_wcu_found_raises_type_error(
        self, boto3_mock
    ):
        """When ProvisionedThroughput has WriteCapacityUnits=0 (falsy), the
        function logs the Glue fallback message but then hits int(write_rate)
        where write_rate is still None — a latent bug. Pin current behavior."""
        response = {
            'Table': {
                'BillingModeSummary': {'BillingMode': 'PROVISIONED'},
                'ProvisionedThroughput': {'WriteCapacityUnits': 0},
            }
        }
        boto3_mock.dynamodb_client.describe_table.return_value = response
        with pytest.raises(TypeError):
            table_info.get_dynamodb_throughput_configs(
                args={}, table_name='t', modes=['write']
            )

    def test_provisioned_no_rcu_found_raises_type_error(
        self, boto3_mock
    ):
        """When ProvisionedThroughput has ReadCapacityUnits=0 (falsy),
        read_rate stays None and int(read_rate) at line 355 raises TypeError."""
        response = {
            'Table': {
                'BillingModeSummary': {'BillingMode': 'PROVISIONED'},
                'ProvisionedThroughput': {'ReadCapacityUnits': 0},
            }
        }
        boto3_mock.dynamodb_client.describe_table.return_value = response
        with pytest.raises(TypeError):
            table_info.get_dynamodb_throughput_configs(
                args={}, table_name='t', modes=['read']
            )

    def test_write_desired_percent_exceeds_cap_provisioned_note(
        self, boto3_mock, caplog, provisioned_table
    ):
        boto3_mock.dynamodb_client.describe_table.return_value = provisioned_table
        import logging
        with caplog.at_level(logging.DEBUG):
            opts = table_info.get_dynamodb_throughput_configs(
                args={'XMaxWriteRate': '1000'},
                table_name='t',
                modes=['write'],
            )
        assert opts['dynamodb.throughput.write.percent'] == '1.5'
        assert '150%' in caplog.text

    def test_write_desired_percent_exceeds_cap_ondemand_note(
        self, boto3_mock, caplog, ondemand_table
    ):
        boto3_mock.dynamodb_client.describe_table.return_value = ondemand_table
        boto3_mock.service_quotas_client.get_service_quota.side_effect = (
            RuntimeError('fail')
        )
        import logging
        with caplog.at_level(logging.DEBUG):
            opts = table_info.get_dynamodb_throughput_configs(
                args={'XMaxWriteRate': '100000'},
                table_name='t',
                modes=['write'],
            )
        assert opts['dynamodb.throughput.write.percent'] == '1.5'
        assert '60000' in caplog.text


class TestThroughputConfigsRegionResolution:
    """Tests that exercise ARN-based region resolution through the configs path."""

    def test_arn_based_table_name_uses_region_from_arn(self, boto3_mock, provisioned_table):
        boto3_mock.dynamodb_client.describe_table.return_value = provisioned_table
        arn = 'arn:aws:dynamodb:ap-southeast-2:123456789012:table/my-table'
        opts = table_info.get_dynamodb_throughput_configs(
            args={}, table_name=arn, modes=['read']
        )
        boto3_mock.client.assert_any_call('dynamodb', region_name='ap-southeast-2')
        assert opts['dynamodb.throughput.read'] == '1000'

    def test_no_region_raises_value_error(self, boto3_mock, monkeypatch):
        import os as _os
        monkeypatch.setattr(table_info, 'os', _os, raising=False)
        boto3_mock.Session.return_value.region_name = None
        monkeypatch.delenv('AWS_REGION', raising=False)
        monkeypatch.delenv('AWS_DEFAULT_REGION', raising=False)
        with pytest.raises(ValueError, match='Unable to determine region_name'):
            table_info.get_dynamodb_throughput_configs(
                args={}, table_name='plain-name', modes=['read']
            )


# --- get_and_print_dynamodb_table_info -------------------------------------


class TestGetAndPrintDynamoDBTableInfo:
    """Tests for the describe_table + print path."""

    @pytest.fixture
    def full_provisioned_response(self):
        return {
            'Table': {
                'TableName': 'my-table',
                'BillingModeSummary': {'BillingMode': 'PROVISIONED'},
                'ProvisionedThroughput': {
                    'ReadCapacityUnits': 500,
                    'WriteCapacityUnits': 250,
                },
                'ItemCount': 1000000,
                'TableSizeBytes': 512000000,
                'KeySchema': [
                    {'AttributeName': 'pk_id', 'KeyType': 'HASH'},
                    {'AttributeName': 'sk_id', 'KeyType': 'RANGE'},
                ],
                'AttributeDefinitions': [
                    {'AttributeName': 'pk_id', 'AttributeType': 'S'},
                    {'AttributeName': 'sk_id', 'AttributeType': 'N'},
                ],
            }
        }

    @pytest.fixture
    def full_ondemand_response(self):
        return {
            'Table': {
                'TableName': 'od-table',
                'BillingModeSummary': {'BillingMode': 'PAY_PER_REQUEST'},
                'OnDemandThroughput': {
                    'MaxReadRequestUnits': 10000,
                    'MaxWriteRequestUnits': 5000,
                },
                'ItemCount': 500,
                'TableSizeBytes': 65536,
                'KeySchema': [
                    {'AttributeName': 'id', 'KeyType': 'HASH'},
                ],
                'AttributeDefinitions': [
                    {'AttributeName': 'id', 'AttributeType': 'S'},
                ],
            }
        }

    @pytest.fixture
    def gsi_table_response(self):
        return {
            'Table': {
                'TableName': 'gsi-table',
                'BillingModeSummary': {'BillingMode': 'PROVISIONED'},
                'ProvisionedThroughput': {
                    'ReadCapacityUnits': 200,
                    'WriteCapacityUnits': 100,
                },
                'GlobalSecondaryIndexes': [
                    {
                        'IndexName': 'my-gsi',
                        'ProvisionedThroughput': {
                            'ReadCapacityUnits': 150,
                            'WriteCapacityUnits': 75,
                        },
                        'ItemCount': 800,
                        'IndexSizeBytes': 40960,
                        'KeySchema': [
                            {'AttributeName': 'gsi_pk', 'KeyType': 'HASH'},
                        ],
                    }
                ],
                'ItemCount': 1000,
                'TableSizeBytes': 51200,
                'KeySchema': [
                    {'AttributeName': 'pk', 'KeyType': 'HASH'},
                ],
                'AttributeDefinitions': [
                    {'AttributeName': 'pk', 'AttributeType': 'S'},
                    {'AttributeName': 'gsi_pk', 'AttributeType': 'S'},
                ],
            }
        }

    def test_provisioned_table_returns_metadata(
        self, boto3_mock, full_provisioned_response
    ):
        boto3_mock.dynamodb_client.describe_table.return_value = full_provisioned_response
        boto3_mock.autoscaling_client.describe_scalable_targets.return_value = {
            'ScalableTargets': []
        }
        result = table_info.get_and_print_dynamodb_table_info('my-table')
        assert result['table_name'] == 'my-table'
        assert result['billing_mode'] == 'PROVISIONED'
        assert result['item_count'] == 1000000
        assert result['size_bytes'] == 512000000
        assert result['key_schema']['pk']['name'] == 'pk_id'
        assert result['key_schema']['pk']['type'] == 'S'
        assert result['key_schema']['sk']['name'] == 'sk_id'
        assert result['key_schema']['sk']['type'] == 'N'
        assert result['write_pricing_category'] == 'std_wcu_pricing'
        assert result['read_pricing_category'] == 'std_rcu_pricing'

    def test_provisioned_table_quiet_mode(
        self, boto3_mock, full_provisioned_response, caplog
    ):
        boto3_mock.dynamodb_client.describe_table.return_value = full_provisioned_response
        boto3_mock.autoscaling_client.describe_scalable_targets.return_value = {
            'ScalableTargets': []
        }
        import logging
        with caplog.at_level(logging.DEBUG):
            result = table_info.get_and_print_dynamodb_table_info(
                'my-table', quiet=True
            )
        assert 'Billing mode' not in caplog.text
        assert result['billing_mode'] == 'PROVISIONED'

    def test_ondemand_table_returns_metadata(
        self, boto3_mock, full_ondemand_response
    ):
        boto3_mock.dynamodb_client.describe_table.return_value = full_ondemand_response
        result = table_info.get_and_print_dynamodb_table_info('od-table')
        assert result['billing_mode'] == 'PAY_PER_REQUEST'
        assert result['item_count'] == 500
        assert result['size_bytes'] == 65536
        assert result['key_schema']['pk']['name'] == 'id'

    def test_ondemand_no_throughput_limits(self, boto3_mock):
        response = {
            'Table': {
                'BillingModeSummary': {'BillingMode': 'PAY_PER_REQUEST'},
                'ItemCount': 10,
                'TableSizeBytes': 1024,
                'KeySchema': [
                    {'AttributeName': 'pk', 'KeyType': 'HASH'},
                ],
                'AttributeDefinitions': [
                    {'AttributeName': 'pk', 'AttributeType': 'S'},
                ],
            }
        }
        boto3_mock.dynamodb_client.describe_table.return_value = response
        result = table_info.get_and_print_dynamodb_table_info('t')
        assert result['billing_mode'] == 'PAY_PER_REQUEST'

    def test_table_not_found_raises_value_error(self, boto3_mock):
        # The source code catches `dynamodb.exceptions.ResourceNotFoundException`.
        # With a MagicMock client, the exception attribute is itself a Mock, so
        # `except dynamodb.exceptions.ResourceNotFoundException` catches any
        # instance of that Mock's class. We create a real exception subclass so
        # the raise/catch works and the f-string in the except block formats OK.
        class ResourceNotFoundException(Exception):
            pass
        boto3_mock.dynamodb_client.exceptions.ResourceNotFoundException = (
            ResourceNotFoundException
        )
        boto3_mock.dynamodb_client.describe_table.side_effect = (
            ResourceNotFoundException('not found')
        )
        with pytest.raises(ValueError, match="does not exist"):
            table_info.get_and_print_dynamodb_table_info('ghost-table')

    def test_gsi_provisioned_returns_index_metadata(
        self, boto3_mock, gsi_table_response
    ):
        boto3_mock.dynamodb_client.describe_table.return_value = gsi_table_response
        boto3_mock.autoscaling_client.describe_scalable_targets.return_value = {
            'ScalableTargets': []
        }
        result = table_info.get_and_print_dynamodb_table_info(
            'gsi-table', index_name='my-gsi'
        )
        assert result['item_count'] == 800
        assert result['size_bytes'] == 40960

    def test_gsi_not_found_returns_none(self, boto3_mock, gsi_table_response):
        boto3_mock.dynamodb_client.describe_table.return_value = gsi_table_response
        result = table_info.get_and_print_dynamodb_table_info(
            'gsi-table', index_name='nonexistent-gsi'
        )
        assert result is None

    def test_infrequent_access_table_class(self, boto3_mock):
        response = {
            'Table': {
                'BillingModeSummary': {'BillingMode': 'PAY_PER_REQUEST'},
                'TableClassSummary': {'TableClass': 'STANDARD_INFREQUENT_ACCESS'},
                'ItemCount': 100,
                'TableSizeBytes': 2048,
                'KeySchema': [
                    {'AttributeName': 'pk', 'KeyType': 'HASH'},
                ],
                'AttributeDefinitions': [
                    {'AttributeName': 'pk', 'AttributeType': 'S'},
                ],
            }
        }
        boto3_mock.dynamodb_client.describe_table.return_value = response
        result = table_info.get_and_print_dynamodb_table_info('ia-table')
        assert result['write_pricing_category'] == 'ia_wcu_pricing'
        assert result['read_pricing_category'] == 'ia_rcu_pricing'

    def test_provisioned_with_autoscaling(
        self, boto3_mock, full_provisioned_response, caplog
    ):
        boto3_mock.dynamodb_client.describe_table.return_value = full_provisioned_response
        boto3_mock.autoscaling_client.describe_scalable_targets.return_value = {
            'ScalableTargets': [{
                'MinCapacity': 100,
                'MaxCapacity': 1000,
            }]
        }
        boto3_mock.autoscaling_client.describe_scaling_policies.return_value = {
            'ScalingPolicies': [{
                'TargetTrackingScalingPolicyConfiguration': {
                    'TargetValue': 70.0,
                }
            }]
        }
        import logging
        with caplog.at_level(logging.DEBUG):
            result = table_info.get_and_print_dynamodb_table_info('my-table')
        assert 'Auto Scaling Enabled: Yes' in caplog.text
        assert 'Min Capacity: 100' in caplog.text
        assert 'Max Capacity: 1,000' in caplog.text
        assert 'Target Value: 70.0' in caplog.text

    def test_provisioned_no_autoscaling(
        self, boto3_mock, full_provisioned_response, caplog
    ):
        boto3_mock.dynamodb_client.describe_table.return_value = full_provisioned_response
        boto3_mock.autoscaling_client.describe_scalable_targets.return_value = {
            'ScalableTargets': []
        }
        import logging
        with caplog.at_level(logging.DEBUG):
            table_info.get_and_print_dynamodb_table_info('my-table')
        assert 'Auto Scaling Enabled: No' in caplog.text

    def test_gsi_ondemand_returns_index_metadata(self, boto3_mock):
        response = {
            'Table': {
                'BillingModeSummary': {'BillingMode': 'PAY_PER_REQUEST'},
                'GlobalSecondaryIndexes': [
                    {
                        'IndexName': 'od-gsi',
                        'OnDemandThroughput': {
                            'MaxReadRequestUnits': 5000,
                            'MaxWriteRequestUnits': 3000,
                        },
                        'ItemCount': 200,
                        'IndexSizeBytes': 10240,
                        'KeySchema': [
                            {'AttributeName': 'gsi_pk', 'KeyType': 'HASH'},
                        ],
                    }
                ],
                'ItemCount': 1000,
                'TableSizeBytes': 51200,
                'KeySchema': [
                    {'AttributeName': 'pk', 'KeyType': 'HASH'},
                ],
                'AttributeDefinitions': [
                    {'AttributeName': 'pk', 'AttributeType': 'S'},
                    {'AttributeName': 'gsi_pk', 'AttributeType': 'S'},
                ],
            }
        }
        boto3_mock.dynamodb_client.describe_table.return_value = response
        result = table_info.get_and_print_dynamodb_table_info(
            'gsi-table', index_name='od-gsi'
        )
        assert result['item_count'] == 200
        assert result['size_bytes'] == 10240

    def test_region_resolved_from_arn(self, boto3_mock):
        response = {
            'Table': {
                'BillingModeSummary': {'BillingMode': 'PAY_PER_REQUEST'},
                'ItemCount': 5,
                'TableSizeBytes': 256,
                'KeySchema': [
                    {'AttributeName': 'pk', 'KeyType': 'HASH'},
                ],
                'AttributeDefinitions': [
                    {'AttributeName': 'pk', 'AttributeType': 'S'},
                ],
            }
        }
        boto3_mock.dynamodb_client.describe_table.return_value = response
        arn = 'arn:aws:dynamodb:eu-central-1:123456789012:table/my-table'
        result = table_info.get_and_print_dynamodb_table_info(arn)
        assert result['region_name'] == 'eu-central-1'


# --- get_and_print_table_scan_cost -----------------------------------------


class TestGetAndPrintTableScanCost:
    """Tests for scan cost estimation with both billing modes."""

    @pytest.fixture
    def pricing_mock(self, monkeypatch):
        mock = MagicMock()
        mock.return_value.get_on_demand_capacity_pricing.return_value = {
            'std_rcu_pricing': '0.00000025',
            'std_wcu_pricing': '0.00000125',
            'ia_rcu_pricing': '0.0000003',
            'ia_wcu_pricing': '0.0000015',
        }
        monkeypatch.setattr(table_info, 'PricingUtility', mock)
        return mock

    @pytest.fixture
    def provisioned_table_info(self):
        return {
            'table_name': 't',
            'region_name': 'us-east-1',
            'billing_mode': 'PROVISIONED',
            'write_pricing_category': 'std_wcu_pricing',
            'read_pricing_category': 'std_rcu_pricing',
            'item_count': 1000,
            'size_bytes': 80960,
        }

    @pytest.fixture
    def ondemand_table_info(self):
        return {
            'table_name': 't',
            'region_name': 'us-west-2',
            'billing_mode': 'PAY_PER_REQUEST',
            'write_pricing_category': 'std_wcu_pricing',
            'read_pricing_category': 'std_rcu_pricing',
            'item_count': 2000,
            'size_bytes': 161920,
        }

    def test_provisioned_scan_returns_cost(
        self, boto3_mock, pricing_mock, provisioned_table_info, caplog
    ):
        import logging
        import math
        with caplog.at_level(logging.DEBUG):
            cost = table_info.get_and_print_table_scan_cost(
                provisioned_table_info, region_name='us-east-1'
            )
        read_units = math.ceil(80960 / 8096)
        rru_cost = 0.00000025
        expected = (read_units * rru_cost) / 1.5
        assert cost == pytest.approx(expected)
        assert 'provisioned scan' in caplog.text

    def test_ondemand_scan_returns_cost(
        self, boto3_mock, pricing_mock, ondemand_table_info, caplog
    ):
        import logging
        import math
        with caplog.at_level(logging.DEBUG):
            cost = table_info.get_and_print_table_scan_cost(
                ondemand_table_info, region_name='us-west-2'
            )
        read_units = math.ceil(161920 / 8096)
        rru_cost = 0.00000025
        expected = read_units * rru_cost
        assert cost == pytest.approx(expected)
        assert 'on-demand scan' in caplog.text

    def test_fraction_reduces_read_units(
        self, boto3_mock, pricing_mock, provisioned_table_info
    ):
        cost_full = table_info.get_and_print_table_scan_cost(
            provisioned_table_info, region_name='us-east-1'
        )
        cost_half = table_info.get_and_print_table_scan_cost(
            provisioned_table_info, region_name='us-east-1', fraction=0.5
        )
        assert cost_half < cost_full

    def test_multiple_scans_multiplies_cost(
        self, boto3_mock, pricing_mock, provisioned_table_info
    ):
        cost_one = table_info.get_and_print_table_scan_cost(
            provisioned_table_info, region_name='us-east-1'
        )
        cost_three = table_info.get_and_print_table_scan_cost(
            provisioned_table_info, region_name='us-east-1', numberOfScans=3
        )
        assert cost_three == pytest.approx(cost_one * 3)

    def test_unknown_billing_mode_returns_zero(
        self, boto3_mock, pricing_mock, provisioned_table_info
    ):
        provisioned_table_info['billing_mode'] = 'UNKNOWN_MODE'
        cost = table_info.get_and_print_table_scan_cost(
            provisioned_table_info, region_name='us-east-1'
        )
        assert cost == 0

    def test_region_from_table_info_fallback(
        self, boto3_mock, pricing_mock, provisioned_table_info
    ):
        cost = table_info.get_and_print_table_scan_cost(
            provisioned_table_info
        )
        assert cost > 0
        pricing_mock.return_value.get_on_demand_capacity_pricing.assert_called_with('us-east-1')


# --- get_and_print_table_write_cost ----------------------------------------


class TestGetAndPrintTableWriteCost:
    """Tests for write cost estimation."""

    @pytest.fixture
    def pricing_mock(self, monkeypatch):
        mock = MagicMock()
        mock.return_value.get_on_demand_capacity_pricing.return_value = {
            'std_wcu_pricing': '0.00000125',
            'std_rcu_pricing': '0.00000025',
            'ia_wcu_pricing': '0.0000015',
            'ia_rcu_pricing': '0.0000003',
        }
        monkeypatch.setattr(table_info, 'PricingUtility', mock)
        return mock

    @pytest.fixture
    def table_info_provisioned(self):
        return {
            'table_name': 't',
            'region_name': 'us-east-1',
            'billing_mode': 'PROVISIONED',
            'write_pricing_category': 'std_wcu_pricing',
            'read_pricing_category': 'std_rcu_pricing',
        }

    @pytest.fixture
    def table_info_ondemand(self):
        return {
            'table_name': 't',
            'region_name': 'us-east-1',
            'billing_mode': 'PAY_PER_REQUEST',
            'write_pricing_category': 'std_wcu_pricing',
            'read_pricing_category': 'std_rcu_pricing',
        }

    def test_zero_items_returns_zero(
        self, boto3_mock, pricing_mock, table_info_provisioned, caplog
    ):
        import logging
        with caplog.at_level(logging.DEBUG):
            cost = table_info.get_and_print_table_write_cost(
                table_info_provisioned, item_count=0, size_bytes=0
            )
        assert cost == 0
        assert 'No items to write' in caplog.text

    def test_provisioned_write_cost(
        self, boto3_mock, pricing_mock, table_info_provisioned
    ):
        import math
        cost = table_info.get_and_print_table_write_cost(
            table_info_provisioned, item_count=1000, size_bytes=2048000
        )
        avg_size = 2048000 / 1000  # 2048
        avg_wu = math.ceil(avg_size / 1024)  # 2
        write_units = 1000 * avg_wu  # 2000
        wru_cost = 0.00000125
        expected = (write_units * wru_cost) / 1.5
        assert cost == pytest.approx(expected)

    def test_ondemand_write_cost(
        self, boto3_mock, pricing_mock, table_info_ondemand
    ):
        import math
        cost = table_info.get_and_print_table_write_cost(
            table_info_ondemand, item_count=500, size_bytes=512000
        )
        avg_size = 512000 / 500  # 1024
        avg_wu = math.ceil(avg_size / 1024)  # 1
        write_units = 500 * avg_wu  # 500
        wru_cost = 0.00000125
        expected = write_units * wru_cost
        assert cost == pytest.approx(expected)

    def test_unknown_billing_mode_returns_zero(
        self, boto3_mock, pricing_mock, table_info_provisioned
    ):
        table_info_provisioned['billing_mode'] = 'SOMETHING_ELSE'
        cost = table_info.get_and_print_table_write_cost(
            table_info_provisioned, item_count=100, size_bytes=10240
        )
        assert cost == 0


# --- get_and_print_table_copy_write_cost -----------------------------------


class TestGetAndPrintTableCopyWriteCost:
    """Tests for copy-write cost estimation."""

    @pytest.fixture
    def pricing_mock(self, monkeypatch):
        mock = MagicMock()
        mock.return_value.get_on_demand_capacity_pricing.return_value = {
            'std_wcu_pricing': '0.00000125',
            'std_rcu_pricing': '0.00000025',
        }
        monkeypatch.setattr(table_info, 'PricingUtility', mock)
        return mock

    @pytest.fixture
    def source_info(self):
        return {
            'item_count': 10000,
            'size_bytes': 10240000,
        }

    @pytest.fixture
    def target_provisioned(self):
        return {
            'region_name': 'us-east-1',
            'billing_mode': 'PROVISIONED',
            'write_pricing_category': 'std_wcu_pricing',
        }

    @pytest.fixture
    def target_ondemand(self):
        return {
            'region_name': 'eu-west-1',
            'billing_mode': 'PAY_PER_REQUEST',
            'write_pricing_category': 'std_wcu_pricing',
        }

    def test_provisioned_copy_cost(
        self, boto3_mock, pricing_mock, source_info, target_provisioned
    ):
        import math
        cost = table_info.get_and_print_table_copy_write_cost(
            source_info, target_provisioned
        )
        item_size = 10240000 / 10000  # 1024
        avg_wu = math.ceil(item_size / 1024)  # 1
        write_units = 10000 * avg_wu  # 10000
        wru_cost = 0.00000125
        expected = (write_units * wru_cost) / 1.5
        assert cost == pytest.approx(expected)

    def test_ondemand_copy_cost(
        self, boto3_mock, pricing_mock, source_info, target_ondemand
    ):
        import math
        cost = table_info.get_and_print_table_copy_write_cost(
            source_info, target_ondemand
        )
        item_size = 10240000 / 10000
        avg_wu = math.ceil(item_size / 1024)
        write_units = 10000 * avg_wu
        wru_cost = 0.00000125
        expected = write_units * wru_cost
        assert cost == pytest.approx(expected)

    def test_zero_items_returns_zero_cost(
        self, boto3_mock, pricing_mock, target_provisioned
    ):
        source = {'item_count': 0, 'size_bytes': 0}
        cost = table_info.get_and_print_table_copy_write_cost(
            source, target_provisioned
        )
        assert cost == 0

    def test_unknown_billing_mode_returns_zero(
        self, boto3_mock, pricing_mock, source_info
    ):
        target = {
            'region_name': 'us-east-1',
            'billing_mode': 'MYSTERY',
            'write_pricing_category': 'std_wcu_pricing',
        }
        cost = table_info.get_and_print_table_copy_write_cost(
            source_info, target
        )
        assert cost == 0


# --- get_quota_value additional branches -----------------------------------


class TestGetQuotaValueFallbackFailure:
    """Cover the branch where NoSuchResourceException fallback itself fails."""

    def test_fallback_to_default_quota_also_fails(self, boto3_mock):
        sq = boto3_mock.service_quotas_client
        sq.get_service_quota.side_effect = botocore.exceptions.ClientError(
            {'Error': {'Code': 'NoSuchResourceException', 'Message': 'no'}},
            'GetServiceQuota',
        )
        sq.get_aws_default_service_quota.side_effect = RuntimeError('inner fail')
        result = table_info.get_quota_value(
            'Table-level read throughput limit', 'us-east-1'
        )
        assert result is None


# --- _default_region -------------------------------------------------------


class TestDefaultRegion:
    @pytest.fixture(autouse=True)
    def inject_os(self, monkeypatch):
        """table_info.py references os.environ but doesn't import os.
        Inject the real os module so the env-var fallback paths execute."""
        import os
        monkeypatch.setattr(table_info, 'os', os, raising=False)

    def test_falls_back_to_aws_region_env(self, boto3_mock, monkeypatch):
        boto3_mock.Session.return_value.region_name = None
        monkeypatch.setenv('AWS_REGION', 'ap-northeast-1')
        monkeypatch.delenv('AWS_DEFAULT_REGION', raising=False)
        result = table_info._default_region()
        assert result == 'ap-northeast-1'

    def test_falls_back_to_aws_default_region_env(self, boto3_mock, monkeypatch):
        boto3_mock.Session.return_value.region_name = None
        monkeypatch.delenv('AWS_REGION', raising=False)
        monkeypatch.setenv('AWS_DEFAULT_REGION', 'sa-east-1')
        result = table_info._default_region()
        assert result == 'sa-east-1'

    def test_returns_session_region_first(self, boto3_mock, monkeypatch):
        boto3_mock.Session.return_value.region_name = 'us-west-2'
        monkeypatch.setenv('AWS_REGION', 'eu-west-1')
        result = table_info._default_region()
        assert result == 'us-west-2'


# --- Additional coverage gaps -----------------------------------------------


class TestGetAndPrintDynamoDBTableInfoNoRegion:
    """Cover the line-61 ValueError path: when neither the ARN-derived region
    nor the default-region helper produces a usable region, the function
    refuses to call boto3 and raises with a clear message.

    This guards against silently calling boto3 with region_name=None, which
    would surface as a confusing botocore error far from the caller's site.
    """

    def test_no_region_raises_value_error(self, boto3_mock, monkeypatch):
        """No ARN region, no session region, no env vars → ValueError."""
        # Ensure both region resolution sources return None.
        monkeypatch.setattr(
            table_info, '_region_from_table_ref', lambda _: None
        )
        monkeypatch.setattr(table_info, '_default_region', lambda: None)

        with pytest.raises(ValueError, match='Unable to determine region_name'):
            table_info.get_and_print_dynamodb_table_info('plain-name')

    def test_no_region_does_not_call_describe_table(self, boto3_mock, monkeypatch):
        """When the region check fails, no AWS client work is done."""
        monkeypatch.setattr(
            table_info, '_region_from_table_ref', lambda _: None
        )
        monkeypatch.setattr(table_info, '_default_region', lambda: None)

        with pytest.raises(ValueError):
            table_info.get_and_print_dynamodb_table_info('plain-name')
        # boto3.client should never have been invoked for dynamodb.
        boto3_mock.dynamodb_client.describe_table.assert_not_called()


class TestGetThroughputConfigsModesDefault:
    """Cover line 307: when callers omit `modes`, both read and write are
    configured. The default-mode path matters because most production verbs
    (load, copy, delete) lean on it implicitly to set up bidirectional
    capacity controls in one call.
    """

    def test_modes_none_defaults_to_read_and_write(
        self, boto3_mock, provisioned_table
    ):
        """modes=None → defaults to ('read', 'write'); both keys appear."""
        boto3_mock.dynamodb_client.describe_table.return_value = provisioned_table
        opts = table_info.get_dynamodb_throughput_configs(
            args={'XMaxReadRate': '200', 'XMaxWriteRate': '50'},
            table_name='t',
            modes=None,
        )
        # Read side present
        assert opts['dynamodb.throughput.read'] == '200'
        assert opts['dynamodb.throughput.read.percent'] == '1.0'
        # Write side present (50/500 = 0.1)
        assert opts['dynamodb.throughput.write.percent'] == '0.1'

    def test_modes_default_arg_omitted_entirely(
        self, boto3_mock, provisioned_table
    ):
        """Calling without specifying modes at all hits the same default."""
        boto3_mock.dynamodb_client.describe_table.return_value = provisioned_table
        opts = table_info.get_dynamodb_throughput_configs(
            args={'XMaxReadRate': '300', 'XMaxWriteRate': '100'},
            table_name='t',
        )
        assert 'dynamodb.throughput.read' in opts
        assert 'dynamodb.throughput.write.percent' in opts


class TestGetThroughputConfigsReadRateFalsyConnector:
    """Cover the 399->401 branch: when `read_rate` is falsy at the connector
    serialization step, the `dynamodb.throughput.read` key is omitted while
    `dynamodb.throughput.read.percent` is still set.

    This branch is taken when a provisioned table has no detectable
    ReadCapacityUnits (e.g. a malformed describe_table response) and the
    user passed a falsy XMaxReadRate (0). The function logs a warning but
    must still emit a valid connector option dict.
    """

    def test_read_rate_zero_emits_only_percent_key(self, boto3_mock):
        """args XMaxReadRate=0 + provisioned table with no RCU → no read key."""
        # Provisioned response with no ReadCapacityUnits at all → falsy
        # provisioned_read, so read_rate stays at the args-supplied 0.
        response = {
            'Table': {
                'BillingModeSummary': {'BillingMode': 'PROVISIONED'},
                'ProvisionedThroughput': {},
            }
        }
        boto3_mock.dynamodb_client.describe_table.return_value = response

        opts = table_info.get_dynamodb_throughput_configs(
            args={'XMaxReadRate': 0, 'XMaxWriteRate': '100'},
            table_name='t',
            modes=['read', 'write'],
        )
        # Branch 399->401: read_rate is falsy → skip the read-rate key,
        # but the read percent key is always set.
        assert 'dynamodb.throughput.read' not in opts
        assert opts['dynamodb.throughput.read.percent'] == '1.0'


# --- _warn_rate_vs_capacity ---------------------------------------------------


class TestWarnRateVsCapacity:
    """Unit tests for the _warn_rate_vs_capacity helper."""

    def test_returns_suggested_range(self):
        low, high = table_info._warn_rate_vs_capacity('t', 'read', 500, 1000)
        assert low == 100  # max(MIN_RECOMMENDED=100, 1000//10=100)
        assert high == 1000

    def test_suggested_low_is_at_least_min_recommended(self):
        low, high = table_info._warn_rate_vs_capacity('t', 'write', 50, 500)
        # 500 // 10 = 50, but MIN_RECOMMENDED_WRITE_RATE=100 wins
        assert low == 100
        assert high == 500

    def test_suggested_low_scales_with_large_capacity(self):
        low, high = table_info._warn_rate_vs_capacity('t', 'read', 5000, 40000)
        # 40000 // 10 = 4000 > MIN=100
        assert low == 4000
        assert high == 40000

    def test_too_high_emits_warning(self, caplog):
        import logging
        with caplog.at_level(logging.WARNING):
            table_info._warn_rate_vs_capacity('t', 'read', 2000, 1000)
        assert 'exceeds table capacity' in caplog.text
        assert '2,000' in caplog.text
        assert '1,000' in caplog.text

    def test_too_low_below_min_emits_warning(self, caplog):
        import logging
        with caplog.at_level(logging.WARNING):
            table_info._warn_rate_vs_capacity('t', 'write', 5, 1000)
        assert 'very low' in caplog.text

    def test_low_relative_to_capacity_emits_warning(self, caplog):
        import logging
        with caplog.at_level(logging.WARNING):
            # capacity=40000, rate=200 is above MIN_RECOMMENDED but below 40000//10=4000
            table_info._warn_rate_vs_capacity('t', 'read', 200, 40000)
        assert 'low relative to table capacity' in caplog.text

    def test_rate_within_range_no_warning(self, caplog):
        import logging
        with caplog.at_level(logging.WARNING):
            table_info._warn_rate_vs_capacity('t', 'read', 500, 1000)
        assert 'WARNING' not in caplog.text
        assert 'exceeds' not in caplog.text
        assert 'low' not in caplog.text


# --- Rate validation integration in get_dynamodb_throughput_configs -----------


class TestRateValidationIntegration:
    """Verify _warn_rate_vs_capacity is called when users specify explicit rates."""

    def test_read_rate_too_high_provisioned_warns(
        self, boto3_mock, provisioned_table, caplog
    ):
        import logging
        boto3_mock.dynamodb_client.describe_table.return_value = provisioned_table
        with caplog.at_level(logging.WARNING):
            table_info.get_dynamodb_throughput_configs(
                args={'XMaxReadRate': '5000'}, table_name='t', modes=['read']
            )
        # provisioned RCU=1000, user wants 5000 → too high
        assert 'exceeds table capacity' in caplog.text

    def test_write_rate_too_high_provisioned_warns(
        self, boto3_mock, provisioned_table, caplog
    ):
        import logging
        boto3_mock.dynamodb_client.describe_table.return_value = provisioned_table
        with caplog.at_level(logging.WARNING):
            table_info.get_dynamodb_throughput_configs(
                args={'XMaxWriteRate': '2000'}, table_name='t', modes=['write']
            )
        # provisioned WCU=500, user wants 2000 → too high
        assert 'exceeds table capacity' in caplog.text

    def test_read_rate_too_high_ondemand_warns(
        self, boto3_mock, ondemand_table_with_limits, caplog
    ):
        import logging
        boto3_mock.dynamodb_client.describe_table.return_value = (
            ondemand_table_with_limits
        )
        with caplog.at_level(logging.WARNING):
            table_info.get_dynamodb_throughput_configs(
                args={'XMaxReadRate': '50000'}, table_name='t', modes=['read']
            )
        # on-demand table read limit=25000, user wants 50000 → too high
        assert 'exceeds table capacity' in caplog.text

    def test_write_rate_too_low_provisioned_warns(
        self, boto3_mock, provisioned_table, caplog
    ):
        import logging
        boto3_mock.dynamodb_client.describe_table.return_value = provisioned_table
        with caplog.at_level(logging.WARNING):
            table_info.get_dynamodb_throughput_configs(
                args={'XMaxWriteRate': '10'}, table_name='t', modes=['write']
            )
        # provisioned WCU=500, user wants 10 → very low (below MIN_RECOMMENDED)
        assert 'very low' in caplog.text

    def test_rate_within_capacity_no_capacity_warning(
        self, boto3_mock, provisioned_table, caplog
    ):
        import logging
        boto3_mock.dynamodb_client.describe_table.return_value = provisioned_table
        with caplog.at_level(logging.WARNING):
            table_info.get_dynamodb_throughput_configs(
                args={'XMaxReadRate': '800'}, table_name='t', modes=['read']
            )
        # provisioned RCU=1000, user wants 800 → within range, no warning
        assert 'exceeds' not in caplog.text
        assert 'low relative' not in caplog.text
        assert 'very low' not in caplog.text

    def test_no_capacity_warning_when_describe_table_fails(
        self, boto3_mock, caplog
    ):
        import logging
        boto3_mock.dynamodb_client.describe_table.side_effect = RuntimeError('nope')
        with caplog.at_level(logging.WARNING):
            table_info.get_dynamodb_throughput_configs(
                args={'XMaxReadRate': '99999'}, table_name='t', modes=['read']
            )
        # describe_table failed → no capacity info → no capacity warning
        assert 'exceeds table capacity' not in caplog.text

    def test_ondemand_no_table_limit_uses_default_40k(
        self, boto3_mock, ondemand_table, caplog
    ):
        import logging
        boto3_mock.dynamodb_client.describe_table.return_value = ondemand_table
        with caplog.at_level(logging.WARNING):
            table_info.get_dynamodb_throughput_configs(
                args={'XMaxWriteRate': '80000'}, table_name='t', modes=['write']
            )
        # on-demand no table limit → default capacity=40000, user wants 80000 → too high
        assert 'exceeds table capacity' in caplog.text
