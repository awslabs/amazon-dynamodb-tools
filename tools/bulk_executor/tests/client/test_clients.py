"""Unit tests for client/src/clients.py.

Covers `Clients.__init__`:
- iam_client: constructed with only the service name (no region kwarg)
- dynamodb_client, glue_client, logs_client, s3_client, service_quotas_client:
  each constructed with the requested region_name=region kwarg
- All six client attributes attached to the instance

Style notes:
- Patch boto3.client at the clients module namespace (`clients.boto3`),
  not the global boto3, because the source binds `import boto3` at module
  scope.
- Use side_effect with a per-service map so any ordering bug shows up
  as a wrong client landing on a wrong attribute.
"""

from unittest.mock import MagicMock

import pytest

import clients as clients_module


@pytest.fixture
def boto_factory(monkeypatch):
    """Patch clients.boto3.client and return (call_log, service_to_mock).

    Each call to boto3.client is recorded in call_log. The returned MagicMock
    for a service is keyed by service name so tests can assert that the
    *exact* mock for service X landed on the right attribute.
    """
    service_to_mock = {
        'iam': MagicMock(name='iam_client'),
        'dynamodb': MagicMock(name='dynamodb_client'),
        'glue': MagicMock(name='glue_client'),
        'logs': MagicMock(name='logs_client'),
        's3': MagicMock(name='s3_client'),
        'service-quotas': MagicMock(name='service_quotas_client'),
    }
    call_log = []

    def fake_client(service, **kwargs):
        call_log.append((service, kwargs))
        return service_to_mock[service]

    boto3_mock = MagicMock()
    boto3_mock.client.side_effect = fake_client
    monkeypatch.setattr(clients_module, 'boto3', boto3_mock)
    return call_log, service_to_mock


class TestClientsInit:
    """Tests for Clients.__init__ (lines 5-11)."""

    def test_constructs_all_six_clients(self, boto_factory):
        """All six boto3.client(...) calls fire when a Clients is built."""
        call_log, _ = boto_factory
        clients_module.Clients('us-east-1')
        services = [service for service, _ in call_log]
        assert services == [
            'iam',
            'dynamodb',
            'glue',
            'logs',
            's3',
            'service-quotas',
        ]

    def test_iam_client_has_no_region(self, boto_factory):
        """Line 6: iam_client built without region_name kwarg (global service)."""
        call_log, _ = boto_factory
        clients_module.Clients('us-west-2')
        iam_call = next(kwargs for service, kwargs in call_log if service == 'iam')
        assert iam_call == {}

    def test_regional_clients_pass_region(self, boto_factory):
        """Lines 7-11: regional clients receive region_name=region."""
        call_log, _ = boto_factory
        clients_module.Clients('eu-central-1')

        regional_services = ['dynamodb', 'glue', 'logs', 's3', 'service-quotas']
        for service in regional_services:
            kwargs = next(k for s, k in call_log if s == service)
            assert kwargs == {'region_name': 'eu-central-1'}, (
                f"{service} should be built with region_name=eu-central-1"
            )

    def test_attributes_bound_to_correct_clients(self, boto_factory):
        """Each attribute holds the boto client for its named service."""
        _, service_to_mock = boto_factory
        instance = clients_module.Clients('us-east-1')

        assert instance.iam_client is service_to_mock['iam']
        assert instance.dynamodb_client is service_to_mock['dynamodb']
        assert instance.glue_client is service_to_mock['glue']
        assert instance.logs_client is service_to_mock['logs']
        assert instance.s3_client is service_to_mock['s3']
        assert instance.service_quotas_client is service_to_mock['service-quotas']

    def test_region_propagation_per_call(self, boto_factory):
        """Different region values land on each regional client per construction."""
        call_log, _ = boto_factory

        clients_module.Clients('ap-south-1')
        # 6 calls per construction
        assert len(call_log) == 6

        # All regional kwargs match the one we passed in
        for service, kwargs in call_log:
            if service == 'iam':
                continue
            assert kwargs.get('region_name') == 'ap-south-1'

    def test_two_instances_use_different_regions(self, boto_factory):
        """Each construction sends its own region; no cached state across instances."""
        call_log, _ = boto_factory

        clients_module.Clients('us-east-1')
        clients_module.Clients('us-west-2')

        # First six calls use us-east-1; next six use us-west-2 (iam has no region)
        first_batch = call_log[:6]
        second_batch = call_log[6:12]
        first_regions = [k.get('region_name') for s, k in first_batch if s != 'iam']
        second_regions = [k.get('region_name') for s, k in second_batch if s != 'iam']
        assert first_regions == ['us-east-1'] * 5
        assert second_regions == ['us-west-2'] * 5
