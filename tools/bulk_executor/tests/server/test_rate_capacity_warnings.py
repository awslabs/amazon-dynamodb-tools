"""Unit tests for issue #89 capacity-vs-request warnings in table_info.

Covers the helpers and the wiring inside get_dynamodb_throughput_configs that
warn when a user-specified XMaxReadRate / XMaxWriteRate exceeds what the table
can actually deliver. Maps to Jason's review checks on PR #231:

- Check #2: provisioned, NO autoscaling, request > provisioned  -> hard warn
- Check #3: provisioned, WITH autoscaling, request > autoscaling max -> hard warn
- #89 nuance: provisioned < request <= autoscaling max -> softer "will scale up" note
- Check #4: on-demand, request > table's OnDemandThroughput max -> hard warn
- Check #5: on-demand, no table max, request > account quota -> hard warn
- Check #1: effective rate too low to move the table's data before the job
  runs out of time -> "will likely time out" warn. The budget is the time
  *remaining* before the job timeout (timeout minus elapsed since job start),
  NOT the raw timeout: multi-phase verbs (e.g. delete = scan then write)
  resolve a later phase's rate only after an earlier phase has already
  consumed part of the timeout, so that phase must race the time left. The
  rate may be user-specified or table-derived; the estimate uses the table
  metadata already read here, so it lives in table_info alongside checks 2-5.

Functions/branches exercised:
- _bare_table_name: plain name, ARN, ARN+index, malformed ARN
- _autoscaling_max_capacity: target present, absent, lookup raises
- _effective_capacity_ceiling: all four billing-mode branches + None fallbacks
- _warn_if_rate_exceeds_capacity: hard warn, soft note, at/under ceiling silent
- _job_elapsed_minutes: zero at import, grows with the monotonic clock, clamped
  non-negative.
- _warn_if_job_may_timeout: read scan-units, write item-units, under/over the
  remaining budget, small-remaining vs full-timeout, zero/negative remaining,
  zero/missing metadata, non-numeric rate.
- get_dynamodb_throughput_configs: read+write wiring, user-only rates,
  graceful degradation, return value unchanged, and that the timeout check
  uses time remaining (timeout minus elapsed) rather than the raw timeout.
"""

import importlib.util
import logging
import sys
from pathlib import Path
from unittest.mock import MagicMock

import botocore.exceptions
import pytest

# Load the real table_info module (same pattern as test_table_info.py): the
# repo conftest mocks python_modules.shared.table_info so verb modules import
# cleanly, but here we want the real implementation loaded from disk.
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
    """Dispatching boto3 mock exposing per-service client mocks."""
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


def _no_autoscaling(boto3_mock):
    """Configure the autoscaling client to report no scalable targets."""
    boto3_mock.autoscaling_client.describe_scalable_targets.return_value = {
        'ScalableTargets': []
    }


def _autoscaling_max(boto3_mock, max_capacity):
    """Configure the autoscaling client to report a target with MaxCapacity."""
    boto3_mock.autoscaling_client.describe_scalable_targets.return_value = {
        'ScalableTargets': [{'MaxCapacity': max_capacity, 'MinCapacity': 5}]
    }


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


def _capacity_warnings(caplog):
    """Return log messages that are the #89 capacity warnings (hard or soft)."""
    return [
        m for m in caplog.messages
        if 'exceeds the table' in m or 'autoscaling will need to scale up' in m.lower()
    ]


def _timeout_warnings(caplog):
    """Return log messages that are the #89 check-1 job-timeout warnings."""
    return [m for m in caplog.messages if 'will likely time out' in m]


# --- _bare_table_name -------------------------------------------------------


class TestBareTableName:
    def test_plain_name_returned_unchanged(self):
        assert table_info._bare_table_name('my-table') == 'my-table'

    def test_arn_yields_table_name(self):
        arn = 'arn:aws:dynamodb:us-east-1:123456789012:table/my-table'
        assert table_info._bare_table_name(arn) == 'my-table'

    def test_arn_with_index_yields_table_name(self):
        arn = 'arn:aws:dynamodb:us-east-1:123456789012:table/my-table/index/gsi1'
        assert table_info._bare_table_name(arn) == 'my-table'

    def test_arn_with_stream_suffix_yields_table_name(self):
        arn = 'arn:aws:dynamodb:us-east-1:1:table/my-table:stream:2024-01-01'
        assert table_info._bare_table_name(arn) == 'my-table'

    def test_non_table_arn_returned_unchanged(self):
        arn = 'arn:aws:dynamodb:us-east-1:1:backup/foo'
        assert table_info._bare_table_name(arn) == arn

    def test_malformed_arn_returned_unchanged(self):
        # startswith arn: but _parse_arn raises -> return as-is
        assert table_info._bare_table_name('arn:broken') == 'arn:broken'

    def test_none_returned_unchanged(self):
        assert table_info._bare_table_name(None) is None


# --- _autoscaling_max_capacity ----------------------------------------------


class TestAutoscalingMaxCapacity:
    def test_returns_max_when_target_present(self, boto3_mock):
        _autoscaling_max(boto3_mock, 8000)
        assert (
            table_info._autoscaling_max_capacity('t', 'us-east-1', 'read') == 8000
        )

    def test_returns_none_when_no_target(self, boto3_mock):
        _no_autoscaling(boto3_mock)
        assert (
            table_info._autoscaling_max_capacity('t', 'us-east-1', 'write') is None
        )

    def test_write_dimension_uses_write_scalable_dimension(self, boto3_mock):
        _autoscaling_max(boto3_mock, 3000)
        table_info._autoscaling_max_capacity('t', 'us-east-1', 'write')
        _, kwargs = boto3_mock.autoscaling_client.describe_scalable_targets.call_args
        assert kwargs['ScalableDimension'] == 'dynamodb:table:WriteCapacityUnits'

    def test_read_dimension_uses_read_scalable_dimension(self, boto3_mock):
        _autoscaling_max(boto3_mock, 3000)
        table_info._autoscaling_max_capacity('t', 'us-east-1', 'read')
        _, kwargs = boto3_mock.autoscaling_client.describe_scalable_targets.call_args
        assert kwargs['ScalableDimension'] == 'dynamodb:table:ReadCapacityUnits'

    def test_lookup_failure_raises(self, boto3_mock):
        # The helper raises on API failure; the ceiling resolver catches it and
        # skips the warning (distinguishing "no autoscaling" from "unknown").
        boto3_mock.autoscaling_client.describe_scalable_targets.side_effect = (
            RuntimeError('boom')
        )
        with pytest.raises(RuntimeError):
            table_info._autoscaling_max_capacity('t', 'us-east-1', 'read')

    def test_arn_table_name_stripped_for_resource_id(self, boto3_mock):
        _autoscaling_max(boto3_mock, 8000)
        arn = 'arn:aws:dynamodb:us-east-1:1:table/my-table'
        table_info._autoscaling_max_capacity(arn, 'us-east-1', 'read')
        _, kwargs = boto3_mock.autoscaling_client.describe_scalable_targets.call_args
        assert kwargs['ResourceIds'] == ['table/my-table']


# --- _effective_capacity_ceiling --------------------------------------------


class TestEffectiveCapacityCeiling:
    def test_provisioned_no_autoscaling(self, boto3_mock, provisioned_table):
        _no_autoscaling(boto3_mock)
        ceiling, source, floor = table_info._effective_capacity_ceiling(
            provisioned_table['Table'], False, 'us-east-1', 't', 'read'
        )
        assert (ceiling, floor) == (1000, None)
        assert 'provisioned' in source

    def test_provisioned_with_autoscaling(self, boto3_mock, provisioned_table):
        _autoscaling_max(boto3_mock, 9000)
        ceiling, source, floor = table_info._effective_capacity_ceiling(
            provisioned_table['Table'], False, 'us-east-1', 't', 'read'
        )
        assert (ceiling, floor) == (9000, 1000)
        assert 'autoscaling' in source

    def test_ondemand_table_max(self, boto3_mock, ondemand_table_with_limits):
        ceiling, source, floor = table_info._effective_capacity_ceiling(
            ondemand_table_with_limits['Table'], True, 'us-east-1', 't', 'write'
        )
        assert (ceiling, floor) == (15000, None)
        assert 'on-demand' in source

    def test_ondemand_falls_back_to_quota(self, boto3_mock, ondemand_table_no_limits):
        boto3_mock.service_quotas_client.get_service_quota.return_value = {
            'Quota': {'Value': 40000}
        }
        ceiling, source, floor = table_info._effective_capacity_ceiling(
            ondemand_table_no_limits['Table'], True, 'us-east-1', 't', 'read'
        )
        assert (ceiling, floor) == (40000, None)
        assert 'quota' in source

    def test_ondemand_no_max_no_quota_returns_none(self, boto3_mock, ondemand_table_no_limits):
        boto3_mock.service_quotas_client.get_service_quota.side_effect = (
            RuntimeError('unavailable')
        )
        boto3_mock.service_quotas_client.get_aws_default_service_quota.side_effect = (
            RuntimeError('unavailable')
        )
        ceiling, source, floor = table_info._effective_capacity_ceiling(
            ondemand_table_no_limits['Table'], True, 'us-east-1', 't', 'read'
        )
        assert ceiling is None

    def test_provisioned_missing_capacity_returns_none(self, boto3_mock):
        ceiling, source, floor = table_info._effective_capacity_ceiling(
            {}, False, 'us-east-1', 't', 'read'
        )
        assert ceiling is None

    def test_ondemand_max_of_zero_falls_back_to_quota(self, boto3_mock):
        # DynamoDB uses 0 / absent to mean "no table-level cap set", not a
        # literal zero ceiling — must fall through to the account quota.
        boto3_mock.service_quotas_client.get_service_quota.return_value = {
            'Quota': {'Value': 40000}
        }
        table_desc = {
            'BillingModeSummary': {'BillingMode': 'PAY_PER_REQUEST'},
            'OnDemandThroughput': {'MaxReadRequestUnits': 0},
        }
        ceiling, source, floor = table_info._effective_capacity_ceiling(
            table_desc, True, 'us-east-1', 't', 'read'
        )
        assert (ceiling, source) == (40000, "account quota")


# --- Check #2: provisioned, no autoscaling ----------------------------------


class TestProvisionedNoAutoscalingWarning:
    def test_read_request_above_provisioned_warns(
        self, boto3_mock, provisioned_table, caplog
    ):
        boto3_mock.dynamodb_client.describe_table.return_value = provisioned_table
        _no_autoscaling(boto3_mock)
        with caplog.at_level(logging.DEBUG):
            table_info.get_dynamodb_throughput_configs(
                args={'XMaxReadRate': '5000'}, table_name='t', modes=['read']
            )
        warnings = _capacity_warnings(caplog)
        assert any(
            'exceeds' in m and '1000' in m and 'read' in m for m in warnings
        ), warnings

    def test_write_request_above_provisioned_warns(
        self, boto3_mock, provisioned_table, caplog
    ):
        boto3_mock.dynamodb_client.describe_table.return_value = provisioned_table
        _no_autoscaling(boto3_mock)
        with caplog.at_level(logging.DEBUG):
            table_info.get_dynamodb_throughput_configs(
                args={'XMaxWriteRate': '5000'}, table_name='t', modes=['write']
            )
        assert any(
            'exceeds' in m and '500' in m and 'write' in m
            for m in _capacity_warnings(caplog)
        )

    def test_request_at_provisioned_no_warning(
        self, boto3_mock, provisioned_table, caplog
    ):
        boto3_mock.dynamodb_client.describe_table.return_value = provisioned_table
        _no_autoscaling(boto3_mock)
        with caplog.at_level(logging.DEBUG):
            table_info.get_dynamodb_throughput_configs(
                args={'XMaxReadRate': '1000'}, table_name='t', modes=['read']
            )
        assert _capacity_warnings(caplog) == []


# --- Check #3 + #89 nuance: provisioned with autoscaling --------------------


class TestProvisionedAutoscalingWarning:
    def test_request_above_autoscaling_max_hard_warns(
        self, boto3_mock, provisioned_table, caplog
    ):
        boto3_mock.dynamodb_client.describe_table.return_value = provisioned_table
        _autoscaling_max(boto3_mock, 4000)
        with caplog.at_level(logging.DEBUG):
            table_info.get_dynamodb_throughput_configs(
                args={'XMaxReadRate': '9000'}, table_name='t', modes=['read']
            )
        assert any(
            'exceeds' in m and 'autoscaling maximum' in m and '4000' in m
            for m in _capacity_warnings(caplog)
        )

    def test_request_between_floor_and_max_soft_note(
        self, boto3_mock, provisioned_table, caplog
    ):
        # provisioned read = 1000, autoscaling max = 8000; request 4000 is
        # above the floor but reachable via scaling -> softer note, not "exceeds".
        boto3_mock.dynamodb_client.describe_table.return_value = provisioned_table
        _autoscaling_max(boto3_mock, 8000)
        with caplog.at_level(logging.DEBUG):
            table_info.get_dynamodb_throughput_configs(
                args={'XMaxReadRate': '4000'}, table_name='t', modes=['read']
            )
        warnings = _capacity_warnings(caplog)
        assert any('scale up' in m.lower() for m in warnings), warnings
        assert not any('exceeds' in m for m in warnings), warnings

    def test_request_at_or_below_floor_no_warning(
        self, boto3_mock, provisioned_table, caplog
    ):
        boto3_mock.dynamodb_client.describe_table.return_value = provisioned_table
        _autoscaling_max(boto3_mock, 8000)
        with caplog.at_level(logging.DEBUG):
            table_info.get_dynamodb_throughput_configs(
                args={'XMaxReadRate': '900'}, table_name='t', modes=['read']
            )
        assert _capacity_warnings(caplog) == []


# --- Check #4: on-demand with table max -------------------------------------


class TestOnDemandTableMaxWarning:
    def test_read_request_above_table_max_warns(
        self, boto3_mock, ondemand_table_with_limits, caplog
    ):
        boto3_mock.dynamodb_client.describe_table.return_value = ondemand_table_with_limits
        with caplog.at_level(logging.DEBUG):
            table_info.get_dynamodb_throughput_configs(
                args={'XMaxReadRate': '30000'}, table_name='t', modes=['read']
            )
        assert any(
            'exceeds' in m and '25000' in m and 'on-demand' in m
            for m in _capacity_warnings(caplog)
        )

    def test_request_at_table_max_no_warning(
        self, boto3_mock, ondemand_table_with_limits, caplog
    ):
        boto3_mock.dynamodb_client.describe_table.return_value = ondemand_table_with_limits
        with caplog.at_level(logging.DEBUG):
            table_info.get_dynamodb_throughput_configs(
                args={'XMaxReadRate': '25000'}, table_name='t', modes=['read']
            )
        assert _capacity_warnings(caplog) == []


# --- Check #5: on-demand, no table max, account quota -----------------------


class TestOnDemandQuotaWarning:
    def test_request_above_account_quota_warns(
        self, boto3_mock, ondemand_table_no_limits, caplog
    ):
        boto3_mock.dynamodb_client.describe_table.return_value = ondemand_table_no_limits
        boto3_mock.service_quotas_client.get_service_quota.return_value = {
            'Quota': {'Value': 40000}
        }
        with caplog.at_level(logging.DEBUG):
            table_info.get_dynamodb_throughput_configs(
                args={'XMaxReadRate': '50000'}, table_name='t', modes=['read']
            )
        assert any(
            'exceeds' in m and '40000' in m and 'quota' in m
            for m in _capacity_warnings(caplog)
        )

    def test_request_below_quota_no_warning(
        self, boto3_mock, ondemand_table_no_limits, caplog
    ):
        boto3_mock.dynamodb_client.describe_table.return_value = ondemand_table_no_limits
        boto3_mock.service_quotas_client.get_service_quota.return_value = {
            'Quota': {'Value': 40000}
        }
        with caplog.at_level(logging.DEBUG):
            table_info.get_dynamodb_throughput_configs(
                args={'XMaxReadRate': '30000'}, table_name='t', modes=['read']
            )
        assert _capacity_warnings(caplog) == []


# --- Wiring guarantees ------------------------------------------------------


# --- _job_elapsed_minutes ---------------------------------------------------


class TestJobElapsedMinutes:
    """Elapsed wall-clock since the Glue job started, from the monotonic clock
    reference captured at module import. Drives 'time remaining' for check #1."""

    def test_zero_at_start(self, monkeypatch):
        monkeypatch.setattr(table_info, '_JOB_START_MONOTONIC', 1000.0)
        monkeypatch.setattr(table_info.time, 'monotonic', lambda: 1000.0)
        assert table_info._job_elapsed_minutes() == 0.0

    def test_grows_with_the_clock(self, monkeypatch):
        monkeypatch.setattr(table_info, '_JOB_START_MONOTONIC', 1000.0)
        monkeypatch.setattr(table_info.time, 'monotonic', lambda: 1000.0 + 120.0)
        assert table_info._job_elapsed_minutes() == pytest.approx(2.0)

    def test_clamped_non_negative_if_clock_regresses(self, monkeypatch):
        # A monotonic clock should not go backwards, but never report negative
        # elapsed (which would inflate the remaining budget past the timeout).
        monkeypatch.setattr(table_info, '_JOB_START_MONOTONIC', 1000.0)
        monkeypatch.setattr(table_info.time, 'monotonic', lambda: 999.0)
        assert table_info._job_elapsed_minutes() == 0.0


# --- _warn_if_job_may_timeout (check #1) ------------------------------------


class TestWarnIfJobMayTimeout:
    def test_read_slow_rate_warns(self, caplog):
        # 1,000,000 read units (8096 * 1e6 bytes) at 100 u/s = ~166 min > 60 min.
        table_desc = {'TableSizeBytes': 8096 * 1_000_000, 'ItemCount': 1_000_000}
        with caplog.at_level(logging.DEBUG):
            table_info._warn_if_job_may_timeout('t', 'read', 100, table_desc, 60)
        warns = _timeout_warnings(caplog)
        assert warns and 'read' in warns[0]

    def test_read_fast_rate_no_warn(self, caplog):
        table_desc = {'TableSizeBytes': 8096 * 1_000_000, 'ItemCount': 1_000_000}
        with caplog.at_level(logging.DEBUG):
            table_info._warn_if_job_may_timeout('t', 'read', 100_000, table_desc, 60)
        assert _timeout_warnings(caplog) == []

    def test_write_slow_rate_warns(self, caplog):
        # 1,000,000 items * 1 write unit each at 100 u/s = ~166 min > 60 min.
        table_desc = {'TableSizeBytes': 1024 * 1_000_000, 'ItemCount': 1_000_000}
        with caplog.at_level(logging.DEBUG):
            table_info._warn_if_job_may_timeout('t', 'write', 100, table_desc, 60)
        warns = _timeout_warnings(caplog)
        assert warns and 'write' in warns[0]

    def test_write_large_items_multiply_units(self, caplog):
        # 4KB items -> 4 write units each, so 100k items = 400k units; at 50 u/s
        # that is 8000s = ~133 min > 60 min.
        table_desc = {'TableSizeBytes': 4096 * 100_000, 'ItemCount': 100_000}
        with caplog.at_level(logging.DEBUG):
            table_info._warn_if_job_may_timeout('t', 'write', 50, table_desc, 60)
        assert _timeout_warnings(caplog)

    def test_custom_timeout_suppresses_warning(self, caplog):
        # Same slow read that warns at 60 min stays quiet with a 7-day timeout.
        table_desc = {'TableSizeBytes': 8096 * 1_000_000, 'ItemCount': 1_000_000}
        with caplog.at_level(logging.DEBUG):
            table_info._warn_if_job_may_timeout('t', 'read', 100, table_desc, 10080)
        assert _timeout_warnings(caplog) == []

    def test_zero_size_skips_read(self, caplog):
        table_desc = {'TableSizeBytes': 0, 'ItemCount': 0}
        with caplog.at_level(logging.DEBUG):
            table_info._warn_if_job_may_timeout('t', 'read', 1, table_desc, 60)
        assert _timeout_warnings(caplog) == []

    def test_zero_items_skips_write(self, caplog):
        table_desc = {'TableSizeBytes': 1024, 'ItemCount': 0}
        with caplog.at_level(logging.DEBUG):
            table_info._warn_if_job_may_timeout('t', 'write', 1, table_desc, 60)
        assert _timeout_warnings(caplog) == []

    def test_missing_metadata_skips(self, caplog):
        with caplog.at_level(logging.DEBUG):
            table_info._warn_if_job_may_timeout('t', 'read', 100, {}, 60)
        assert _timeout_warnings(caplog) == []

    def test_non_numeric_rate_skips(self, caplog):
        table_desc = {'TableSizeBytes': 8096 * 1_000_000, 'ItemCount': 1_000_000}
        with caplog.at_level(logging.DEBUG):
            table_info._warn_if_job_may_timeout('t', 'read', 'abc', table_desc, 60)
        assert _timeout_warnings(caplog) == []

    def test_none_rate_skips(self, caplog):
        table_desc = {'TableSizeBytes': 8096 * 1_000_000, 'ItemCount': 1_000_000}
        with caplog.at_level(logging.DEBUG):
            table_info._warn_if_job_may_timeout('t', 'read', None, table_desc, 60)
        assert _timeout_warnings(caplog) == []

    def test_zero_rate_skips(self, caplog):
        table_desc = {'TableSizeBytes': 8096 * 1_000_000, 'ItemCount': 1_000_000}
        with caplog.at_level(logging.DEBUG):
            table_info._warn_if_job_may_timeout('t', 'read', 0, table_desc, 60)
        assert _timeout_warnings(caplog) == []

    def test_uses_log_warning_level(self, caplog):
        table_desc = {'TableSizeBytes': 8096 * 1_000_000, 'ItemCount': 1_000_000}
        with caplog.at_level(logging.DEBUG):
            table_info._warn_if_job_may_timeout('t', 'read', 100, table_desc, 60)
        records = [r for r in caplog.records if 'will likely time out' in r.getMessage()]
        assert records and all(r.levelno == logging.WARNING for r in records)

    def test_last_arg_is_remaining_budget_not_raw_timeout(self, caplog):
        # A read that comfortably fits a full 60-min timeout (~17 min at
        # 1000 u/s for 1M units) must still warn when only 10 min remain,
        # because the budget is the time LEFT, not the original timeout.
        table_desc = {'TableSizeBytes': 8096 * 1_000_000, 'ItemCount': 1_000_000}
        with caplog.at_level(logging.DEBUG):
            table_info._warn_if_job_may_timeout('t', 'read', 1000, table_desc, 10)
        assert _timeout_warnings(caplog)

    def test_same_rate_no_warn_with_full_budget(self, caplog):
        # Same rate/table as above but the full 60 min available -> fits, quiet.
        table_desc = {'TableSizeBytes': 8096 * 1_000_000, 'ItemCount': 1_000_000}
        with caplog.at_level(logging.DEBUG):
            table_info._warn_if_job_may_timeout('t', 'read', 1000, table_desc, 60)
        assert _timeout_warnings(caplog) == []

    def test_zero_remaining_budget_warns_when_work_left(self, caplog):
        # No time left but data still to move -> always a timeout warning.
        table_desc = {'TableSizeBytes': 8096 * 1_000_000, 'ItemCount': 1_000_000}
        with caplog.at_level(logging.DEBUG):
            table_info._warn_if_job_may_timeout('t', 'read', 100_000, table_desc, 0)
        assert _timeout_warnings(caplog)

    def test_warning_names_remaining_time(self, caplog):
        # The message must frame the budget as time remaining, not the timeout,
        # so a reader mid-job understands why a "fast enough" rate is flagged.
        table_desc = {'TableSizeBytes': 8096 * 1_000_000, 'ItemCount': 1_000_000}
        with caplog.at_level(logging.DEBUG):
            table_info._warn_if_job_may_timeout('t', 'read', 1000, table_desc, 10)
        warns = _timeout_warnings(caplog)
        assert warns and 'remaining' in warns[0].lower()


# --- check #1 wiring inside get_dynamodb_throughput_configs -----------------


class TestTimeoutWiring:
    def _big_provisioned(self):
        return {
            'Table': {
                'BillingModeSummary': {'BillingMode': 'PROVISIONED'},
                'ProvisionedThroughput': {
                    'ReadCapacityUnits': 100,
                    'WriteCapacityUnits': 100,
                },
                'TableSizeBytes': 8096 * 1_000_000,
                'ItemCount': 1_000_000,
            }
        }

    def test_table_derived_slow_rate_warns(self, boto3_mock, caplog):
        # No XMax* given: read_rate derives from the 100 RCU provisioned level,
        # which is far too slow for a 1M-read-unit table -> timeout warning.
        boto3_mock.dynamodb_client.describe_table.return_value = self._big_provisioned()
        _no_autoscaling(boto3_mock)
        with caplog.at_level(logging.DEBUG):
            table_info.get_dynamodb_throughput_configs(
                args={}, table_name='t', modes=['read']
            )
        assert _timeout_warnings(caplog)

    def test_custom_xtimeout_suppresses(self, boto3_mock, caplog):
        boto3_mock.dynamodb_client.describe_table.return_value = self._big_provisioned()
        _no_autoscaling(boto3_mock)
        with caplog.at_level(logging.DEBUG):
            table_info.get_dynamodb_throughput_configs(
                args={'XTimeout': 10080}, table_name='t', modes=['read']
            )
        assert _timeout_warnings(caplog) == []

    def test_non_numeric_xtimeout_falls_back_to_default(self, boto3_mock, caplog):
        boto3_mock.dynamodb_client.describe_table.return_value = self._big_provisioned()
        _no_autoscaling(boto3_mock)
        with caplog.at_level(logging.DEBUG):
            table_info.get_dynamodb_throughput_configs(
                args={'XTimeout': 'garbage'}, table_name='t', modes=['read']
            )
        # Falls back to the 60-min default, under which the slow rate warns.
        assert _timeout_warnings(caplog)

    def test_return_value_unchanged_by_timeout_warning(self, boto3_mock, caplog):
        boto3_mock.dynamodb_client.describe_table.return_value = self._big_provisioned()
        _no_autoscaling(boto3_mock)
        with caplog.at_level(logging.DEBUG):
            opts = table_info.get_dynamodb_throughput_configs(
                args={}, table_name='t', modes=['read']
            )
        assert opts == {'dynamodb.throughput.read': '100'}

    def _fast_enough_for_full_timeout(self):
        # 1M read units at 2000 RCU = ~8.3 min; fits a 60-min job comfortably,
        # so with a full budget this must NOT warn.
        return {
            'Table': {
                'BillingModeSummary': {'BillingMode': 'PROVISIONED'},
                'ProvisionedThroughput': {
                    'ReadCapacityUnits': 2000,
                    'WriteCapacityUnits': 2000,
                },
                'TableSizeBytes': 8096 * 1_000_000,
                'ItemCount': 1_000_000,
            }
        }

    def test_elapsed_time_shrinks_the_budget(self, boto3_mock, monkeypatch, caplog):
        # THE #89 check-1 regression: a rate that fits the full 60-min timeout
        # must warn once most of the timeout is already gone. This is the
        # multi-phase delete case — the write rate is resolved only after the
        # scan has burned ~55 min, leaving ~5 min for an ~8-min job.
        boto3_mock.dynamodb_client.describe_table.return_value = self._fast_enough_for_full_timeout()
        _no_autoscaling(boto3_mock)
        monkeypatch.setattr(table_info, '_job_elapsed_minutes', lambda: 55.0)
        with caplog.at_level(logging.DEBUG):
            table_info.get_dynamodb_throughput_configs(
                args={'XTimeout': 60}, table_name='t', modes=['read']
            )
        assert _timeout_warnings(caplog)

    def test_no_warning_when_little_elapsed(self, boto3_mock, monkeypatch, caplog):
        # Same rate/table/timeout, but early in the job: the full budget is
        # available, the ~8-min job fits 60 min, so no warning.
        boto3_mock.dynamodb_client.describe_table.return_value = self._fast_enough_for_full_timeout()
        _no_autoscaling(boto3_mock)
        monkeypatch.setattr(table_info, '_job_elapsed_minutes', lambda: 0.0)
        with caplog.at_level(logging.DEBUG):
            table_info.get_dynamodb_throughput_configs(
                args={'XTimeout': 60}, table_name='t', modes=['read']
            )
        assert _timeout_warnings(caplog) == []


class TestWiringGuarantees:
    def test_no_warning_when_rate_table_derived(
        self, boto3_mock, provisioned_table, caplog
    ):
        # No XMax* supplied: read_rate is derived from provisioned capacity and
        # by definition cannot exceed it -> no capacity warning.
        boto3_mock.dynamodb_client.describe_table.return_value = provisioned_table
        _no_autoscaling(boto3_mock)
        with caplog.at_level(logging.DEBUG):
            table_info.get_dynamodb_throughput_configs(
                args={}, table_name='t', modes=['read']
            )
        assert _capacity_warnings(caplog) == []

    def test_both_dimensions_warn_independently(
        self, boto3_mock, provisioned_table, caplog
    ):
        boto3_mock.dynamodb_client.describe_table.return_value = provisioned_table
        _no_autoscaling(boto3_mock)
        with caplog.at_level(logging.DEBUG):
            table_info.get_dynamodb_throughput_configs(
                args={'XMaxReadRate': '9000', 'XMaxWriteRate': '9000'},
                table_name='t', modes=['read', 'write'],
            )
        warnings = _capacity_warnings(caplog)
        assert any('read' in m and '1000' in m for m in warnings)
        assert any('write' in m and '500' in m for m in warnings)

    def test_autoscaling_lookup_failure_skips_warning(
        self, boto3_mock, provisioned_table, caplog
    ):
        # If autoscaling lookup raises we can't tell whether the table could
        # scale to meet the request, so we skip the capacity warning rather
        # than emit a false "exceeds provisioned" — and never crash the job.
        boto3_mock.dynamodb_client.describe_table.return_value = provisioned_table
        boto3_mock.autoscaling_client.describe_scalable_targets.side_effect = (
            RuntimeError('boom')
        )
        with caplog.at_level(logging.DEBUG):
            opts = table_info.get_dynamodb_throughput_configs(
                args={'XMaxReadRate': '5000'}, table_name='t', modes=['read']
            )
        assert opts == {'dynamodb.throughput.read': '5000'}
        assert _capacity_warnings(caplog) == []

    def test_autoscaling_lookup_failure_warns_visibility_lost(
        self, boto3_mock, provisioned_table, caplog
    ):
        # Jason's option (b): when the role can't read autoscaling targets we
        # proceed, but must surface that we're doing so without visibility into
        # the table's metrics (and point at the missing permission).
        boto3_mock.dynamodb_client.describe_table.return_value = provisioned_table
        boto3_mock.autoscaling_client.describe_scalable_targets.side_effect = (
            RuntimeError('AccessDeniedException')
        )
        with caplog.at_level(logging.DEBUG):
            table_info.get_dynamodb_throughput_configs(
                args={'XMaxReadRate': '5000'}, table_name='t', modes=['read']
            )
        visibility = [
            r for r in caplog.records
            if r.levelno == logging.WARNING
            and 'without knowledge of the table' in r.message
            and 'DescribeScalableTargets' in r.message
        ]
        assert visibility, [r.message for r in caplog.records]

    def test_return_value_unchanged_by_warning_logic(
        self, boto3_mock, provisioned_table, caplog
    ):
        # The connector options must be exactly the user rate regardless of the
        # capacity warning firing (warnings are observational only).
        boto3_mock.dynamodb_client.describe_table.return_value = provisioned_table
        _no_autoscaling(boto3_mock)
        with caplog.at_level(logging.DEBUG):
            opts = table_info.get_dynamodb_throughput_configs(
                args={'XMaxReadRate': '9000', 'XMaxWriteRate': '9000'},
                table_name='t', modes=['read', 'write'],
            )
        assert opts == {
            'dynamodb.throughput.read': '9000',
            'dynamodb.throughput.write': '9000',
        }

    def test_uses_log_warning_not_deprecated_warn(
        self, boto3_mock, provisioned_table, monkeypatch, caplog
    ):
        # log.warn is deprecated; ensure the capacity warning path calls
        # log.warning. We assert the record level is WARNING.
        boto3_mock.dynamodb_client.describe_table.return_value = provisioned_table
        _no_autoscaling(boto3_mock)
        with caplog.at_level(logging.DEBUG):
            table_info.get_dynamodb_throughput_configs(
                args={'XMaxReadRate': '5000'}, table_name='t', modes=['read']
            )
        capacity_records = [
            r for r in caplog.records if 'exceeds the table' in r.getMessage()
        ]
        assert capacity_records
        assert all(r.levelno == logging.WARNING for r in capacity_records)
