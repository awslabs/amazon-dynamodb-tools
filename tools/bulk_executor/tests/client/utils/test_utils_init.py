"""Unit tests for client/src/utils/__init__.py.

Covers the helper functions and validators in the `utils` package's
own __init__ module:
- warn(): yellow ANSI prefix to stderr
- convert_client_dict_to_script_args(): keep/skip rules for X* keys, the
  _ENV_OR_SCRIPT_KEYS exception list, str() coercion of values
- get_args_from_processed_args(): keep only X-prefixed keys
- filter_none_or_false_values(): drop None/False/0/empty-string entries
- validate_timeout(): in-range int → int, out-of-range int → argparse error,
  non-int → argparse error
- parse_action(): pulls XAction from argv, leaves remaining args alone
- glue_job_arguments(): parser construction + argument set
- environment_arguments(): parser construction + argument set
- parse_environment_arguments(): two-tuple from environment_arguments
- _get_table_info(): describe_table happy path, ResourceNotFoundException
  → None, other ClientError → re-raise
- validate_tables(): missing-table exit, missing-index exit, PITR enabled,
  PITR disabled exit, PITR cross-account validation warning skip,
  PITR ClientError other → exit, schema mismatch exit, schema match
  with GSI/LSI add/remove/diff warnings, multi-table OK
- _default_region(): boto3 session region, env var fallback
- _parse_arn(): valid ARN parse, malformed → ValueError
- _region_from_table_ref(): None/empty input, non-arn, non-dynamodb service,
  non-table resource, valid table ARN region extraction

Style notes:
- The package is named `utils`, so the test does `import utils as utils_module`
  to avoid collision with anything else named `utils`. Functions are accessed
  via the module reference where mocking is needed.
- For `validate_tables`, we patch utils.Clients to keep the test from making
  any real boto3 calls. Each mock client is a MagicMock with describe_table
  / describe_continuous_backups stubbed via side_effect or return_value.
- argparse SystemExit (code 2) and explicit `sys.exit("msg")` (code 1) both
  raise SystemExit; tests assert the SystemExit and inspect captured output
  via capsys when the error message matters.
"""

import argparse
import os
from unittest.mock import MagicMock, patch

import pytest
from botocore.exceptions import ClientError

import utils as utils_module
from utils import (
    _default_region,
    _get_table_info,
    _parse_arn,
    _region_from_table_ref,
    convert_client_dict_to_script_args,
    environment_arguments,
    filter_none_or_false_values,
    get_args_from_processed_args,
    glue_job_arguments,
    parse_action,
    parse_environment_arguments,
    validate_tables,
    validate_timeout,
    warn,
)


# --- warn -------------------------------------------------------------------

class TestWarn:
    """Tests for warn (lines 57-58)."""

    def test_writes_to_stderr_with_warn_prefix(self, capsys):
        """Yellow [WARN] prefix + message + reset code, on stderr."""
        warn("oh no")
        captured = capsys.readouterr()
        assert captured.out == ""
        assert "[WARN] oh no" in captured.err
        # ANSI color codes wrap the message
        assert "\x1b[33;20m" in captured.err
        assert "\x1b[0m" in captured.err


# --- convert_client_dict_to_script_args -------------------------------------

class TestConvertClientDictToScriptArgs:
    """Tests for convert_client_dict_to_script_args (lines 60-65)."""

    def test_keeps_non_x_prefixed_keys(self):
        """Keys not starting with 'X' are emitted as --key value pairs."""
        result = convert_client_dict_to_script_args({'foo': 'bar', 'baz': 'qux'})
        # Order is dict-insertion-order
        assert result == ['--foo', 'bar', '--baz', 'qux']

    def test_skips_x_prefixed_keys(self):
        """X-prefixed keys are dropped (control args, not script args)."""
        result = convert_client_dict_to_script_args({'XAction': 'fill', 'foo': 'bar'})
        assert result == ['--foo', 'bar']

    def test_keeps_env_or_script_x_keys(self):
        """X-prefixed keys in _ENV_OR_SCRIPT_KEYS are kept."""
        result = convert_client_dict_to_script_args({
            'XMaxWriteRate': 1000,
            'XAction': 'fill',
        })
        assert result == ['--XMaxWriteRate', '1000']

    def test_keeps_xmaxreadrate_too(self):
        """Both XMaxWriteRate and XMaxReadRate are in the allowlist."""
        result = convert_client_dict_to_script_args({'XMaxReadRate': 500})
        assert result == ['--XMaxReadRate', '500']

    def test_str_coerces_values(self):
        """Non-string values are coerced via str()."""
        result = convert_client_dict_to_script_args({'count': 42, 'flag': True})
        assert result == ['--count', '42', '--flag', 'True']

    def test_empty_dict_returns_empty_list(self):
        assert convert_client_dict_to_script_args({}) == []


# --- get_args_from_processed_args -------------------------------------------

class TestGetArgsFromProcessedArgs:
    """Tests for get_args_from_processed_args (line 67-68)."""

    def test_filters_to_x_prefixed_keys(self):
        result = get_args_from_processed_args({
            'XAction': 'fill',
            'XRegion': 'us-east-1',
            'foo': 'bar',
            'count': 5,
        })
        assert result == {'XAction': 'fill', 'XRegion': 'us-east-1'}

    def test_no_x_keys_returns_empty(self):
        assert get_args_from_processed_args({'foo': 'bar'}) == {}

    def test_empty_input(self):
        assert get_args_from_processed_args({}) == {}


# --- filter_none_or_false_values --------------------------------------------

class TestFilterNoneOrFalseValues:
    """Tests for filter_none_or_false_values (line 70-71)."""

    def test_drops_none_values(self):
        assert filter_none_or_false_values({'a': 1, 'b': None}) == {'a': 1}

    def test_drops_false_values(self):
        # The implementation uses `if v` so anything falsy is dropped
        result = filter_none_or_false_values({'a': True, 'b': False, 'c': 0, 'd': ''})
        assert result == {'a': True}

    def test_keeps_truthy_values(self):
        result = filter_none_or_false_values({'a': 1, 'b': 'x', 'c': True, 'd': [0]})
        assert result == {'a': 1, 'b': 'x', 'c': True, 'd': [0]}

    def test_empty_input(self):
        assert filter_none_or_false_values({}) == {}


# --- validate_timeout -------------------------------------------------------

class TestValidateTimeout:
    """Tests for validate_timeout (lines 73-81)."""

    def test_in_range_returns_int(self):
        assert validate_timeout("60") == 60

    def test_lower_bound_inclusive(self):
        assert validate_timeout("1") == 1

    def test_upper_bound_inclusive(self):
        assert validate_timeout("10080") == 10080

    def test_zero_is_out_of_range(self):
        with pytest.raises(argparse.ArgumentTypeError, match="between 1 and 10080"):
            validate_timeout("0")

    def test_above_max_is_out_of_range(self):
        with pytest.raises(argparse.ArgumentTypeError, match="between 1 and 10080"):
            validate_timeout("10081")

    def test_negative_is_out_of_range(self):
        with pytest.raises(argparse.ArgumentTypeError, match="between 1 and 10080"):
            validate_timeout("-5")

    def test_non_integer_string_raises(self):
        with pytest.raises(argparse.ArgumentTypeError, match="must be an integer"):
            validate_timeout("abc")

    def test_float_string_raises(self):
        with pytest.raises(argparse.ArgumentTypeError, match="must be an integer"):
            validate_timeout("3.14")


# --- parse_action -----------------------------------------------------------

class TestParseAction:
    """Tests for parse_action (lines 85-94)."""

    def test_pulls_xaction_from_argv(self, monkeypatch):
        """First positional arg becomes XAction; rest are unknown."""
        monkeypatch.setattr('sys.argv', ['bulk', 'fill', '--foo', 'bar'])
        ns, unknown = parse_action()
        assert ns.XAction == 'fill'
        assert '--foo' in unknown
        assert 'bar' in unknown

    def test_no_argv_xaction_is_none(self, monkeypatch):
        """Optional positional → XAction=None when missing."""
        monkeypatch.setattr('sys.argv', ['bulk'])
        ns, unknown = parse_action()
        assert ns.XAction is None
        assert unknown == []


# --- glue_job_arguments / environment_arguments -----------------------------

class TestGlueJobArguments:
    """Tests for glue_job_arguments (lines 96-110)."""

    def test_returns_argument_parser(self):
        parser = glue_job_arguments()
        assert isinstance(parser, argparse.ArgumentParser)

    def test_parses_execution_class(self):
        parser = glue_job_arguments()
        ns, _ = parser.parse_known_args(['--XExecutionClass', 'FLEX'])
        assert ns.XExecutionClass == 'FLEX'

    def test_invalid_execution_class_rejected(self, capsys):
        parser = glue_job_arguments()
        with pytest.raises(SystemExit):
            parser.parse_known_args(['--XExecutionClass', 'GARBAGE'])

    def test_parses_timeout_via_validator(self):
        parser = glue_job_arguments()
        ns, _ = parser.parse_known_args(['--XTimeout', '30'])
        assert ns.XTimeout == 30

    def test_timeout_validation_rejects_out_of_range(self, capsys):
        parser = glue_job_arguments()
        with pytest.raises(SystemExit):
            parser.parse_known_args(['--XTimeout', '99999'])

    def test_parses_idle_timeout_via_validator(self):
        parser = glue_job_arguments()
        ns, _ = parser.parse_known_args(['--XIdleTimeout', '10'])
        assert ns.XIdleTimeout == 10

    def test_idle_timeout_validation_rejects_out_of_range(self, capsys):
        parser = glue_job_arguments()
        with pytest.raises(SystemExit):
            parser.parse_known_args(['--XIdleTimeout', '99999'])

    def test_idle_timeout_validation_rejects_zero(self, capsys):
        parser = glue_job_arguments()
        with pytest.raises(SystemExit):
            parser.parse_known_args(['--XIdleTimeout', '0'])

    def test_parses_number_of_workers(self):
        parser = glue_job_arguments()
        ns, _ = parser.parse_known_args(['--XNumberOfWorkers', '50'])
        assert ns.XNumberOfWorkers == 50

    def test_parses_worker_type(self):
        parser = glue_job_arguments()
        ns, _ = parser.parse_known_args(['--XWorkerType', 'G.2X'])
        assert ns.XWorkerType == 'G.2X'

    def test_invalid_worker_type_rejected(self):
        parser = glue_job_arguments()
        with pytest.raises(SystemExit):
            parser.parse_known_args(['--XWorkerType', 'BAD.WORKER'])

    def test_parses_boolean_flags(self):
        parser = glue_job_arguments()
        ns, _ = parser.parse_known_args(['--XWaitForDPU', '--XContinuousLogging'])
        assert ns.XWaitForDPU is True
        assert ns.XContinuousLogging is True

    def test_parses_dynamodb_rate_overrides(self):
        parser = glue_job_arguments()
        ns, _ = parser.parse_known_args([
            '--XMaxWriteRate', '1000',
            '--XMaxReadRate', '500',
        ])
        assert ns.XMaxWriteRate == 1000
        assert ns.XMaxReadRate == 500

    def test_default_attribute_suppressed_when_unset(self):
        """SUPPRESS default means unset args don't appear on the namespace."""
        parser = glue_job_arguments()
        ns, _ = parser.parse_known_args([])
        assert not hasattr(ns, 'XExecutionClass')
        assert not hasattr(ns, 'XTimeout')


class TestEnvironmentArguments:
    """Tests for environment_arguments (lines 112-122)."""

    def test_parses_account_and_region(self):
        parser = environment_arguments()
        ns, _ = parser.parse_known_args([
            '--XAccount', '123456789012',
            '--XRegion', 'us-east-2',
        ])
        assert ns.XAccount == '123456789012'
        assert ns.XRegion == 'us-east-2'

    def test_parses_debug_and_dev_flags(self):
        parser = environment_arguments()
        ns, _ = parser.parse_known_args(['--XDebug', '--XDev'])
        assert ns.XDebug is True
        assert ns.XDev is True

    def test_unset_flags_suppressed(self):
        parser = environment_arguments()
        ns, _ = parser.parse_known_args([])
        # SUPPRESS default → attributes not set
        assert not hasattr(ns, 'XAccount')
        assert not hasattr(ns, 'XDebug')


class TestParseEnvironmentArguments:
    """Tests for parse_environment_arguments (lines 125-126)."""

    def test_uses_argv(self, monkeypatch):
        monkeypatch.setattr('sys.argv', ['bulk', '--XRegion', 'eu-west-1', 'extra'])
        ns, unknown = parse_environment_arguments()
        assert ns.XRegion == 'eu-west-1'
        assert 'extra' in unknown


# --- _get_table_info --------------------------------------------------------

class TestGetTableInfo:
    """Tests for _get_table_info (lines 128-135)."""

    def test_returns_table_dict_on_success(self):
        ddb = MagicMock()
        table_payload = {'TableName': 'foo', 'KeySchema': []}
        ddb.describe_table.return_value = {'Table': table_payload}
        result = _get_table_info(ddb, 'foo')
        assert result is table_payload
        ddb.describe_table.assert_called_once_with(TableName='foo')

    def test_returns_none_on_resource_not_found(self):
        ddb = MagicMock()
        ddb.describe_table.side_effect = ClientError(
            {'Error': {'Code': 'ResourceNotFoundException', 'Message': 'no'}},
            'DescribeTable',
        )
        assert _get_table_info(ddb, 'missing') is None

    def test_reraises_other_client_errors(self):
        ddb = MagicMock()
        err = ClientError(
            {'Error': {'Code': 'AccessDeniedException', 'Message': 'nope'}},
            'DescribeTable',
        )
        ddb.describe_table.side_effect = err
        with pytest.raises(ClientError):
            _get_table_info(ddb, 'denied')


# --- validate_tables --------------------------------------------------------

@pytest.fixture
def patched_clients(monkeypatch):
    """Patch utils.Clients with a factory that returns a fresh MagicMock per
    region. Tests can pre-configure each region's clients via the returned
    `by_region` dict before calling validate_tables.
    """
    by_region = {}

    def factory(region):
        if region not in by_region:
            client = MagicMock()
            client.dynamodb_client = MagicMock()
            by_region[region] = client
        return by_region[region]

    monkeypatch.setattr(utils_module, 'Clients', factory)
    # Make _default_region deterministic across tests
    monkeypatch.setattr(utils_module, '_default_region', lambda: 'us-east-1')
    return by_region


def _table_info(name, key_schema=None, attrs=None, gsis=None, lsis=None):
    ks = key_schema or [{'AttributeName': 'pk', 'KeyType': 'HASH'}]
    ad = attrs or [{'AttributeName': 'pk', 'AttributeType': 'S'}]
    payload = {
        'TableName': name,
        'KeySchema': ks,
        'AttributeDefinitions': ad,
    }
    if gsis is not None:
        payload['GlobalSecondaryIndexes'] = gsis
    if lsis is not None:
        payload['LocalSecondaryIndexes'] = lsis
    return payload


class TestValidateTables:
    """Tests for validate_tables (lines 137-248)."""

    def test_missing_table_exits(self, patched_clients):
        client = MagicMock()
        client.dynamodb_client = MagicMock()
        client.dynamodb_client.describe_table.side_effect = ClientError(
            {'Error': {'Code': 'ResourceNotFoundException', 'Message': 'no'}},
            'DescribeTable',
        )
        patched_clients['us-east-1'] = client

        parser = MagicMock()
        with pytest.raises(SystemExit, match="does not exist"):
            validate_tables({}, parser, 'missing-table')

    def test_missing_index_exits(self, patched_clients):
        client = MagicMock()
        client.dynamodb_client = MagicMock()
        client.dynamodb_client.describe_table.return_value = {
            'Table': _table_info('t1', gsis=[{'IndexName': 'gsi-a'}]),
        }
        patched_clients['us-east-1'] = client

        parser = MagicMock()
        with pytest.raises(SystemExit, match="Index 'missing-gsi'"):
            validate_tables({}, parser, 't1', index='missing-gsi')

    def test_existing_index_passes(self, patched_clients):
        client = MagicMock()
        client.dynamodb_client = MagicMock()
        client.dynamodb_client.describe_table.return_value = {
            'Table': _table_info('t1', gsis=[{'IndexName': 'gsi-a'}]),
        }
        patched_clients['us-east-1'] = client

        # Should not raise
        validate_tables({}, MagicMock(), 't1', index='gsi-a')

    def test_pitr_enabled_passes(self, patched_clients):
        client = MagicMock()
        client.dynamodb_client.describe_table.return_value = {
            'Table': _table_info('t1'),
        }
        client.dynamodb_client.describe_continuous_backups.return_value = {
            'ContinuousBackupsDescription': {
                'PointInTimeRecoveryDescription': {
                    'PointInTimeRecoveryStatus': 'ENABLED',
                },
            },
        }
        patched_clients['us-east-1'] = client

        validate_tables({}, MagicMock(), 't1', pitr_enabled=True)

    def test_pitr_disabled_exits(self, patched_clients):
        client = MagicMock()
        client.dynamodb_client.describe_table.return_value = {
            'Table': _table_info('t1'),
        }
        client.dynamodb_client.describe_continuous_backups.return_value = {
            'ContinuousBackupsDescription': {
                'PointInTimeRecoveryDescription': {
                    'PointInTimeRecoveryStatus': 'DISABLED',
                },
            },
        }
        patched_clients['us-east-1'] = client

        with pytest.raises(SystemExit, match="point in time recovery"):
            validate_tables({}, MagicMock(), 't1', pitr_enabled=True)

    def test_pitr_cross_account_warning_skip(self, patched_clients, capsys):
        """ValidationException w/ cross-account message → warn + skip, no exit."""
        client = MagicMock()
        client.dynamodb_client.describe_table.return_value = {
            'Table': _table_info('t1'),
        }
        client.dynamodb_client.describe_continuous_backups.side_effect = ClientError(
            {
                'Error': {
                    'Code': 'ValidationException',
                    'Message': 'PITR is only supported by accounts that match the table',
                },
            },
            'DescribeContinuousBackups',
        )
        patched_clients['us-east-1'] = client

        validate_tables({}, MagicMock(), 't1', pitr_enabled=True)
        out = capsys.readouterr().out
        assert "Skipping PITR check" in out

    def test_pitr_other_client_error_exits(self, patched_clients):
        client = MagicMock()
        client.dynamodb_client.describe_table.return_value = {
            'Table': _table_info('t1'),
        }
        client.dynamodb_client.describe_continuous_backups.side_effect = ClientError(
            {'Error': {'Code': 'AccessDeniedException', 'Message': 'nope'}},
            'DescribeContinuousBackups',
        )
        patched_clients['us-east-1'] = client

        with pytest.raises(SystemExit, match="Could not check PITR"):
            validate_tables({}, MagicMock(), 't1', pitr_enabled=True)

    def test_schemas_match_happy_path(self, patched_clients):
        """Two tables with identical key schema and identical GSIs/LSIs: no warnings, no exits."""
        client = MagicMock()
        # Same describe_table for both; differentiate via TableName argument
        info_a = _table_info(
            't1',
            key_schema=[{'AttributeName': 'pk', 'KeyType': 'HASH'}],
            attrs=[{'AttributeName': 'pk', 'AttributeType': 'S'}],
            gsis=[{'IndexName': 'gsi1', 'KeySchema': []}],
            lsis=[{'IndexName': 'lsi1', 'KeySchema': []}],
        )
        info_b = _table_info(
            't2',
            key_schema=[{'AttributeName': 'pk', 'KeyType': 'HASH'}],
            attrs=[{'AttributeName': 'pk', 'AttributeType': 'S'}],
            gsis=[{'IndexName': 'gsi1', 'KeySchema': []}],
            lsis=[{'IndexName': 'lsi1', 'KeySchema': []}],
        )

        def describe(*, TableName):
            return {'Table': info_a if TableName == 't1' else info_b}

        client.dynamodb_client.describe_table.side_effect = describe
        patched_clients['us-east-1'] = client

        validate_tables({}, MagicMock(), 't1', 't2', schemas_match=True)

    def test_schemas_match_key_schema_mismatch_exits(self, patched_clients):
        """Different key schema between tables → exit with mismatch message."""
        client = MagicMock()
        info_a = _table_info(
            't1',
            key_schema=[{'AttributeName': 'pk', 'KeyType': 'HASH'}],
            attrs=[{'AttributeName': 'pk', 'AttributeType': 'S'}],
        )
        info_b = _table_info(
            't2',
            key_schema=[{'AttributeName': 'id', 'KeyType': 'HASH'}],
            attrs=[{'AttributeName': 'id', 'AttributeType': 'S'}],
        )

        def describe(*, TableName):
            return {'Table': info_a if TableName == 't1' else info_b}

        client.dynamodb_client.describe_table.side_effect = describe
        patched_clients['us-east-1'] = client

        with pytest.raises(SystemExit, match="Primary key schema mismatch"):
            validate_tables({}, MagicMock(), 't1', 't2', schemas_match=True)

    def test_schemas_match_gsi_lsi_warnings(self, patched_clients, capsys):
        """Different GSI/LSI sets between tables emit warnings, no exit."""
        client = MagicMock()
        info_a = _table_info(
            't1',
            key_schema=[{'AttributeName': 'pk', 'KeyType': 'HASH'}],
            attrs=[{'AttributeName': 'pk', 'AttributeType': 'S'}],
            gsis=[
                {'IndexName': 'shared', 'KeySchema': [], 'Projection': {'a': 1}},
                {'IndexName': 'only-in-a', 'KeySchema': []},
            ],
            lsis=[{'IndexName': 'lsi-shared', 'KeySchema': [], 'Projection': {'a': 1}}],
        )
        info_b = _table_info(
            't2',
            key_schema=[{'AttributeName': 'pk', 'KeyType': 'HASH'}],
            attrs=[{'AttributeName': 'pk', 'AttributeType': 'S'}],
            gsis=[
                {'IndexName': 'shared', 'KeySchema': [], 'Projection': {'b': 2}},
                {'IndexName': 'only-in-b', 'KeySchema': []},
            ],
            lsis=[
                {'IndexName': 'lsi-shared', 'KeySchema': [], 'Projection': {'b': 2}},
                {'IndexName': 'lsi-only-b', 'KeySchema': []},
            ],
        )

        def describe(*, TableName):
            return {'Table': info_a if TableName == 't1' else info_b}

        client.dynamodb_client.describe_table.side_effect = describe
        patched_clients['us-east-1'] = client

        validate_tables({}, MagicMock(), 't1', 't2', schemas_match=True)
        captured = capsys.readouterr()
        # Warnings go to stderr (warn() helper)
        assert "GSI 'only-in-a' is in 't1' but missing from 't2'" in captured.err
        assert "GSI 'only-in-b' is in 't2' but missing from 't1'" in captured.err
        assert "GSI 'shared' differs" in captured.err
        assert "LSI 'lsi-only-b'" in captured.err
        assert "LSI 'lsi-shared' differs" in captured.err

    def test_arn_table_uses_arn_region(self, patched_clients):
        """Table ref ARN routes Clients() construction to that region."""
        client = MagicMock()
        client.dynamodb_client.describe_table.return_value = {
            'Table': _table_info('t1'),
        }
        patched_clients['eu-west-2'] = client

        arn = 'arn:aws:dynamodb:eu-west-2:111122223333:table/MyTable'
        validate_tables({}, MagicMock(), arn)
        # Ensure region-specific client got the call
        client.dynamodb_client.describe_table.assert_called_once_with(TableName=arn)


# --- _default_region --------------------------------------------------------

class TestDefaultRegion:
    """Tests for _default_region (lines 274-279)."""

    def test_uses_session_region(self, monkeypatch):
        """Boto session reports a region → use it."""
        session = MagicMock()
        session.region_name = 'us-east-1'
        boto3_mock = MagicMock()
        boto3_mock.Session.return_value = session
        monkeypatch.setattr(utils_module, 'boto3', boto3_mock)
        # Clear envs so we can't accidentally hit the fallback
        monkeypatch.delenv('AWS_REGION', raising=False)
        monkeypatch.delenv('AWS_DEFAULT_REGION', raising=False)
        assert _default_region() == 'us-east-1'

    def test_falls_back_to_aws_region_env(self, monkeypatch):
        """Session region missing → AWS_REGION env."""
        session = MagicMock()
        session.region_name = None
        boto3_mock = MagicMock()
        boto3_mock.Session.return_value = session
        monkeypatch.setattr(utils_module, 'boto3', boto3_mock)
        monkeypatch.setenv('AWS_REGION', 'us-west-2')
        monkeypatch.delenv('AWS_DEFAULT_REGION', raising=False)
        assert _default_region() == 'us-west-2'

    def test_falls_back_to_aws_default_region_env(self, monkeypatch):
        """Session region + AWS_REGION missing → AWS_DEFAULT_REGION env."""
        session = MagicMock()
        session.region_name = None
        boto3_mock = MagicMock()
        boto3_mock.Session.return_value = session
        monkeypatch.setattr(utils_module, 'boto3', boto3_mock)
        monkeypatch.delenv('AWS_REGION', raising=False)
        monkeypatch.setenv('AWS_DEFAULT_REGION', 'ap-south-1')
        assert _default_region() == 'ap-south-1'

    def test_returns_none_when_nothing_configured(self, monkeypatch):
        """All sources empty → returns None."""
        session = MagicMock()
        session.region_name = None
        boto3_mock = MagicMock()
        boto3_mock.Session.return_value = session
        monkeypatch.setattr(utils_module, 'boto3', boto3_mock)
        monkeypatch.delenv('AWS_REGION', raising=False)
        monkeypatch.delenv('AWS_DEFAULT_REGION', raising=False)
        assert _default_region() is None


# --- _parse_arn -------------------------------------------------------------

class TestParseArn:
    """Tests for _parse_arn (lines 281-300)."""

    def test_valid_arn(self):
        parsed = _parse_arn('arn:aws:dynamodb:us-east-1:111122223333:table/MyTable')
        assert parsed == {
            'partition': 'aws',
            'service': 'dynamodb',
            'region': 'us-east-1',
            'account': '111122223333',
            'resource': 'table/MyTable',
        }

    def test_resource_with_colons(self):
        """ARN parser splits on ':' but caps at 5 splits, so resource keeps remaining colons."""
        parsed = _parse_arn('arn:aws:s3:::bucket:with:colons')
        assert parsed['resource'] == 'bucket:with:colons'
        assert parsed['region'] == ''
        assert parsed['account'] == ''

    def test_invalid_arn_raises(self):
        with pytest.raises(ValueError, match="Invalid ARN"):
            _parse_arn('not-an-arn')

    def test_missing_arn_prefix_raises(self):
        """ARN must begin with literal 'arn'."""
        with pytest.raises(ValueError, match="Invalid ARN"):
            _parse_arn('xrn:aws:dynamodb:us-east-1:111:table/T')

    def test_empty_string_raises(self):
        with pytest.raises(ValueError, match="Invalid ARN"):
            _parse_arn('')


# --- _region_from_table_ref -------------------------------------------------

class TestRegionFromTableRef:
    """Tests for _region_from_table_ref (lines 302-311)."""

    def test_none_returns_none(self):
        assert _region_from_table_ref(None) is None

    def test_empty_returns_none(self):
        assert _region_from_table_ref('') is None

    def test_plain_table_name_returns_none(self):
        """Non-ARN input yields None (use default region)."""
        assert _region_from_table_ref('MyTable') is None

    def test_dynamodb_table_arn_returns_region(self):
        arn = 'arn:aws:dynamodb:us-west-2:111122223333:table/MyTable'
        assert _region_from_table_ref(arn) == 'us-west-2'

    def test_non_dynamodb_arn_returns_none(self):
        """ARN for a non-dynamodb service yields None."""
        arn = 'arn:aws:s3:::bucket-name'
        assert _region_from_table_ref(arn) is None

    def test_dynamodb_arn_non_table_resource_returns_none(self):
        """ARN for dynamodb but resource isn't a table (e.g. backup)."""
        arn = 'arn:aws:dynamodb:us-east-1:111122223333:backup/foo'
        assert _region_from_table_ref(arn) is None
