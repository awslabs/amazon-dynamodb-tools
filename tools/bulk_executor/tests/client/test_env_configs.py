"""Unit tests for EnvConfigs.

Covers `client/src/env_configs.py`:
- __init__: AWS region resolution, sts client construction, account ID and
  session role lookup, NoCredentialsError branch (print + exit), final
  configuration banner print
- _get_aws_account_id: success path, ClientError branch (print + exit)
- _get_aws_session_role: success path, ClientError branch (print + exit)
- _get_aws_region: XRegion override, default boto3 region fallback,
  empty/None region branch (print + exit), ClientError branch

Style notes:
- Mock boto3.Session and boto3.client at the env_configs module namespace,
  not the global boto3 module — env_configs imports `boto3` directly so
  the bound name is `env_configs.boto3.client`.
- Use SystemExit assertions (the source calls `exit(1)` which raises
  SystemExit) and capsys to verify the banner / SDK-style messages print.
"""

from unittest.mock import MagicMock, patch

import botocore.exceptions
import pytest

import env_configs


# --- Fixtures ---------------------------------------------------------------

@pytest.fixture
def mock_sts_client():
    """STS client that returns a happy-path get_caller_identity response."""
    client = MagicMock()
    client.get_caller_identity.return_value = {
        'Account': '111122223333',
        'Arn': 'arn:aws:iam::111122223333:user/jane',
    }
    return client


@pytest.fixture
def patched_boto(monkeypatch, mock_sts_client):
    """Patch boto3.Session().region_name and boto3.client('sts', ...)."""
    session = MagicMock()
    session.region_name = 'us-east-1'

    boto3_mock = MagicMock()
    boto3_mock.Session.return_value = session
    boto3_mock.client.return_value = mock_sts_client
    monkeypatch.setattr(env_configs, 'boto3', boto3_mock)
    return MagicMock(boto3=boto3_mock, session=session, sts=mock_sts_client)


def _client_error(code='AccessDenied', op='GetCallerIdentity'):
    return botocore.exceptions.ClientError(
        {'Error': {'Code': code, 'Message': f'{code} occurred'}},
        op,
    )


# --- __init__ ---------------------------------------------------------------

class TestEnvConfigsInit:
    """Tests for EnvConfigs.__init__ (lines 9-30)."""

    def test_happy_path_populates_attributes(self, patched_boto, capsys):
        """Lines 10-21: full success path sets aws_region, account, role."""
        instance = env_configs.EnvConfigs({})

        assert instance.aws_region == 'us-east-1'
        assert instance.aws_account_id == '111122223333'
        assert instance.aws_session_role == 'arn:aws:iam::111122223333:user/jane'

    def test_uses_x_region_override(self, patched_boto, capsys):
        """Line 55: args['XRegion'] takes precedence over default boto session."""
        instance = env_configs.EnvConfigs({'XRegion': 'eu-west-2'})

        assert instance.aws_region == 'eu-west-2'

    def test_creates_sts_client_with_resolved_region(self, patched_boto, capsys):
        """Line 12: boto3.client('sts', region_name=resolved_region)."""
        env_configs.EnvConfigs({'XRegion': 'ap-south-1'})

        patched_boto.boto3.client.assert_called_once_with('sts', region_name='ap-south-1')

    def test_prints_banner_with_account_region_role(self, patched_boto, capsys):
        """Lines 23-30: final summary banner contains account, region, role."""
        env_configs.EnvConfigs({})

        out = capsys.readouterr().out
        assert '111122223333' in out, "banner shows account id"
        assert 'us-east-1' in out, "banner shows region"
        assert 'arn:aws:iam::111122223333:user/jane' in out, "banner shows role arn"
        assert 'Bulk Executor environment configurations' in out

    def test_no_credentials_error_prints_message_and_exits(self, monkeypatch, capsys):
        """Lines 17-21: NoCredentialsError → SDK-style print + exit(1)."""
        sts = MagicMock()
        sts.get_caller_identity.side_effect = botocore.exceptions.NoCredentialsError()

        session = MagicMock(region_name='us-east-1')
        boto3_mock = MagicMock()
        boto3_mock.Session.return_value = session
        boto3_mock.client.return_value = sts
        monkeypatch.setattr(env_configs, 'boto3', boto3_mock)

        with pytest.raises(SystemExit) as exc_info:
            env_configs.EnvConfigs({})

        assert exc_info.value.code == 1
        out = capsys.readouterr().out
        assert 'Unable to locate credentials' in out
        assert 'aws configure' in out


# --- _get_aws_account_id ----------------------------------------------------

class TestGetAwsAccountId:
    """Tests for _get_aws_account_id (lines 32-40)."""

    def test_returns_account_field_from_caller_identity(self, patched_boto):
        """Line 35: returns caller_identity['Account']."""
        instance = env_configs.EnvConfigs.__new__(env_configs.EnvConfigs)
        result = instance._get_aws_account_id(patched_boto.sts)
        assert result == '111122223333'

    def test_client_error_prints_and_exits(self, capsys):
        """Lines 36-40: ClientError → print SDK message + exit(1)."""
        sts = MagicMock()
        sts.get_caller_identity.side_effect = _client_error()

        instance = env_configs.EnvConfigs.__new__(env_configs.EnvConfigs)
        with pytest.raises(SystemExit) as exc_info:
            instance._get_aws_account_id(sts)

        assert exc_info.value.code == 1
        out = capsys.readouterr().out
        assert 'Unable to locate AWS Account ID' in out


# --- _get_aws_session_role --------------------------------------------------

class TestGetAwsSessionRole:
    """Tests for _get_aws_session_role (lines 42-50)."""

    def test_returns_arn_from_caller_identity(self, patched_boto):
        """Line 45: returns caller_identity['Arn']."""
        instance = env_configs.EnvConfigs.__new__(env_configs.EnvConfigs)
        result = instance._get_aws_session_role(patched_boto.sts)
        assert result == 'arn:aws:iam::111122223333:user/jane'

    def test_client_error_prints_and_exits(self, capsys):
        """Lines 46-50: ClientError → print SDK message + exit(1)."""
        sts = MagicMock()
        sts.get_caller_identity.side_effect = _client_error()

        instance = env_configs.EnvConfigs.__new__(env_configs.EnvConfigs)
        with pytest.raises(SystemExit) as exc_info:
            instance._get_aws_session_role(sts)

        assert exc_info.value.code == 1
        out = capsys.readouterr().out
        assert 'Unable to locate AWS Session Role' in out


# --- _get_aws_region --------------------------------------------------------

class TestGetAwsRegion:
    """Tests for _get_aws_region (lines 52-65)."""

    def test_x_region_takes_precedence(self, monkeypatch):
        """Line 55: args['XRegion'] overrides boto3 default region."""
        session = MagicMock(region_name='us-east-1')
        boto3_mock = MagicMock()
        boto3_mock.Session.return_value = session
        monkeypatch.setattr(env_configs, 'boto3', boto3_mock)

        instance = env_configs.EnvConfigs.__new__(env_configs.EnvConfigs)
        assert instance._get_aws_region({'XRegion': 'us-west-2'}) == 'us-west-2'

    def test_falls_back_to_default_session_region(self, monkeypatch):
        """Line 55: when XRegion absent, uses boto3.Session().region_name."""
        session = MagicMock(region_name='ca-central-1')
        boto3_mock = MagicMock()
        boto3_mock.Session.return_value = session
        monkeypatch.setattr(env_configs, 'boto3', boto3_mock)

        instance = env_configs.EnvConfigs.__new__(env_configs.EnvConfigs)
        assert instance._get_aws_region({}) == 'ca-central-1'

    def test_x_region_overrides_even_when_default_set(self, monkeypatch):
        """Line 55: 'or' precedence — truthy XRegion wins regardless of default."""
        session = MagicMock(region_name='us-east-1')
        boto3_mock = MagicMock()
        boto3_mock.Session.return_value = session
        monkeypatch.setattr(env_configs, 'boto3', boto3_mock)

        instance = env_configs.EnvConfigs.__new__(env_configs.EnvConfigs)
        assert instance._get_aws_region({'XRegion': 'eu-north-1'}) == 'eu-north-1'

    def test_no_region_anywhere_prints_and_exits(self, monkeypatch, capsys):
        """Lines 56-59: no XRegion + no default region → print + exit(1)."""
        session = MagicMock(region_name=None)
        boto3_mock = MagicMock()
        boto3_mock.Session.return_value = session
        monkeypatch.setattr(env_configs, 'boto3', boto3_mock)

        instance = env_configs.EnvConfigs.__new__(env_configs.EnvConfigs)
        with pytest.raises(SystemExit) as exc_info:
            instance._get_aws_region({})

        assert exc_info.value.code == 1
        out = capsys.readouterr().out
        assert 'Unable to locate AWS Region' in out

    def test_empty_x_region_falls_back_to_default(self, monkeypatch):
        """Line 55: empty string XRegion is falsy, falls through to default."""
        session = MagicMock(region_name='us-east-1')
        boto3_mock = MagicMock()
        boto3_mock.Session.return_value = session
        monkeypatch.setattr(env_configs, 'boto3', boto3_mock)

        instance = env_configs.EnvConfigs.__new__(env_configs.EnvConfigs)
        assert instance._get_aws_region({'XRegion': ''}) == 'us-east-1'

    def test_client_error_in_session_prints_and_exits(self, monkeypatch, capsys):
        """Lines 61-65: ClientError from boto3.Session → print + exit(1)."""
        boto3_mock = MagicMock()
        boto3_mock.Session.side_effect = _client_error('SomeError', 'Session')
        monkeypatch.setattr(env_configs, 'boto3', boto3_mock)

        instance = env_configs.EnvConfigs.__new__(env_configs.EnvConfigs)
        with pytest.raises(SystemExit) as exc_info:
            instance._get_aws_region({})

        assert exc_info.value.code == 1
        out = capsys.readouterr().out
        assert 'Unable to locate AWS Region' in out
