"""Unit tests for utils.role_validator — custom role permission warnings.

When users pass --XRole with a custom IAM role name, validate_custom_role_permissions
inspects the role's trust policy and attached managed policies to warn about missing
minimum permissions required by the Bulk Executor Glue job.

The function WARNS (via log.warning) but never exits — it's advisory, not blocking.
"""

from unittest.mock import MagicMock, patch
import json

import pytest

from utils.role_validator import validate_custom_role_permissions


@pytest.fixture
def iam_client():
    return MagicMock()


def _trust_policy_with_glue():
    return json.dumps({
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {"Service": "glue.amazonaws.com"},
                "Action": "sts:AssumeRole",
            }
        ],
    })


def _trust_policy_without_glue():
    return json.dumps({
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {"Service": "ec2.amazonaws.com"},
                "Action": "sts:AssumeRole",
            }
        ],
    })


def _trust_policy_multiple_principals():
    return json.dumps({
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {"Service": ["lambda.amazonaws.com", "glue.amazonaws.com"]},
                "Action": "sts:AssumeRole",
            }
        ],
    })


class TestTrustPolicyValidation:
    """Warn when the role's trust policy doesn't allow glue.amazonaws.com."""

    def test_warns_when_trust_policy_missing_glue_principal(self, iam_client):
        iam_client.get_role.return_value = {
            "Role": {"AssumeRolePolicyDocument": json.loads(_trust_policy_without_glue())}
        }
        iam_client.list_attached_role_policies.return_value = {
            "AttachedPolicies": [
                {"PolicyArn": "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"},
                {"PolicyArn": "arn:aws:iam::aws:policy/AmazonDynamoDBReadOnlyAccess"},
            ]
        }

        warnings = validate_custom_role_permissions(iam_client, "MyCustomRole")

        assert any("glue.amazonaws.com" in w for w in warnings)

    def test_no_trust_warning_when_glue_principal_present(self, iam_client):
        iam_client.get_role.return_value = {
            "Role": {"AssumeRolePolicyDocument": json.loads(_trust_policy_with_glue())}
        }
        iam_client.list_attached_role_policies.return_value = {
            "AttachedPolicies": [
                {"PolicyArn": "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"},
                {"PolicyArn": "arn:aws:iam::aws:policy/AmazonDynamoDBReadOnlyAccess"},
            ]
        }

        warnings = validate_custom_role_permissions(iam_client, "MyCustomRole")

        assert not any("glue.amazonaws.com" in w for w in warnings)

    def test_no_trust_warning_when_glue_in_list_of_principals(self, iam_client):
        iam_client.get_role.return_value = {
            "Role": {"AssumeRolePolicyDocument": json.loads(_trust_policy_multiple_principals())}
        }
        iam_client.list_attached_role_policies.return_value = {
            "AttachedPolicies": [
                {"PolicyArn": "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"},
                {"PolicyArn": "arn:aws:iam::aws:policy/AmazonDynamoDBReadOnlyAccess"},
            ]
        }

        warnings = validate_custom_role_permissions(iam_client, "MyCustomRole")

        assert not any("glue.amazonaws.com" in w for w in warnings)


class TestManagedPolicyValidation:
    """Warn when the role is missing required managed policies."""

    def test_warns_when_missing_glue_service_role_policy(self, iam_client):
        iam_client.get_role.return_value = {
            "Role": {"AssumeRolePolicyDocument": json.loads(_trust_policy_with_glue())}
        }
        iam_client.list_attached_role_policies.return_value = {
            "AttachedPolicies": [
                {"PolicyArn": "arn:aws:iam::aws:policy/AmazonDynamoDBReadOnlyAccess"},
            ]
        }

        warnings = validate_custom_role_permissions(iam_client, "MyCustomRole")

        assert any("AWSGlueServiceRole" in w for w in warnings)

    def test_warns_when_missing_dynamodb_access(self, iam_client):
        iam_client.get_role.return_value = {
            "Role": {"AssumeRolePolicyDocument": json.loads(_trust_policy_with_glue())}
        }
        iam_client.list_attached_role_policies.return_value = {
            "AttachedPolicies": [
                {"PolicyArn": "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"},
            ]
        }

        warnings = validate_custom_role_permissions(iam_client, "MyCustomRole")

        assert any("DynamoDB" in w for w in warnings)

    def test_no_policy_warning_when_all_minimum_policies_present(self, iam_client):
        iam_client.get_role.return_value = {
            "Role": {"AssumeRolePolicyDocument": json.loads(_trust_policy_with_glue())}
        }
        iam_client.list_attached_role_policies.return_value = {
            "AttachedPolicies": [
                {"PolicyArn": "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"},
                {"PolicyArn": "arn:aws:iam::aws:policy/AmazonDynamoDBFullAccess"},
            ]
        }

        warnings = validate_custom_role_permissions(iam_client, "MyCustomRole")

        assert len(warnings) == 0

    def test_read_only_dynamodb_satisfies_requirement(self, iam_client):
        iam_client.get_role.return_value = {
            "Role": {"AssumeRolePolicyDocument": json.loads(_trust_policy_with_glue())}
        }
        iam_client.list_attached_role_policies.return_value = {
            "AttachedPolicies": [
                {"PolicyArn": "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"},
                {"PolicyArn": "arn:aws:iam::aws:policy/AmazonDynamoDBReadOnlyAccess"},
            ]
        }

        warnings = validate_custom_role_permissions(iam_client, "MyCustomRole")

        assert not any("DynamoDB" in w for w in warnings)


class TestMultipleWarnings:
    """When multiple issues exist, all are reported."""

    def test_returns_all_warnings_together(self, iam_client):
        iam_client.get_role.return_value = {
            "Role": {"AssumeRolePolicyDocument": json.loads(_trust_policy_without_glue())}
        }
        iam_client.list_attached_role_policies.return_value = {
            "AttachedPolicies": []
        }

        warnings = validate_custom_role_permissions(iam_client, "MyCustomRole")

        assert len(warnings) == 3  # trust + glue service role + dynamodb


class TestErrorHandling:
    """Graceful degradation when IAM calls fail."""

    def test_returns_empty_on_get_role_failure(self, iam_client):
        iam_client.get_role.side_effect = Exception("access denied")

        warnings = validate_custom_role_permissions(iam_client, "MyCustomRole")

        assert warnings == []

    def test_returns_empty_on_list_policies_failure(self, iam_client):
        iam_client.get_role.return_value = {
            "Role": {"AssumeRolePolicyDocument": json.loads(_trust_policy_with_glue())}
        }
        iam_client.list_attached_role_policies.side_effect = Exception("access denied")

        warnings = validate_custom_role_permissions(iam_client, "MyCustomRole")

        assert warnings == []


class TestIntegrationWithBootstrap:
    """Validate that _get_role_name calls validate and logs warnings for custom roles."""

    def test_custom_role_triggers_validation_and_logs_warnings(self):
        with patch('infrastructure.bootstrap.Clients') as MockClients:
            clients = MagicMock()
            clients.iam_client = MagicMock()
            clients.s3_client = MagicMock()
            clients.glue_client = MagicMock()
            clients.logs_client = MagicMock()
            MockClients.return_value = clients

            from infrastructure.bootstrap import BootstrapInfrastructure
            env = MagicMock(aws_region='us-east-1', aws_account_id='123456789012')
            instance = BootstrapInfrastructure(env)

        instance._is_existing_role = MagicMock(return_value=True)

        with patch(
            'infrastructure.bootstrap.validate_custom_role_permissions',
            return_value=["Role trust policy does not include glue.amazonaws.com"],
        ) as mock_validate:
            with patch('infrastructure.bootstrap.log') as mock_log:
                instance._get_role_name({'XRole': 'MyCustomRole'})

                mock_validate.assert_called_once_with(
                    instance.iam_client, 'MyCustomRole'
                )
                mock_log.warning.assert_called()
                warning_msg = mock_log.warning.call_args[0][0]
                assert "glue.amazonaws.com" in warning_msg

    def test_standard_role_does_not_trigger_validation(self):
        with patch('infrastructure.bootstrap.Clients') as MockClients:
            clients = MagicMock()
            clients.iam_client = MagicMock()
            clients.s3_client = MagicMock()
            clients.glue_client = MagicMock()
            clients.logs_client = MagicMock()
            MockClients.return_value = clients

            from infrastructure.bootstrap import BootstrapInfrastructure
            env = MagicMock(aws_region='us-east-1', aws_account_id='123456789012')
            instance = BootstrapInfrastructure(env)

        with patch(
            'infrastructure.bootstrap.validate_custom_role_permissions'
        ) as mock_validate:
            instance._get_role_name({'XRole': 'READ-ONLY'})
            mock_validate.assert_not_called()

    def test_custom_role_with_no_warnings_logs_nothing(self):
        with patch('infrastructure.bootstrap.Clients') as MockClients:
            clients = MagicMock()
            clients.iam_client = MagicMock()
            clients.s3_client = MagicMock()
            clients.glue_client = MagicMock()
            clients.logs_client = MagicMock()
            MockClients.return_value = clients

            from infrastructure.bootstrap import BootstrapInfrastructure
            env = MagicMock(aws_region='us-east-1', aws_account_id='123456789012')
            instance = BootstrapInfrastructure(env)

        instance._is_existing_role = MagicMock(return_value=True)

        with patch(
            'infrastructure.bootstrap.validate_custom_role_permissions',
            return_value=[],
        ):
            with patch('infrastructure.bootstrap.log') as mock_log:
                instance._get_role_name({'XRole': 'MyCustomRole'})
                mock_log.warning.assert_not_called()
