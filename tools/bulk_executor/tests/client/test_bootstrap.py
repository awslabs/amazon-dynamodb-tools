"""Unit tests for BootstrapInfrastructure._create_or_update_glue_job.

Focused on the args -> default_arguments translation that controls which
flags get baked into the Glue job's DefaultArguments. Issue #85 covers
XRole/XRegion; XAccount is the same shape — a CLI flag that's defined
in argparse but never read at runtime, so persisting it to DefaultArguments
is just dead state.

Tests are written test-first against the current behavior so that they
serve as a regression harness for each exclusion.

Extended coverage: every other public/private method on
BootstrapInfrastructure — IAM role lifecycle, S3 bucket lifecycle,
CloudWatch log groups, Python module zipping, the interactive
_prompt_for_role flow, and the top-level bootstrap() orchestrator.
"""

import json
from unittest.mock import MagicMock, call, patch

import pytest
from botocore.exceptions import ClientError


@pytest.fixture
def bootstrap():
    """Construct a BootstrapInfrastructure with all AWS clients mocked.

    Returns the instance with named MagicMock clients attached (iam_client,
    s3_client, glue_client, logs_client) so individual tests can inspect
    .call_args / .return_value on them.
    """
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

    # Stub the bucket-name resolution so we don't hit get_job during the
    # tests we care about — the fix line is upstream of any bucket logic.
    instance._get_glue_job_bucket_name = MagicMock(return_value='fake-bucket')
    return instance


def _run(bootstrap, args, *, existing=True):
    """Helper: invoke _create_or_update_glue_job(args), return the
    DefaultArguments dict that was sent to the Glue API."""
    with patch('infrastructure.bootstrap.is_existing_glue_job', return_value=existing):
        bootstrap._create_or_update_glue_job(args)
    if existing:
        return bootstrap.glue_client.update_job.call_args.kwargs['JobUpdate']['DefaultArguments']
    return bootstrap.glue_client.create_job.call_args.kwargs['DefaultArguments']


# -- existing behavior ---------------------------------------------------

class TestExistingBehavior:
    """Behavior that must remain stable before and after the fix."""

    def test_x_prefixed_arg_is_forwarded(self, bootstrap):
        result = _run(bootstrap, {'XNumberOfWorkers': 10})
        assert result.get('--XNumberOfWorkers') == '10'

    def test_non_x_prefixed_arg_is_dropped(self, bootstrap):
        result = _run(bootstrap, {'foo': 'bar', 'XWorkerType': 'G.1X'})
        assert '--foo' not in result
        assert result.get('--XWorkerType') == 'G.1X'

    def test_value_is_stringified(self, bootstrap):
        result = _run(bootstrap, {'XContinuousLogging': True})
        assert result.get('--XContinuousLogging') == 'True'

    def test_multiple_x_args_all_forwarded(self, bootstrap):
        result = _run(bootstrap, {
            'XNumberOfWorkers': 5,
            'XWorkerType': 'G.2X',
            'XTimeout': 60,
        })
        assert result.get('--XNumberOfWorkers') == '5'
        assert result.get('--XWorkerType') == 'G.2X'
        assert result.get('--XTimeout') == '60'

    def test_empty_args_no_x_keys_forwarded(self, bootstrap):
        result = _run(bootstrap, {})
        x_keys = [k for k in result if k.startswith('--X')]
        assert x_keys == [], f"Expected no X-prefixed default args, got {x_keys}"

    def test_unknown_x_prefixed_key_is_forwarded(self, bootstrap):
        # An X-prefixed key the bootstrap doesn't know about should still
        # pass through — this is the open-ended pass-through that XRole/
        # XRegion currently piggyback on (the bug we're fixing).
        result = _run(bootstrap, {'XCustomFlag': 'value123'})
        assert result.get('--XCustomFlag') == 'value123'

    def test_create_path_matches_update_path(self, bootstrap):
        # Both branches (create_job vs update_job) build default_arguments
        # the same way. Lock that in so a future refactor that diverges
        # them is caught.
        update_result = _run(bootstrap, {'XWorkerType': 'G.1X'}, existing=True)
        # Reset mocks before second run
        bootstrap.glue_client.reset_mock()
        create_result = _run(bootstrap, {'XWorkerType': 'G.1X'}, existing=False)
        assert update_result.get('--XWorkerType') == create_result.get('--XWorkerType')


# -- _get_role_name -----------------------------------------------------

class TestGetRoleName:
    """Coverage for _get_role_name across custom-role and standard-role paths."""

    def test_no_role_specified_returns_read_only_default(self, bootstrap):
        from infrastructure.constants import GLUE_JOB_ROOT_ROLE_NAME, READ_ONLY_ROLE_ID
        name = bootstrap._get_role_name({})
        assert name == f"{GLUE_JOB_ROOT_ROLE_NAME}-{READ_ONLY_ROLE_ID}-us-east-1"

    def test_read_only_role_param_returns_read_only(self, bootstrap):
        from infrastructure.constants import (
            GLUE_JOB_ROOT_ROLE_NAME,
            READ_ONLY_ROLE_ID,
            ROLE_TYPE_READ_ONLY,
        )
        name = bootstrap._get_role_name({'XRole': ROLE_TYPE_READ_ONLY})
        assert name == f"{GLUE_JOB_ROOT_ROLE_NAME}-{READ_ONLY_ROLE_ID}-us-east-1"

    def test_read_write_role_param_returns_read_write(self, bootstrap):
        from infrastructure.constants import (
            GLUE_JOB_ROOT_ROLE_NAME,
            READ_WRITE_ROLE_ID,
            ROLE_TYPE_READ_WRITE,
        )
        name = bootstrap._get_role_name({'XRole': ROLE_TYPE_READ_WRITE})
        assert name == f"{GLUE_JOB_ROOT_ROLE_NAME}-{READ_WRITE_ROLE_ID}-us-east-1"

    def test_existing_custom_role_is_returned(self, bootstrap):
        bootstrap._is_existing_role = MagicMock(return_value=True)
        assert bootstrap._get_role_name({'XRole': 'MyCustomRole'}) == 'MyCustomRole'
        bootstrap._is_existing_role.assert_called_once_with('MyCustomRole')

    def test_missing_custom_role_exits(self, bootstrap, capsys):
        bootstrap._is_existing_role = MagicMock(return_value=False)
        with pytest.raises(SystemExit) as exc:
            bootstrap._get_role_name({'XRole': 'NoSuchRole'})
        assert exc.value.code == 1
        out = capsys.readouterr().out
        assert 'NoSuchRole' in out


# -- _check_custom_role_permissions -------------------------------------

class TestCheckCustomRolePermissions:
    """Coverage for the custom role minimum-permissions warning."""

    def test_all_permissions_allowed_no_warning(self, bootstrap, caplog):
        import logging
        bootstrap.iam_client.simulate_principal_policy.return_value = {
            'EvaluationResults': [
                {'EvalActionName': action, 'EvalDecision': 'allowed'}
                for action in bootstrap.REQUIRED_ACTIONS
            ]
        }
        with caplog.at_level(logging.WARNING):
            bootstrap._check_custom_role_permissions('GoodRole')
        assert 'missing required permissions' not in caplog.text

    def test_some_permissions_denied_emits_warning(self, bootstrap, caplog):
        import logging
        results = [
            {'EvalActionName': 'dynamodb:DescribeTable', 'EvalDecision': 'allowed'},
            {'EvalActionName': 'dynamodb:Scan', 'EvalDecision': 'allowed'},
            {'EvalActionName': 's3:GetObject', 'EvalDecision': 'implicitDeny'},
            {'EvalActionName': 's3:PutObject', 'EvalDecision': 'allowed'},
            {'EvalActionName': 'logs:CreateLogGroup', 'EvalDecision': 'allowed'},
            {'EvalActionName': 'logs:PutLogEvents', 'EvalDecision': 'allowed'},
            {'EvalActionName': 'pricing:GetProducts', 'EvalDecision': 'explicitDeny'},
            {'EvalActionName': 'servicequotas:GetServiceQuota', 'EvalDecision': 'allowed'},
        ]
        bootstrap.iam_client.simulate_principal_policy.return_value = {
            'EvaluationResults': results
        }
        with caplog.at_level(logging.WARNING):
            bootstrap._check_custom_role_permissions('PartialRole')
        assert 'missing required permissions' in caplog.text
        assert 's3:GetObject' in caplog.text
        assert 'pricing:GetProducts' in caplog.text
        assert 'dynamodb:DescribeTable' not in caplog.text

    def test_simulate_api_error_does_not_block(self, bootstrap, caplog):
        import logging
        bootstrap.iam_client.simulate_principal_policy.side_effect = RuntimeError('access denied')
        with caplog.at_level(logging.DEBUG):
            bootstrap._check_custom_role_permissions('AnyRole')
        assert 'Unable to verify permissions' in caplog.text

    def test_constructs_correct_arn(self, bootstrap):
        bootstrap.iam_client.simulate_principal_policy.return_value = {
            'EvaluationResults': []
        }
        bootstrap._check_custom_role_permissions('MyRole')
        call_kwargs = bootstrap.iam_client.simulate_principal_policy.call_args.kwargs
        assert call_kwargs['PolicySourceArn'] == 'arn:aws:iam::123456789012:role/MyRole'
        assert call_kwargs['ActionNames'] == bootstrap.REQUIRED_ACTIONS

    def test_get_role_name_calls_check_for_custom_role(self, bootstrap):
        bootstrap._is_existing_role = MagicMock(return_value=True)
        bootstrap._check_custom_role_permissions = MagicMock()
        bootstrap._get_role_name({'XRole': 'CustomRole'})
        bootstrap._check_custom_role_permissions.assert_called_once_with('CustomRole')

    def test_get_role_name_skips_check_for_standard_roles(self, bootstrap):
        from infrastructure.constants import ROLE_TYPE_READ_ONLY
        bootstrap._check_custom_role_permissions = MagicMock()
        bootstrap._get_role_name({'XRole': ROLE_TYPE_READ_ONLY})
        bootstrap._check_custom_role_permissions.assert_not_called()


# -- _is_existing_role --------------------------------------------------

class TestIsExistingRole:
    def test_role_exists_returns_true(self, bootstrap):
        bootstrap.iam_client.get_role.return_value = {'Role': {}}
        assert bootstrap._is_existing_role('MyRole') is True
        bootstrap.iam_client.get_role.assert_called_once_with(RoleName='MyRole')

    def test_no_such_entity_returns_false(self, bootstrap):
        class NoSuchEntityException(Exception):
            pass
        bootstrap.iam_client.exceptions.NoSuchEntityException = NoSuchEntityException
        bootstrap.iam_client.get_role.side_effect = NoSuchEntityException()
        assert bootstrap._is_existing_role('Missing') is False

    def test_unexpected_error_exits(self, bootstrap):
        class NoSuchEntityException(Exception):
            pass
        bootstrap.iam_client.exceptions.NoSuchEntityException = NoSuchEntityException
        bootstrap.iam_client.get_role.side_effect = RuntimeError('boom')
        with pytest.raises(SystemExit) as exc:
            bootstrap._is_existing_role('R')
        assert exc.value.code == 1


# -- _is_write_access_enabled -------------------------------------------

class TestIsWriteAccessEnabled:
    def test_no_role_returns_false(self, bootstrap):
        assert bootstrap._is_write_access_enabled({}) is False

    def test_read_only_returns_false(self, bootstrap):
        from infrastructure.constants import ROLE_TYPE_READ_ONLY
        assert bootstrap._is_write_access_enabled({'XRole': ROLE_TYPE_READ_ONLY}) is False

    def test_read_write_returns_true(self, bootstrap):
        from infrastructure.constants import ROLE_TYPE_READ_WRITE
        assert bootstrap._is_write_access_enabled({'XRole': ROLE_TYPE_READ_WRITE}) is True

    def test_custom_role_returns_none(self, bootstrap):
        assert bootstrap._is_write_access_enabled({'XRole': 'CustomRole'}) is None


# -- _add_glue_job_role -------------------------------------------------

class TestAddGlueJobRole:
    """Coverage for the IAM role + policy creation flow."""

    def test_creates_role_and_attaches_read_only_policies(self, bootstrap):
        from infrastructure.constants import ROLE_TYPE_READ_ONLY
        bootstrap._prompt_for_role = MagicMock()
        bootstrap.iam_client.create_role.return_value = {
            'Role': {'Arn': 'arn:aws:iam::123456789012:role/test'}
        }

        bootstrap._add_glue_job_role({'XRole': ROLE_TYPE_READ_ONLY})

        bootstrap._prompt_for_role.assert_called_once()
        bootstrap.iam_client.create_role.assert_called_once()
        # Verify trust policy is JSON encoded with glue.amazonaws.com principal
        kwargs = bootstrap.iam_client.create_role.call_args.kwargs
        trust = json.loads(kwargs['AssumeRolePolicyDocument'])
        assert trust['Statement'][0]['Principal']['Service'] == 'glue.amazonaws.com'

        # ReadOnly policies attached
        attached = [
            c.kwargs['PolicyArn']
            for c in bootstrap.iam_client.attach_role_policy.call_args_list
        ]
        assert 'arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole' in attached
        assert 'arn:aws:iam::aws:policy/AmazonDynamoDBReadOnlyAccess' in attached
        assert 'arn:aws:iam::aws:policy/AmazonDynamoDBFullAccess' not in attached

        # Inline policies for pricing + quotas
        inline_names = [
            c.kwargs['PolicyName']
            for c in bootstrap.iam_client.put_role_policy.call_args_list
        ]
        assert 'MinimalPricingAccess' in inline_names
        assert 'MinimalQuotasAccess' in inline_names

    def test_creates_role_and_attaches_read_write_policies(self, bootstrap):
        from infrastructure.constants import ROLE_TYPE_READ_WRITE
        bootstrap._prompt_for_role = MagicMock()
        bootstrap.iam_client.create_role.return_value = {
            'Role': {'Arn': 'arn:aws:iam::123456789012:role/test'}
        }

        bootstrap._add_glue_job_role({'XRole': ROLE_TYPE_READ_WRITE})

        attached = [
            c.kwargs['PolicyArn']
            for c in bootstrap.iam_client.attach_role_policy.call_args_list
        ]
        assert 'arn:aws:iam::aws:policy/AmazonDynamoDBFullAccess' in attached
        assert 'arn:aws:iam::aws:policy/AmazonDynamoDBReadOnlyAccess' not in attached

    def test_role_already_exists_returns_without_attaching_policies(self, bootstrap):
        from infrastructure.constants import ROLE_TYPE_READ_ONLY
        bootstrap._prompt_for_role = MagicMock()

        class EntityAlreadyExistsException(Exception):
            pass
        bootstrap.iam_client.exceptions.EntityAlreadyExistsException = (
            EntityAlreadyExistsException
        )
        bootstrap.iam_client.create_role.side_effect = EntityAlreadyExistsException()

        bootstrap._add_glue_job_role({'XRole': ROLE_TYPE_READ_ONLY})

        # Early return — no policy attachments
        bootstrap.iam_client.attach_role_policy.assert_not_called()
        bootstrap.iam_client.put_role_policy.assert_not_called()

    def test_unexpected_create_role_error_exits(self, bootstrap):
        from infrastructure.constants import ROLE_TYPE_READ_ONLY
        bootstrap._prompt_for_role = MagicMock()

        class EntityAlreadyExistsException(Exception):
            pass
        bootstrap.iam_client.exceptions.EntityAlreadyExistsException = (
            EntityAlreadyExistsException
        )
        bootstrap.iam_client.create_role.side_effect = RuntimeError('boom')

        with pytest.raises(SystemExit) as exc:
            bootstrap._add_glue_job_role({'XRole': ROLE_TYPE_READ_ONLY})
        assert exc.value.code == 1

    def test_attach_policy_failure_exits(self, bootstrap):
        from infrastructure.constants import ROLE_TYPE_READ_ONLY
        bootstrap._prompt_for_role = MagicMock()
        bootstrap.iam_client.create_role.return_value = {
            'Role': {'Arn': 'arn:aws:iam::123456789012:role/test'}
        }
        bootstrap.iam_client.attach_role_policy.side_effect = RuntimeError('iam-fail')

        with pytest.raises(SystemExit) as exc:
            bootstrap._add_glue_job_role({'XRole': ROLE_TYPE_READ_ONLY})
        assert exc.value.code == 1


# -- _create_or_update_glue_job exception path --------------------------

class TestCreateOrUpdateGlueJobErrors:
    def test_glue_api_error_exits(self, bootstrap):
        bootstrap.glue_client.update_job.side_effect = RuntimeError('glue-fail')
        with patch('infrastructure.bootstrap.is_existing_glue_job', return_value=True):
            with pytest.raises(SystemExit) as exc:
                bootstrap._create_or_update_glue_job({})
        assert exc.value.code == 1

    def test_existing_job_create_not_allowed_no_call(self, bootstrap):
        with patch('infrastructure.bootstrap.is_existing_glue_job', return_value=False):
            bootstrap._create_or_update_glue_job({}, is_create_allowed=False)
        bootstrap.glue_client.create_job.assert_not_called()
        bootstrap.glue_client.update_job.assert_not_called()

    def test_continuous_logging_disabled_attaches_extra_files(self, bootstrap):
        # default arg path: enable_continuous_cloudwatch_log is False -> '--extra-files'
        with patch('infrastructure.bootstrap.is_existing_glue_job', return_value=True):
            bootstrap._create_or_update_glue_job({})
        update = bootstrap.glue_client.update_job.call_args.kwargs['JobUpdate']
        assert '--extra-files' in update['DefaultArguments']

    def test_continuous_logging_enabled_no_extra_files(self, bootstrap):
        with patch('infrastructure.bootstrap.is_existing_glue_job', return_value=True):
            bootstrap._create_or_update_glue_job({'XContinuousLogging': True})
        update = bootstrap.glue_client.update_job.call_args.kwargs['JobUpdate']
        assert '--extra-files' not in update['DefaultArguments']
        assert update['DefaultArguments']['--enable-continuous-cloudwatch-log'] == 'true'


# -- _bucket_exists -----------------------------------------------------

class TestBucketExists:
    def test_returns_true_when_head_bucket_succeeds(self, bootstrap):
        s3 = MagicMock()
        s3.head_bucket.return_value = {}
        assert bootstrap._bucket_exists(s3, 'b') is True
        s3.head_bucket.assert_called_once_with(Bucket='b')

    def test_returns_false_on_403(self, bootstrap):
        s3 = MagicMock()
        s3.head_bucket.side_effect = ClientError(
            {'Error': {'Code': '403', 'Message': 'Forbidden'}}, 'HeadBucket'
        )
        assert bootstrap._bucket_exists(s3, 'b') is False

    def test_returns_false_on_404(self, bootstrap):
        s3 = MagicMock()
        s3.head_bucket.side_effect = ClientError(
            {'Error': {'Code': '404', 'Message': 'Not Found'}}, 'HeadBucket'
        )
        assert bootstrap._bucket_exists(s3, 'b') is False

    def test_reraises_unexpected_client_error(self, bootstrap):
        s3 = MagicMock()
        s3.head_bucket.side_effect = ClientError(
            {'Error': {'Code': '500', 'Message': 'Boom'}}, 'HeadBucket'
        )
        with pytest.raises(ClientError):
            bootstrap._bucket_exists(s3, 'b')


# -- _upload_job_root_to_s3 ---------------------------------------------

class TestUploadJobRootToS3:
    def test_creates_bucket_when_missing_in_non_default_region(self, bootstrap):
        bootstrap.aws_region = 'us-west-2'
        bootstrap._bucket_exists = MagicMock(return_value=False)

        bootstrap._upload_job_root_to_s3()

        # Bucket is created with LocationConstraint
        create_kwargs = bootstrap.s3_client.create_bucket.call_args.kwargs
        assert create_kwargs['CreateBucketConfiguration'] == {
            'LocationConstraint': 'us-west-2'
        }
        # Policy applied
        bootstrap.s3_client.put_bucket_policy.assert_called_once()
        # Script file uploaded
        bootstrap.s3_client.upload_file.assert_called_once()

    def test_creates_bucket_in_us_east_1_omits_location_constraint(self, bootstrap):
        bootstrap.aws_region = 'us-east-1'
        bootstrap._bucket_exists = MagicMock(return_value=False)

        bootstrap._upload_job_root_to_s3()

        create_kwargs = bootstrap.s3_client.create_bucket.call_args.kwargs
        assert 'CreateBucketConfiguration' not in create_kwargs

    def test_skips_create_when_bucket_exists(self, bootstrap):
        bootstrap._bucket_exists = MagicMock(return_value=True)

        bootstrap._upload_job_root_to_s3()

        bootstrap.s3_client.create_bucket.assert_not_called()
        # Policy and upload still applied
        bootstrap.s3_client.put_bucket_policy.assert_called_once()
        bootstrap.s3_client.upload_file.assert_called_once()

    def test_create_bucket_failure_exits(self, bootstrap):
        bootstrap._bucket_exists = MagicMock(return_value=False)
        bootstrap.s3_client.create_bucket.side_effect = RuntimeError('boom')

        with pytest.raises(SystemExit) as exc:
            bootstrap._upload_job_root_to_s3()
        assert exc.value.code == 1

    def test_put_bucket_policy_failure_exits(self, bootstrap):
        bootstrap._bucket_exists = MagicMock(return_value=True)
        bootstrap.s3_client.put_bucket_policy.side_effect = RuntimeError('boom')

        with pytest.raises(SystemExit) as exc:
            bootstrap._upload_job_root_to_s3()
        assert exc.value.code == 1

    def test_secure_transport_policy_shape(self, bootstrap):
        bootstrap._bucket_exists = MagicMock(return_value=True)

        bootstrap._upload_job_root_to_s3()

        policy_kwargs = bootstrap.s3_client.put_bucket_policy.call_args.kwargs
        policy = json.loads(policy_kwargs['Policy'])
        statement = policy['Statement'][0]
        assert statement['Effect'] == 'Deny'
        assert statement['Condition']['Bool']['aws:SecureTransport'] == 'false'


# -- _get_glue_job_bucket_name + _get_glue_job_details ------------------

class TestGetGlueJobBucketName:
    @pytest.fixture
    def fresh_bootstrap(self):
        """Bootstrap WITHOUT the auto-stub of _get_glue_job_bucket_name."""
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
        return instance

    def test_returns_persisted_bucket_when_present(self, fresh_bootstrap):
        fresh_bootstrap._get_glue_job_details = MagicMock(return_value={
            'Job': {'DefaultArguments': {'--s3-bucket-name': 'persisted-bucket'}}
        })
        assert fresh_bootstrap._get_glue_job_bucket_name() == 'persisted-bucket'

    def test_generates_new_bucket_when_no_existing_job(self, fresh_bootstrap):
        fresh_bootstrap._get_glue_job_details = MagicMock(return_value=None)
        name = fresh_bootstrap._get_glue_job_bucket_name()
        assert name.startswith('aws-glue-bulk-dynamodb-us-east-1-123456789012-')
        # Suffix is 9 chars
        suffix = name.rsplit('-', 1)[-1]
        assert len(suffix) == 9

    def test_generates_new_bucket_when_existing_job_has_no_bucket_arg(
        self, fresh_bootstrap
    ):
        fresh_bootstrap._get_glue_job_details = MagicMock(return_value={
            'Job': {'DefaultArguments': {}}
        })
        name = fresh_bootstrap._get_glue_job_bucket_name()
        assert name.startswith('aws-glue-bulk-dynamodb-us-east-1-123456789012-')

    def test_get_job_details_returns_job_when_present(self, fresh_bootstrap):
        fresh_bootstrap.glue_client.get_job.return_value = {'Job': {'Name': 'x'}}
        assert fresh_bootstrap._get_glue_job_details() == {'Job': {'Name': 'x'}}

    def test_get_job_details_returns_none_on_entity_not_found(self, fresh_bootstrap):
        fresh_bootstrap.glue_client.get_job.side_effect = ClientError(
            {'Error': {'Code': 'EntityNotFoundException', 'Message': 'nope'}},
            'GetJob',
        )
        assert fresh_bootstrap._get_glue_job_details() is None

    def test_get_job_details_unexpected_error_exits(self, fresh_bootstrap):
        fresh_bootstrap.glue_client.get_job.side_effect = ClientError(
            {'Error': {'Code': 'AccessDenied', 'Message': 'no'}}, 'GetJob'
        )
        with pytest.raises(SystemExit) as exc:
            fresh_bootstrap._get_glue_job_details()
        assert exc.value.code == 1


# -- _create_python_modules_archive + update_python_modules_in_s3 -------

class TestPythonModulesArchive:
    def test_create_archive_makes_tmp_dir_and_zips(self, bootstrap):
        with patch('infrastructure.bootstrap.os.makedirs') as makedirs, \
             patch('infrastructure.bootstrap.module_zipper') as module_zipper:
            bootstrap._create_python_modules_archive()
            makedirs.assert_called_once()
            args, kwargs = makedirs.call_args
            assert args[0].endswith('/tmp')
            assert kwargs.get('exist_ok') is True
            module_zipper.zip_module.assert_called_once()

    def test_update_python_modules_in_s3_uploads_archive(self, bootstrap):
        with patch.object(bootstrap, '_create_python_modules_archive') as mk:
            bootstrap.update_python_modules_in_s3()
            mk.assert_called_once()
            bootstrap.s3_client.upload_file.assert_called_once()
            args = bootstrap.s3_client.upload_file.call_args.args
            # local zip path, bucket, server zip path
            assert args[1] == 'fake-bucket'


# -- _upload_property_files_to_s3 ---------------------------------------

class TestUploadPropertyFilesToS3:
    def test_uploads_log4j_properties_file(self, bootstrap):
        from infrastructure.constants import LOG4J_PROPERTIES_FILE
        bootstrap._upload_property_files_to_s3()
        bootstrap.s3_client.upload_file.assert_called_once_with(
            f"./{LOG4J_PROPERTIES_FILE}", 'fake-bucket', LOG4J_PROPERTIES_FILE
        )


# -- _create_glue_log_groups --------------------------------------------

class TestCreateGlueLogGroups:
    def test_creates_each_log_group_and_sets_retention(self, bootstrap):
        from infrastructure.constants import (
            GLUE_LOG_GROUP_NAMES,
            GLUE_LOG_GROUP_RETENTION_IN_DAYS,
        )
        bootstrap._create_glue_log_groups()

        # One create per log group
        create_calls = bootstrap.logs_client.create_log_group.call_args_list
        assert len(create_calls) == len(GLUE_LOG_GROUP_NAMES)
        # Retention set per group
        retention_calls = bootstrap.logs_client.put_retention_policy.call_args_list
        assert len(retention_calls) == len(GLUE_LOG_GROUP_NAMES)
        for c in retention_calls:
            assert c.kwargs['retentionInDays'] == GLUE_LOG_GROUP_RETENTION_IN_DAYS

    def test_existing_log_group_still_updates_retention(self, bootstrap):
        from infrastructure.constants import GLUE_LOG_GROUP_NAMES
        bootstrap.logs_client.create_log_group.side_effect = ClientError(
            {'Error': {'Code': 'ResourceAlreadyExistsException', 'Message': 'exists'}},
            'CreateLogGroup',
        )

        bootstrap._create_glue_log_groups()
        # Retention still applied for each group
        assert bootstrap.logs_client.put_retention_policy.call_count == len(
            GLUE_LOG_GROUP_NAMES
        )

    def test_unexpected_client_error_propagates(self, bootstrap):
        bootstrap.logs_client.create_log_group.side_effect = ClientError(
            {'Error': {'Code': 'AccessDenied', 'Message': 'nope'}}, 'CreateLogGroup'
        )
        with pytest.raises(ClientError):
            bootstrap._create_glue_log_groups()

    def test_unexpected_non_client_error_exits(self, bootstrap):
        bootstrap.logs_client.create_log_group.side_effect = RuntimeError('boom')
        with pytest.raises(SystemExit) as exc:
            bootstrap._create_glue_log_groups()
        assert exc.value.code == 1


# -- _prompt_for_role ---------------------------------------------------

class TestPromptForRole:
    def test_pre_configured_read_only_no_prompt(self, bootstrap):
        from infrastructure.constants import ROLE_TYPE_READ_ONLY
        args = {'XRole': ROLE_TYPE_READ_ONLY}
        # Should not call input()
        with patch('builtins.input') as mock_input:
            bootstrap._prompt_for_role(args)
            mock_input.assert_not_called()
        assert args['XRole'] == ROLE_TYPE_READ_ONLY

    def test_pre_configured_read_write_no_prompt(self, bootstrap):
        from infrastructure.constants import ROLE_TYPE_READ_WRITE
        args = {'XRole': ROLE_TYPE_READ_WRITE}
        with patch('builtins.input') as mock_input:
            bootstrap._prompt_for_role(args)
            mock_input.assert_not_called()
        assert args['XRole'] == ROLE_TYPE_READ_WRITE

    def test_custom_role_string_no_prompt(self, bootstrap):
        # Any non-standard role string short-circuits the prompt
        args = {'XRole': 'MyCustom'}
        with patch('builtins.input') as mock_input:
            bootstrap._prompt_for_role(args)
            mock_input.assert_not_called()
        assert args['XRole'] == 'MyCustom'

    def test_user_chooses_1_for_read_only(self, bootstrap):
        from infrastructure.constants import ROLE_TYPE_READ_ONLY
        args = {}
        with patch('builtins.input', return_value='1'):
            bootstrap._prompt_for_role(args)
        assert args['XRole'] == ROLE_TYPE_READ_ONLY

    def test_user_chooses_read_only_lower(self, bootstrap):
        from infrastructure.constants import ROLE_TYPE_READ_ONLY
        args = {}
        with patch('builtins.input', return_value=ROLE_TYPE_READ_ONLY.lower()):
            bootstrap._prompt_for_role(args)
        assert args['XRole'] == ROLE_TYPE_READ_ONLY

    def test_user_chooses_2_for_read_write(self, bootstrap):
        from infrastructure.constants import ROLE_TYPE_READ_WRITE
        args = {}
        with patch('builtins.input', return_value='2'):
            bootstrap._prompt_for_role(args)
        assert args['XRole'] == ROLE_TYPE_READ_WRITE

    def test_user_chooses_3_for_custom_existing_role(self, bootstrap):
        args = {}
        bootstrap._is_existing_role = MagicMock(return_value=True)
        with patch('builtins.input', side_effect=['3', 'MyCustomRole']):
            bootstrap._prompt_for_role(args)
        assert args['XRole'] == 'MyCustomRole'

    def test_user_chooses_3_for_custom_missing_role_exits(self, bootstrap):
        args = {}
        bootstrap._is_existing_role = MagicMock(return_value=False)
        with patch('builtins.input', side_effect=['3', 'NoSuchRole']):
            with pytest.raises(SystemExit) as exc:
                bootstrap._prompt_for_role(args)
            assert exc.value.code == 1

    def test_invalid_choice_then_valid(self, bootstrap, capsys):
        from infrastructure.constants import ROLE_TYPE_READ_ONLY
        args = {}
        with patch('builtins.input', side_effect=['x', '1']):
            bootstrap._prompt_for_role(args)
        assert args['XRole'] == ROLE_TYPE_READ_ONLY
        assert 'Invalid choice' in capsys.readouterr().out

    def test_eof_during_role_prompt_exits(self, bootstrap):
        args = {}
        with patch('builtins.input', side_effect=EOFError()):
            with pytest.raises(SystemExit) as exc:
                bootstrap._prompt_for_role(args)
            assert exc.value.code == 1

    def test_custom_role_empty_then_valid(self, bootstrap, capsys):
        args = {}
        bootstrap._is_existing_role = MagicMock(return_value=True)
        with patch('builtins.input', side_effect=['3', '', 'GoodRole']):
            bootstrap._prompt_for_role(args)
        assert args['XRole'] == 'GoodRole'
        assert 'cannot be empty' in capsys.readouterr().out

    def test_custom_role_eof_exits(self, bootstrap):
        args = {}
        with patch('builtins.input', side_effect=['3', EOFError()]):
            with pytest.raises(SystemExit) as exc:
                bootstrap._prompt_for_role(args)
            assert exc.value.code == 1


# -- bootstrap() top-level orchestrator ---------------------------------

class TestBootstrapOrchestrator:
    def test_bootstrap_calls_each_step_in_order(self, bootstrap):
        bootstrap._add_glue_job_role = MagicMock()
        bootstrap._create_glue_log_groups = MagicMock()
        bootstrap._ensure_dynamodb_glue_connection = MagicMock()
        bootstrap._create_or_update_glue_job = MagicMock()
        bootstrap._upload_job_root_to_s3 = MagicMock()
        bootstrap.update_python_modules_in_s3 = MagicMock()
        bootstrap._upload_property_files_to_s3 = MagicMock()

        manager = MagicMock()
        manager.attach_mock(bootstrap._add_glue_job_role, 'add_role')
        manager.attach_mock(bootstrap._create_glue_log_groups, 'log_groups')
        manager.attach_mock(bootstrap._ensure_dynamodb_glue_connection, 'connection')
        manager.attach_mock(bootstrap._create_or_update_glue_job, 'create_job')
        manager.attach_mock(bootstrap._upload_job_root_to_s3, 'upload_root')
        manager.attach_mock(bootstrap.update_python_modules_in_s3, 'modules')
        manager.attach_mock(bootstrap._upload_property_files_to_s3, 'props')

        args = {'XRole': 'READ-ONLY'}
        bootstrap.bootstrap(args)

        # Connection MUST come before create_job — Glue requires the
        # connection to exist before a job can be created with it
        # attached via Connections={'Connections': [name]}.
        assert manager.mock_calls == [
            call.add_role(args),
            call.log_groups(),
            call.connection(),
            call.create_job(args),
            call.upload_root(),
            call.modules(),
            call.props(),
        ]


class TestEnsureDynamodbGlueConnection:
    """The DataFrame-based DynamoDB connector requires this connection."""

    def test_existing_connection_is_a_noop(self, bootstrap):
        # get_connection succeeds → no create_connection call.
        bootstrap.glue_client.get_connection.return_value = {'Connection': {}}

        bootstrap._ensure_dynamodb_glue_connection()

        from infrastructure.constants import GLUE_DYNAMODB_CONNECTION_NAME
        bootstrap.glue_client.get_connection.assert_called_once_with(
            Name=GLUE_DYNAMODB_CONNECTION_NAME
        )
        bootstrap.glue_client.create_connection.assert_not_called()

    def test_missing_connection_is_created(self, bootstrap):
        from infrastructure.constants import GLUE_DYNAMODB_CONNECTION_NAME

        class _ENF(Exception):
            pass

        bootstrap.glue_client.exceptions.EntityNotFoundException = _ENF
        bootstrap.glue_client.get_connection.side_effect = _ENF()

        bootstrap._ensure_dynamodb_glue_connection()

        bootstrap.glue_client.create_connection.assert_called_once()
        ci = bootstrap.glue_client.create_connection.call_args.kwargs['ConnectionInput']
        assert ci['Name'] == GLUE_DYNAMODB_CONNECTION_NAME
        assert ci['ConnectionType'] == 'DYNAMODB'
        assert ci['ConnectionProperties'] == {}
        # ValidateForComputeEnvironments must include SPARK so Glue loads
        # the DynamoDB DataFrame connector library.
        assert 'SPARK' in ci['ValidateForComputeEnvironments']

    def test_create_failure_exits(self, bootstrap):
        class _ENF(Exception):
            pass

        bootstrap.glue_client.exceptions.EntityNotFoundException = _ENF
        bootstrap.glue_client.get_connection.side_effect = _ENF()
        bootstrap.glue_client.create_connection.side_effect = RuntimeError('boom')

        with pytest.raises(SystemExit):
            bootstrap._ensure_dynamodb_glue_connection()


class TestCreateJobAttachesConnection:
    """Both job-creation paths must wire the DYNAMODB connection."""

    def test_update_path_includes_connections(self, bootstrap):
        from infrastructure.constants import GLUE_DYNAMODB_CONNECTION_NAME

        with patch('infrastructure.bootstrap.is_existing_glue_job', return_value=True):
            bootstrap._create_or_update_glue_job({})

        kwargs = bootstrap.glue_client.update_job.call_args.kwargs['JobUpdate']
        assert kwargs['Connections'] == {
            'Connections': [GLUE_DYNAMODB_CONNECTION_NAME]
        }

    def test_create_path_includes_connections(self, bootstrap):
        from infrastructure.constants import GLUE_DYNAMODB_CONNECTION_NAME

        with patch('infrastructure.bootstrap.is_existing_glue_job', return_value=False):
            bootstrap._create_or_update_glue_job({})

        kwargs = bootstrap.glue_client.create_job.call_args.kwargs
        assert kwargs['Connections'] == {
            'Connections': [GLUE_DYNAMODB_CONNECTION_NAME]
        }


# -- __init__ wiring ----------------------------------------------------

class TestBootstrapInit:
    def test_init_wires_clients_and_env(self):
        with patch('infrastructure.bootstrap.Clients') as MockClients:
            clients = MagicMock()
            clients.iam_client = MagicMock(name='iam')
            clients.s3_client = MagicMock(name='s3')
            clients.glue_client = MagicMock(name='glue')
            clients.logs_client = MagicMock(name='logs')
            MockClients.return_value = clients

            from infrastructure.bootstrap import BootstrapInfrastructure
            env = MagicMock(aws_region='eu-west-1', aws_account_id='9876')
            instance = BootstrapInfrastructure(env)

        MockClients.assert_called_once_with('eu-west-1')
        assert instance.aws_region == 'eu-west-1'
        assert instance.aws_account_id == '9876'
        assert instance.iam_client is clients.iam_client
        assert instance.s3_client is clients.s3_client
        assert instance.glue_client is clients.glue_client
        assert instance.logs_client is clients.logs_client
