"""Unit tests for client/src/infrastructure/teardown.py.

Covers TeardownInfrastructure across its three teardown phases:

- _delete_glue_job_bucket_and_server_objects: list_objects_v2 -> batched
  delete_objects -> delete_bucket; pagination at 1000-key boundary;
  NoSuchBucket short-circuit; BucketNotEmpty soft-warn; missing bucket
  name early return; per-object delete error fail.
- _delete_glue_job_role: pulls the role name out of the persisted Glue
  job DefaultArguments, refuses to delete custom roles, otherwise calls
  detach_role_policy / delete_role_policy / delete_role with proper
  exit-on-error behavior. _has_default_role_name region-suffix check.
- _delete_glue_job: existence check -> delete_job; logs both branches;
  exits on unexpected exceptions.
- _get_glue_job_role_name / _get_glue_job_details / _get_glue_job_bucket_name:
  EntityNotFoundException short-circuits, other ClientError exits, happy
  path returns the persisted argument.

All boto3 clients are MagicMocks. The teardown module imports its
verifier dependency as `is_existing_glue_job`, so it's patched at the
teardown module's namespace where it's looked up at call time.
"""

from unittest.mock import MagicMock, patch

import pytest
from botocore.exceptions import ClientError


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _entity_not_found_error(op='GetJob'):
    return ClientError(
        {'Error': {'Code': 'EntityNotFoundException', 'Message': 'missing'}},
        op,
    )


def _client_error(code, op='Op'):
    return ClientError({'Error': {'Code': code, 'Message': code}}, op)


def _make_teardown(region='us-east-1'):
    """Construct TeardownInfrastructure with all AWS clients mocked.

    Returns the instance with iam_client / s3_client / glue_client all
    swapped for MagicMocks. The s3 / iam exception classes are also
    set up as real exception subclasses so the teardown code's
    `except self.s3_client.exceptions.NoSuchBucket` branches are
    catchable.
    """
    with patch('infrastructure.teardown.Clients') as MockClients:
        clients = MagicMock()
        clients.iam_client = MagicMock()
        clients.s3_client = MagicMock()
        clients.glue_client = MagicMock()
        MockClients.return_value = clients

        # Wire real exception subclasses onto the mock client `.exceptions`
        # namespaces so `except client.exceptions.NoSuchBucket` works.
        class _NoSuchBucket(Exception):
            pass

        class _S3ClientError(Exception):
            def __init__(self, response):
                self.response = response
                super().__init__(str(response))

        class _NoSuchEntityException(Exception):
            pass

        clients.s3_client.exceptions.NoSuchBucket = _NoSuchBucket
        clients.s3_client.exceptions.ClientError = _S3ClientError
        clients.iam_client.exceptions.NoSuchEntityException = _NoSuchEntityException

        from infrastructure.teardown import TeardownInfrastructure
        env = MagicMock(aws_region=region)
        instance = TeardownInfrastructure(env)

    # Attach the exception classes to the instance for tests to raise.
    instance._NoSuchBucket = _NoSuchBucket
    instance._S3ClientError = _S3ClientError
    instance._NoSuchEntityException = _NoSuchEntityException
    return instance


# ---------------------------------------------------------------------------
# Construction
# ---------------------------------------------------------------------------

class TestConstruction:
    """The constructor wires the regional AWS clients onto self."""

    def test_init_records_region_and_clients(self):
        td = _make_teardown(region='eu-west-2')
        assert td.aws_region == 'eu-west-2'
        # The clients should be set (MagicMock instances). Just verify they
        # aren't None — the contract is "assigned during __init__".
        assert td.iam_client is not None
        assert td.s3_client is not None
        assert td.glue_client is not None


# ---------------------------------------------------------------------------
# _get_glue_job_role_name / _get_glue_job_details / _get_glue_job_bucket_name
# ---------------------------------------------------------------------------

class TestGetGlueJobRoleName:
    """The persisted role name is read out of the Glue job DefaultArguments."""

    def test_returns_persisted_role_name(self):
        td = _make_teardown()
        td.glue_client.get_job.return_value = {
            'Job': {
                'DefaultArguments': {
                    '--glue-job-role-name': 'AWSGlueServiceRoleBulkDynamoDB-DdbReadWrite-us-east-1'
                }
            }
        }
        result = td._get_glue_job_role_name()
        assert result == 'AWSGlueServiceRoleBulkDynamoDB-DdbReadWrite-us-east-1'

    def test_exits_when_glue_job_does_not_exist(self):
        td = _make_teardown()
        td.glue_client.get_job.side_effect = _entity_not_found_error()
        with pytest.raises(SystemExit) as excinfo:
            td._get_glue_job_role_name()
        assert excinfo.value.code == 1

    def test_exits_on_unexpected_client_error(self):
        td = _make_teardown()
        td.glue_client.get_job.side_effect = _client_error('AccessDenied')
        with pytest.raises(SystemExit) as excinfo:
            td._get_glue_job_role_name()
        assert excinfo.value.code == 1


class TestGetGlueJobDetails:
    """The internal _get_glue_job_details helper."""

    def test_returns_response_on_success(self):
        td = _make_teardown()
        td.glue_client.get_job.return_value = {'Job': {'Name': 'x'}}
        assert td._get_glue_job_details() == {'Job': {'Name': 'x'}}

    def test_returns_none_on_entity_not_found(self):
        td = _make_teardown()
        td.glue_client.get_job.side_effect = _entity_not_found_error()
        assert td._get_glue_job_details() is None

    def test_exits_on_other_client_error(self):
        td = _make_teardown()
        td.glue_client.get_job.side_effect = _client_error('Throttling')
        with pytest.raises(SystemExit):
            td._get_glue_job_details()


class TestGetGlueJobBucketName:
    """The persisted S3 bucket name is read out of the Glue job DefaultArguments."""

    def test_returns_persisted_bucket_name(self):
        td = _make_teardown()
        td.glue_client.get_job.return_value = {
            'Job': {'DefaultArguments': {'--s3-bucket-name': 'my-bucket'}}
        }
        assert td._get_glue_job_bucket_name() == 'my-bucket'

    def test_returns_none_when_bucket_arg_missing(self):
        td = _make_teardown()
        td.glue_client.get_job.return_value = {'Job': {'DefaultArguments': {}}}
        assert td._get_glue_job_bucket_name() is None

    def test_returns_none_when_glue_job_missing(self):
        td = _make_teardown()
        td.glue_client.get_job.side_effect = _entity_not_found_error()
        assert td._get_glue_job_bucket_name() is None


# ---------------------------------------------------------------------------
# _has_default_role_name
# ---------------------------------------------------------------------------

class TestHasDefaultRoleName:
    """Region-suffixed default role names get matched, custom names don't."""

    def test_recognises_read_only_default_role(self):
        td = _make_teardown(region='us-east-1')
        assert td._has_default_role_name(
            'AWSGlueServiceRoleBulkDynamoDB-DdbReadOnly-us-east-1'
        ) is True

    def test_recognises_read_write_default_role(self):
        td = _make_teardown(region='us-east-1')
        assert td._has_default_role_name(
            'AWSGlueServiceRoleBulkDynamoDB-DdbReadWrite-us-east-1'
        ) is True

    def test_rejects_custom_role(self):
        td = _make_teardown(region='us-east-1')
        assert td._has_default_role_name('MyCustomRole') is False

    def test_region_mismatch_treated_as_custom(self):
        # A role tagged with a different region is not one we provisioned,
        # so we must not delete it.
        td = _make_teardown(region='us-east-1')
        assert td._has_default_role_name(
            'AWSGlueServiceRoleBulkDynamoDB-DdbReadWrite-us-west-2'
        ) is False


# ---------------------------------------------------------------------------
# _delete_glue_job
# ---------------------------------------------------------------------------

class TestDeleteGlueJob:
    """delete_glue_job dispatches based on the verifier check."""

    def test_deletes_when_job_exists(self):
        td = _make_teardown()
        with patch('infrastructure.teardown.is_existing_glue_job', return_value=True):
            td._delete_glue_job()
        td.glue_client.delete_job.assert_called_once_with(JobName='bulk_dynamodb')

    def test_skips_delete_when_job_missing(self):
        td = _make_teardown()
        with patch('infrastructure.teardown.is_existing_glue_job', return_value=False):
            td._delete_glue_job()
        td.glue_client.delete_job.assert_not_called()

    def test_exits_when_delete_raises(self):
        td = _make_teardown()
        td.glue_client.delete_job.side_effect = RuntimeError('boom')
        with patch('infrastructure.teardown.is_existing_glue_job', return_value=True):
            with pytest.raises(SystemExit) as excinfo:
                td._delete_glue_job()
        assert excinfo.value.code == 1


# ---------------------------------------------------------------------------
# _delete_inline_policies
# ---------------------------------------------------------------------------

class TestDeleteInlinePolicies:
    """Inline policies are listed then deleted one-by-one."""

    def test_deletes_each_inline_policy(self):
        td = _make_teardown()
        td.iam_client.list_role_policies.return_value = {
            'PolicyNames': ['policy-a', 'policy-b']
        }
        td._delete_inline_policies('role-x')
        assert td.iam_client.delete_role_policy.call_count == 2
        td.iam_client.delete_role_policy.assert_any_call(
            RoleName='role-x', PolicyName='policy-a'
        )
        td.iam_client.delete_role_policy.assert_any_call(
            RoleName='role-x', PolicyName='policy-b'
        )

    def test_returns_silently_when_role_missing(self):
        td = _make_teardown()
        td.iam_client.list_role_policies.side_effect = (
            td._NoSuchEntityException()
        )
        td._delete_inline_policies('role-x')
        td.iam_client.delete_role_policy.assert_not_called()

    def test_exits_on_unexpected_list_error(self):
        td = _make_teardown()
        td.iam_client.list_role_policies.side_effect = RuntimeError('oops')
        with pytest.raises(SystemExit) as excinfo:
            td._delete_inline_policies('role-x')
        assert excinfo.value.code == 1

    def test_exits_when_individual_delete_fails(self):
        td = _make_teardown()
        td.iam_client.list_role_policies.return_value = {
            'PolicyNames': ['policy-a']
        }
        td.iam_client.delete_role_policy.side_effect = RuntimeError('nope')
        with pytest.raises(SystemExit) as excinfo:
            td._delete_inline_policies('role-x')
        assert excinfo.value.code == 1


# ---------------------------------------------------------------------------
# _detach_managed_policies
# ---------------------------------------------------------------------------

class TestDetachManagedPolicies:
    """Managed policies are listed then detached one-by-one."""

    def test_detaches_each_managed_policy(self):
        td = _make_teardown()
        td.iam_client.list_attached_role_policies.return_value = {
            'AttachedPolicies': [
                {'PolicyName': 'A', 'PolicyArn': 'arn:aws:iam::aws:policy/A'},
                {'PolicyName': 'B', 'PolicyArn': 'arn:aws:iam::aws:policy/B'},
            ]
        }
        td._detach_managed_policies('role-x')
        assert td.iam_client.detach_role_policy.call_count == 2
        td.iam_client.detach_role_policy.assert_any_call(
            RoleName='role-x', PolicyArn='arn:aws:iam::aws:policy/A'
        )

    def test_returns_silently_when_role_missing(self):
        td = _make_teardown()
        td.iam_client.list_attached_role_policies.side_effect = (
            td._NoSuchEntityException()
        )
        td._detach_managed_policies('role-x')
        td.iam_client.detach_role_policy.assert_not_called()

    def test_exits_on_unexpected_list_error(self):
        td = _make_teardown()
        td.iam_client.list_attached_role_policies.side_effect = RuntimeError(
            'oops'
        )
        with pytest.raises(SystemExit) as excinfo:
            td._detach_managed_policies('role-x')
        assert excinfo.value.code == 1

    def test_exits_when_individual_detach_fails(self):
        td = _make_teardown()
        td.iam_client.list_attached_role_policies.return_value = {
            'AttachedPolicies': [
                {'PolicyName': 'A', 'PolicyArn': 'arn:aws:iam::aws:policy/A'},
            ]
        }
        td.iam_client.detach_role_policy.side_effect = RuntimeError('nope')
        with pytest.raises(SystemExit):
            td._detach_managed_policies('role-x')


# ---------------------------------------------------------------------------
# _delete_glue_job_role
# ---------------------------------------------------------------------------

class TestDeleteGlueJobRole:
    """delete_glue_job_role guards on default-role detection then deletes."""

    def test_skips_custom_roles(self):
        td = _make_teardown()
        td._get_glue_job_role_name = MagicMock(return_value='MyCustomRole')
        td._detach_managed_policies = MagicMock()
        td._delete_inline_policies = MagicMock()
        td._delete_glue_job_role()
        td.iam_client.delete_role.assert_not_called()
        td._detach_managed_policies.assert_not_called()
        td._delete_inline_policies.assert_not_called()

    def test_deletes_default_role_with_full_cleanup(self):
        td = _make_teardown(region='us-east-1')
        role_name = 'AWSGlueServiceRoleBulkDynamoDB-DdbReadWrite-us-east-1'
        td._get_glue_job_role_name = MagicMock(return_value=role_name)
        td._detach_managed_policies = MagicMock()
        td._delete_inline_policies = MagicMock()
        td._delete_glue_job_role()
        td._detach_managed_policies.assert_called_once_with(role_name)
        td._delete_inline_policies.assert_called_once_with(role_name)
        td.iam_client.delete_role.assert_called_once_with(RoleName=role_name)

    def test_exits_when_delete_role_raises(self):
        td = _make_teardown(region='us-east-1')
        role_name = 'AWSGlueServiceRoleBulkDynamoDB-DdbReadOnly-us-east-1'
        td._get_glue_job_role_name = MagicMock(return_value=role_name)
        td._detach_managed_policies = MagicMock()
        td._delete_inline_policies = MagicMock()
        td.iam_client.delete_role.side_effect = RuntimeError('boom')
        with pytest.raises(SystemExit) as excinfo:
            td._delete_glue_job_role()
        assert excinfo.value.code == 1


# ---------------------------------------------------------------------------
# _delete_glue_job_bucket_and_server_objects
# ---------------------------------------------------------------------------

class TestDeleteGlueJobBucketAndServerObjects:
    """The S3 phase: list, batch-delete server/ keys, then drop the bucket."""

    def test_returns_when_bucket_name_missing(self):
        td = _make_teardown()
        td._get_glue_job_bucket_name = MagicMock(return_value=None)
        td._delete_glue_job_bucket_and_server_objects()
        td.s3_client.list_objects_v2.assert_not_called()
        td.s3_client.delete_bucket.assert_not_called()

    def test_returns_when_bucket_does_not_exist(self):
        td = _make_teardown()
        td._get_glue_job_bucket_name = MagicMock(return_value='b')
        td.s3_client.list_objects_v2.side_effect = td._NoSuchBucket()
        td._delete_glue_job_bucket_and_server_objects()
        td.s3_client.delete_objects.assert_not_called()
        td.s3_client.delete_bucket.assert_not_called()

    def test_exits_when_list_objects_fails(self):
        td = _make_teardown()
        td._get_glue_job_bucket_name = MagicMock(return_value='b')
        td.s3_client.list_objects_v2.side_effect = RuntimeError('boom')
        with pytest.raises(SystemExit) as excinfo:
            td._delete_glue_job_bucket_and_server_objects()
        assert excinfo.value.code == 1

    def test_deletes_objects_and_bucket_happy_path(self):
        td = _make_teardown()
        td._get_glue_job_bucket_name = MagicMock(return_value='b')
        td.s3_client.list_objects_v2.return_value = {
            'Contents': [{'Key': 'server/a'}, {'Key': 'server/b'}]
        }
        td.s3_client.delete_objects.return_value = {
            'Deleted': [{'Key': 'server/a'}, {'Key': 'server/b'}]
        }
        td._delete_glue_job_bucket_and_server_objects()
        td.s3_client.delete_objects.assert_called_once_with(
            Bucket='b',
            Delete={'Objects': [{'Key': 'server/a'}, {'Key': 'server/b'}]},
        )
        td.s3_client.delete_bucket.assert_called_once_with(Bucket='b')

    def test_no_contents_skips_delete_objects_but_still_deletes_bucket(self):
        td = _make_teardown()
        td._get_glue_job_bucket_name = MagicMock(return_value='b')
        td.s3_client.list_objects_v2.return_value = {}
        td._delete_glue_job_bucket_and_server_objects()
        td.s3_client.delete_objects.assert_not_called()
        td.s3_client.delete_bucket.assert_called_once_with(Bucket='b')

    def test_batches_keys_in_chunks_of_1000(self):
        # 2500 keys -> 3 batches: 1000 + 1000 + 500.
        td = _make_teardown()
        td._get_glue_job_bucket_name = MagicMock(return_value='b')
        keys = [{'Key': f'server/{i}'} for i in range(2500)]
        td.s3_client.list_objects_v2.return_value = {'Contents': keys}
        td.s3_client.delete_objects.return_value = {'Deleted': []}
        td._delete_glue_job_bucket_and_server_objects()
        assert td.s3_client.delete_objects.call_count == 3
        sizes = [
            len(c.kwargs['Delete']['Objects'])
            for c in td.s3_client.delete_objects.call_args_list
        ]
        assert sizes == [1000, 1000, 500]

    def test_exits_when_delete_objects_returns_errors(self):
        td = _make_teardown()
        td._get_glue_job_bucket_name = MagicMock(return_value='b')
        td.s3_client.list_objects_v2.return_value = {
            'Contents': [{'Key': 'server/a'}]
        }
        td.s3_client.delete_objects.return_value = {
            'Errors': [{'Key': 'server/a', 'Message': 'x', 'Code': 'AccessDenied'}]
        }
        with pytest.raises(SystemExit) as excinfo:
            td._delete_glue_job_bucket_and_server_objects()
        assert excinfo.value.code == 1

    def test_warns_and_continues_when_bucket_not_empty(self):
        td = _make_teardown()
        td._get_glue_job_bucket_name = MagicMock(return_value='b')
        td.s3_client.list_objects_v2.return_value = {}
        td.s3_client.delete_bucket.side_effect = td._S3ClientError(
            {'Error': {'Code': 'BucketNotEmpty', 'Message': 'not empty'}}
        )
        # No SystemExit; teardown must continue even though delete failed.
        td._delete_glue_job_bucket_and_server_objects()

    def test_exits_when_delete_bucket_fails_unexpectedly(self):
        td = _make_teardown()
        td._get_glue_job_bucket_name = MagicMock(return_value='b')
        td.s3_client.list_objects_v2.return_value = {}
        td.s3_client.delete_bucket.side_effect = td._S3ClientError(
            {'Error': {'Code': 'AccessDenied', 'Message': 'no'}}
        )
        with pytest.raises(SystemExit) as excinfo:
            td._delete_glue_job_bucket_and_server_objects()
        assert excinfo.value.code == 1

    def test_returns_when_delete_bucket_says_no_such_bucket(self):
        td = _make_teardown()
        td._get_glue_job_bucket_name = MagicMock(return_value='b')
        td.s3_client.list_objects_v2.return_value = {}
        td.s3_client.delete_bucket.side_effect = td._NoSuchBucket()
        # Should swallow and return.
        td._delete_glue_job_bucket_and_server_objects()


# ---------------------------------------------------------------------------
# teardown() (the orchestrator)
# ---------------------------------------------------------------------------

class TestTeardown:
    """The public teardown() runs the three phases in the locked order."""

    def test_runs_phases_in_order(self):
        td = _make_teardown()
        td._delete_glue_job_bucket_and_server_objects = MagicMock()
        td._delete_glue_job_role = MagicMock()
        td._delete_glue_job = MagicMock()

        # Use a parent mock to record relative call ordering across the
        # three phase methods.
        parent = MagicMock()
        parent.attach_mock(td._delete_glue_job_bucket_and_server_objects, 'bucket')
        parent.attach_mock(td._delete_glue_job_role, 'role')
        parent.attach_mock(td._delete_glue_job, 'job')

        td.teardown()

        names = [c[0] for c in parent.mock_calls]
        assert names == ['bucket', 'role', 'job']
