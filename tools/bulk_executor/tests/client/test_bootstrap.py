"""Unit tests for BootstrapInfrastructure._create_or_update_glue_job.

Focused on the args -> default_arguments translation that controls which
flags get baked into the Glue job's DefaultArguments. Issue #85 covers
XRole/XRegion; XAccount is the same shape — a CLI flag that's defined
in argparse but never read at runtime, so persisting it to DefaultArguments
is just dead state.

Tests are written test-first against the current behavior so that they
serve as a regression harness for each exclusion.
"""

from unittest.mock import MagicMock, patch

import pytest


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


# -- XRole / XRegion regression harness ----------------------------------

class TestXRoleXRegionExclusion:
    """Tests for issue #85: XRole and XRegion are bootstrap-time concerns
    and must not be persisted to the Glue job's DefaultArguments.

    These tests describe the desired behavior. On unfixed code (current
    main) they FAIL — XRole and XRegion DO end up in default_arguments.
    On fixed code they PASS.
    """

    def test_xrole_excluded_from_default_arguments(self, bootstrap):
        result = _run(bootstrap, {'XRole': 'READ-ONLY'})
        assert '--XRole' not in result, (
            "XRole is a bootstrap-time concern and must not be persisted "
            "to the Glue job's DefaultArguments (issue #85)."
        )

    def test_xregion_excluded_from_default_arguments(self, bootstrap):
        result = _run(bootstrap, {'XRegion': 'us-west-2'})
        assert '--XRegion' not in result, (
            "XRegion is a bootstrap-time concern and must not be persisted "
            "to the Glue job's DefaultArguments (issue #85)."
        )

    def test_xrole_and_xregion_both_excluded(self, bootstrap):
        result = _run(bootstrap, {'XRole': 'READ-WRITE', 'XRegion': 'eu-west-1'})
        assert '--XRole' not in result
        assert '--XRegion' not in result

    def test_xrole_excluded_but_other_x_args_retained(self, bootstrap):
        # Critical: the fix must surgical — only XRole/XRegion are filtered,
        # everything else with X-prefix keeps passing through.
        result = _run(bootstrap, {
            'XRole': 'READ-ONLY',
            'XNumberOfWorkers': 10,
            'XWorkerType': 'G.1X',
        })
        assert '--XRole' not in result
        assert result.get('--XNumberOfWorkers') == '10'
        assert result.get('--XWorkerType') == 'G.1X'

    def test_xregion_excluded_but_other_x_args_retained(self, bootstrap):
        result = _run(bootstrap, {
            'XRegion': 'us-east-1',
            'XTimeout': 120,
            'XRetries': 3,
        })
        assert '--XRegion' not in result
        assert result.get('--XTimeout') == '120'
        assert result.get('--XRetries') == '3'


# -- XAccount regression harness -----------------------------------------

class TestXAccountExclusion:
    """XAccount is defined in argparse (utils/__init__.py) but is never
    read anywhere in the codebase. The actual aws_account_id used at
    runtime comes from sts.get_caller_identity(), not from --XAccount.

    Persisting --XAccount in the Glue job's DefaultArguments is dead
    state — same shape as XRole and XRegion before issue #85. This
    class describes the desired exclusion behavior.

    On unfixed code these tests FAIL (XAccount currently passes through).
    On fixed code they PASS.
    """

    def test_xaccount_excluded_from_default_arguments(self, bootstrap):
        result = _run(bootstrap, {'XAccount': '123456789012'})
        assert '--XAccount' not in result, (
            "XAccount is defined in argparse but never read at runtime; "
            "the runtime account id comes from sts.get_caller_identity(). "
            "Don't persist it to DefaultArguments."
        )

    def test_xaccount_excluded_but_other_x_args_retained(self, bootstrap):
        # Same surgical-fix property as the XRole/XRegion tests.
        result = _run(bootstrap, {
            'XAccount': '123456789012',
            'XNumberOfWorkers': 10,
            'XWorkerType': 'G.1X',
        })
        assert '--XAccount' not in result
        assert result.get('--XNumberOfWorkers') == '10'
        assert result.get('--XWorkerType') == 'G.1X'

    def test_all_bootstrap_time_args_excluded_together(self, bootstrap):
        # Combined harness: all three bootstrap-time args present, all
        # three excluded, all other X args retained.
        result = _run(bootstrap, {
            'XRole': 'READ-WRITE',
            'XRegion': 'eu-west-1',
            'XAccount': '123456789012',
            'XNumberOfWorkers': 5,
            'XWorkerType': 'G.2X',
        })
        assert '--XRole' not in result
        assert '--XRegion' not in result
        assert '--XAccount' not in result
        assert result.get('--XNumberOfWorkers') == '5'
        assert result.get('--XWorkerType') == 'G.2X'


# -- Role persistence remains observable ---------------------------------

class TestRoleNameStillPersisted:
    """Removing --XRole from DefaultArguments must NOT remove the user's
    ability to verify which role the Glue job is using. The canonical
    'what role is this job running as?' key has always been
    --glue-job-role-name, not --XRole.

    These tests lock in that guarantee for all three role-input cases
    (READ-ONLY, READ-WRITE, custom-role). teardown.py already reads
    --glue-job-role-name as its source of truth.
    """

    def test_read_only_role_persisted_under_canonical_key(self, bootstrap):
        result = _run(bootstrap, {'XRole': 'READ-ONLY'})
        # The managed READ-ONLY name is the canonical AWS Glue Service Role
        # for read-only DynamoDB access in the configured region.
        assert result.get('--glue-job-role-name') == \
            'AWSGlueServiceRoleBulkDynamoDB-DdbReadOnly-us-east-1'

    def test_read_write_role_persisted_under_canonical_key(self, bootstrap):
        result = _run(bootstrap, {'XRole': 'READ-WRITE'})
        assert result.get('--glue-job-role-name') == \
            'AWSGlueServiceRoleBulkDynamoDB-DdbReadWrite-us-east-1'

    def test_custom_role_persisted_under_canonical_key(self, bootstrap):
        # Custom roles are passed through verbatim. Mock the existence
        # check so the bootstrap doesn't try to look up a fake IAM role.
        bootstrap._is_existing_role = MagicMock(return_value=True)
        result = _run(bootstrap, {'XRole': 'MyCustomGlueRole'})
        assert result.get('--glue-job-role-name') == 'MyCustomGlueRole'

    def test_role_arn_in_update_call_uses_canonical_name(self, bootstrap):
        # The Role ARN passed to glue_client.update_job is built from
        # the canonical role name, not from XRole. Verifying this means
        # users can also inspect the live Glue job's Role attribute (via
        # AWS console / get_job) to confirm correctness.
        bootstrap._is_existing_role = MagicMock(return_value=True)
        _run(bootstrap, {'XRole': 'MyCustomGlueRole'})
        call_kwargs = bootstrap.glue_client.update_job.call_args.kwargs
        assert call_kwargs['JobUpdate']['Role'] == \
            'arn:aws:iam::123456789012:role/MyCustomGlueRole'
