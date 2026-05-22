"""Unit tests for BootstrapInfrastructure._create_or_update_glue_job.

Focused on the args -> default_arguments translation that controls which
flags get baked into the Glue job's DefaultArguments. This is the surface
issue #85 covers: XRole/XRegion are useful during bootstrap for role
selection but should not be persisted on the long-running Glue job.

Tests are written test-first against the current behavior so that they
serve as a regression harness for the XRole/XRegion fix.
"""

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

# bootstrap.py imports `from __version__ import __version__ as VERSION`,
# which lives at tools/bulk_executor/__version__.py — two parents up from
# this file's client/src grandparent. Add the bulk_executor root to the path
# before the import in conftest does its work.
_BULK_EXECUTOR_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_BULK_EXECUTOR_ROOT))


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
