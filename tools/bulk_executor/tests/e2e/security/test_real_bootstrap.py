"""Tier 2: real bootstrap with the documented IAM policy.

Truth oracle for the simulator. Two tests:

  1. Positive: full README policy attached → bootstrap+teardown succeed.
  2. Random-action negative: pick one documented action at random, REMOVE it
     from the policy, attach the reduced policy → bootstrap fails AND the
     error message names the removed action.

The random-negative test rotates across the action space over time. Any
single run only validates one action's denial-mode, but enough runs cover
every action — and gaps between simulator and reality (e.g. a typoed action
that simulator reports denied but service silently ignores) eventually
surface.

Skipped automatically when the simulator suite hasn't passed in this run —
no point burning real IAM resources on a known-broken policy.

Cost: ~$0 per test (bootstrap creates a Glue job + S3 bucket but doesn't
run any jobs). Runtime: ~3 min positive + ~30s negative.
"""
from __future__ import annotations

import os
import random
import subprocess
import sys
from pathlib import Path

import pytest

from tests.e2e.security.policy import all_actions, policy_without_action
from tests.e2e.security.temp_user import temp_iam_user_with_policy

REPO_ROOT = Path(__file__).resolve().parents[3]
BULK_CLI = REPO_ROOT / "bulk"
VENV_PYTHON = REPO_ROOT / ".venv" / "bin" / "python"


def _run_bulk(args: list[str], creds: dict[str, str], region: str) -> subprocess.CompletedProcess:
    """Invoke ./bulk with a clean env that has only the temp user's creds + region."""
    env = {
        # Keep PATH so subprocesses can find executables.
        "PATH": os.environ.get("PATH", ""),
        "HOME": os.environ.get("HOME", ""),
        # Override creds. Strip any AWS_PROFILE / AWS_SESSION_TOKEN so SDK
        # doesn't fall back to ambient credentials.
        "AWS_ACCESS_KEY_ID": creds["aws_access_key_id"],
        "AWS_SECRET_ACCESS_KEY": creds["aws_secret_access_key"],
        "AWS_DEFAULT_REGION": region,
    }
    return subprocess.run(
        [str(VENV_PYTHON), str(BULK_CLI), *args],
        cwd=str(REPO_ROOT),
        env=env,
        capture_output=True,
        text=True,
        timeout=600,
    )


@pytest.fixture(scope="module")
def simulator_passed(request) -> bool:
    """Did the simulator suite pass in this pytest invocation?"""
    # If pytest hasn't collected the simulator tests, we can't know — assume yes.
    # When invoked via 'make test-e2e-security' both files run; this fixture
    # checks the session-level pass/fail state.
    reporter = request.config.pluginmanager.getplugin("terminalreporter")
    if reporter is None:
        return True
    failed = reporter.stats.get("failed", [])
    sim_failed = [r for r in failed if "test_simulator" in r.nodeid]
    return not sim_failed


def test_documented_policy_can_actually_bootstrap(
    e2e_config, bootstrap_policy, simulator_passed
):
    """The README-documented policy must successfully bootstrap a real account.

    Creates a temp IAM user with the doc'd policy, runs `bulk bootstrap --XRole
    READ-ONLY`, asserts exit 0. On success, runs teardown to leave the account
    clean.

    Failure means the README is incorrect — bootstrap needs more than the doc
    claims. The error output names the missing permission(s) for the followup
    README fix.
    """
    if not simulator_passed:
        pytest.skip("simulator suite failed; skipping real-IAM test to avoid cost")

    with temp_iam_user_with_policy(bootstrap_policy) as creds:
        bootstrap = _run_bulk(
            ["bootstrap", "--XRole", "READ-ONLY"],
            creds=creds,
            region=e2e_config.aws_region,
        )

        # Always print so the developer sees what happened.
        sys.stdout.write(bootstrap.stdout)
        sys.stderr.write(bootstrap.stderr)

        if bootstrap.returncode != 0:
            pytest.fail(
                "Bootstrap failed with the documented IAM policy attached.\n"
                f"Exit code: {bootstrap.returncode}\n"
                f"This means the README policy is INSUFFICIENT — it claims a "
                f"permission set that doesn't actually let bootstrap complete.\n"
                f"Look in stderr above for AccessDenied messages naming the "
                f"missing action(s), then update README and re-run."
            )

        # Clean up so the next run starts fresh and we don't leave artifacts.
        teardown = _run_bulk(
            ["teardown"],
            creds=creds,
            region=e2e_config.aws_region,
        )
        sys.stdout.write(teardown.stdout)
        sys.stderr.write(teardown.stderr)
        assert teardown.returncode == 0, (
            "Bootstrap succeeded but teardown failed — account left in dirty "
            "state. Run 'make test-e2e-cleanup' or manually delete the "
            "bulk_dynamodb Glue job/connection and aws-glue-bulk-dynamodb-* bucket."
        )


# Actions where simulator-vs-real divergence is least informative — typically
# because they're only exercised at teardown (after bootstrap has already
# returned successfully) so removing them won't fail bootstrap. We exclude
# them from the random-negative pool so the test stays signal-rich.
_NEGATIVE_TEST_SKIP_ACTIONS = {
    "iam:DeleteRole",
    "iam:DeleteRolePolicy",
    "iam:DetachRolePolicy",
    "iam:ListRolePolicies",
    "glue:DeleteJob",
    "glue:DeleteConnection",
    "s3:DeleteBucket",
    "s3:DeleteObject",
}


def test_random_action_removal_breaks_bootstrap(
    e2e_config, bootstrap_policy, simulator_passed
):
    """Pick one bootstrap-relevant documented action at random, remove it,
    assert bootstrap fails AND the error names the action.

    This is the simulator's truth oracle, action-by-action. The random
    rotation means runs over time cover the whole bootstrap-active action
    space without exploding test runtime. (Teardown-only actions are
    excluded — removing them wouldn't fail bootstrap so they'd produce a
    false negative.)
    """
    if not simulator_passed:
        pytest.skip("simulator suite failed; skipping real-IAM test to avoid cost")

    candidates = [
        (sid, action) for sid, action in all_actions(bootstrap_policy)
        if action not in _NEGATIVE_TEST_SKIP_ACTIONS
    ]
    sid, removed_action = random.choice(candidates)
    print(f"\n[random-negative] this run removes {sid}:{removed_action}\n")

    reduced = policy_without_action(bootstrap_policy, removed_action)

    with temp_iam_user_with_policy(reduced) as creds:
        bootstrap = _run_bulk(
            ["bootstrap", "--XRole", "READ-ONLY"],
            creds=creds,
            region=e2e_config.aws_region,
        )
        sys.stdout.write(bootstrap.stdout)
        sys.stderr.write(bootstrap.stderr)

        # If bootstrap somehow succeeded without this action, the action is
        # decorative — README claims it as required but bootstrap doesn't
        # actually need it. Either drop from README or document why.
        assert bootstrap.returncode != 0, (
            f"Bootstrap unexpectedly SUCCEEDED with {removed_action} removed.\n"
            f"This action appears decorative — README claims it required but "
            f"bootstrap completed without it. Either remove from README or add "
            f"this action to _NEGATIVE_TEST_SKIP_ACTIONS with a 'why' comment."
        )

        # The error must name the missing action by ARN-formatted IAM name.
        # (AccessDenied messages from AWS include the action like 'iam:CreateRole'.)
        combined = bootstrap.stdout + bootstrap.stderr
        assert removed_action in combined, (
            f"Bootstrap failed (good) but the error didn't name {removed_action}.\n"
            f"This means the failure mode for this action is opaque — operator "
            f"won't know which permission to add. Make the failure self-naming "
            f"or document the failure pattern."
        )
