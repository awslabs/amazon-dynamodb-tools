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

import boto3
import pytest

from infrastructure.verifier import is_existing_glue_job
from tests.e2e.helpers.assertions import assert_builtin_role_shape
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
    sim_failed = [r for r in failed if "test_iam_policy_simulated" in r.nodeid]
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

        # Exit 0 is necessary but NOT sufficient (see AGENTS.md invariant #1:
        # assert the resulting state, not just the exit code). Prove the
        # built-in role was actually created with the fresh-bootstrap shape
        # (exists, glue-only trust policy, required managed policies attached)
        # before we tear it down.
        assert_builtin_role_shape(e2e_config.aws_region, "READ-ONLY")

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


# Actions the random-negative test cannot meaningfully exercise, mapped to WHY.
#
# A dict rather than a set on purpose: the reason is data, so
# test_skip_list_is_not_stale can check every entry is still a documented action
# and still carries an explanation. A set with comments rots silently -- an entry
# can outlive the action it excuses, and then nothing negatively tests it.
#
# Two shapes appear here. Most are UNREACHABLE from this test's single flow
# (first bootstrap, --XRole READ-ONLY, no teardown), so removing them cannot fail
# it. One is deliberately NON-FATAL, so removing it must not fail bootstrap --
# that direction is asserted positively in
# test_courtesy_permission_denial_does_not_break_bootstrap.
#
# When you change a call's failure mode, revisit this list. Making an action
# non-fatal without adding it here turns the next random draw into a false
# "decorative" failure; making one fatal again without removing it here leaves a
# silent hole. See ai_lint/rules/permission_failure_handling.md.
_NEGATIVE_TEST_SKIP_ACTIONS = {
    "iam:DeleteRole": "teardown-only; bootstrap returns before it is reached",
    "iam:DeleteRolePolicy": "teardown-only; bootstrap returns before it is reached",
    "iam:DetachRolePolicy": "teardown-only; bootstrap returns before it is reached",
    "iam:ListRolePolicies": "teardown-only; bootstrap returns before it is reached",
    "iam:ListAttachedRolePolicies": (
        "same shape as iam:ListRolePolicies above: called only from teardown and "
        "from role_validator's custom-role check (where a denial degrades). "
        "Bootstrap's own flow never calls it, so removing it cannot fail this "
        "test. Found the hard way -- it was the one sibling the original list "
        "missed, and a random draw surfaced it."
    ),
    "glue:DeleteJob": "teardown-only; bootstrap returns before it is reached",
    "glue:DeleteConnection": "teardown-only; bootstrap returns before it is reached",
    "s3:DeleteBucket": "teardown-only; bootstrap returns before it is reached",
    "s3:DeleteObject": "teardown-only; bootstrap returns before it is reached",
    "glue:UpdateJob": (
        "re-bootstrap only: bootstrap branches existing job -> update_job, no job "
        "-> create_job. This test always starts with no job, so it takes "
        "create_job and removing UpdateJob can never break it. Still genuinely "
        "required on the re-bootstrap path."
    ),
    "iam:UpdateAssumeRolePolicy": (
        "existing-role only: reached in _add_glue_job_role's "
        "EntityAlreadyExistsException handler, where create_role was skipped so the "
        "trust policy is re-applied separately. Teardown deletes the role, so each "
        "run starts roleless and takes create_role, which sets the trust policy "
        "itself. Required whenever the role already exists -- which since #326 is "
        "every re-bootstrap, not just after a __version__ bump."
    ),
    "iam:GetRole": (
        "custom-role only: called from _is_existing_role, whose callers both sit "
        "behind 'a custom --XRole name was provided'. This test uses --XRole "
        "READ-ONLY, a magic value, so the branch never runs. Required for the "
        "custom-role path, where a denial is fatal."
    ),
    "logs:DescribeLogGroups": (
        "deliberately NON-FATAL since #301: used only by the retention read, whose "
        "purpose is to avoid clobbering a retention the account owner chose, so a "
        "denial warns and bootstrap continues. Removing it therefore cannot fail "
        "bootstrap. Still genuinely used -- without it we can't manage retention. "
        "The must-not-fail direction is asserted in "
        "test_courtesy_permission_denial_does_not_break_bootstrap."
    ),
}


def test_skip_list_is_not_stale(bootstrap_policy):
    """Every skip entry must name a documented action and carry a reason.

    Cheap guard on the skip-list's one real weakness: it can rot silently. An
    entry that outlives the action it excuses (renamed, or dropped from the
    policy) leaves a permanently-skipped ghost, and nobody notices because
    skipping is invisible. Offline -- no AWS calls.

    What this deliberately does NOT catch: an action that became fatal again
    while its entry stayed. That needs judgment, so it lives in
    ai_lint/rules/permission_failure_handling.md instead of here.
    """
    documented = {action for _, action in all_actions(bootstrap_policy)}

    unknown = set(_NEGATIVE_TEST_SKIP_ACTIONS) - documented
    assert not unknown, (
        "skip-list excuses actions the README policy no longer documents:\n"
        + "\n".join(f"  - {a}" for a in sorted(unknown))
        + "\nRemove the stale entries -- they are skipping nothing."
    )

    empty = [a for a, why in _NEGATIVE_TEST_SKIP_ACTIONS.items() if not (why or "").strip()]
    assert not empty, (
        "skip-list entries must explain why the action is unreachable or "
        f"deliberately non-fatal: {sorted(empty)}"
    )


def test_courtesy_permission_denial_does_not_break_bootstrap(
    e2e_config, bootstrap_policy, simulator_passed
):
    """A deliberately non-fatal permission must WARN and still bootstrap (#301).

    The inverse of the random-negative test, and the only live coverage of the
    degrade behavior #301/#306 introduced. logs:DescribeLogGroups is used only by
    the retention read -- whose whole purpose is to avoid clobbering a retention
    the account owner chose -- so denying it must not stop anything. Before #301
    this exact input produced an AccessDenied traceback and exit(1), which is how
    #294 was found.

    Costs one temp IAM user and one bootstrap (no Glue job run, so ~$0).
    """
    if not simulator_passed:
        pytest.skip("simulator suite failed; skipping real-IAM test to avoid cost")

    action = "logs:DescribeLogGroups"
    assert action in _NEGATIVE_TEST_SKIP_ACTIONS, (
        f"{action} must be skip-listed as non-fatal for this test to be coherent"
    )

    reduced = policy_without_action(bootstrap_policy, action)
    with temp_iam_user_with_policy(reduced) as creds:
        try:
            bootstrap = _run_bulk(
                ["bootstrap", "--XRole", "READ-WRITE"],
                creds=creds,
                region=e2e_config.aws_region,
            )
            sys.stdout.write(bootstrap.stdout)
            sys.stderr.write(bootstrap.stderr)
            combined = bootstrap.stdout + bootstrap.stderr
        finally:
            # This test SUCCEEDS at bootstrapping, so it leaves a Glue job
            # behind -- which flips test_random_action_removal_breaks_bootstrap
            # onto the update_job path and makes glue:CreateJob unreachable
            # there. Restore "no job" so ordering can't corrupt a later test.
            _run_bulk(["teardown"], creds=creds, region=e2e_config.aws_region)

        assert bootstrap.returncode == 0, (
            f"Bootstrap FAILED with only {action} removed. That permission is "
            f"deliberately non-fatal (#301) -- a denial must warn and continue, "
            f"not stop the bootstrap. Either the degrade path regressed, or the "
            f"call moved somewhere it can no longer be caught."
        )
        assert "Could not manage the retention policy" in combined, (
            "the denial must be reported, not silently swallowed -- an operator "
            "needs to know retention was left unmanaged"
        )
        assert "Traceback" not in combined, (
            "a handled denial must not surface as a traceback"
        )


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

    # Enforce the precondition the skip-list depends on. glue:CreateJob and
    # glue:UpdateJob are mutually exclusive -- bootstrap calls one or the other
    # depending on whether a job already exists -- so exactly one of them is
    # reachable here, and _NEGATIVE_TEST_SKIP_ACTIONS excuses UpdateJob on the
    # assumption that it's CreateJob. Nothing enforced that, so a preceding test
    # that left a job behind silently inverted it and CreateJob was reported
    # "decorative". Fail loudly instead of producing a confusing false alarm.
    assert not is_existing_glue_job(boto3.client("glue", region_name=e2e_config.aws_region)), (
        "A Glue job already exists, so this test would take the update_job path "
        "and glue:CreateJob would be unreachable -- inverting the skip-list's "
        "assumption. Something ran before this test and left a job behind; it "
        "must tear down. See _NEGATIVE_TEST_SKIP_ACTIONS."
    )

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
