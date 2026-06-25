"""Tier 1: IAM policy simulator tests.

Validates the README-documented bootstrap policy against iam:SimulateCustomPolicy.
No IAM users created. Fast (~30s), cheap (~$0).

Two assertions per Sid:
  (a) full policy: every documented action is `allowed`.
  (b) policy minus this Sid: at least one of its actions becomes denied.

If (a) fails, the README claims a permission that wouldn't actually be granted —
either a typo (action name wrong) or a doc-vs-IAM-grammar mismatch (resource scope
too narrow).

If (b) fails, the Sid is decorative — removing it changes nothing — meaning the
documentation is over-specified or the simulator's evaluation is missing context.
"""
from __future__ import annotations

import pytest

from tests.e2e.security.actions import ActionProbe
from tests.e2e.security.policy import policy_without_statement
from tests.e2e.security.simulator import simulate


@pytest.fixture(scope="session")
def full_policy_decisions(bootstrap_policy, probes):
    """Cache the full-policy simulation across tests."""
    return simulate(bootstrap_policy, probes)


def test_full_policy_allows_every_documented_action(full_policy_decisions):
    """The README policy must `allowed` every action it documents.

    Failure here means the doc is wrong — either an action is misspelled,
    or the resource scope is too narrow for the operation.
    """
    denied = {k: v for k, v in full_policy_decisions.items() if v != "allowed"}
    assert not denied, (
        "README policy denies actions it documents as required:\n"
        + "\n".join(f"  - {k}: {v}" for k, v in sorted(denied.items()))
    )


@pytest.mark.parametrize(
    "sid",
    ["glueRoleAdmin", "passrole", "s3", "glue", "glueConnection", "logs"],
)
def test_removing_statement_denies_at_least_one_action(
    bootstrap_policy, probes, sid
):
    """Each Sid must be load-bearing.

    If we can remove a Sid and every action stays allowed, the Sid is decorative
    and the README is over-specified (or another statement subsumes it).
    """
    reduced = policy_without_statement(bootstrap_policy, sid)
    decisions = simulate(reduced, probes)

    sid_actions = [f"{p.sid}:{p.action}" for p in probes if p.sid == sid]
    still_allowed = [a for a in sid_actions if decisions.get(a) == "allowed"]

    assert len(still_allowed) < len(sid_actions), (
        f"Removing Sid '{sid}' didn't deny any of its actions:\n"
        + "\n".join(f"  - {a}: {decisions.get(a)}" for a in sid_actions)
        + f"\nThis Sid appears decorative — either the doc is over-specified "
        f"or another Sid is granting these actions."
    )
