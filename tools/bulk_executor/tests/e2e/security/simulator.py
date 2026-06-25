"""Wrapper around iam:SimulateCustomPolicy.

Why custom (not principal): we want to simulate a *hypothetical* policy
without first creating a user/role. SimulateCustomPolicy takes the raw
policy JSON as a string, no IAM resources required.
"""
from __future__ import annotations

import json
from typing import Any

import boto3

from tests.e2e.security.actions import ActionProbe, passrole_context


def simulate(policy: dict[str, Any], probes: list[ActionProbe]) -> dict[str, str]:
    """Return {f'{sid}:{action}': 'allowed'|'implicitDeny'|'explicitDeny'} for each probe.

    Uses iam:SimulateCustomPolicy which evaluates `policy` against `(action, resource)`
    pairs without requiring the policy to be attached to anything.
    """
    iam = boto3.client("iam")
    policy_json = json.dumps(policy)

    # Group probes by action so each SimulateCustomPolicy call covers multiple resources.
    # The API accepts a list of actions and a list of resources, evaluating cross-product.
    out: dict[str, str] = {}
    for probe in probes:
        kwargs: dict[str, Any] = {
            "PolicyInputList": [policy_json],
            "ActionNames": [probe.action],
            "ResourceArns": [probe.resource],
        }
        if probe.action == "iam:PassRole":
            kwargs["ContextEntries"] = passrole_context()

        resp = iam.simulate_custom_policy(**kwargs)
        decision = resp["EvaluationResults"][0]["EvalDecision"]
        out[f"{probe.sid}:{probe.action}"] = decision
    return out
