"""Parse the bootstrap IAM policy out of README.md.

The whole point of this suite is to validate the *documented* policy is the
*real* policy required to bootstrap. So we read it from the README, not from
a hand-copied duplicate that could drift.
"""
from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[3]
README_PATH = REPO_ROOT / "README.md"

# The policy lives in the first JSON code block under "## Installation and bootstrap".
# We anchor on the canonical Sid set instead of headings so the test survives
# README rearrangement.
_REQUIRED_SIDS = {"glueRoleAdmin", "passrole", "s3", "glue", "glueConnection", "logs"}


def parse_bootstrap_policy() -> dict[str, Any]:
    """Return the README's bootstrap IAM policy as a dict.

    Walks every fenced code block (the README uses bare ``` fences, not
    ```json) and returns the first one that parses as JSON and contains all
    required Sids. Raises if no block matches.
    """
    text = README_PATH.read_text()
    for block in re.findall(r"```[a-zA-Z]*\s*\n(.*?)\n```", text, re.DOTALL):
        stripped = block.strip()
        if not stripped.startswith("{"):
            continue
        try:
            doc = json.loads(stripped)
        except json.JSONDecodeError:
            continue
        if not isinstance(doc, dict):
            continue
        sids = {s.get("Sid") for s in doc.get("Statement", [])}
        if _REQUIRED_SIDS.issubset(sids):
            return doc
    raise RuntimeError(
        f"Could not locate bootstrap IAM policy in {README_PATH}. "
        f"Expected a code block whose Statement Sids include {_REQUIRED_SIDS}."
    )


def policy_without_statement(policy: dict[str, Any], sid: str) -> dict[str, Any]:
    """Return a deep-copy of policy with the named-Sid statement removed."""
    return {
        **policy,
        "Statement": [s for s in policy["Statement"] if s.get("Sid") != sid],
    }


def policy_without_action(policy: dict[str, Any], action: str) -> dict[str, Any]:
    """Return a deep-copy of policy with the given action removed wherever it appears.

    If a statement's Action list shrinks to empty, the whole statement is dropped.
    """
    new_statements = []
    for stmt in policy["Statement"]:
        actions = stmt["Action"]
        if isinstance(actions, str):
            actions = [actions]
        remaining = [a for a in actions if a != action]
        if remaining:
            new_statements.append({**stmt, "Action": remaining})
        # else: every action in this Sid was the one removed; drop the statement entirely
    return {**policy, "Statement": new_statements}


def all_actions(policy: dict[str, Any]) -> list[tuple[str, str]]:
    """Flatten policy into [(sid, action), ...] for per-action assertions."""
    out: list[tuple[str, str]] = []
    for stmt in policy["Statement"]:
        sid = stmt.get("Sid", "<no-sid>")
        actions = stmt["Action"]
        if isinstance(actions, str):
            actions = [actions]
        for a in actions:
            out.append((sid, a))
    return out
