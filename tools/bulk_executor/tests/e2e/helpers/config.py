"""Per-developer e2e config: prompt on first run, persist to .e2e-config."""
from __future__ import annotations

import json
import sys
from dataclasses import dataclass, asdict
from pathlib import Path

CONFIG_PATH = Path(__file__).resolve().parent.parent / ".e2e-config"

COST_BANNER = """\
================================================================
  bulk_executor e2e: {suite}

  This suite runs real Glue jobs in your AWS account.
  COST: a few dollars per run.   WALL TIME: ~10-15 min per full run.

  Press Ctrl-C now to abort. Otherwise answer the prompts below.
================================================================
"""


@dataclass
class E2EConfig:
    aws_account_id: str
    aws_region: str
    read_table: str
    write_table: str
    bootstrap_confirmed: bool

    @classmethod
    def load(cls) -> "E2EConfig | None":
        if not CONFIG_PATH.exists():
            return None
        data = json.loads(CONFIG_PATH.read_text())
        return cls(**data)

    def save(self) -> None:
        CONFIG_PATH.write_text(json.dumps(asdict(self), indent=2) + "\n")


def _prompt(label: str) -> str:
    while True:
        answer = input(f"{label}: ").strip()
        if answer:
            return answer
        print("  (required — please enter a value)", file=sys.stderr)


def _prompt_yes_no(label: str) -> bool:
    while True:
        answer = input(f"{label} [y/N]: ").strip().lower()
        if answer in {"y", "yes"}:
            return True
        if answer in {"n", "no", ""}:
            return False


def prompt_and_save(suite: str) -> E2EConfig:
    print(COST_BANNER.format(suite=suite))
    cfg = E2EConfig(
        aws_account_id=_prompt("AWS account ID"),
        aws_region=_prompt("AWS region (e.g. us-west-2)"),
        read_table=_prompt("DynamoDB read-only test table"),
        write_table=_prompt("DynamoDB writable test table (load smoke target)"),
        bootstrap_confirmed=_prompt_yes_no(
            "Confirm you have run 'bulk bootstrap' on this account+region"
        ),
    )
    if not cfg.bootstrap_confirmed:
        sys.exit(
            "\nAborted: e2e suite requires a current 'bulk bootstrap' on the target "
            "account+region. Run 'bulk bootstrap' then re-invoke this suite."
        )
    cfg.save()
    print(f"\nSaved to {CONFIG_PATH}")
    print("Delete that file to be re-prompted.\n")
    return cfg


def load_or_prompt(suite: str) -> E2EConfig:
    return E2EConfig.load() or prompt_and_save(suite)
