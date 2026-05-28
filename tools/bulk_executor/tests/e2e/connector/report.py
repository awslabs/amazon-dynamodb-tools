"""Render the Connector Smoke Report and persist it to tests/e2e/results/."""
from __future__ import annotations

import time
from pathlib import Path
from typing import Iterable

from tests.e2e.connector.conftest import PerfRow

RESULTS_DIR = Path(__file__).resolve().parent.parent / "results"


def _fmt_seconds(value: float | None) -> str:
    return f"{value:.1f}s" if value is not None else "n/a"


def _fmt_dpu(value: float | None) -> str:
    return f"{value:.1f}" if value is not None else "n/a"


def _fmt_items(value: int | None) -> str:
    return f"{value:,}" if value is not None else "n/a"


def render_report(rows: Iterable[PerfRow]) -> None:
    rows = list(rows)
    if not rows:
        print("\n(no perf rows captured — every test failed before reporting)")
        return

    header = f"{'verb':<10} {'wall':>10} {'dpu_s':>10} {'items':>12}"
    sep = "=" * len(header)
    body_lines = []
    for row in rows:
        body_lines.append(
            f"{row.verb:<10} "
            f"{_fmt_seconds(row.wall_seconds):>10} "
            f"{_fmt_dpu(row.dpu_seconds):>10} "
            f"{_fmt_items(row.items):>12}"
        )

    text = "\n".join([
        "",
        sep,
        "       Connector Smoke Report",
        sep,
        header,
        "-" * len(header),
        *body_lines,
        sep,
        "",
    ])
    print(text)

    RESULTS_DIR.mkdir(exist_ok=True)
    timestamp = time.strftime("%Y%m%d-%H%M%S")
    md_path = RESULTS_DIR / f"connector-smoke-{timestamp}.md"
    md_path.write_text(
        "# Connector Smoke Report\n\n"
        f"_Generated {time.strftime('%Y-%m-%d %H:%M:%S')}_\n\n"
        "```\n" + text.strip() + "\n```\n"
    )
    print(f"Report saved: {md_path.relative_to(md_path.parents[3])}")
