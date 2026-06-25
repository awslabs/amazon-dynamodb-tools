"""Render the Command Smoke Report. Same shape as connector report, different title."""
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
        print("\n(no command perf rows captured — every test failed before reporting)")
        return

    header = f"{'command':<14} {'wall':>10} {'dpu_s':>10} {'items':>12}"
    sep = "=" * len(header)
    body_lines = []
    for row in rows:
        body_lines.append(
            f"{row.command:<14} "
            f"{_fmt_seconds(row.wall_seconds):>10} "
            f"{_fmt_dpu(row.dpu_seconds):>10} "
            f"{_fmt_items(row.items):>12}"
        )

    text = "\n".join([
        "",
        sep,
        "          Command Smoke Report",
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
    md_path = RESULTS_DIR / f"command-smoke-{timestamp}.md"
    md_path.write_text(
        "# Command Smoke Report\n\n"
        f"_Generated {time.strftime('%Y-%m-%d %H:%M:%S')}_\n\n"
        "```\n" + text.strip() + "\n```\n"
    )
    print(f"Report saved: {md_path.relative_to(md_path.parents[3])}")
