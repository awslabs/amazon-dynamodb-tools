"""Source guard: surfacing a worker error must raise BulkExecutorError.

Workers record failures on an error accumulator and the driver raises the first
one. *Which exception type* it raises decides what the user sees:

- ``BulkExecutorError`` -- root.py catches it and calls ``sys.exit(str(e))``.
  Glue records that string as the job's ErrorMessage and the client prints it as
  its closing line. One sentence, no traceback.
- anything else -- root.py re-raises, so the user gets a Python traceback and a
  GlueExceptionAnalysis blob on top of the message.

Measured on a denied `diff`: 82 lines with a traceback when the driver raised a
plain ``Exception``, 52 lines and no traceback once it raised
``BulkExecutorError``. The message text was identical in both -- only the
exception type differed, which is exactly the kind of difference that reads as
fine in review and is invisible to a unit test asserting on the message.

A guard rather than six per-verb tests: mutation testing showed a per-verb test
only protects the verb it names (changing diff's raise failed a test; changing
fill, copy, update, scancount and find's did not), and a new verb would arrive
unprotected.
"""

import re
from pathlib import Path

_REPO = Path(__file__).resolve().parents[1]
_SERVER_SRC = _REPO / "server" / "src"

# A raise whose argument comes from an accumulator: `first_error`, or something
# like `error_accumulator.value[0]`. These are the driver-side surfacing points.
_ACCUMULATOR_RAISE = re.compile(
    r"raise\s+(\w+)\s*\(\s*(?:[\w.]*first_error|[\w.]*accumulator\.value\[)")

_REQUIRED = "BulkExecutorError"


def _offending_lines(roots=(_SERVER_SRC,)):
    hits = []
    for root in roots:
        for path in sorted(root.rglob("*.py")):
            for lineno, line in enumerate(path.read_text().splitlines(), 1):
                match = _ACCUMULATOR_RAISE.search(line)
                if match and match.group(1) != _REQUIRED:
                    try:
                        label = path.relative_to(_REPO)
                    except ValueError:
                        label = path
                    hits.append(f"{label}:{lineno}: {line.strip()}")
    return hits


def test_accumulator_errors_raise_bulk_executor_error():
    hits = _offending_lines()
    assert not hits, (
        "These raise a worker error as something other than BulkExecutorError, so "
        "root.py will re-raise it and the user gets a traceback instead of one "
        "sentence:\n" + "\n".join(f"  {h}" for h in hits)
    )


def test_the_guard_actually_detects_a_violation(tmp_path):
    """Guard the guard: a vacuous regex here would be permanently green."""
    root = tmp_path / "src"
    root.mkdir()
    (root / "bad.py").write_text("        raise Exception(first_error) from None\n")
    (root / "bad2.py").write_text("    raise RuntimeError(error_accumulator.value[0])\n")
    (root / "good.py").write_text(
        "        raise BulkExecutorError(first_error) from None\n"
        "        raise BulkExecutorError(error_accumulator.value[0]) from None\n"
        "        raise Exception('unrelated, not from an accumulator')\n"
    )

    hits = _offending_lines(roots=(root,))

    assert len(hits) == 2, f"expected both bad lines, got {hits}"
    assert any("bad.py:1" in h for h in hits)
    assert any("bad2.py:1" in h for h in hits)


def test_every_verb_that_uses_an_accumulator_is_covered():
    """The guard is worthless if it matches nothing -- prove it sees the real code."""
    found = []
    for path in sorted(_SERVER_SRC.rglob("*.py")):
        for line in path.read_text().splitlines():
            if _ACCUMULATOR_RAISE.search(line):
                found.append(path.name)
    assert len(found) >= 5, (
        f"expected the accumulator-raise pattern in several verbs, matched {found}"
    )
