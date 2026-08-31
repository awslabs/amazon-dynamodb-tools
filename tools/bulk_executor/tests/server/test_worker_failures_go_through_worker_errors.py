"""Source guard: worker failures are recorded and surfaced only via worker_errors.

shared/worker_errors.py owns the two decisions the user actually sees: whether we
understood the failure (understood -> one sentence; unexpected -> the worker's
traceback printed to the console first), and that the raise is a BulkExecutorError so
root.py exits with a one-line reason instead of re-raising into a Glue analysis blob.

Both decisions are bypassed silently. A verb that does `error_accumulator.add([msg])`
records a bare string, so nothing knows whether a traceback was warranted; a verb that
does `raise Exception(first_error)` gets a Py4J-shaped failure whose last line is a
traceback. Measured on a denied `diff`: 82 lines with a re-raise, 52 lines and no
traceback through the seam -- the message text was identical, only the path differed,
which reads as fine in review and is invisible to a unit test asserting on messages.

A guard rather than per-verb tests: mutation testing showed a per-verb test only
protects the verb it names (changing diff's raise failed a test; changing fill, copy,
update, scancount and find's did not), and a new verb would arrive unprotected.
"""

import re
from pathlib import Path

_REPO = Path(__file__).resolve().parents[2]
_SERVER_SRC = _REPO / "server" / "src"
_SEAM = "worker_errors.py"

# A raise whose argument comes from an accumulator: `first_error`, or something like
# `error_accumulator.value[0]`. Surfacing belongs to raise_first_worker_error.
_ACCUMULATOR_RAISE = re.compile(
    r"raise\s+\w+\s*\(\s*(?:[\w.]*first_error|[\w.]*accumulator\.value\[)")

# A direct add to an error accumulator. Recording belongs to record_worker_failure or
# record_understood_failure, which decide whether a traceback comes with it.
_DIRECT_ADD = re.compile(r"\w*error_accumulator\s*\.\s*add\s*\(")


def _offending_lines(roots=(_SERVER_SRC,)):
    hits = []
    for root in roots:
        for path in sorted(root.rglob("*.py")):
            if path.name == _SEAM:
                continue
            for lineno, line in enumerate(path.read_text().splitlines(), 1):
                for pattern, why in ((_ACCUMULATOR_RAISE, "raises an accumulated error directly"),
                                     (_DIRECT_ADD, "records without classifying")):
                    if pattern.search(line):
                        label = path.relative_to(_REPO) if root is _SERVER_SRC else path
                        hits.append(f"{label}:{lineno}: {why}: {line.strip()}")
    return hits


def test_worker_failures_only_move_through_the_seam():
    hits = _offending_lines()
    assert not hits, (
        "These bypass shared/worker_errors.py, so the user gets a traceback where one "
        "sentence was due, or one sentence where the traceback was the whole point:\n"
        + "\n".join(f"  {h}" for h in hits)
    )


def test_the_guard_actually_detects_a_violation(tmp_path):
    """Guard the guard: a vacuous regex here would be permanently green."""
    root = tmp_path / "src"
    root.mkdir()
    (root / "bad_raise.py").write_text("        raise Exception(first_error) from None\n")
    (root / "bad_raise2.py").write_text("    raise BulkExecutorError(error_accumulator.value[0])\n")
    (root / "bad_add.py").write_text('    error_accumulator.add([f"Error in worker {n}"])\n')
    (root / "good.py").write_text(
        "        raise_first_worker_error(error_accumulator)\n"
        "        record_worker_failure(error_accumulator, e, 'Error in worker 3')\n"
        "        record_understood_failure(error_accumulator, 'Schema validation error')\n"
        "        failure_accumulator.add(1)\n"
        "        raise BulkExecutorError('unrelated, not from an accumulator')\n"
    )

    hits = _offending_lines(roots=(root,))

    assert len(hits) == 3, f"expected all three bad lines, got {hits}"
    assert any("bad_raise.py:1" in h for h in hits)
    assert any("bad_raise2.py:1" in h for h in hits)
    assert any("bad_add.py:1" in h for h in hits)


def test_the_seam_is_actually_used():
    """The guard is worthless if the code simply stopped reporting worker failures."""
    surfacing = [path.name for path in sorted(_SERVER_SRC.rglob("*.py"))
                 if "raise_first_worker_error(" in path.read_text() and path.name != _SEAM]
    assert len(surfacing) >= 5, (
        f"expected several verbs to surface worker failures, found {surfacing}")


def test_the_client_watches_for_the_banner_the_job_prints():
    """The client suppresses Glue's exception-analysis noise once the job has explained
    itself. That handshake is two string literals in two trees: if they drift, an
    unexpected failure gets the worker traceback *and* a Glue traceback of our plumbing
    on top of it, which is exactly what the banner exists to prevent."""
    server = (_SERVER_SRC / "python_modules" / "shared" / "worker_errors.py").read_text()
    banner = re.search(r'UNEXPECTED_FAILURE_BANNER = "([^"]+)"', server)
    assert banner, "worker_errors.py no longer defines UNEXPECTED_FAILURE_BANNER"

    client = (_REPO / "client" / "src" / "runner.py").read_text()
    assert banner.group(1) in client, (
        f"client/src/runner.py does not watch for {banner.group(1)!r}; "
        "JOB_EXPLAINED_THE_FAILURE has drifted from worker_errors.py")
