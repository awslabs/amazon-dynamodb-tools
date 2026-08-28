"""Source guard: nothing may call the deprecated Logger.warn().

`Logger.warn` has been deprecated since Python 3.3 and is undocumented
(`logging.Logger.warn.__doc__` is None on 3.13). It still works, so this is not
about a crash today -- it's about three things:

1. **Every call emits a DeprecationWarning.** In a CLI that streams Glue logs to
   the user's terminal, that noise lands in front of the person running the
   command, and it lands specifically on error/cleanup paths -- the moment they
   are already reading output carefully.
2. **Undocumented deprecated APIs do get removed.** CPython removed a batch of
   them in 3.12/3.13. When `Logger.warn` goes, every call site becomes an
   AttributeError, and ours sit in `teardown` and in the rate limiter's cleanup
   -- paths that run while tidying up, where a crash is most confusing and least
   likely to be covered by a happy-path test.
3. **It reads as a different method.** `warn` vs `warning` is easy to skim past
   in review, which is how four call sites accumulated.

A test rather than an ai_lint rule, deliberately. `ai_lint/README.md` scopes the
linter to invariants "easy to verify by reading code but very hard to express as
a deterministic unit test". "Does this string appear" is the opposite of that, and
a mechanical check is strictly better here: the hand-maintained list in AGENTS.md
recorded three call sites and missed the fourth
(`DistributedDynamoDBMonitorAggregator.py`), which this check found immediately.
"""

import re
from pathlib import Path

_REPO = Path(__file__).resolve().parents[1]
_SOURCE_ROOTS = (_REPO / "client" / "src", _REPO / "server" / "src")

# `.warn(` but not `.warning(` -- the char after "warn" is the discriminator.
# Deliberately not anchored to the name `log`: a future `logger.warn(` or
# `self._log.warn(` is the same mistake.
_WARN_CALL = re.compile(r"\.warn\(")

# warnings.warn() is the correct API for the warnings module and must not be
# flagged. Nothing uses it today; this keeps the check honest if something does.
_ALLOWED_QUALIFIERS = ("warnings",)


def _offending_lines(roots=_SOURCE_ROOTS):
    """Roots are a parameter so the self-test can pass a fixture directory.

    Monkeypatching the module global looked cleaner but silently did nothing: the
    test module is importable under two names, so the patch landed on a second
    copy and the self-test passed against the real tree instead of the fixture.
    """
    hits = []
    for root in roots:
        for path in sorted(root.rglob("*.py")):
            for lineno, line in enumerate(path.read_text().splitlines(), 1):
                for match in _WARN_CALL.finditer(line):
                    qualifier = line[: match.start()].split()[-1] if line[: match.start()].split() else ""
                    if any(qualifier.endswith(allowed) for allowed in _ALLOWED_QUALIFIERS):
                        continue
                    try:
                        label = path.relative_to(_REPO)
                    except ValueError:      # a fixture dir outside the repo
                        label = path
                    hits.append(f"{label}:{lineno}: {line.strip()}")
    return hits


def test_no_source_file_calls_the_deprecated_logger_warn():
    hits = _offending_lines()
    assert not hits, (
        "Deprecated Logger.warn() found -- use log.warning() instead:\n"
        + "\n".join(f"  {h}" for h in hits)
        + "\n\nLogger.warn has been deprecated since Python 3.3, is undocumented, "
        "and emits a DeprecationWarning on every call. See this test's module "
        "docstring for why that matters here."
    )


def test_the_guard_actually_detects_a_violation(tmp_path):
    """Guard the guard: prove the regex matches warn( and spares warning().

    Without this, a typo in the pattern would make the check silently vacuous --
    permanently green while catching nothing, which is worse than not having it.
    """
    fake_root = tmp_path / "src"
    fake_root.mkdir()
    (fake_root / "bad.py").write_text("log.warn('nope')\n")
    (fake_root / "good.py").write_text(
        "log.warning('fine')\nwarnings.warn('also fine')\n"
    )
    hits = _offending_lines(roots=(fake_root,))

    assert len(hits) == 1, f"expected exactly the bad.py hit, got {hits}"
    assert "bad.py:1" in hits[0]
