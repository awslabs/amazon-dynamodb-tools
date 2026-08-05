"""Unit tests for the server-side log formatter.

Covers `server/src/python_modules/shared/logger.py`:

- BulkDynamoDBServerSideFormatter: the leading "<asctime> <LEVEL>" shape of a
  formatted WARNING/ERROR record. This is a *contract* test: the client log
  tailer (client/src/runner.py) colors lines by matching that leading shape with
  _SERVER_LOG_LEVEL_RE. If this formatter's asctime/level layout changes (e.g. a
  custom datefmt drops the comma-milliseconds), the client silently stops
  coloring by level and falls back to keyword guessing. This test fails loudly
  instead, pointing at the coupling.
- init(): root-logger level selection (default INFO, XDebug -> DEBUG, None args).
"""
import importlib.util
import logging
import re
from pathlib import Path

# conftest.py replaces `shared.logger` / `python_modules.shared.logger` with a
# Mock so unrelated server tests need not configure logging. To assert on the
# *real* formatter we load logger.py directly from its file path, under a
# throwaway module name so the global mock is left intact.
_LOGGER_PATH = (
    Path(__file__).resolve().parents[2]
    / "server/src/python_modules/shared/logger.py"
)
_spec = importlib.util.spec_from_file_location("_real_server_logger", _LOGGER_PATH)
_logger_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_logger_mod)
BulkDynamoDBServerSideFormatter = _logger_mod.BulkDynamoDBServerSideFormatter


# Kept in lockstep with runner._SERVER_LOG_LEVEL_RE. If you change one, change both.
_CLIENT_LEVEL_RE = re.compile(r'^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3} (\w+)\b')


def _format(level, levelname):
    record = logging.LogRecord(
        name='root', level=level, pathname=__file__, lineno=1,
        msg='hello world', args=(), exc_info=None,
    )
    record.levelname = levelname
    return BulkDynamoDBServerSideFormatter().format(record)


class TestServerLogLevelPrefixContract:
    def test_warning_line_matches_client_level_regex(self):
        line = _format(logging.WARNING, 'WARNING')
        m = _CLIENT_LEVEL_RE.match(line)
        assert m is not None, f"formatter output no longer matches client regex: {line!r}"
        assert m.group(1) == 'WARNING'

    def test_error_line_matches_client_level_regex(self):
        line = _format(logging.ERROR, 'ERROR')
        m = _CLIENT_LEVEL_RE.match(line)
        assert m is not None, f"formatter output no longer matches client regex: {line!r}"
        assert m.group(1) == 'ERROR'

    def test_info_line_has_no_level_prefix(self):
        # INFO uses the bare "%(message)s" format, so it must NOT match — the
        # client leaves such lines uncolored, which is the intended behavior.
        line = _format(logging.INFO, 'INFO')
        assert _CLIENT_LEVEL_RE.match(line) is None
        assert line == 'hello world'


class TestInit:
    """init() configures the root logger; restore it so other tests are unaffected."""

    def _restore(self, root, saved_level, saved_handlers):
        root.setLevel(saved_level)
        root.handlers[:] = saved_handlers

    def test_defaults_to_info_level_when_no_debug(self):
        root = logging.getLogger()
        saved_level, saved_handlers = root.level, root.handlers[:]
        try:
            _logger_mod.init({})
            assert root.level == logging.INFO
        finally:
            self._restore(root, saved_level, saved_handlers)

    def test_xdebug_enables_debug_level(self):
        root = logging.getLogger()
        saved_level, saved_handlers = root.level, root.handlers[:]
        try:
            _logger_mod.init({'XDebug': True})
            assert root.level == logging.DEBUG
        finally:
            self._restore(root, saved_level, saved_handlers)

    def test_none_args_treated_as_empty(self):
        # args=None must not raise and behaves like {} (INFO level).
        root = logging.getLogger()
        saved_level, saved_handlers = root.level, root.handlers[:]
        try:
            _logger_mod.init(None)
            assert root.level == logging.INFO
        finally:
            self._restore(root, saved_level, saved_handlers)
