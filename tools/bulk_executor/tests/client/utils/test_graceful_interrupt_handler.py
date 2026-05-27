"""Unit tests for GracefulInterruptHandler.

Covers `client/src/utils/graceful_interrupt_handler.py`:
- __init__: default signal is SIGINT, custom sig accepted
- __enter__: snapshots original handler, installs new handler, returns
  self, sets interrupted/released to False
- __exit__: calls release() unconditionally
- release: idempotent (returns False on second call), restores original
  handler on first call, returns True
- Signal-driven flow: handler sets interrupted=True, calls release;
  context manager preserves outer handler after exit

Style notes:
- We patch `signal.signal` and `signal.getsignal` at the
  graceful_interrupt_handler module namespace because the source binds
  `import signal` at module scope.
- Avoid `signal.raise_signal` to keep tests deterministic on macOS where
  signal delivery to the main thread is the only legal path; instead,
  invoke the installed handler directly via the captured arg.
"""

import signal
from unittest.mock import MagicMock

import pytest

from utils import graceful_interrupt_handler as gih_module
from utils.graceful_interrupt_handler import GracefulInterruptHandler


# --- __init__ ---------------------------------------------------------------

class TestInit:
    """Tests for __init__ (lines 6-7)."""

    def test_default_signal_is_sigint(self):
        h = GracefulInterruptHandler()
        assert h.sig == signal.SIGINT

    def test_custom_signal_stored(self):
        h = GracefulInterruptHandler(sig=signal.SIGTERM)
        assert h.sig == signal.SIGTERM


# --- __enter__ --------------------------------------------------------------

class TestEnter:
    """Tests for __enter__ (lines 9-20)."""

    def test_enter_returns_self(self, monkeypatch):
        """Line 20: __enter__ returns the handler instance."""
        monkeypatch.setattr(gih_module.signal, 'signal', MagicMock())
        monkeypatch.setattr(gih_module.signal, 'getsignal', MagicMock(return_value=signal.SIG_DFL))

        h = GracefulInterruptHandler()
        result = h.__enter__()

        assert result is h

    def test_enter_initializes_state_flags(self, monkeypatch):
        """Lines 10-11: interrupted and released start False."""
        monkeypatch.setattr(gih_module.signal, 'signal', MagicMock())
        monkeypatch.setattr(gih_module.signal, 'getsignal', MagicMock(return_value=signal.SIG_DFL))

        h = GracefulInterruptHandler()
        h.__enter__()

        assert h.interrupted is False
        assert h.released is False

    def test_enter_snapshots_original_handler(self, monkeypatch):
        """Line 13: getsignal called with self.sig; result stored."""
        original = MagicMock(name='original_sigint_handler')
        getsignal_mock = MagicMock(return_value=original)
        monkeypatch.setattr(gih_module.signal, 'getsignal', getsignal_mock)
        monkeypatch.setattr(gih_module.signal, 'signal', MagicMock())

        h = GracefulInterruptHandler(sig=signal.SIGINT)
        h.__enter__()

        getsignal_mock.assert_called_once_with(signal.SIGINT)
        assert h.original_handler is original

    def test_enter_installs_new_handler(self, monkeypatch):
        """Line 19: signal.signal called with self.sig and new handler."""
        signal_mock = MagicMock()
        monkeypatch.setattr(gih_module.signal, 'signal', signal_mock)
        monkeypatch.setattr(gih_module.signal, 'getsignal', MagicMock(return_value=signal.SIG_DFL))

        h = GracefulInterruptHandler(sig=signal.SIGINT)
        h.__enter__()

        # First call (in __enter__) installs the new handler
        first_call = signal_mock.call_args_list[0]
        assert first_call.args[0] == signal.SIGINT
        installed_handler = first_call.args[1]
        assert callable(installed_handler)
        # And it's not the original (which was SIG_DFL)
        assert installed_handler is not signal.SIG_DFL


# --- handler behavior -------------------------------------------------------

class TestInstalledHandler:
    """Tests for the inner handler closure (lines 15-17)."""

    def test_handler_sets_interrupted_and_releases(self, monkeypatch):
        """Lines 16-17: when fired, handler calls release() and flips interrupted."""
        signal_mock = MagicMock()
        monkeypatch.setattr(gih_module.signal, 'signal', signal_mock)
        monkeypatch.setattr(gih_module.signal, 'getsignal', MagicMock(return_value=signal.SIG_DFL))

        h = GracefulInterruptHandler(sig=signal.SIGINT)
        h.__enter__()

        # Pull the installed handler back out and fire it manually
        installed_handler = signal_mock.call_args_list[0].args[1]
        installed_handler(signal.SIGINT, None)

        assert h.interrupted is True
        assert h.released is True


# --- __exit__ ---------------------------------------------------------------

class TestExit:
    """Tests for __exit__ (lines 22-23)."""

    def test_exit_calls_release(self, monkeypatch):
        """Line 23: __exit__ unconditionally calls release()."""
        signal_mock = MagicMock()
        monkeypatch.setattr(gih_module.signal, 'signal', signal_mock)
        monkeypatch.setattr(gih_module.signal, 'getsignal', MagicMock(return_value=signal.SIG_DFL))

        h = GracefulInterruptHandler()
        h.__enter__()
        assert h.released is False

        h.__exit__(None, None, None)
        assert h.released is True

    def test_context_manager_round_trip_restores_handler(self, monkeypatch):
        """Lines 22-23 + 29: original handler is reinstalled on exit."""
        original = MagicMock(name='original_sigint_handler')
        signal_mock = MagicMock()
        monkeypatch.setattr(gih_module.signal, 'signal', signal_mock)
        monkeypatch.setattr(gih_module.signal, 'getsignal', MagicMock(return_value=original))

        with GracefulInterruptHandler(sig=signal.SIGINT) as h:
            assert h.released is False

        # First call (enter): install new handler
        # Second call (release via exit): restore original
        assert signal_mock.call_count == 2
        restore_call = signal_mock.call_args_list[1]
        assert restore_call.args == (signal.SIGINT, original)


# --- release ---------------------------------------------------------------

class TestRelease:
    """Tests for release (lines 25-32)."""

    def test_release_first_call_returns_true(self, monkeypatch):
        """Line 32: first release returns True."""
        monkeypatch.setattr(gih_module.signal, 'signal', MagicMock())
        monkeypatch.setattr(gih_module.signal, 'getsignal', MagicMock(return_value=signal.SIG_DFL))

        h = GracefulInterruptHandler()
        h.__enter__()
        assert h.release() is True
        assert h.released is True

    def test_release_second_call_returns_false(self, monkeypatch):
        """Lines 26-27: subsequent calls return False without re-installing handler."""
        monkeypatch.setattr(gih_module.signal, 'signal', MagicMock())
        monkeypatch.setattr(gih_module.signal, 'getsignal', MagicMock(return_value=signal.SIG_DFL))

        h = GracefulInterruptHandler()
        h.__enter__()
        h.release()
        assert h.release() is False, "second release is a no-op"

    def test_release_only_restores_once(self, monkeypatch):
        """Lines 26-29: signal.signal is called twice total (enter + first release)."""
        signal_mock = MagicMock()
        monkeypatch.setattr(gih_module.signal, 'signal', signal_mock)
        monkeypatch.setattr(gih_module.signal, 'getsignal', MagicMock(return_value=signal.SIG_DFL))

        h = GracefulInterruptHandler()
        h.__enter__()
        h.release()
        h.release()  # no-op
        h.release()  # no-op

        # Enter + one release = 2 signal.signal calls
        assert signal_mock.call_count == 2
