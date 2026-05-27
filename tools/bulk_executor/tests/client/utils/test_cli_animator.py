"""Unit tests for cli_animator.

Covers `client/src/utils/cli_animator.py`:
- Module constants: SPINNER_CHARS, NUM_SPINNERS, SPINNER_INTERVAL_SECONDS
- with_spinner_animation: outer-loop time guard, inner cycle through
  spinner chars, ANSI carriage-return write, time.sleep between frames,
  early break when wait time elapsed mid-cycle, custom animation_message

Style notes:
- `time.time` and `time.sleep` are patched at the cli_animator module
  namespace because the source binds `import time` at module scope.
- Stdout is captured via capsys to verify the carriage-return spinner
  string format.
- A monotonically-increasing `time.time` mock drives the loop deterministically:
  start=0, then a sequence of values that exceed wait_time_in_seconds at
  the desired iteration count.
"""

from utils import cli_animator


# --- Module constants -------------------------------------------------------

class TestModuleConstants:
    """Tests for module-level constants (lines 5-7)."""

    def test_spinner_chars(self):
        """Line 5: SPINNER_CHARS is the 4-char spinner."""
        assert cli_animator.SPINNER_CHARS == ['|', '/', '-', '\\']

    def test_num_spinners(self):
        """Line 6: NUM_SPINNERS is 4."""
        assert cli_animator.NUM_SPINNERS == 4

    def test_spinner_interval_seconds(self):
        """Line 7: SPINNER_INTERVAL_SECONDS frame timing."""
        assert cli_animator.SPINNER_INTERVAL_SECONDS == 0.15


# --- with_spinner_animation -------------------------------------------------

class TestWithSpinnerAnimation:
    """Tests for with_spinner_animation (lines 9-23)."""

    def test_zero_wait_does_not_print(self, monkeypatch, capsys):
        """Line 11: outer while exits immediately when elapsed >= wait_time."""
        # First time.time call returns start; next call returns same value
        # so elapsed = 0; but loop condition `elapsed < wait_time` with
        # wait_time=0 → False, so we never enter the loop body.
        times = iter([100.0, 100.0])
        monkeypatch.setattr(cli_animator.time, 'time', lambda: next(times))

        sleeps = []
        monkeypatch.setattr(cli_animator.time, 'sleep', lambda s: sleeps.append(s))

        cli_animator.with_spinner_animation(0)

        out = capsys.readouterr().out
        assert out == ""
        assert sleeps == []

    def test_single_frame_then_exits(self, monkeypatch, capsys):
        """Lines 12-23: prints one frame then breaks once elapsed exceeds wait."""
        # time.time call sequence:
        #   1) start_time     -> 0
        #   2) outer-while    -> 0 (0 < 1, enter body)
        #   3) inner elapsed  -> 0 (0 < 1, print + sleep)
        #   4) inner elapsed  -> 2 (>= 1, break inner)
        #   5) outer-while    -> 2 (>= 1, exit outer)
        times = iter([0.0, 0.0, 0.0, 2.0, 2.0])
        monkeypatch.setattr(cli_animator.time, 'time', lambda: next(times))

        sleeps = []
        monkeypatch.setattr(cli_animator.time, 'sleep', lambda s: sleeps.append(s))

        cli_animator.with_spinner_animation(1, animation_message="loading")

        out = capsys.readouterr().out
        # First spinner char is '|'
        assert '\r| loading' in out
        assert sleeps == [0.15], "sleeps once at SPINNER_INTERVAL_SECONDS"

    def test_default_animation_message_empty(self, monkeypatch, capsys):
        """Line 9: default animation_message is empty string."""
        times = iter([0.0, 0.0, 0.0, 5.0, 5.0])
        monkeypatch.setattr(cli_animator.time, 'time', lambda: next(times))
        monkeypatch.setattr(cli_animator.time, 'sleep', lambda s: None)

        cli_animator.with_spinner_animation(1)

        out = capsys.readouterr().out
        # Format is "{char} {animation_message}" — empty msg → "| "
        assert '\r| ' in out

    def test_iterates_through_spinner_chars(self, monkeypatch, capsys):
        """Line 12: inner for-loop cycles through all SPINNER_CHARS."""
        # Drive 4 frames then break
        # time.time calls:
        # start=0, outer=0, inner=0 (frame 1), inner=0 (frame 2),
        # inner=0 (frame 3), inner=0 (frame 4), inner=10 (break),
        # outer=10 (break outer)
        times = iter([0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 10.0, 10.0])
        monkeypatch.setattr(cli_animator.time, 'time', lambda: next(times))
        monkeypatch.setattr(cli_animator.time, 'sleep', lambda s: None)

        cli_animator.with_spinner_animation(5, animation_message="x")

        out = capsys.readouterr().out
        for char in cli_animator.SPINNER_CHARS:
            assert f'\r{char} x' in out, f"frame for {char!r} should appear"

    def test_break_inner_when_elapsed_exceeds_wait(self, monkeypatch, capsys):
        """Lines 13-15: inner loop breaks when elapsed >= wait_time without printing."""
        # start=0, outer=0 (enter), inner elapsed=2 (>=1, break) → no print
        # outer elapsed=2 (>=1, exit outer)
        times = iter([0.0, 0.0, 2.0, 2.0])
        monkeypatch.setattr(cli_animator.time, 'time', lambda: next(times))

        sleeps = []
        monkeypatch.setattr(cli_animator.time, 'sleep', lambda s: sleeps.append(s))

        cli_animator.with_spinner_animation(1, animation_message="x")

        out = capsys.readouterr().out
        # Inner break fired before any print/sleep
        assert out == ""
        assert sleeps == []

    def test_custom_animation_message_in_output(self, monkeypatch, capsys):
        """Line 18: animation_message embedded in '{char} {msg}' format."""
        times = iter([0.0, 0.0, 0.0, 99.0, 99.0])
        monkeypatch.setattr(cli_animator.time, 'time', lambda: next(times))
        monkeypatch.setattr(cli_animator.time, 'sleep', lambda s: None)

        cli_animator.with_spinner_animation(1, animation_message="processing rows")

        out = capsys.readouterr().out
        assert 'processing rows' in out
