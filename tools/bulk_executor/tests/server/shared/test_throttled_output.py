"""Unit tests for server/src/python_modules/shared/throttled_output.py.

Covers:

- ThrottledWriter.write: byte-exactness -- the wrapped stream receives exactly the
  bytes handed in, in order, unsplit, with nothing buffered.
- **That it never flushes.** The module's one rule: taking over flushing is what made
  an earlier version emit one CloudWatch event per line and lose 48% of the output,
  so it gets dedicated regression tests.
- **That the write happens before the pause**, so output is never held back.
- The pacing arithmetic: nothing sleeps while under budget, a burst costs
  bytes/rate seconds, credit is earned back from elapsed time, debt accumulates
  instead of producing a swarm of sub-millisecond sleeps, and sleep overshoot is not
  compounded into later writes.
- Delegation, so it is a drop-in for sys.stdout (including a real flush()).
- install(): replaces sys.stdout, is idempotent, leaves stderr alone.

Time is injected (fake clock + recording sleep) so the tests assert on the *computed*
delays rather than actually sleeping -- a real-time test of a 100 KB/s throttle would
take 40 seconds to prove the 4 MB case.
"""

import sys

import pytest

from python_modules.shared import throttled_output
from python_modules.shared.throttled_output import ThrottledWriter, install


class FakeClock:
    """A monotonic clock that only advances when told to, or by sleeping."""

    def __init__(self):
        self.now = 1000.0

    def __call__(self):
        return self.now

    def advance(self, seconds):
        self.now += seconds


class RecordingStream:
    def __init__(self):
        self.writes = []
        self.flushes = 0

    def write(self, text):
        self.writes.append(text)
        return len(text)

    def flush(self):
        self.flushes += 1

    # Something for the delegation tests to reach for.
    encoding = 'utf-8'

    def isatty(self):
        return False


def _recording_sleep(clock, slept):
    """A fake sleep that advances the fake clock, like the real one does.

    Without the advance, the writer's next-allowed time runs away from `now` and
    delays compound quadratically -- a property of the stub, not the code.
    """
    def sleep(seconds):
        slept.append(seconds)
        clock.advance(seconds)
    return sleep


@pytest.fixture
def wired():
    """1000 bytes/sec with a 0.05s minimum sleep -- round numbers so the expected
    delays are obvious by inspection rather than arithmetic."""
    clock = FakeClock()
    slept = []
    stream = RecordingStream()
    writer = ThrottledWriter(stream, bytes_per_second=1000, min_sleep_seconds=0.05,
                             clock=clock, sleep=_recording_sleep(clock, slept))
    return writer, stream, clock, slept


class TestNeverFlushes:
    """The module's one rule, and the bug it exists to prevent."""

    def test_a_burst_of_lines_produces_no_flushes(self, wired):
        """REGRESSION.

        An earlier version flushed after every write -- and print(x) is two writes --
        so Glue's log agent emitted one CloudWatch event per line. A 100,000-row find
        became 100,043 events at 710/sec, sat above the 500-events/sec threshold for
        134 of 141 seconds, and CloudWatch discarded 48% of the rows. Left alone the
        pipeline batches for itself: 78 events/sec, measured, for the same find.
        """
        writer, stream, _, _ = wired
        for _ in range(500):
            writer.write('x' * 19)
            writer.write('\n')          # exactly what print() does
        assert stream.flushes == 0, (
            "flushing on our own initiative is what breached the 500-events/sec "
            "limit -- batching is the log agent's job")

    def test_a_large_write_produces_no_flushes(self, wired):
        writer, stream, _, _ = wired
        writer.write('x' * 5000)
        assert stream.flushes == 0

    def test_explicit_flush_still_reaches_the_stream(self, wired):
        """We never flush unprompted, but print(flush=True) must still work."""
        writer, stream, _, _ = wired
        writer.flush()
        assert stream.flushes == 1


class TestByteExactness:
    """Whatever goes in comes out -- unchanged, in order, unsplit, immediately."""

    def test_small_write_passes_through_untouched(self, wired):
        writer, stream, _, _ = wired
        writer.write('hello\n')
        assert stream.writes == ['hello\n']

    def test_a_large_write_is_passed_through_whole(self, wired):
        """Deliberately not split into paced pieces.

        A 400 KB item therefore hits the pipe in one go -- ~400 KB inside a one-second
        window, under the ~1 MB/s where loss begins, and ~13 CloudWatch events against
        a limit of 500. Splitting it made no measurable difference, so the simpler
        behavior wins.
        """
        writer, stream, _, _ = wired
        payload = ''.join(chr(ord('a') + (i % 26)) for i in range(5000))
        writer.write(payload)
        assert stream.writes == [payload], "one write in, one write out"

    def test_write_returns_the_byte_count(self, wired):
        writer, _, _, _ = wired
        assert writer.write('x' * 250) == 250

    def test_empty_write_is_a_no_op(self, wired):
        writer, stream, _, slept = wired
        assert writer.write('') == 0
        assert stream.writes == ['']
        assert slept == []
        assert writer.bytes_written == 0


class TestWriteBeforePause:
    """Output first, pause second -- never hold bytes back waiting on a timer."""

    def test_the_bytes_are_written_before_the_sleep(self):
        clock = FakeClock()
        timeline = []
        stream = RecordingStream()

        def sleep(seconds):
            timeline.append('sleep')
            clock.advance(seconds)

        writer = ThrottledWriter(stream, bytes_per_second=1000, min_sleep_seconds=0.05,
                                 clock=clock, sleep=sleep)
        original_write = stream.write

        def watching_write(text):
            timeline.append('write')
            return original_write(text)

        stream.write = watching_write
        writer.write('x' * 500)         # 0.5s of debt -> must sleep

        assert timeline == ['write', 'sleep'], (
            "a pause before the write would delay output the user could already have")


class TestPacing:
    """The arithmetic that actually protects us."""

    def test_first_small_write_does_not_sleep(self, wired):
        writer, _, _, slept = wired
        writer.write('x' * 10)          # 0.01s of debt, under the minimum
        assert slept == []

    def test_a_burst_costs_bytes_over_rate(self, wired):
        writer, _, _, slept = wired
        writer.write('x' * 1000)        # 1000 bytes at 1000 B/s == 1s
        assert sum(slept) == pytest.approx(1.0)

    def test_debt_accumulates_across_writes_then_is_paid_once(self, wired):
        """Small writes must not each sleep; the debt is carried until it is worth
        paying, then settled in one longer sleep."""
        writer, _, _, slept = wired
        for _ in range(20):
            writer.write('x' * 10)      # 200 bytes == 0.2s of budget
        assert sum(slept) == pytest.approx(0.2, abs=0.06)
        assert writer.sleeps <= 4, "few, larger sleeps -- not one per write"

    def test_time_passing_earns_credit_back(self, wired):
        """Output spread out by the job's own work must not be penalized.

        Writes small enough to stay under the minimum-sleep floor carry their debt,
        and elapsed time wipes it -- so ordinary log-scale output never pauses however
        long the run goes on.
        """
        writer, _, clock, slept = wired
        for _ in range(10):
            writer.write('x' * 10)      # 0.01s of debt each, all under the floor
            clock.advance(1.0)          # ... and a second of credit each
        assert slept == [], "a slow trickle of output should never sleep"

    def test_idle_time_does_not_bank_unlimited_credit(self, wired):
        """Credit is a reset to `now`, not a running surplus.

        Without the reset, a long quiet stretch would bank arbitrary credit and the
        next burst would go out completely unpaced -- which is the whole failure we
        are preventing. So after idling, a burst must still cost its own airtime.
        """
        writer, _, clock, slept = wired
        writer.write('x' * 10)
        clock.advance(60.0)             # a minute of scanning, no output
        writer.write('x' * 500)         # 0.5s of airtime, owed in full
        assert sum(slept) == pytest.approx(0.5), (
            "idling must not buy the right to dump a burst unpaced")

    def test_one_large_write_pays_immediately(self, wired):
        """The deliberate consequence of pausing *after* the write.

        A write big enough to owe more than the minimum sleep settles up on the spot
        rather than deferring to the next write. It costs a needless pause when a verb
        prints one big thing and then goes quiet for a while -- worth it, because the
        alternative is holding bytes back before writing them, and a write that large
        is exactly the burst we are trying to spread out.
        """
        writer, _, _, slept = wired
        writer.write('x' * 500)         # 0.5s owed, over the 0.05s floor
        assert sum(slept) == pytest.approx(0.5)

    def test_sleep_overshoot_is_not_compounded(self):
        """An earlier version re-read the clock after sleeping, folding each sleep's
        overshoot into the schedule; the rate drifted to 58 KB/s against a 100 KB/s
        setting. We must never re-pay time already spent."""
        clock = FakeClock()
        slept = []
        started = clock.now

        def overshooting_sleep(seconds):
            slept.append(seconds)
            clock.advance(seconds * 3)  # wildly overshoot

        writer = ThrottledWriter(RecordingStream(), bytes_per_second=1000,
                                 min_sleep_seconds=0.05, clock=clock,
                                 sleep=overshooting_sleep)
        for _ in range(10):
            writer.write('x' * 100)     # 1000 bytes == 1.0s owed in total
        assert sum(slept) <= 1.01, (
            f"asked for {sum(slept):.2f}s against a 1.0s budget -- overshoot is being "
            f"re-paid, which is the drift this guards against")
        assert clock.now - started >= 1.0, "the byte rate must still be honored"

    def test_real_world_case_ten_max_size_items(self):
        """The case that motivated this: 10 x 400 KB items at the shipped rate."""
        clock = FakeClock()
        slept = []
        writer = ThrottledWriter(RecordingStream(), clock=clock,
                                 sleep=_recording_sleep(clock, slept))
        for _ in range(10):
            writer.write('x' * (400 * 1024) + '\n')
        assert 35 < sum(slept) < 45, f"expected ~40s of pacing, got {sum(slept):.1f}s"

    def test_hundred_thousand_rows_costs_about_two_minutes(self):
        """13.2 MB of find output at 100 KB/s, and not via 200,000 tiny sleeps."""
        clock = FakeClock()
        slept = []
        writer = ThrottledWriter(RecordingStream(), clock=clock,
                                 sleep=_recording_sleep(clock, slept))
        row = '{"pk":"k0001","sk":"abcdefghijklmnop","payload":"' + 'x' * 60 + '"}'
        for _ in range(100_000):
            writer.write(row)
            writer.write('\n')
        assert 100 < sum(slept) < 140, f"expected ~110-130s, got {sum(slept):.0f}s"
        assert writer.sleeps < 10_000, (
            f"{writer.sleeps} sleeps for 200,000 writes -- the minimum-sleep floor is "
            f"not working, and millisecond sleeps overshoot badly")

    def test_tracks_totals_for_diagnostics(self, wired):
        writer, _, _, _ = wired
        writer.write('x' * 250)
        assert writer.bytes_written == 250
        assert writer.seconds_slept == pytest.approx(0.25)


class TestDelegation:
    """Must be a drop-in for sys.stdout."""

    def test_unknown_attributes_delegate(self, wired):
        writer, _, _, _ = wired
        assert writer.encoding == 'utf-8'
        assert writer.isatty() is False

    def test_missing_attribute_still_raises(self, wired):
        writer, _, _, _ = wired
        with pytest.raises(AttributeError):
            writer.no_such_attribute


class TestInstall:
    def test_replaces_stdout_and_returns_the_writer(self, monkeypatch):
        stream = RecordingStream()
        monkeypatch.setattr(sys, 'stdout', stream)
        writer = install()
        assert sys.stdout is writer
        assert isinstance(writer, ThrottledWriter)
        print('routed through the throttle')
        assert stream.writes, "print() must reach the real stream"

    def test_is_idempotent(self, monkeypatch):
        """Installing twice would stack throttles and halve the effective rate."""
        monkeypatch.setattr(sys, 'stdout', RecordingStream())
        first = install()
        second = install()
        assert first is second
        assert sys.stdout is first

    def test_leaves_stderr_alone(self, monkeypatch):
        """Server logging writes to stderr; diagnostics must not queue behind a big
        result set."""
        stderr_before = sys.stderr
        monkeypatch.setattr(sys, 'stdout', RecordingStream())
        install()
        assert sys.stderr is stderr_before

    def test_uses_the_documented_default_rate(self):
        assert throttled_output.BYTES_PER_SECOND == 100 * 1024
        assert ThrottledWriter(RecordingStream())._bytes_per_second == 100 * 1024
