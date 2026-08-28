"""Unit tests for server/src/python_modules/shared/throttled_output.py.

Covers:

- ThrottledWriter.write: byte-exactness (the wrapped stream receives exactly the
  bytes handed in, in order), chunking of large writes, per-chunk flush.
- The pacing arithmetic: no sleep while under budget, a burst spread over
  bytes/rate seconds, budget carried across separate write() calls, and credit
  earned back by wall-clock time passing between writes.
- Attribute/flush delegation so it is a drop-in for sys.stdout.
- install(): replaces sys.stdout, is idempotent, and leaves stderr alone.

Time is injected (fake clock + recording sleep) so the tests assert on the
*computed* delays rather than actually sleeping -- a real-time test of a 100 KB/s
throttle would take 40 seconds to prove the 4 MB case.
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
        self.chunks = []
        self.flushes = 0

    def write(self, text):
        self.chunks.append(text)
        return len(text)

    def flush(self):
        self.flushes += 1

    # Something for delegation tests to reach for.
    encoding = 'utf-8'

    def isatty(self):
        return False


def _recording_sleep(clock, slept):
    """A fake sleep that advances the fake clock, like the real one does.

    Without the advance, the writer's next-allowed time runs away from `now` and
    delays compound quadratically -- which is a property of the stub, not the code.
    """
    def sleep(seconds):
        slept.append(seconds)
        clock.advance(seconds)
    return sleep


@pytest.fixture
def wired():
    """A writer at 1000 bytes/sec with 100-byte chunks -- round numbers so the
    expected delays are obvious by inspection rather than arithmetic."""
    clock = FakeClock()
    slept = []
    stream = RecordingStream()
    writer = ThrottledWriter(stream, bytes_per_second=1000, chunk_bytes=100,
                             clock=clock, sleep=_recording_sleep(clock, slept))
    return writer, stream, clock, slept


class TestByteExactness:
    """Whatever goes in comes out, unchanged and in order."""

    def test_small_write_passes_through_untouched(self, wired):
        writer, stream, _, _ = wired
        writer.write('hello\n')
        assert stream.chunks == ['hello\n']

    def test_large_write_is_chunked_but_reassembles_exactly(self, wired):
        writer, stream, _, _ = wired
        payload = ''.join(chr(ord('a') + (i % 26)) for i in range(1050))
        writer.write(payload)
        assert ''.join(stream.chunks) == payload, "throttling must not alter bytes"
        assert len(stream.chunks) == 11, "1050 bytes / 100-byte chunks"
        assert all(len(c) <= 100 for c in stream.chunks)

    def test_write_returns_the_byte_count(self, wired):
        writer, _, _, _ = wired
        assert writer.write('x' * 250) == 250

    def test_empty_write_is_a_no_op(self, wired):
        writer, stream, _, slept = wired
        writer.write('')
        assert stream.chunks == []
        assert slept == []

    def test_flushes_every_chunk(self, wired):
        """Pacing only reaches CloudWatch if the bytes leave the process; without a
        per-chunk flush, buffering would hand the log agent one block anyway."""
        writer, stream, _, _ = wired
        writer.write('x' * 300)
        assert stream.flushes == 3


class TestPacing:
    """The arithmetic that actually protects us."""

    def test_first_write_is_immediate(self, wired):
        writer, _, _, slept = wired
        writer.write('x' * 100)
        assert slept == [], "nothing to catch up on yet"

    def test_burst_is_spread_over_bytes_over_rate(self, wired):
        """1000 bytes at 1000 B/s == 1 second of pacing, minus the first free chunk."""
        writer, _, _, slept = wired
        writer.write('x' * 1000)
        # 10 chunks: the first goes immediately, each subsequent one waits 100/1000s.
        assert len(slept) == 9
        assert sum(slept) == pytest.approx(0.9)

    def test_budget_carries_across_separate_writes(self, wired):
        """A verb printing 10 items in a loop must be paced as one stream, not
        given a fresh allowance per print()."""
        writer, _, _, slept = wired
        for _ in range(5):
            writer.write('x' * 100)
        assert sum(slept) == pytest.approx(0.4), "4 of the 5 chunks had to wait"

    def test_time_passing_earns_credit_back(self, wired):
        """Output spread out by the job's own work must not be penalized."""
        writer, _, clock, slept = wired
        writer.write('x' * 100)
        clock.advance(5.0)          # the verb spent 5s computing
        writer.write('x' * 100)
        assert slept == [], "a slow trickle of output should never sleep"

    def test_partial_credit_only_waits_for_the_remainder(self, wired):
        writer, _, clock, slept = wired
        writer.write('x' * 100)     # reserves until t+0.1
        clock.advance(0.04)
        writer.write('x' * 100)
        assert sum(slept) == pytest.approx(0.06)

    def test_tracks_totals_for_diagnostics(self, wired):
        """250 bytes in 100-byte chunks pays for the first two chunks only: a
        chunk's own airtime is reserved but never waited on, so the stream ends as
        soon as the last chunk is written. One chunk of slack over a whole run."""
        writer, _, _, _ = wired
        writer.write('x' * 250)
        assert writer.bytes_written == 250
        assert writer.seconds_slept == pytest.approx(0.2)

    def test_real_world_case_ten_max_size_items(self):
        """The case that motivated this: 10 x 400 KB items at the shipped rate.

        Asserts the cost is tens of seconds (acceptable for a bulk job that spends
        ~a minute starting up), not minutes.
        """
        clock = FakeClock()
        slept = []
        writer = ThrottledWriter(RecordingStream(), clock=clock,
                                 sleep=_recording_sleep(clock, slept))
        for _ in range(10):
            writer.write('x' * (400 * 1024) + '\n')
        assert 35 < sum(slept) < 45, f"expected ~40s of pacing, got {sum(slept):.1f}s"


class TestDelegation:
    """Must be a drop-in for sys.stdout."""

    def test_flush_reaches_the_wrapped_stream(self, wired):
        writer, stream, _, _ = wired
        before = stream.flushes
        writer.flush()
        assert stream.flushes == before + 1

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
        assert stream.chunks, "print() must reach the real stream"

    def test_is_idempotent(self, monkeypatch):
        """Installing twice would stack throttles and halve the effective rate."""
        monkeypatch.setattr(sys, 'stdout', RecordingStream())
        first = install()
        second = install()
        assert first is second
        assert sys.stdout is first

    def test_leaves_stderr_alone(self, monkeypatch):
        """Server logging writes to stderr; diagnostics must not queue behind a
        big result set."""
        stderr_before = sys.stderr
        monkeypatch.setattr(sys, 'stdout', RecordingStream())
        install()
        assert sys.stderr is stderr_before

    def test_uses_the_documented_default_rate(self):
        assert throttled_output.BYTES_PER_SECOND == 100 * 1024
        stream = RecordingStream()
        writer = ThrottledWriter(stream)
        assert writer._bytes_per_second == 100 * 1024
