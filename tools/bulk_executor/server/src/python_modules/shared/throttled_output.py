"""Rate-limit console output so CloudWatch Live Tail can actually deliver it.

The console path is: driver stdout -> Glue log agent -> CloudWatch Logs -> the
client's Live Tail session. Live Tail *silently discards* events when they arrive
faster than it will deliver them: no error, no retry, and not always its own
`sessionMetadata.sampled` flag. The job still reports success, so a truncated or
corrupted answer looks exactly like a complete one.

Live Tail has **two** limits, and fixing one can breach the other:

  bytes   silent, unflagged loss measured from ~1 MB/s upward (issue #315)
  events  more than 500 matched in one second and CloudWatch delivers 500,
          discarding the rest -- this one at least sets `sampled`

Measured (issue #315, 2026-08-28, us-east-1):

- A real `find` printing 100,000 rows peaked at 5.3 MB/s ingested, ~2 MB/s
  delivered, and lost **79% of its rows** with `sampled: false` throughout.
- An isolated single-stream probe lost **28.8% at 3.9 MB/s**.
- Loss is intermittent -- an identical repeat of the lossy run delivered
  everything -- so "it worked when I tried it" proves nothing.
- Nothing has ever been observed to go missing at kilobyte-per-second rates.

The nastiest case is a large item, not a large result set. A DynamoDB item can be
400 KB, so a 10-item preview is up to 4 MB in one burst -- and since a 400 KB line
exceeds CloudWatch's 256 KB event limit it is split across ~13 events, so losing
one chunk yields **malformed JSON in the middle of an item** rather than a missing
row (issue #321).

So instead of printing less, print slower: pass every write straight through, then
pause if that write put us over BYTES_PER_SECOND. Ten maximum-size items cost ~40
extra seconds, which is cheap against the ~minute Glue spends starting up.

WE NEVER FLUSH. That is the one rule here, and it is not an oversight:

    An earlier version flushed after every write, and print(x) is two writes, so
    every line was flushed. Glue's log agent then emitted one CloudWatch event *per
    line*: a 100,000-row find became 100,043 events at 710/sec (822 peak), sat above
    the 500-events/sec threshold for 134 of 141 seconds, and CloudWatch discarded
    48% of the rows. The byte pacing was working perfectly; the event rate ate us.

Left alone, the pipeline batches output into large events by itself -- measured at
78 events/sec for a 100,000-row find, ~6x under the event limit. Pacing the writes
is enough; deciding when bytes leave the process is the log agent's job, not ours.

Two things deliberately NOT done, each tried and measured first:

- **No buffering or coalescing.** A version that gathered writes into 32 KB blocks
  and flushed them worked, but needed a pending queue, a max-hold timer, an atexit
  hook and a tolerant shutdown flush -- machinery that produced two bugs of its own.
- **No splitting of large writes.** A 400 KB item therefore reaches the pipe in one
  go. That is fine: it is ~400 KB inside a one-second window (under the ~1 MB/s where
  loss starts) and ~13 events at once (against a limit of 500), and the pause that
  follows separates it from the next item. Splitting it into paced pieces bought no
  measurable improvement -- merged lines stayed at 1-2 per 100,000 rows either way,
  because that artifact comes from the client's reassembler, not from us.

**This is not a licence to print unbounded volume.** Throttling converts a data-loss
problem into a wall-clock problem: at 100 KB/s, 100 MB of output would take ~17
minutes and eat the (default 60 minute) Glue timeout. Per-verb caps still matter --
see `ai_lint/rules/console_output_rate.md` and issues #319 / #321.
"""

import sys
import time

# Roughly 10x below the lowest rate at which loss has been observed (~1 MB/s), and
# ~40x below the burst a large-item preview produces today.
BYTES_PER_SECOND = 100 * 1024

# Don't sleep for less than this; let the debt accumulate and pay it in one longer
# sleep instead. A 100,000-row find is 200,000 writes, and sleeping ~1 ms after each
# one both overshoots badly (short sleeps have poor resolution, and the error is all
# in one direction) and wastes the time on scheduler overhead. Fewer, larger sleeps
# keep the delivered rate close to the nominal one.
MIN_SLEEP_SECONDS = 0.05


class ThrottledWriter:
    """A pass-through stdout wrapper that pauses after writing too much, too fast.

    Byte-exact: the wrapped stream receives exactly the bytes handed in, in order,
    unsplit, with nothing buffered here. Only the *timing* differs -- and flushing is
    left entirely to the stream, which is what keeps the event count low.

    The write happens first and the pause second, so output is never held back
    waiting for a pause to end. While a run is under budget nothing sleeps at all, so
    ordinary log-scale output is untouched. `clock` and `sleep` are injectable for
    tests.
    """

    def __init__(self, stream, bytes_per_second=BYTES_PER_SECOND,
                 min_sleep_seconds=MIN_SLEEP_SECONDS,
                 clock=time.monotonic, sleep=time.sleep):
        self._stream = stream
        self._bytes_per_second = bytes_per_second
        self._min_sleep_seconds = min_sleep_seconds
        self._clock = clock
        self._sleep = sleep
        # Time at which the byte budget allows the next write. None until the first
        # one, so output starts immediately.
        self._next_allowed = None
        self.bytes_written = 0
        self.seconds_slept = 0.0
        self.sleeps = 0

    def write(self, text):
        # Straight through, and deliberately no flush -- see the module docstring.
        written = self._stream.write(text)
        if text:
            self._pace(len(text))
        return written

    def _pace(self, byte_count):
        self.bytes_written += byte_count
        now = self._clock()
        # Time spent between writes is credit earned: a verb whose output is naturally
        # spread out never pays anything.
        if self._next_allowed is None or now > self._next_allowed:
            self._next_allowed = now
        self._next_allowed += byte_count / self._bytes_per_second

        debt = self._next_allowed - now
        if debt > self._min_sleep_seconds:
            self._sleep(debt)
            self.seconds_slept += debt
            self.sleeps += 1
            # Deliberately not re-reading the clock: an oversleeping OS would
            # otherwise push the schedule out permanently, and that drift made an
            # earlier version deliver 58 KB/s against a 100 KB/s setting.

    def __getattr__(self, name):
        # Everything else -- flush, encoding, isatty, fileno, ... -- goes to the real
        # stream untouched. In particular print(flush=True) still flushes for real; we
        # simply never do it on our own initiative.
        return getattr(self._stream, name)


def install(stream=None, bytes_per_second=BYTES_PER_SECOND):
    """Route sys.stdout through a ThrottledWriter and return it.

    Only stdout: server logging writes to stderr (logging.StreamHandler's default),
    and diagnostics should never be delayed behind a big result set. Idempotent --
    installing twice would stack two throttles and halve the effective rate.

    No shutdown hook is needed, because nothing is ever held back here.
    """
    target = stream if stream is not None else sys.stdout
    if isinstance(target, ThrottledWriter):
        return target
    throttled = ThrottledWriter(target, bytes_per_second=bytes_per_second)
    sys.stdout = throttled
    return throttled
