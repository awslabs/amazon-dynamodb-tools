"""Rate-limit console output so CloudWatch Live Tail can actually deliver it.

The console path is: driver stdout -> Glue log agent -> CloudWatch Logs -> the
client's Live Tail session. Live Tail *silently discards* events when they arrive
faster than it will deliver them: no error, no retry, and not always its own
`sessionMetadata.sampled` flag. The job still reports success, so a truncated or
corrupted answer looks exactly like a complete one.

Measured (issue #315, 2026-08-28, us-east-1):

- A real `find` printing 100,000 rows peaked at 5.3 MB/s ingested, ~2 MB/s
  delivered, and lost **79% of its rows** with `sampled: false` on all 118 updates.
- An isolated single-stream probe lost **28.8% at 3.9 MB/s**.
- It is intermittent -- an identical repeat of the lossy run delivered everything --
  so "it worked when I tried it" proves nothing.
- Nothing has ever been observed to go missing at kilobyte-per-second rates.

The nastiest case is a large item, not a large result set. A DynamoDB item can be
400 KB, so a 10-item preview is up to 4 MB emitted in one burst -- and since a
400 KB line exceeds CloudWatch's 256 KB event limit it is split across ~13 events,
so losing one chunk yields **malformed JSON in the middle of an item** rather than a
missing row (issue #321).

So instead of printing less, print slower: hold output to BYTES_PER_SECOND and let
the job take a few extra seconds. These are bulk jobs measured in minutes, and Glue
startup alone costs about a minute, so ~4 seconds per 400 KB item is cheap next to
losing the data. Ten maximum-size items take ~40s.

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

# Large writes are emitted in pieces so the pacing is smooth rather than one long
# stall followed by a flood. Kept well under CloudWatch's 256 KB per-event limit so
# a chunk is never itself split. The client's GlueLogReassembler already rejoins
# messages split mid-line -- that is exactly what it exists for -- so a chunked line
# still reaches the user as one line.
CHUNK_BYTES = 16 * 1024


class ThrottledWriter:
    """A write-through stdout wrapper that paces output to a byte budget.

    Byte-exact: the bytes written to the wrapped stream are the bytes handed in,
    unchanged and in order. Only the *timing* differs.

    Small writes never sleep while the run is under budget, so ordinary log-scale
    output is unaffected. `clock` and `sleep` are injectable for tests.
    """

    def __init__(self, stream, bytes_per_second=BYTES_PER_SECOND,
                 chunk_bytes=CHUNK_BYTES, clock=time.monotonic, sleep=time.sleep):
        self._stream = stream
        self._bytes_per_second = bytes_per_second
        self._chunk_bytes = chunk_bytes
        self._clock = clock
        self._sleep = sleep
        # Earliest time the next byte may be written. Starts unset so the very
        # first write goes out immediately.
        self._next_allowed = None
        self.bytes_written = 0
        self.seconds_slept = 0.0

    def write(self, text):
        for start in range(0, len(text), self._chunk_bytes):
            self._write_chunk(text[start:start + self._chunk_bytes])
        return len(text)

    def _write_chunk(self, chunk):
        now = self._clock()
        allowed_at = now if self._next_allowed is None else max(now, self._next_allowed)
        delay = allowed_at - now
        if delay > 0:
            self._sleep(delay)
            self.seconds_slept += delay
        self._stream.write(chunk)
        # Flush per chunk: the pacing only reaches CloudWatch if the bytes actually
        # leave the process. Without this, Python's buffering would hand the log
        # agent one big block anyway and the sleeps would buy nothing.
        self._stream.flush()
        self.bytes_written += len(chunk)
        self._next_allowed = allowed_at + len(chunk) / self._bytes_per_second

    def flush(self):
        self._stream.flush()

    def __getattr__(self, name):
        # Delegate everything else (encoding, isatty, fileno, ...) so this is a
        # drop-in for sys.stdout.
        return getattr(self._stream, name)


def install(stream=None, bytes_per_second=BYTES_PER_SECOND):
    """Route sys.stdout through a ThrottledWriter and return it.

    Only stdout: server logging writes to stderr (logging.StreamHandler's default),
    and diagnostics should never be delayed behind a big result set. Idempotent --
    installing twice would stack two throttles and halve the effective rate.
    """
    target = stream if stream is not None else sys.stdout
    if isinstance(target, ThrottledWriter):
        return target
    throttled = ThrottledWriter(target, bytes_per_second=bytes_per_second)
    sys.stdout = throttled
    return throttled
