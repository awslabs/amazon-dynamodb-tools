import itertools
import re
import time


# How far out of order CloudWatch Live Tail delivery can be.
#
# Live Tail does not deliver in order. Measured across three runs of a
# 100,000-row `find` (issue #323): 22, 22 and 19 timestamp inversions in arrival
# order, the worst arriving 856 ms behind events already delivered. Every chunk
# boundary sits mid-line, so absorbing chunks in arrival order splices the tail of
# one record onto the head of a record from somewhere else -- two chunks 35,320
# characters apart in the stored stream arrived adjacent -- and the newline between
# them is in neither chunk. That printed 5, 5 and 11 pairs of rows sharing a line,
# and with --orderby it also means printed rows can be out of order.
#
# So events are held briefly and released in delivery order rather than arrival
# order. 2000 ms is ~2.3x the worst inversion observed.
#
# The previous mechanism (buffer_time_ms, comparing wall clock against the *event*
# timestamp) could never work: Live Tail delivers events already ~1.3 s old, so
# every event was instantly releasable, nothing was ever held, and the sort never
# had two blocks to order. Releasing against a watermark of the newest event seen
# is immune to that, because it is driven by the data rather than by delivery lag.
REORDER_WINDOW_MS = 2000

# Safety valves, so a burst cannot make the hold window unbounded in memory.
# At the server's 100 KB/s pacing (shared/throttled_output.py) a 2 s window is
# ~200 KB; these only bind if pacing is absent or the window is widened.
MAX_HELD_EVENTS = 20_000
MAX_HELD_BYTES = 8 * 1024 * 1024


# A line beginning with the server log prefix "<asctime> <LEVEL>" starts a new
# logical record; any line without it is a continuation (e.g. a stack-trace
# frame). Mirrors runner._SERVER_LOG_LEVEL_RE (kept without a capture group).
# Used to re-split an emitted event that bundled more than one record together
# (e.g. an INFO line and a WARNING delivered in a single CloudWatch event with an
# internal newline), so each record is handled/colored on its own -- while a
# multi-line trace, whose continuation lines carry no prefix, stays one record.
_NEW_RECORD_RE = re.compile(r'^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3} \w+\b')


def _split_on_record_boundaries(event):
    """Split one log event into per-logical-record events.

    A new record begins only at a line matching _NEW_RECORD_RE; continuation
    lines stay attached to the preceding record. Byte-preserving: concatenating
    the returned events' messages reproduces the input message exactly. Returns
    a single-element list when there is nothing to split.
    """
    message = event['message']
    lines = message.splitlines(keepends=True)
    records = []
    current = []
    for line in lines:
        if current and _NEW_RECORD_RE.match(line):
            records.append(''.join(current))
            current = []
        current.append(line)
    if current:
        records.append(''.join(current))

    if len(records) <= 1:
        return [event]
    return [{**event, 'message': rec} for rec in records]


class GlueLogReassembler:
    """
    Buffers and reassembles Glue logs that are split and out-of-order.
    Intended to be used as a wrapper around your per-batch log handling.
    """

    def __init__(self, reorder_window_ms=REORDER_WINDOW_MS,
                 max_held_events=MAX_HELD_EVENTS, max_held_bytes=MAX_HELD_BYTES,
                 clock=time.monotonic):
        self.reorder_window_ms = reorder_window_ms
        self.max_held_events = max_held_events
        self.max_held_bytes = max_held_bytes
        self._clock = clock
        self.held = []  # kept sorted by _order_key
        self.partial = None  # Holds a partial message line
        self._arrivals = itertools.count()
        self._watermark = None  # newest ingestionTime seen
        self._held_bytes = 0

    @staticmethod
    def _order_key(item):
        """Delivery order: ingestionTime, then timestamp, then arrival.

        ingestionTime is the only field that recovers the true order -- sorting by
        timestamp alone still left 3, 4 and 7 merged rows across the three measured
        runs, while this key left none. It is not guaranteed unique (a single
        PutLogEvents batch can share it), hence the timestamp and arrival
        tie-breakers, which at least make the result deterministic.
        """
        ingestion, timestamp, arrival = item[0], item[1], item[2]
        return (ingestion, timestamp, arrival)

    def process(self, new_events):
        """
        Accepts a list of raw log events (unordered, possibly split).
        Returns a list of reassembled, ordered log events.
        """
        arrived_at = self._clock()
        for event in new_events:
            # Events built by our own tests and older code paths may carry no
            # ingestionTime; falling back to the timestamp keeps them ordered
            # sensibly rather than sorting them all to the front.
            ingestion = event.get('ingestionTime', event['timestamp'])
            self.held.append(
                (ingestion, event['timestamp'], next(self._arrivals), arrived_at, event))
            self._held_bytes += len(event['message'])
            if self._watermark is None or ingestion > self._watermark:
                self._watermark = ingestion

        self.held.sort(key=self._order_key)

        reassembled = []
        for _i, _t, _a, _arrived, event in self._release(arrived_at):
            reassembled.extend(self._absorb(event))
        return reassembled

    def _release(self, now):
        """Take everything a later arrival can no longer overtake.

        Three ways an event becomes releasable:

        1. **Watermark.** Its ingestionTime is more than reorder_window_ms behind
           the newest we have seen, so anything still in flight sorts after it.
           This is the correctness rule.
        2. **Waited out.** It has been held for the window in real time. Liveness
           only: without it, a stream that goes quiet would sit on its last events
           until the next arrival or flush().
        3. **Over capacity.** Bounded memory beats perfect ordering.
        """
        cutoff = None if self._watermark is None else self._watermark - self.reorder_window_ms
        keep_from = 0
        for ingestion, _ts, _arrival, arrived_at, event in self.held:
            behind_watermark = cutoff is not None and ingestion <= cutoff
            waited_out = (now - arrived_at) * 1000 >= self.reorder_window_ms
            over_capacity = (len(self.held) - keep_from > self.max_held_events
                             or self._held_bytes > self.max_held_bytes)
            if behind_watermark or waited_out or over_capacity:
                keep_from += 1
                self._held_bytes -= len(event['message'])
            else:
                break  # sorted, so nothing after this is releasable either

        released, self.held = self.held[:keep_from], self.held[keep_from:]
        return released

    def _absorb(self, event):
        """Fold one event into self.partial, returning any completed records.

        Shared by process() and flush(). It used to be written out twice, and the
        two copies disagreed: flush() rebuilt the partial as
        {'timestamp', 'message'}, dropping logGroupIdentifier and logStreamName,
        which _pretty_print_log_event indexes directly. Nothing surfaces that
        today -- LiveTail delivers events already older than buffer_time_ms, so
        _partition_by_time never holds any and flush() has nothing to drain. But
        the mechanism is intact, so the dead copy was one AWS timing change away
        from mattering. One copy can't drift from itself.
        """
        emitted = []
        msg = event['message']

        if self.partial:
            self.partial['message'] += msg
        else:
            self.partial = event.copy()

        if msg.endswith('\n'):
            emitted.extend(_split_on_record_boundaries(self.partial))
            self.partial = None  # Reset buffer

        return emitted

    def flush(self):
        """Force flush everything still held, in order, plus any partial line."""
        # held is kept sorted by process(), so no sort is needed here -- and a
        # redundant one would be code no test could distinguish.
        held, self.held = self.held, []
        self._held_bytes = 0

        output = []
        for _i, _t, _a, _arrived, event in held:
            output.extend(self._absorb(event))

        # Final forced flush of any dangling partial
        if self.partial:
            output.extend(_split_on_record_boundaries(self.partial))
            self.partial = None

        return output
