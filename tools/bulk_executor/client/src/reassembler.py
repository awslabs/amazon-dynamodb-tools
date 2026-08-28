import re
import time


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

    def __init__(self, buffer_time_ms=1000):
        self.buffer_time_ms = buffer_time_ms
        self.buffer = []  # (timestamp, log_event)
        self.partial = None  # Holds a partial message line

    def process(self, new_events):
        """
        Accepts a list of raw log events (unordered, possibly split).
        Returns a list of reassembled, ordered log events.
        """
        now = time.time() * 1000
        self.buffer.extend((e['timestamp'], e) for e in new_events)

        # Sort and partition
        self.buffer.sort(key=lambda x: x[0])
        ready, self.buffer = self._partition_by_time(self.buffer, now)

        # Reassemble long lines
        reassembled = []
        for _, event in ready:
            reassembled.extend(self._absorb(event))
        return reassembled

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
        """Force flush remaining buffered logs and any partial line."""
        flushed = [e for _, e in sorted(self.buffer, key=lambda x: x[0])]
        self.buffer.clear()

        output = []
        for event in flushed:
            output.extend(self._absorb(event))

        # Final forced flush of any dangling partial
        if self.partial:
            output.extend(_split_on_record_boundaries(self.partial))
            self.partial = None

        return output

    def _partition_by_time(self, items, current_time):
        ready = []
        pending = []
        for timestamp, event in items:
            if current_time - timestamp > self.buffer_time_ms:
                ready.append((timestamp, event))
            else:
                pending.append((timestamp, event))
        return ready, pending
