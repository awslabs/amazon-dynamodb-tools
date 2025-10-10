import time


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
            msg = event['message']
            if self.partial:
                self.partial['message'] += msg
            else:
                self.partial = event.copy()


            if msg.endswith('\n'):
                reassembled.append(self.partial)
                self.partial = None  # Reset buffer

        return reassembled

    def flush(self):
        """Force flush remaining buffered logs and any partial line."""
        flushed = [e for _, e in sorted(self.buffer, key=lambda x: x[0])]
        self.buffer.clear()

        output = []
        for event in flushed:
            msg = event['message']
            if self.partial:
                self.partial['message'] += msg
            else:
                self.partial = {'timestamp': event['timestamp'], 'message': msg}

            if msg.endswith('\n'):
                output.append(self.partial)
                self.partial = None

        # Final forced flush of any dangling partial
        if self.partial:
            output.append(self.partial)
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
