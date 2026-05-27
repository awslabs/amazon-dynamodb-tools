"""Unit tests for GlueLogReassembler.

Covers `client/src/reassembler.py`:
- __init__: default and custom buffer_time_ms, empty buffer/partial state
- process: timestamp-sorted ingestion, time-based partition (ready vs pending),
  reassembly of multi-fragment lines, newline-terminated emission, partial
  carry-over across calls
- flush: returns sorted-by-timestamp dump, completes partial line if newline
  arrives, emits dangling partial without newline, clears internal state
- _partition_by_time: items older than buffer_time_ms are ready, fresher
  items remain pending

Style notes:
- `time.time()` is patched at the reassembler module namespace
  (`reassembler.time.time`) because the source binds `import time` at module
  scope. Setting `now` deterministically lets us drive the time partition
  cutoff without flakiness.
- Timestamps in events are milliseconds (Glue convention); `now` is
  computed as `time.time() * 1000` in the source.
"""

from unittest.mock import patch

import pytest

import reassembler


def _evt(ts, msg):
    """Build a log event dict in CloudWatch/Glue shape."""
    return {'timestamp': ts, 'message': msg}


# --- __init__ ---------------------------------------------------------------

class TestInit:
    """Tests for GlueLogReassembler.__init__ (lines 10-13)."""

    def test_default_buffer_time(self):
        r = reassembler.GlueLogReassembler()
        assert r.buffer_time_ms == 1000

    def test_custom_buffer_time(self):
        r = reassembler.GlueLogReassembler(buffer_time_ms=5000)
        assert r.buffer_time_ms == 5000

    def test_starts_with_empty_buffer_and_no_partial(self):
        r = reassembler.GlueLogReassembler()
        assert r.buffer == []
        assert r.partial is None


# --- _partition_by_time -----------------------------------------------------

class TestPartitionByTime:
    """Tests for _partition_by_time (lines 67-75)."""

    def test_old_items_are_ready(self):
        r = reassembler.GlueLogReassembler(buffer_time_ms=1000)
        # current_time=10000, items older than 1s ago (ts < 9000) are ready
        items = [(8000, _evt(8000, 'old\n')), (8500, _evt(8500, 'mid\n'))]
        ready, pending = r._partition_by_time(items, 10000)
        assert len(ready) == 2
        assert pending == []

    def test_fresh_items_remain_pending(self):
        r = reassembler.GlueLogReassembler(buffer_time_ms=1000)
        # ts=9500, current=10000, diff=500 < 1000 → pending
        items = [(9500, _evt(9500, 'fresh\n'))]
        ready, pending = r._partition_by_time(items, 10000)
        assert ready == []
        assert len(pending) == 1

    def test_split_around_threshold(self):
        r = reassembler.GlueLogReassembler(buffer_time_ms=1000)
        items = [
            (5000, _evt(5000, 'old\n')),
            (9500, _evt(9500, 'fresh\n')),
        ]
        ready, pending = r._partition_by_time(items, 10000)
        assert len(ready) == 1 and ready[0][0] == 5000
        assert len(pending) == 1 and pending[0][0] == 9500


# --- process ----------------------------------------------------------------

class TestProcess:
    """Tests for process (lines 15-41)."""

    def test_emits_complete_line_after_buffer_window(self):
        """Newline-terminated message older than buffer_time_ms gets emitted."""
        r = reassembler.GlueLogReassembler(buffer_time_ms=1000)
        with patch.object(reassembler.time, 'time', return_value=10):  # now=10000ms
            result = r.process([_evt(5000, 'hello\n')])

        assert len(result) == 1
        assert result[0]['message'] == 'hello\n'
        assert result[0]['timestamp'] == 5000

    def test_buffers_fresh_events(self):
        """Events newer than buffer window stay in buffer, none returned."""
        r = reassembler.GlueLogReassembler(buffer_time_ms=1000)
        with patch.object(reassembler.time, 'time', return_value=10):  # now=10000ms
            result = r.process([_evt(9800, 'too fresh\n')])

        assert result == []
        assert len(r.buffer) == 1

    def test_sorts_out_of_order_events(self):
        """Buffer is sorted by timestamp before partitioning."""
        r = reassembler.GlueLogReassembler(buffer_time_ms=1000)
        with patch.object(reassembler.time, 'time', return_value=20):  # now=20000ms
            # Inputs out of order; both old enough to be ready
            result = r.process([
                _evt(8000, 'second\n'),
                _evt(5000, 'first\n'),
            ])

        assert [e['message'] for e in result] == ['first\n', 'second\n']

    def test_reassembles_split_line_within_one_call(self):
        """Two fragments without trailing newline + final newline → one merged line."""
        r = reassembler.GlueLogReassembler(buffer_time_ms=1000)
        with patch.object(reassembler.time, 'time', return_value=10):  # now=10000ms
            result = r.process([
                _evt(5000, 'part-a-'),
                _evt(5500, 'part-b\n'),
            ])

        assert len(result) == 1
        assert result[0]['message'] == 'part-a-part-b\n'

    def test_partial_carries_across_process_calls(self):
        """Partial line started in one call completes in the next."""
        r = reassembler.GlueLogReassembler(buffer_time_ms=1000)

        # First call: only the prefix arrives (no newline yet)
        with patch.object(reassembler.time, 'time', return_value=10):
            result1 = r.process([_evt(5000, 'prefix-')])
        assert result1 == []
        assert r.partial is not None
        assert r.partial['message'] == 'prefix-'

        # Second call: the suffix arrives with newline
        with patch.object(reassembler.time, 'time', return_value=11):  # now=11000ms
            result2 = r.process([_evt(6000, 'suffix\n')])

        assert len(result2) == 1
        assert result2[0]['message'] == 'prefix-suffix\n'
        assert r.partial is None

    def test_pending_events_not_consumed(self):
        """Mix of ready + pending: only ready ones flow through; rest stay buffered."""
        r = reassembler.GlueLogReassembler(buffer_time_ms=1000)
        with patch.object(reassembler.time, 'time', return_value=10):  # now=10000ms
            result = r.process([
                _evt(5000, 'ready\n'),
                _evt(9800, 'fresh\n'),
            ])

        assert len(result) == 1
        assert result[0]['message'] == 'ready\n'
        assert len(r.buffer) == 1
        assert r.buffer[0][0] == 9800

    def test_empty_input(self):
        """Empty event list returns empty result."""
        r = reassembler.GlueLogReassembler()
        with patch.object(reassembler.time, 'time', return_value=10):
            assert r.process([]) == []


# --- flush ------------------------------------------------------------------

class TestFlush:
    """Tests for flush (lines 43-65)."""

    def test_empty_state_returns_empty(self):
        """Flush with no buffer and no partial returns []."""
        r = reassembler.GlueLogReassembler()
        assert r.flush() == []

    def test_flushes_buffered_complete_line(self):
        """Buffered complete line emerges via flush regardless of buffer_time_ms."""
        r = reassembler.GlueLogReassembler(buffer_time_ms=1000000)  # absurdly long
        with patch.object(reassembler.time, 'time', return_value=10):
            r.process([_evt(5000, 'queued\n')])

        # Was pending (1ms ago < 1000s); flush should still emit
        result = r.flush()

        assert len(result) == 1
        assert result[0]['message'] == 'queued\n'
        assert r.buffer == []

    def test_flushes_in_timestamp_order(self):
        """Flush sorts the buffer by timestamp before processing."""
        r = reassembler.GlueLogReassembler(buffer_time_ms=1000000)
        with patch.object(reassembler.time, 'time', return_value=10):
            r.process([
                _evt(7000, 'second\n'),
                _evt(3000, 'first\n'),
            ])

        result = r.flush()
        assert [e['message'] for e in result] == ['first\n', 'second\n']

    def test_dangling_partial_emitted_without_newline(self):
        """If a partial line never got its newline, flush emits it anyway (lines 61-63)."""
        r = reassembler.GlueLogReassembler(buffer_time_ms=1000)

        # Make a partial via process()
        with patch.object(reassembler.time, 'time', return_value=10):
            r.process([_evt(5000, 'incomplete-msg')])
        assert r.partial is not None

        # No more buffered events; flush should still emit the dangling partial.
        result = r.flush()

        assert len(result) == 1
        assert result[0]['message'] == 'incomplete-msg'
        assert r.partial is None

    def test_flush_completes_partial_with_buffered_newline(self):
        """Lines 51-58: flush of a partial + buffered terminator concatenates them."""
        r = reassembler.GlueLogReassembler(buffer_time_ms=1000)

        # Establish a partial first using a short buffer so the leading
        # fragment ages out and becomes 'ready' inside process().
        with patch.object(reassembler.time, 'time', return_value=10):  # now=10000ms
            r.process([_evt(5000, 'lead-')])
        assert r.partial is not None

        # Inflate the buffer window so subsequent events stay pending
        # — they should only be drained by flush.
        r.buffer_time_ms = 1000000
        with patch.object(reassembler.time, 'time', return_value=10):
            r.process([_evt(6000, 'tail\n')])

        result = r.flush()

        assert len(result) == 1
        assert result[0]['message'] == 'lead-tail\n'
        assert r.partial is None
        assert r.buffer == []

    def test_flush_clears_buffer(self):
        """After flush, buffer is empty."""
        r = reassembler.GlueLogReassembler(buffer_time_ms=1000000)
        with patch.object(reassembler.time, 'time', return_value=10):
            r.process([_evt(5000, 'msg\n')])

        r.flush()
        assert r.buffer == []

    def test_flush_no_partial_starts_fresh(self):
        """Lines 53-54: when partial is None, flush copies event to start a new partial."""
        r = reassembler.GlueLogReassembler(buffer_time_ms=1000000)
        with patch.object(reassembler.time, 'time', return_value=10):
            r.process([_evt(5000, 'standalone\n')])  # buffered, fresh

        result = r.flush()
        assert len(result) == 1
        assert result[0]['message'] == 'standalone\n'
        assert result[0]['timestamp'] == 5000
