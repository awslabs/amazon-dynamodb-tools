"""Unit tests for GlueLogReassembler.

Covers `client/src/reassembler.py`:

- __init__: default and custom reorder window, empty held/partial state
- _order_key: ordering by ingestionTime, then timestamp, then arrival; fallback
  when ingestionTime is absent
- _release: the three ways an event becomes releasable -- behind the watermark
  (correctness), waited out in real time (liveness), over capacity (memory)
- process: out-of-order delivery corrected, reassembly of split lines, partial
  carry-over across calls, byte preservation
- flush: drains held events in order, completes or emits a dangling partial,
  clears state, preserves every key an event carried
- _split_on_record_boundaries: an emitted event bundling multiple logical
  records is split per record, while a multi-line trace stays whole

**Why the ordering tests exist (issue #323).** Live Tail does not deliver in
order. Measured across three runs of a 100,000-row `find`: 22, 22 and 19
timestamp inversions in arrival order, worst 856 ms behind events already
delivered. Because every chunk boundary sits mid-line, absorbing in arrival
order splices the tail of one record onto the head of a record from elsewhere,
and the newline between them is in neither chunk -- printing 5, 5 and 11 pairs
of rows sharing a line, and (with --orderby) rows out of order.

The previous mechanism could not have caught it: `buffer_time_ms` compared the
wall clock against the *event* timestamp, and Live Tail delivers events already
~1.3 s old, so every event was instantly releasable and nothing was ever held.
Tests here therefore drive an injected clock and set `ingestionTime`
explicitly; a test that only checks "old events come out" would pass against
the broken code.

Style notes:
- The clock is injected (`clock=...`) rather than patched, since ordering is
  driven by data (ingestionTime watermark) and only liveness uses wall time.
- Timestamps and ingestionTime are milliseconds (CloudWatch convention); the
  injected clock is in seconds (`time.monotonic` convention).
"""

import pytest

import reassembler


def _evt(ts, msg, ing=None):
    """A log event in CloudWatch/Glue shape. ingestionTime defaults to ts."""
    return {'timestamp': ts, 'message': msg,
            'ingestionTime': ts if ing is None else ing}


class _Clock:
    """A monotonic clock, in seconds, that only moves when told to."""

    def __init__(self, now=100.0):
        self.now = now

    def __call__(self):
        return self.now

    def advance(self, seconds):
        self.now += seconds


def _reassembler(window_ms=2000, clock=None, **kw):
    return reassembler.GlueLogReassembler(
        reorder_window_ms=window_ms, clock=clock or _Clock(), **kw)


# --- __init__ ---------------------------------------------------------------

class TestInit:

    def test_default_window_is_two_seconds(self):
        """2000 ms is ~2.3x the worst inversion measured (856 ms)."""
        r = reassembler.GlueLogReassembler()
        assert r.reorder_window_ms == 2000
        assert reassembler.REORDER_WINDOW_MS == 2000

    def test_custom_window(self):
        assert _reassembler(window_ms=5000).reorder_window_ms == 5000

    def test_starts_empty(self):
        r = reassembler.GlueLogReassembler()
        assert r.held == []
        assert r.partial is None


# --- ordering (issue #323) --------------------------------------------------

class TestOrdering:
    """The heart of the fix: delivery order is not stream order."""

    def test_out_of_order_arrival_is_corrected(self):
        r = _reassembler()
        r.process([_evt(2000, 'second\n', ing=2000)])
        r.process([_evt(1000, 'first\n', ing=1000)])
        out = [e['message'] for e in r.flush()]
        assert out == ['first\n', 'second\n']

    def test_regression_the_real_bug_shape(self):
        """A faithful minimum of what was measured in production.

        Chunk A ends mid-line (no newline). Its true continuation is B, which
        begins with the newline. C is a later chunk starting with '{'. Live Tail
        delivered C before B -- an inversion -- so absorbing in arrival order
        glued C onto A and produced '}{': two rows sharing one line.
        """
        a = _evt(1000, '{"pk":"a"}', ing=1000)
        b = _evt(1100, '\n{"pk":"b"}\n', ing=1100)
        c = _evt(1200, '{"pk":"c"}\n', ing=1200)

        r = _reassembler()
        for event in (a, c, b):          # note: c arrives before b
            r.process([event])
        stream = ''.join(e['message'] for e in r.flush())

        assert '}{' not in stream, f"rows merged onto one line: {stream!r}"
        assert stream == '{"pk":"a"}\n{"pk":"b"}\n{"pk":"c"}\n'

    def test_ingestion_time_beats_timestamp(self):
        """Sorting by timestamp alone still left 3, 4 and 7 merged rows across the
        three measured runs; ingestionTime is what recovers the true order.

        The two keys deliberately DISAGREE here: ordering by timestamp puts these
        the wrong way round, so a regression to a timestamp-only key fails.
        """
        r = _reassembler()
        r.process([_evt(9000, 'ingested-first\n', ing=1000)])
        r.process([_evt(1000, 'ingested-second\n', ing=2000)])
        out = [e['message'] for e in r.flush()]
        assert out == ['ingested-first\n', 'ingested-second\n'], (
            "ingestionTime must win: by timestamp these sort the other way")

    def test_ties_fall_back_to_timestamp_then_arrival(self):
        """A single PutLogEvents batch can share ingestionTime, so ties must at
        least be deterministic."""
        r = _reassembler()
        r.process([_evt(20, 'b\n', ing=500), _evt(10, 'a\n', ing=500)])
        assert [e['message'] for e in r.flush()] == ['a\n', 'b\n']

        r2 = _reassembler()
        r2.process([_evt(10, 'first\n', ing=500), _evt(10, 'second\n', ing=500)])
        assert [e['message'] for e in r2.flush()] == ['first\n', 'second\n'], \
            "identical keys keep arrival order"

    def test_release_through_process_is_ordered_not_arrival_ordered(self):
        """The release path itself must emit in order, not just flush().

        Everything here is released by the watermark inside process(), so no
        flush() sort can rescue it -- this is the case that matters in production,
        where flush() only runs once at teardown.
        """
        r = _reassembler(window_ms=1000)
        emitted = []
        for event in (_evt(3000, 'third\n', ing=3000),
                      _evt(1000, 'first\n', ing=1000),
                      _evt(2000, 'second\n', ing=2000),
                      _evt(9000, 'trigger\n', ing=9000)):
            emitted += [e['message'] for e in r.process([event])]
        emitted += [e['message'] for e in r.flush()]
        # Each is released as soon as the watermark makes it safe, so they come out
        # across several calls -- but the sequence must be the stream's, not arrival's.
        assert emitted == ['first\n', 'second\n', 'third\n', 'trigger\n'], f"got {emitted}"

    def test_missing_ingestion_time_falls_back_to_timestamp(self):
        """Events built without ingestionTime must not all sort to the front."""
        r = _reassembler()
        r.process([{'timestamp': 2000, 'message': 'second\n'}])
        r.process([{'timestamp': 1000, 'message': 'first\n'}])
        assert [e['message'] for e in r.flush()] == ['first\n', 'second\n']


# --- _release ---------------------------------------------------------------

class TestRelease:

    def test_held_until_the_watermark_moves_past_the_window(self):
        r = _reassembler(window_ms=2000)
        assert r.process([_evt(1000, 'early\n')]) == [], "nothing to compare against yet"
        assert len(r.held) == 1

        assert r.process([_evt(2500, 'mid\n')]) == [], "watermark only 1500ms ahead"
        out = r.process([_evt(3100, 'late\n')])
        assert [e['message'] for e in out] == ['early\n'], \
            "watermark 2100ms past 'early' releases exactly it"

    def test_waited_out_releases_without_a_new_watermark(self):
        """Liveness: a stream that goes quiet must not sit on its last events."""
        clock = _Clock()
        r = _reassembler(window_ms=2000, clock=clock)
        assert r.process([_evt(1000, 'only\n')]) == []
        clock.advance(2.0)
        assert [e['message'] for e in r.process([])] == ['only\n']

    def test_over_capacity_by_count_releases_oldest_first(self):
        r = _reassembler(max_held_events=3)
        out = []
        for i in range(6):
            out += [e['message'] for e in r.process([_evt(1000 + i, f'row{i}\n')])]
        assert out == ['row0\n', 'row1\n', 'row2\n'], f"got {out}"
        assert len(r.held) == 3

    def test_over_capacity_by_bytes_releases(self):
        r = _reassembler(max_held_bytes=20)
        assert r.process([_evt(1000, 'x' * 10 + '\n')]) == []
        out = r.process([_evt(1001, 'y' * 30 + '\n')])
        assert out, "exceeding the byte cap must release rather than grow"

    def test_capacity_accounting_shrinks_on_release(self):
        clock = _Clock()
        r = _reassembler(clock=clock)
        r.process([_evt(1000, 'abc\n')])
        assert r._held_bytes == 4
        clock.advance(2.0)
        r.process([])
        assert r._held_bytes == 0


# --- process ----------------------------------------------------------------

class TestProcess:

    def test_reassembles_split_line_within_one_call(self):
        clock = _Clock()
        r = _reassembler(clock=clock)
        r.process([_evt(5000, 'part-a-'), _evt(5001, 'part-b\n')])
        clock.advance(2.0)
        out = r.process([])
        assert [e['message'] for e in out] == ['part-a-part-b\n']

    def test_partial_carries_across_process_calls(self):
        clock = _Clock()
        r = _reassembler(clock=clock)
        r.process([_evt(5000, 'prefix-')])
        clock.advance(2.0)
        assert r.process([]) == []
        assert r.partial is not None and r.partial['message'] == 'prefix-'

        r.process([_evt(6000, 'suffix\n')])
        clock.advance(2.0)
        out = r.process([])
        assert [e['message'] for e in out] == ['prefix-suffix\n']
        assert r.partial is None

    def test_empty_input(self):
        assert _reassembler().process([]) == []

    def test_byte_preserving_across_a_shuffled_stream(self):
        """Whatever the arrival order, the emitted bytes are the stream's bytes."""
        chunks = ['{"a":1}\n', '{"b":', '2}\n{"c":3}', '\n{"d":4}\n']
        events = [_evt(1000 + i, c, ing=1000 + i) for i, c in enumerate(chunks)]
        shuffled = [events[2], events[0], events[3], events[1]]

        r = _reassembler()
        for e in shuffled:
            r.process([e])
        assert ''.join(e['message'] for e in r.flush()) == ''.join(chunks)


# --- flush ------------------------------------------------------------------

class TestFlush:

    def test_empty_state_returns_empty(self):
        assert reassembler.GlueLogReassembler().flush() == []

    def test_drains_held_events_in_order(self):
        r = _reassembler(window_ms=10_000_000)
        r.process([_evt(7000, 'second\n', ing=7000), _evt(3000, 'first\n', ing=3000)])
        assert [e['message'] for e in r.flush()] == ['first\n', 'second\n']
        assert r.held == []

    def test_dangling_partial_emitted_without_newline(self):
        clock = _Clock()
        r = _reassembler(clock=clock)
        r.process([_evt(5000, 'incomplete-msg')])
        clock.advance(2.0)
        r.process([])
        assert r.partial is not None

        out = r.flush()
        assert [e['message'] for e in out] == ['incomplete-msg']
        assert r.partial is None

    def test_completes_partial_with_held_terminator(self):
        clock = _Clock()
        r = _reassembler(clock=clock)
        r.process([_evt(5000, 'lead-')])
        clock.advance(2.0)
        r.process([])
        assert r.partial is not None

        r.process([_evt(6000, 'tail\n')])       # still held: watermark hasn't moved on
        out = r.flush()
        assert [e['message'] for e in out] == ['lead-tail\n']
        assert r.partial is None
        assert r.held == []

    def test_clears_held_and_byte_count(self):
        r = _reassembler(window_ms=10_000_000)
        r.process([_evt(5000, 'msg\n')])
        r.flush()
        assert r.held == []
        assert r._held_bytes == 0


# --- record-boundary splitting ----------------------------------------------

class TestSplitOnRecordBoundaries:
    """A completed event that merged multiple logical records is re-split so each
    record is emitted separately, while multi-line traces stay whole."""

    _WARN = '2026-08-04 08:08:15,393 WARNING [MainThread] root - too slow'
    _INFO_NO_PREFIX = '[before] Max read rate set to specified limit: 20'

    def test_helper_splits_info_then_warning(self):
        event = {'timestamp': 1, 'message': f'{self._INFO_NO_PREFIX}\n{self._WARN}\n'}
        result = reassembler._split_on_record_boundaries(event)
        assert [e['message'] for e in result] == [
            f'{self._INFO_NO_PREFIX}\n',
            f'{self._WARN}\n',
        ]

    def test_helper_keeps_multiline_trace_as_one_record(self):
        trace = (
            '2026-08-04 04:17:36,000 ERROR root - Boom\n'
            'Traceback (most recent call last):\n'
            '  File "x.py", line 1\n'
            'ValueError: boom\n'
        )
        event = {'timestamp': 1, 'message': trace}
        result = reassembler._split_on_record_boundaries(event)
        assert len(result) == 1
        assert result[0]['message'] == trace

    def test_helper_single_record_unchanged(self):
        event = {'timestamp': 1, 'message': f'{self._WARN}\n'}
        assert reassembler._split_on_record_boundaries(event) == [event]

    def test_helper_no_prefix_lines_stay_one_record(self):
        event = {'timestamp': 1, 'message': 'line one\nline two\nline three\n'}
        assert reassembler._split_on_record_boundaries(event) == [event]

    def test_helper_is_byte_preserving(self):
        original = f'{self._INFO_NO_PREFIX}\n{self._WARN}\ntail-continuation\n'
        event = {'timestamp': 7, 'message': original}
        result = reassembler._split_on_record_boundaries(event)
        assert ''.join(e['message'] for e in result) == original
        assert all(e['timestamp'] == 7 for e in result)

    def test_process_splits_merged_records(self):
        clock = _Clock()
        r = _reassembler(clock=clock)
        r.process([_evt(5000, f'{self._INFO_NO_PREFIX}\n{self._WARN}\n')])
        clock.advance(2.0)
        result = r.process([])
        assert [e['message'] for e in result] == [
            f'{self._INFO_NO_PREFIX}\n',
            f'{self._WARN}\n',
        ]

    def test_flush_splits_dangling_merged_records(self):
        r = _reassembler(window_ms=10_000_000)
        r.process([_evt(5000, f'{self._INFO_NO_PREFIX}\n{self._WARN}')])
        result = r.flush()
        assert [e['message'] for e in result] == [
            f'{self._INFO_NO_PREFIX}\n',
            self._WARN,
        ]


class TestFlushPreservesEventKeys:
    """flush() and process() must fold events identically.

    They were once written out twice and disagreed: flush() rebuilt the partial as
    {'timestamp', 'message'}, dropping logGroupIdentifier and logStreamName, which
    _pretty_print_log_event indexes directly -- a KeyError swallowed by the broad
    handler in _watch_log_group, abandoning the flush. Now reachable in normal
    operation, because events really are held (that is the point of the reorder
    window), so this is a live guarantee rather than a latent one.
    """

    def test_flush_and_process_fold_an_event_identically(self):
        event = {
            'timestamp': 5000,
            'ingestionTime': 5000,
            'message': 'tail of the job output\n',
            'logStreamName': 'jr_x',
            'logGroupIdentifier': '123456789012:/aws-glue/jobs/output',
        }

        r = _reassembler(window_ms=10_000_000)
        assert r.process([dict(event)]) == [], "must still be held"
        assert len(r.held) == 1, "otherwise this test proves nothing"

        out = r.flush()

        assert len(out) == 1
        assert set(out[0]) == set(event), (
            "flush() must preserve every key process() would -- "
            "_pretty_print_log_event indexes logGroupIdentifier directly"
        )
