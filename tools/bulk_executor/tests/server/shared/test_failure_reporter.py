"""Unit tests for server/src/python_modules/shared/failure_reporter.py.

Covers BoundedFailureReporter: counts every failure, logs only the first N per
partition, says once that it stopped, and feeds an accumulator so the driver can report
the real total.

Why this class exists (#319): worker output never reaches the user -- the client
discards every `<job_run_id>_g-*` event -- so per-item failure logging is invisible,
unpaced, and pure cost. `find`'s delete path printed one line per failed item
interpolating the whole item, across 200 partitions: up to 800 MB nobody reads. The
count cap and the line-size cap are independent, and this class owns the count half;
callers own the line by passing a key rather than an item.
"""

import pytest

from python_modules.shared.failure_reporter import (
    MAX_REPORTED_PER_PARTITION,
    BoundedFailureReporter,
)


class FakeAccumulator:
    """Stands in for a Spark accumulator, which only supports add()."""

    def __init__(self):
        self.value = 0

    def add(self, n):
        self.value += n


@pytest.fixture
def wired():
    emitted = []
    acc = FakeAccumulator()
    reporter = BoundedFailureReporter('Delete', acc, max_reported=3, emit=emitted.append)
    return reporter, acc, emitted


class TestCounting:
    """Every failure counts, whether or not it is logged."""

    def test_counts_beyond_the_log_cap(self, wired):
        reporter, acc, _ = wired
        for i in range(10):
            reporter.report({'pk': i}, 'boom')
        assert reporter.count == 10
        assert acc.value == 10, "the driver's total must be exact, not capped"

    def test_works_without_an_accumulator(self):
        """update passes none -- it already counts via failed_count."""
        emitted = []
        reporter = BoundedFailureReporter('Update condition', emit=emitted.append)
        reporter.report({'pk': 1}, 'condition not met')
        assert reporter.count == 1
        assert emitted, "still logs"


class TestLogCap:
    def test_logs_only_the_first_n(self, wired):
        reporter, _, emitted = wired
        for i in range(10):
            reporter.report({'pk': i}, 'boom')
        failures = [m for m in emitted if m.startswith('Delete failed for')]
        assert len(failures) == 3

    def test_says_once_that_it_stopped(self, wired):
        reporter, _, emitted = wired
        for i in range(10):
            reporter.report({'pk': i}, 'boom')
        notes = [m for m in emitted if 'only the first' in m]
        assert len(notes) == 1, "one note, not one per suppressed failure"
        assert '3' in notes[0], "name the cap"
        assert 'total is reported at the end' in notes[0], "point at the real total"

    def test_under_the_cap_emits_no_note(self, wired):
        reporter, _, emitted = wired
        reporter.report({'pk': 1}, 'boom')
        reporter.report({'pk': 2}, 'boom')
        assert not any('only the first' in m for m in emitted)

    def test_message_names_the_label_and_identifier(self, wired):
        reporter, _, emitted = wired
        reporter.report({'pk': 'abc'}, 'ProvisionedThroughputExceeded')
        assert emitted == ["Delete failed for {'pk': 'abc'}: ProvisionedThroughputExceeded"]


class TestDefaults:
    def test_default_cap_is_small_enough_for_800_workers(self):
        """The multiplier is the partition count: 200 partitions for find deletes, 800
        workers for update. At ~100 bytes a line the default must stay in the hundreds
        of KB, not the hundreds of MB."""
        assert MAX_REPORTED_PER_PARTITION == 10
        worst_case_bytes = 800 * MAX_REPORTED_PER_PARTITION * 100
        assert worst_case_bytes < 1_000_000, f"{worst_case_bytes:,} bytes is too much"

    def test_uses_the_default_cap_when_not_given_one(self):
        emitted = []
        reporter = BoundedFailureReporter('Delete', emit=emitted.append)
        for i in range(MAX_REPORTED_PER_PARTITION + 3):
            reporter.report({'pk': i}, 'boom')
        failures = [m for m in emitted if m.startswith('Delete failed for')]
        assert len(failures) == MAX_REPORTED_PER_PARTITION
