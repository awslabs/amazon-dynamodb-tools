"""Unit tests for server/src/python_modules/shared/worker_errors.py.

Covers classify_failure (understood AWS codes, AWS's own authorization wording,
BulkExecutorError passthrough, everything else unexpected), record_worker_failure
(message shape, the understood= override, whether a traceback is captured),
record_understood_failure, and raise_first_worker_error (nothing recorded, understood,
unexpected, and the bare-string fallback).

The visible contract these protect: an understood failure raises BulkExecutorError, so
root.py exits with one sentence and no traceback; an unexpected one stays an ordinary
exception and prints the worker's traceback first, because the frames that name the bug
are the worker's. Either way the failure reason -- the last line the user sees -- is one
line.
"""

import botocore.exceptions
import pytest

from python_modules.shared import worker_errors
from python_modules.shared.bulk_executor_error import BulkExecutorError
from python_modules.shared.worker_errors import (
    UNEXPECTED_FAILURE_BANNER,
    classify_failure,
    raise_first_worker_error,
    record_understood_failure,
    record_worker_failure,
)


class FakeAccumulator:
    """Stands in for a Spark ListAccumulator, which only supports add()."""

    def __init__(self):
        self.value = []

    def add(self, entries):
        self.value.extend(entries)


@pytest.fixture(autouse=True)
def real_error_helpers(monkeypatch):
    """tests/server/conftest.py mocks shared.errors, so restore the two readers."""
    def get_error_code(e):
        return getattr(e, 'response', {}).get('Error', {}).get('Code')

    def get_error_message(e):
        return getattr(e, 'response', {}).get('Error', {}).get('Message') or str(e)

    monkeypatch.setattr(worker_errors, 'get_error_code', get_error_code)
    monkeypatch.setattr(worker_errors, 'get_error_message', get_error_message)


def _client_error(code, message):
    return botocore.exceptions.ClientError(
        {'Error': {'Code': code, 'Message': message}}, 'Scan')


class TestClassifyFailure:

    def test_bulk_executor_error_is_understood(self):
        assert classify_failure(BulkExecutorError('bad --where')) == (True, 'bad --where')

    @pytest.mark.parametrize('code', sorted(worker_errors.ENVIRONMENT_ERROR_CODES))
    def test_environmental_codes_are_understood_without_a_verb_saying_so(self, code):
        """A denial or a throttle is the operator's to fix whichever verb hit it."""
        understood, message = classify_failure(_client_error(code, 'denied'))
        assert understood
        assert message == 'denied'

    def test_unlisted_aws_code_is_unexpected(self):
        understood, message = classify_failure(_client_error('InternalServerError', 'oops'))
        assert not understood
        assert message == 'oops'

    @pytest.mark.parametrize('code', [
        'ValidationException',
        'ResourceNotFoundException',
        'ConditionalCheckFailedException',
        'TransactionConflictException',
        'ItemCollectionSizeLimitExceededException',
        'LimitExceededException',
    ])
    def test_verb_dependent_codes_are_unexpected_unless_the_verb_says_otherwise(self, code):
        """Whether these are understood depends on whether the verb expected them, so the
        shared classifier must not decide for it. `fill` expects a ValidationException from
        a bad generator and phrases it; code that builds its own request does not, and a
        traceback is the only useful report there."""
        understood, _ = classify_failure(_client_error(code, 'nope'))
        assert not understood, (
            f"{code} is understood only where a verb expected it -- it must reach the "
            "traceback path by default")

    def test_authorization_wording_is_understood_without_a_code(self):
        """The Glue connector phrases denials in text rather than in a code."""
        understood, _ = classify_failure(
            Exception('User: arn:aws:sts::1:x is not authorized to perform: dynamodb:Scan'))
        assert understood

    def test_expired_token_wording_is_understood(self):
        understood, _ = classify_failure(
            Exception('The security token included in the request is expired'))
        assert understood

    def test_a_bug_of_ours_is_unexpected(self):
        assert classify_failure(KeyError('pk')) == (False, "'pk'")


class TestRecordWorkerFailure:

    def test_understood_failure_records_no_traceback(self):
        acc = FakeAccumulator()
        record_worker_failure(acc, _client_error('AccessDeniedException', 'denied'),
                              'Error in worker 3')
        assert acc.value == [('Error in worker 3: denied', None)]

    def test_unexpected_failure_records_the_worker_traceback(self):
        acc = FakeAccumulator()
        try:
            raise KeyError('pk')
        except KeyError as e:
            record_worker_failure(acc, e, 'Error in worker 3')

        (message, detail), = acc.value
        assert message == "Error in worker 3: KeyError: 'pk'", \
            "the type, because a KeyError message is bare"
        assert 'Traceback' in detail and "raise KeyError('pk')" in detail

    def test_understood_false_forces_a_traceback_for_code_we_do_not_own(self):
        """A user transform can raise something we would otherwise recognise."""
        acc = FakeAccumulator()
        try:
            raise BulkExecutorError('the transform said so')
        except BulkExecutorError as e:
            record_worker_failure(acc, e, 'Transform function raised an exception',
                                  understood=False)

        (message, detail), = acc.value
        assert message.startswith('Transform function raised an exception: BulkExecutorError:')
        assert 'Traceback' in detail

    def test_understood_true_suppresses_the_traceback(self):
        acc = FakeAccumulator()
        record_worker_failure(acc, KeyError('pk'), 'Error in worker 3', understood=True)
        assert acc.value == [("Error in worker 3: 'pk'", None)]


class TestRecordUnderstoodFailure:

    def test_records_the_message_as_given(self):
        acc = FakeAccumulator()
        record_understood_failure(acc, 'Schema validation error: no such key')
        assert acc.value == [('Schema validation error: no such key', None)]


class TestRaiseFirstWorkerError:

    def test_nothing_recorded_returns_quietly(self):
        raise_first_worker_error(FakeAccumulator())

    def test_understood_failure_raises_without_printing(self, capsys):
        acc = FakeAccumulator()
        record_understood_failure(acc, 'Access denied on table foo')

        with pytest.raises(BulkExecutorError, match='Access denied on table foo'):
            raise_first_worker_error(acc)

        assert capsys.readouterr().out == '', "an understood failure needs no traceback"

    def test_unexpected_failure_stays_an_exception_and_prints_the_traceback(self, capsys):
        acc = FakeAccumulator()
        try:
            raise KeyError('pk')
        except KeyError as e:
            record_worker_failure(acc, e, 'Error in worker 3')

        with pytest.raises(Exception) as raised:
            raise_first_worker_error(acc)

        assert not isinstance(raised.value, BulkExecutorError), \
            "BulkExecutorError means we understood it; this one we did not"
        out = capsys.readouterr().out
        assert UNEXPECTED_FAILURE_BANNER in out and 'Traceback' in out
        assert 'Traceback' not in str(raised.value), \
            "the Glue failure reason is the last line the user sees; keep it to one"

    def test_only_the_first_failure_is_surfaced(self, capsys):
        """Every worker may record; the user should see one report, not two hundred."""
        acc = FakeAccumulator()
        for segment in range(3):
            try:
                raise KeyError('pk')
            except KeyError as e:
                record_worker_failure(acc, e, f'Error in worker {segment}')

        with pytest.raises(Exception, match='Error in worker 0'):
            raise_first_worker_error(acc)

        assert capsys.readouterr().out.count('most recent call last') == 1

    def test_a_bare_string_still_surfaces(self):
        """Not recorded through the helpers: raise it rather than lose it. There is no
        traceback to show, so it takes the polite path."""
        acc = FakeAccumulator()
        acc.add(['something went wrong'])

        with pytest.raises(BulkExecutorError, match='something went wrong'):
            raise_first_worker_error(acc)
