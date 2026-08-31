"""Recording a worker failure, and surfacing it from the driver.

Workers cannot raise: an exception escaping a worker costs four Spark task retries,
aborts the job, and reaches the driver as a Py4J wrapper with the cause buried. So a
worker records on an accumulator and returns, and the driver surfaces the first
entry.

Whether we *understood* the failure decides what the user gets:

- understood -- a permission denial, throttling, a validation rejection, a generated
  item missing the table's key. The user can act on it and there is nothing to debug,
  so the driver raises BulkExecutorError: root.py exits with the sentence, and nobody
  needs to know an exception was involved.
- unexpected -- a bug in our code, or a user-supplied generator or transform doing
  something we cannot anticipate. This one stays an exception, because that is what it
  is. The driver prints the worker's traceback first: the frames that name the bug are
  the worker's, and Glue's own traceback would only show our plumbing.

Either way the job fails and the failure reason is one line describing the problem.
The traceback goes above that line, never into it.
"""

import traceback

from python_modules.shared.bulk_executor_error import BulkExecutorError
from python_modules.shared.errors import get_error_code, get_error_message

# AWS rejections we understand well enough to explain rather than dump. Each is a
# condition the operator can do something about -- grant a permission, slow down,
# fix the item shape, point at a table that exists.
UNDERSTOOD_ERROR_CODES = frozenset({
    'AccessDeniedException',
    'UnrecognizedClientException',
    'InvalidSignatureException',
    'ProvisionedThroughputExceededException',
    'ThrottlingException',
    'RequestLimitExceeded',
    'LimitExceededException',
    'ValidationException',
    'ResourceNotFoundException',
    'ConditionalCheckFailedException',
    'ItemCollectionSizeLimitExceededException',
    'TransactionConflictException',
})

# The Java SDK and the Glue connector phrase authorization failures in text rather
# than in a code we can read, so match on AWS's own wording as a fallback.
_UNDERSTOOD_PHRASES = (
    'is not authorized to perform',
    'AccessDenied',
    'security token included in the request is expired',
)

# Printed immediately before an unexpected failure's traceback. The client watches for
# it to suppress Glue's exception-analysis blob, so the two have to agree -- a guard
# test checks that they do.
UNEXPECTED_FAILURE_BANNER = "A worker failed in a way we did not expect. Traceback from the worker:"


def classify_failure(exception):
    """Return (understood, message) for an exception caught in a worker."""
    if isinstance(exception, BulkExecutorError):
        return True, str(exception)

    message = str(get_error_message(exception))
    if get_error_code(exception) in UNDERSTOOD_ERROR_CODES:
        return True, message
    if any(phrase in message for phrase in _UNDERSTOOD_PHRASES):
        return True, message
    return False, message


def record_worker_failure(error_accumulator, exception, context, understood=None):
    """Record one worker failure. `context` prefixes the message, e.g. "Error in
    worker 3". Pass understood=False for code we do not own -- a user generator or
    transform -- where the traceback is what the user needs to see.
    """
    classified, message = classify_failure(exception)
    if understood is None:
        understood = classified
    if understood:
        detail = None
    else:
        # The type, because an unexpected message alone can be as bare as "'pk'".
        message = f"{type(exception).__name__}: {message}"
        # Captured here: by the time the driver raises, the frames are gone.
        detail = traceback.format_exc()
    error_accumulator.add([(f"{context}: {message}", detail)])


def record_understood_failure(error_accumulator, message):
    """Record a failure we have already phrased for the user."""
    error_accumulator.add([(message, None)])


def raise_first_worker_error(error_accumulator):
    """Surface the first recorded failure, if any. Called by the driver after
    collect()/foreachPartition."""
    if not error_accumulator.value:
        return

    entry = error_accumulator.value[0]
    # A traceback is recorded only for what we did not understand, so its presence
    # is the classification. A bare string predates the helpers above and carries none.
    message, detail = entry if isinstance(entry, tuple) else (str(entry), None)

    if detail:
        print(UNEXPECTED_FAILURE_BANNER)
        print(detail)
        # Left as an exception, because that is what an unexpected failure is. root.py
        # re-raises it, and Glue records `message` as the job's one-line reason.
        raise Exception(message) from None

    raise BulkExecutorError(message) from None
