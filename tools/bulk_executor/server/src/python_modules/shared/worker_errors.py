"""Recording a worker failure, and surfacing it from the driver.

Workers cannot raise: an exception escaping a worker costs four Spark task retries,
aborts the job, and reaches the driver as a Py4J wrapper with the cause buried. So a
worker records on an accumulator and returns, and the driver surfaces the first
entry.

Whether we *understood* the failure decides what the user gets:

- understood -- the verb expected this and phrased it (BulkExecutorError,
  record_understood_failure, or understood=True), or the failure is environmental:
  a denial, expired credentials, throttling that outlived the SDK's retries. The user
  can act on it and there is nothing to debug, so the driver raises BulkExecutorError:
  root.py exits with the sentence, and nobody needs to know an exception was involved.

  Whether an AWS error code is "understood" is a property of the verb, not the code:
  ValidationException is a phrasable mistake in `fill` and a bug in code that builds its
  own request. Only ENVIRONMENT_ERROR_CODES are classified without a verb saying so.
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

# Failures about the caller's environment rather than our code path: whichever verb
# hits one of these, the operator grants a permission or slows down, and a traceback
# could not help anyone. These are the only codes classified without a verb's say-so.
#
# Deliberately absent: ValidationException, ResourceNotFoundException,
# ConditionalCheckFailedException, TransactionConflictException,
# ItemCollectionSizeLimitExceededException, LimitExceededException. Whether those are
# understood depends entirely on whether the verb expected them -- a ValidationException
# is a phrasable mistake in `fill` (the generator's items don't fit the schema) and a bug
# anywhere that builds its own request. A verb that expects one says so, with
# record_understood_failure() or by raising BulkExecutorError where it recognises the
# condition; `fill`, `update` and the batch writer all do. Anything nobody expected keeps
# its traceback, which is the point.
ENVIRONMENT_ERROR_CODES = frozenset({
    'AccessDeniedException',
    'UnrecognizedClientException',
    'InvalidSignatureException',
    'ProvisionedThroughputExceededException',
    'ThrottlingException',
    'RequestLimitExceeded',
})

# The Java SDK and the Glue connector phrase authorization failures in text rather
# than in a code we can read, so match on AWS's own wording as a fallback.
_ENVIRONMENT_PHRASES = (
    'is not authorized to perform',
    'AccessDenied',
    'security token included in the request is expired',
)

# Printed immediately before an unexpected failure's traceback. The client watches for
# it to suppress Glue's exception-analysis blob, so the two have to agree -- a guard
# test checks that they do.
UNEXPECTED_FAILURE_BANNER = "A worker failed in a way we did not expect. Traceback from the worker:"


def classify_failure(exception):
    """Return (understood, message) for an exception caught in a worker.

    Understood without the verb's involvement means environmental (see
    ENVIRONMENT_ERROR_CODES); everything else defaults to unexpected, so a verb that
    expected a failure has to say so.
    """
    if isinstance(exception, BulkExecutorError):
        return True, str(exception)

    message = str(get_error_message(exception))
    if get_error_code(exception) in ENVIRONMENT_ERROR_CODES:
        return True, message
    if any(phrase in message for phrase in _ENVIRONMENT_PHRASES):
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
