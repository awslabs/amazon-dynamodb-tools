"""Surfacing a failure that happened on the driver.

`worker_errors.py` handles the failures a worker records; this handles the ones the
driver hits directly -- a denied `.load()` through the Glue connector, a source file
that isn't the format the user claimed, a typo in `--transform`. The classification is
the same one, imported from there, because the question is the same: did we understand
this?

- understood -- a denial, expired credentials, throttling, or something a verb already
  phrased as a BulkExecutorError. One sentence, no traceback.
- unexpected -- anything else. The traceback is printed once, on the console, and the
  job still exits with a one-line reason.

Either way `root.py` exits rather than re-raising, so Glue's exception-analysis blob and
its Py4J restatement of the same error never reach the user. That matters most for the
connector: a denied read arrives as a Py4JJavaError whose str() is a 300-line Java stack
with AWS's sentence buried on line two, and re-raising it means the last thing the user
sees is `An error occurred while calling o304.load`.
"""

import sys
import traceback

from python_modules.shared.bulk_executor_error import BulkExecutorError
from python_modules.shared.logger import log
from python_modules.shared.worker_errors import classify_failure

UNEXPECTED_FAILURE_BANNER = "The job failed in a way we did not expect. Traceback:"

# Printed in front of an understood failure's sentence. Two jobs: it tells the reader this
# is the explanation rather than one more log line, and the client watches for it to
# suppress Glue's exception-analysis blob. Glue emits that blob for a clean sys.exit only
# sometimes -- observed on a denied `find` but not a denied `count` in the same batch -- so
# the marker has to be there every time, not only when a BulkExecutorError was involved.
#
# Named rather than generic because the client matches it as a substring anywhere in a log
# line. "Failure: " alone appeared only in our own output across every run captured for
# #332, but Spark has shapes like ExecutorLostFailure and FetchFailure that could put
# "Failure: " in a line of its own -- and a false match would suppress Glue's diagnostics
# for a failure we had not explained.
EXPLAINED_FAILURE_PREFIX = "Bulk Executor failure: "

# Cap on what we hand to sys.exit(): Glue records it as the job's ErrorMessage and the
# client prints it as its closing line. AWS's authorization sentences run ~400 chars,
# which is worth keeping whole; a Java stack that slipped through is not.
MAX_REASON_CHARS = 800


def _one_line(message):
    """Collapse to a single line and bound the length. The reason is a closing line, not
    a report -- anything longer has already been printed above it."""
    collapsed = " ".join(str(message).split())
    if len(collapsed) > MAX_REASON_CHARS:
        collapsed = collapsed[:MAX_REASON_CHARS - 3] + "..."
    return collapsed


def surface(exception):
    """Report a driver-side failure and exit. Never returns.

    Called by root.py for anything that escapes a verb.
    """
    understood, message = classify_failure(exception)
    if not understood:
        # Ours to debug, or a user's generator/transform: the frames are the report, and
        # printing them ourselves means the client suppresses Glue's blob (it watches for
        # this banner and for BulkExecutorError) and the closing line stays short.
        print(UNEXPECTED_FAILURE_BANNER)
        print(traceback.format_exc())
        # classify_failure already extracted the readable message; the type goes in front
        # because an unexpected message alone can be as bare as "'pk'".
        reason = f"{type(exception).__name__}: {message}"
    else:
        reason = message

    reason = _one_line(reason)
    if isinstance(exception, BulkExecutorError):
        # Keep the name users have seen since before any of this, and the client's
        # long-standing suppression marker.
        log.error(f"BulkExecutorError: {reason}")
    else:
        log.error(f"{EXPLAINED_FAILURE_PREFIX}{reason}")
    sys.exit(reason)
