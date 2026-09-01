import importlib
import sys
import traceback
import warnings

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.transforms import *
from pyspark.context import SparkContext
from python_modules.shared import driver_errors
from python_modules.shared.bulk_executor_error import BulkExecutorError


def _get_first_system_exit_line():
    """
    When a worker calls exit("message") we can pull the message out of the full trace.
    Then we repeat it here in the driver code so the end user sees the original nice message.
    """
    estr = traceback.format_exc()
    marker = "SystemExit: "
    for line in estr.splitlines():
        if line.startswith(marker):
            return line[len(marker):] # print what comes after the exit marker
    return None

def _get_parsed_glue_job_args(argv):
    """
    Retrieve the parsed Glue Job Parameters. Supports the handling of optional params when needed.

    Args:
      argv: The `sys.argv` configured under DefaultArguments for the Glue Job.

    Returns:
      dict: The parsed Glue Job arguments.

    Raises:
      ValueError: If a required Glue Job Default Argument is missing (ex. dynamo_db_table_name).
    """
    parsed_args = {}
    i = 1  # Start after the script name
    while i < len(argv):
        if argv[i].startswith('--'):
            key = argv[i].lstrip('--')
            if i + 1 < len(argv) and not argv[i + 1].startswith('--'):
                value = argv[i + 1]
                i += 1
            else:
                value = None  # Handle cases where no value is provided
            parsed_args[key] = value
        i += 1
    if parsed_args.get('XDebug'):
        print(f"Parsed arguments: {parsed_args}")
    return parsed_args


# Initialize the Spark and Glue contexts
spark_context = SparkContext.getOrCreate()
glue_context = GlueContext(spark_context)
job = Job(glue_context)

# awsglue's own DynamicFrame.toDF() calls pyspark's internal DataFrame
# constructor, which emits a UserWarning users can't act on. Suppress it once
# here, after the contexts are built (so nothing in Spark/Glue startup resets
# the filter) and before any verb is imported, so every verb inherits it
# instead of each one re-declaring the same filter. Pinned to this exact
# message on purpose — a blanket UserWarning ignore would hide real warnings.
warnings.filterwarnings(
    "ignore", message="DataFrame constructor is internal. Do not directly use it.")

# Import the module
module_path = 'python_modules'

sys.path.append(module_path)
parsed_args = _get_parsed_glue_job_args(sys.argv)

action_module = parsed_args.get('XAction', 'default').replace('-', '_')

module_name = f"python_modules.{action_module}"
action_script_function_name = 'run'

# Fix the import path to use the correct module path
from python_modules.shared.logger import init, log # Import order intentional to determine if XDebug flag present (for debug logging)
init(parsed_args)

# Pace stdout so CloudWatch Live Tail can actually deliver what we print. Live Tail
# silently drops events that arrive faster than it will deliver them -- 79% of rows
# lost on a measured run, with its own `sampled` flag reporting false -- so printing
# a large result in one burst can truncate or corrupt it while the job still reports
# success (issues #315, #321). Installed here, once, so every verb inherits it
# rather than each one pacing its own prints. stdout only: logging goes to stderr and
# diagnostics should not queue behind a big result set.
from python_modules.shared.throttled_output import install as _install_output_throttle
_install_output_throttle()

try:
    module = importlib.import_module(module_name)
except ImportError:
    # Intentionally not doing 'from None' to show what went wrong
    # because if we get here, the client-side check passed so it's prob a server-side verb code issue
    raise Exception(f"Could not find action '{action_module}'")
else:
    # Module was imported successfully
    if hasattr(module, action_script_function_name):
        action_script_function = getattr(module, action_script_function_name)
        try:
            action_script_function(job, spark_context, glue_context, parsed_args)  # Run the function
        except BaseException as e:
            # Everything a verb can fail with lands here, and shared/driver_errors.py
            # decides what the user sees: a denial or other understood failure exits with
            # one sentence, anything else prints its traceback first and then exits with a
            # one-line reason. Exiting rather than re-raising is the point -- a re-raise
            # hands the user Glue's exception-analysis blob plus Py4J's restatement of the
            # same error, with AWS's actual sentence buried inside a Java stack (#332).
            #
            # BaseException, not Exception: a worker calling exit() reaches the driver as a
            # SystemExit, and Py4J wraps KeyboardInterrupt-style aborts too. sys.exit from
            # driver_errors.surface() raises SystemExit itself, so let that through.
            if isinstance(e, SystemExit):
                raise
            driver_errors.surface(e)
    else:
        raise Exception(f"Could not find the function '{action_script_function_name}' within the module '{module_name}'.")

job.commit()  # Commit the job successfully
spark_context.stop() # Stop the Spark context
