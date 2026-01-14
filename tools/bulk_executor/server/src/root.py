import importlib
import sys
import traceback

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.transforms import *
from pyspark.context import SparkContext


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

# Import the module
module_path = 'python_modules'

sys.path.append(module_path)
parsed_args = _get_parsed_glue_job_args(sys.argv)

action_module = parsed_args.get('XAction', 'default')
module_name = f"python_modules.{action_module}"
action_script_function_name = 'run'

# Fix the import path to use the correct module path
from python_modules.shared.logger import init, log # Import order intentional to determine if XDebug flag present (for debug logging)
init(parsed_args)

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
        except Exception as e:
            raise # Just let it propagate
    else:
        raise Exception(f"Could not find the function '{action_script_function_name}' within the module '{module_name}'.")

job.commit()  # Commit the job successfully
spark_context.stop() # Stop the Spark context
