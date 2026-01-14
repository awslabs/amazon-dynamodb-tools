import sys

# Custom Library Imports
sys.path.append('/server/src')
from python_modules import find


def run(job, spark_context, glue_context, parsed_args):
    find.run(job, spark_context, glue_context, parsed_args)
