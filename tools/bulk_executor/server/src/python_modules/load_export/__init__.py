from ..shared.export.pipeline import run_export_pipeline

TRANSFORM_PACKAGE = 'python_modules.load_export.transform'


def run(job, spark_context, glue_context, parsed_args):
    run_export_pipeline(spark_context, parsed_args, transform_package=TRANSFORM_PACKAGE)
