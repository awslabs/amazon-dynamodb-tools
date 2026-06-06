from ..shared.bulk_executor_error import BulkExecutorError
from ..shared.export.pipeline import run_export_pipeline
from ..shared.export.utils.enums import ExportLoadType

TRANSFORM_PACKAGE = 'python_modules.revert_export.transform'


def _post_validate(validation):
    """Fail fast if export is not incremental with NEW_AND_OLD_IMAGES."""
    manifest_data = validation['manifest_data']
    if manifest_data['export_type'] != ExportLoadType.INCREMENTAL.value:
        raise BulkExecutorError(
            f"revert-export requires an incremental export, but this export is: {manifest_data['export_type']}. "
            f"Only incremental exports can be reverted."
        )
    if manifest_data.get('output_view') != 'NEW_AND_OLD_IMAGES':
        raise BulkExecutorError(
            f"revert-export requires output view NEW_AND_OLD_IMAGES, but this export has: {manifest_data.get('output_view')}. "
            f"Revert needs both old and new images to undo changes."
        )


def _revert(record):
    record.new_image = record.old_image
    return record


def run(job, spark_context, glue_context, parsed_args):
    run_export_pipeline(
        spark_context, parsed_args,
        transform_package=TRANSFORM_PACKAGE,
        post_validate=_post_validate,
        post_transform=_revert,
    )
