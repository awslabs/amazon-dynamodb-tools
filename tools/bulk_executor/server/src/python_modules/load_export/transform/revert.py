from python_modules.shared.export.parsers.records import IncrementalExportRecord


def transform_incremental_record(record: IncrementalExportRecord) -> list[IncrementalExportRecord]:
    """Revert transform: undoes changes by swapping old_image into new_image.

    - Additions (old_image=None, new_image=item): becomes a delete (new_image=None)
    - Deletions (old_image=item, new_image=None): becomes a put (new_image=old_image)
    - Updates (old_image=prev, new_image=curr): becomes a put of old state (new_image=old_image)
    """
    record.new_image = record.old_image
    return [record]
