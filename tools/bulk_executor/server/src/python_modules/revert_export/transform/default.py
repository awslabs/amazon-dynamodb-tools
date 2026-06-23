from python_modules.shared.export.parsers.records import FullExportRecord, IncrementalExportRecord

def transform_full_record(record: FullExportRecord) -> list[FullExportRecord]:
    # revert-export only supports incremental exports since full exports lack the operation metadata needed to reverse changes
    raise NotImplementedError("revert-export does not support full exports")


def transform_incremental_record(record: IncrementalExportRecord) -> list[IncrementalExportRecord]:
    """Default passthrough for incremental export records."""
    return [record]
