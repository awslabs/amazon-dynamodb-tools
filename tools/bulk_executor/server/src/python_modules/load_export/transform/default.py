from ..parsers.records import FullExportRecord, IncrementalExportRecord


def transform_full_record(record: FullExportRecord) -> list[FullExportRecord]:
    """Default passthrough for full export records."""
    return [record]


def transform_incremental_record(record: IncrementalExportRecord) -> list[IncrementalExportRecord]:
    """Default passthrough for incremental export records."""
    return [record]
