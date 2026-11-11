from ...logger import log
from ...worker_errors import record_understood_failure
from ..readers.export_reader import get_export_file_paths
from ..parsers.parser_factory import ParserFactory
from ..utils.enums import ExportLoadType


def read_and_parse(spark_context, manifest_data, path_resolver, key_schema, error_accumulator):
    """Read export files into record RDD.

    Returns:
        tuple: (records_rdd, export_load_type, parser, total_expected_items)
    """
    log.debug("Resolving export file paths...")
    file_paths, total_expected_items = get_export_file_paths(
        data_files=manifest_data['data_files'],
        file_base_path=path_resolver.get_base_path()
    )

    log.debug("Reading and parsing export files with Spark...")
    all_lines_rdd = spark_context.textFile(",".join(file_paths))

    export_type = manifest_data['export_type']
    export_load_type = ExportLoadType.INCREMENTAL if export_type == 'INCREMENTAL_EXPORT' else ExportLoadType.FULL
    parser = ParserFactory.get_parser(export_load_type, key_schema)
    log.debug(f"Parser of type {type(parser).__name__} returned successfully...")

    # A malformed export line is the user's data, not our bug: record it and drop
    # the line rather than let the ValueError escape the worker (four Spark task
    # retries, job abort, cause buried in a Py4J wrapper -- the #327 shape).
    def _parse_line(line):
        try:
            return parser.parse_to_record(line)
        except ValueError as e:
            record_understood_failure(error_accumulator, str(e))
            return None

    records_rdd = all_lines_rdd.map(_parse_line).filter(lambda record: record is not None)
    return records_rdd, export_load_type, parser, total_expected_items
