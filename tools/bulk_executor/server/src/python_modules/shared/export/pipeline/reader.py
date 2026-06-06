from ...logger import log
from ..readers.export_reader import get_export_file_paths
from ..parsers.parser_factory import ParserFactory
from ..utils.enums import ExportLoadType


def read_and_parse(spark_context, manifest_data, path_resolver, key_schema):
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

    records_rdd = all_lines_rdd.map(parser.parse_to_record)
    return records_rdd, export_load_type, parser, total_expected_items
