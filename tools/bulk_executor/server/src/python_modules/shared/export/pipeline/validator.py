import boto3

from ...logger import log
from ...table_info import get_and_print_dynamodb_table_info

from ..validators.manifest_validator import ManifestValidator
from ..validators.data_file_validator import DataFileValidator
from ..validators.key_schema_validator import KeySchemaValidator
from ..validators.s3_validator import S3Validator
from ..utils.file_loader import FileLoader


def validate(path_resolver, table_name):
    """Validate S3 path, table, manifests, checksums, and key schema.

    Returns:
        dict with table_info, key_schema, manifest_data, key_schema_result
        or None if export contains 0 items.
    """
    s3_client = boto3.client('s3')
    file_loader = FileLoader(s3_client=s3_client)
    s3_validator = S3Validator(s3_client)

    log.debug("Validating S3 export path exists...")
    s3_validator.validate_path_exists(path_resolver)

    log.debug("Validating destination table exists...")
    table_info = get_and_print_dynamodb_table_info(table_name, quiet=True)
    key_schema = table_info['key_schema']
    log.debug(f"Destination table validation completed successfully: {key_schema}")

    log.debug("Validating and parsing manifest files...")
    manifest_validator = ManifestValidator(file_loader)
    manifest_data = manifest_validator.validate_and_parse_manifests(path_resolver)
    log.debug("Manifest validation completed successfully")

    if manifest_data['total_item_count'] == 0:
        log.info("Export contains 0 items, nothing to load. Exiting.")
        return None

    log.debug("Validating data file checksums...")
    data_file_validator = DataFileValidator(file_loader)
    checksum_result = data_file_validator.validate(
        data_files=manifest_data['data_files'],
        base_path=path_resolver.get_base_path()
    )
    log.debug("Data file checksum validation completed successfully")

    log.debug("Validating key schema against verified data files...")
    key_schema_validator = KeySchemaValidator(file_loader)
    key_schema_result = key_schema_validator.validate(
        verified_files=checksum_result['verified_files'],
        base_path=path_resolver.get_base_path(),
        key_schema=key_schema,
        export_type=manifest_data['export_type']
    )
    log.debug("Key schema validation completed successfully")

    log.info(f"S3 export: {manifest_data['total_item_count']:,} items across {len(manifest_data['data_files']):,} files ({manifest_data['export_type']}, {manifest_data['output_format']})")
    log.debug("All validations passed successfully")

    return {
        'table_info': table_info,
        'key_schema': key_schema,
        'manifest_data': manifest_data,
        'key_schema_result': key_schema_result,
    }
