from ...logger import log
from ...table_info import get_and_print_table_write_cost


def estimate_cost(table_info, manifest_data, key_schema_result):
    """Estimate and log DynamoDB write costs."""
    log.debug("Estimating DynamoDB write costs...")
    avg_item_size = key_schema_result.get('avg_item_size', 0)
    estimated_size_bytes = avg_item_size * manifest_data['total_item_count']
    get_and_print_table_write_cost(table_info, manifest_data['total_item_count'], estimated_size_bytes)
