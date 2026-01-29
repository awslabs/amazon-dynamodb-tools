def filter_item(item):
    """
    Example filter that only includes items with a specific attribute condition.

    This example filters items that have a 'status' attribute equal to 'active'.
    Modify this logic for your specific filtering needs.

    Args:
        item (dict): DynamoDB item to filter

    Returns:
        bool: True to include the item, False to exclude it
    """
    # Example: Only include items where status is 'active'
    return item.get('status') == 'active' # DDB attributes are case sensitive


def filter_by_pk_prefix(item):
    """
    Alternative filter function that filters by partition key prefix.

    Args:
        item (dict): DynamoDB item to filter

    Returns:
        bool: True to include the item, False to exclude it
    """
    # Example: Only include items where pk starts with 'USER#'
    pk = item.get('pk', '') # DDB attributes are case sensitive
    return pk.startswith('USER#')