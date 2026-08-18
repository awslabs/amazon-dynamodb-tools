# Only items where FILTER_ATTRIBUTE == FILTER_VALUE are copied to the target table.
FILTER_ATTRIBUTE = "status"
FILTER_VALUE = "active"


def transform_item(item):
    """
    Example: Only copy items where the 'status' attribute is 'active'.

    `item` is a plain Python dict, already deserialized by boto3
    (e.g. {"pk": "user123", "status": "active"}, not DynamoDB's
    {"pk": {"S": "user123"}, "status": {"S": "active"}} wire format).

    Returns:
        dict: the item unchanged, to copy it.
        None: to skip the item — it will not be written to the target table.
    """
    if item.get(FILTER_ATTRIBUTE) == FILTER_VALUE:
        return item
    return None
