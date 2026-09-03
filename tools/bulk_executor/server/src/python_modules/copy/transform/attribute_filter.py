"""Example copy transform: only copy items matching an attribute value.

The copy analogue of load_export/transform/load_only_active.py. Kept generic
via the two constants below so it also serves as a filter template.
"""

# Only items where FILTER_ATTRIBUTE == FILTER_VALUE are copied to the target table.
FILTER_ATTRIBUTE = "status"
FILTER_VALUE = "active"


def transform_item(item):
    """
    Example: only copy items where the 'status' attribute is 'active'.

    `item` is a plain Python dict, already deserialized by boto3
    (e.g. {"pk": "user123", "status": "active"}, not DynamoDB's
    {"pk": {"S": "user123"}, "status": {"S": "active"}} wire format).

    Numeric values arrive as `decimal.Decimal`, not `int`/`float`, so a
    comparison like `item["count"] == 5` works but `isinstance(v, int)`
    will not match.

    Returns:
        list[dict]: single-element list with the item, to copy it.
        list: empty list to skip the item — it is not written to the target.
    """
    if item.get(FILTER_ATTRIBUTE) == FILTER_VALUE:
        return [item]
    return []
