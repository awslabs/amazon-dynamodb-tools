"""Example copy transform: redact named PII attributes during copy.

The copy analogue of load_export/transform/pii_mask_attribute.py. That one
masks (keeps first/last character) and uses the table key schema to leave
pk/sk untouched. A copy transform_item only receives the item, not the key
schema, so this one cannot do that automatically — see the warning below.
"""

# Attribute names to redact, if present on the item. Matching is case-sensitive.
#
# WARNING: do NOT list your table's partition key or sort key here. Redacting a
# key attribute rewrites every item's key to the same placeholder, collapsing
# the whole table onto one item in the target. The runner's post-transform key
# check will not catch this — the attribute is still present, just identical
# for every row.
PII_ATTRIBUTES = [
    "Name",
    "Email",
]

REDACTED_VALUE = "***REDACTED***"


def transform_item(item):
    """
    Example: redact PII attributes during copy.

    Replaces the value of any attribute listed in PII_ATTRIBUTES with a fixed
    placeholder, rather than deleting it, so the item keeps its shape.

    `item` is a plain Python dict, already deserialized by boto3
    (e.g. {"Id": 42, "Name": "Alice", "status": "active"}, not DynamoDB's
    {"Id": {"N": "42"}, "Name": {"S": "Alice"}} wire format). Numeric values
    arrive as `decimal.Decimal`, not `int`/`float`.

    Returns:
        list[dict]: single-element list with the modified item.

    Example result:
        Input:  {"Id": 42, "Name": "Alice", "Email": "alice@example.com"}
        Output: {"Id": 42, "Name": "***REDACTED***", "Email": "***REDACTED***"}
    """
    for attr in PII_ATTRIBUTES:
        if attr in item:
            item[attr] = REDACTED_VALUE
    return [item]
