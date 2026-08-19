# Attribute names to redact, if present on the item.
# Add more names here as needed — matching is case-sensitive.
PII_ATTRIBUTES = [
    "Name",
    "Email",
]

REDACTED_VALUE = "***REDACTED***"


def transform_item(item):
    """
    Example: Redact PII attributes during copy.

    Replaces the value of any attribute listed in PII_ATTRIBUTES with a
    fixed placeholder, rather than deleting it — this keeps the item's
    shape (and any key attributes that happen to share a name) intact.

    `item` is a plain Python dict, already deserialized by boto3
    (e.g. {"Id": 42, "Name": "Alice", "status": "active"}, not DynamoDB's
    {"Id": {"N": "42"}, "Name": {"S": "Alice"}} wire format).

    Returns:
        dict: the modified item (copy proceeds as a normal PUT).

    Example result:
        Input:  {"Id": 42, "Name": "Alice", "Email": "alice@example.com"}
        Output: {"Id": 42, "Name": "***REDACTED***", "Email": "***REDACTED***"}
    """
    for attr in PII_ATTRIBUTES:
        if attr in item:
            item[attr] = REDACTED_VALUE
    return item
