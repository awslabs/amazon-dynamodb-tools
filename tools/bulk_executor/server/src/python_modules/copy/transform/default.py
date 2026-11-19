"""Default passthrough transform for the copy verb.

Mirrors load_export/transform/default.py and revert_export/transform/default.py:
a no-op that copies every item unchanged. Useful as a starting point for a
custom transform, and as the thing `--transform default` resolves to.
"""


def transform_item(item):
    """Return the item unchanged, so the copy proceeds as a plain PUT.

    `item` is a plain Python dict already deserialized by boto3 — e.g.
    {"pk": "user123", "status": "active"}, not the DynamoDB wire format
    {"pk": {"S": "user123"}}.
    """
    return item
