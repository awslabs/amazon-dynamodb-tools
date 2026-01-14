import time
from decimal import Decimal

# Takes an item returned by a table.scan() and returns core kwargs suitable for table.update_item().
# We only need the Key, UpdateExpression, ExpressionAttributeNames, and ExpressionAttributeValues.
# We don't run the update here because we want the retry logic and consumption monitoring externalized.
# Returning empty is allowed, means no update needed to this item.

def generate(item):
    #print(item)

    # Extract primary key values directly
    pk = item.get("pk")
    sk = item.get("sk")

    if pk is None or sk is None:
        raise ValueError(f"Item is missing expected primary key attributes: '{pk}' and '{sk}'")

    # Time as seconds.millis
    now = time.time()
    now_decimal = Decimal(str(now))

    update_kwargs = {
        "Key": {"pk": pk, "sk": sk},
        "UpdateExpression": "SET #touched = :touched",
        "ConditionExpression": "attribute_not_exists(#touched) OR #touched < :touched",
        "ExpressionAttributeNames": {"#touched": "touched"},
        "ExpressionAttributeValues": {":touched": now_decimal}  # now is a number
    }

    return update_kwargs
