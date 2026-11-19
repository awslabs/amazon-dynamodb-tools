"""
Shared item transformation for DynamoDB zero-downtime migration.

Both the backfill script and stream replay Lambda import this module
to ensure identical transformation logic. If your migration requires
schema changes, modify the transform() function here.
"""

from decimal import Decimal


def transform(item, source_event=None):
    """
    Transform an item before writing to the target table.

    Override this function for schema migrations. Both the backfill
    and stream replay call this with every item before writing.

    Args:
        item: Dict of the DynamoDB item (Python-native types)
        source_event: For stream replay, the event name ('INSERT', 'MODIFY', 'REMOVE').
                      For backfill, None.

    Returns:
        Transformed item dict, or None to skip this item.

    Example - rename an attribute:
        item['order_id'] = item.pop('orderId', item.get('order_id'))
        return item

    Example - add a computed field:
        item['search_key'] = f"{item['tenant']}#{item['created_at']}"
        return item

    Example - filter out items:
        if item.get('status') == 'DELETED':
            return None
        return item
    """
    return item
