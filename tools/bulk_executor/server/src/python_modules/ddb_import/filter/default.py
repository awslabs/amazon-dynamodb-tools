def filter_item(item):
    """
    Default filter that includes all items (no filtering).
    
    Args:
        item (dict): DynamoDB item to filter
        
    Returns:
        bool: True to include the item, False to exclude it
    """
    return True
