import importlib

help_text = f"""
    Purpose of "delete":
        Delete items in a DynamoDB table
        Required --table parameter
        Optional --where parameter to specify a match criteria, using Spark SQL syntax
        Optional --orderby parameter to specify a sort attribute, with optional asc/desc suffix
        Optional --limit parameter to limit the number of items processed
        Requires PITR be enabled

    Examples:
        # Delete all items with a timestamp before some threshold
        bulk delete --table users --where "timestamp < '2024-01-01'"

        # Delete the 100 oldest items
        bulk delete --table products --orderby timestamp --limit 100

        # Delete the 100 newest items
        bulk delete --table products --orderby timestamp desc --limit 100
    """

def run(env_configs):
    return importlib.import_module('.find', package=__package__).run(env_configs, verb="delete", help_text=help_text)
