import importlib

help_text = f"""
    Purpose of "count":
        Count items in a DynamoDB table
        Required --table parameter
        Optional --where parameter to specify a match criteria, using Spark SQL syntax

    Examples:
        bulk count --table products
        bulk count --table users --where "age > 21"
        bulk count --table orders --where "status = 'pending'"
    """

def run(env_configs):
    return importlib.import_module('.find', package=__package__).run(env_configs, verb="count", help_text=help_text)
