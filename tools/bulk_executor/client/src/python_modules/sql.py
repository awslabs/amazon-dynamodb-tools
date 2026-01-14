import logging as log
import utils
import argparse
import re
from utils.custom_parser import BulkArgumentParser

help_text = f"""
    Purpose of "sql":
        Execute arbitrary Spark SQL queries against a DynamoDB table
        Required --table parameter
        Required --query parameter to specify the SQL query to execute
        Optional --limit parameter to limit the number of results returned

    Examples:
        bulk sql --table products --query "SELECT COUNT(*) FROM products"
        bulk sql --table users --query "SELECT age, COUNT(*) FROM users GROUP BY age" --limit 100
"""

def validate_sql_query(parser, query, table_name):
    """Validate SQL query for basic safety and correctness"""
    query_upper = query.upper().strip()
    
    # Check for mutating operations
    mutating_keywords = ['DROP', 'DELETE', 'INSERT', 'UPDATE', 'ALTER', 'CREATE', 'TRUNCATE']
    for keyword in mutating_keywords:
        if re.search(rf'\b{keyword}\b', query_upper):
            parser.error(f"SQL query contains mutating keyword '{keyword}'. Only SELECT queries are supported.")
    
    # Ensure query starts with SELECT
    if not query_upper.startswith('SELECT'):
        parser.error("SQL query must start with SELECT. Only SELECT queries are supported.")
    
    # Check that query references the correct table name
    table_name_upper = table_name.upper()
    table_alias_upper = table_name.replace('-', '_').replace('.', '_').upper()
    
    # Look for the table name in FROM clause (with or without backticks)
    from_pattern = rf'\bFROM\s+(`?{re.escape(table_name_upper)}`?|{re.escape(table_alias_upper)})\b'
    if not re.search(from_pattern, query_upper):
        parser.error(f"SQL query must reference table '{table_name}' in FROM clause. Check table name spelling.")

    return True

def run(env_configs):
    glue_job_parent = utils.glue_job_arguments()
    environment_parent = utils.environment_arguments()

    parser = BulkArgumentParser("bulk sql", help_text=help_text, parents=[glue_job_parent, environment_parent])
    parser.add_argument('verb', help=argparse.SUPPRESS)
    parser.add_argument('--table', required=True, type=str, help='Table name')
    parser.add_argument('--query', required=True, type=str, help='SQL query to execute')
    parser.add_argument('--limit', type=int, default=argparse.SUPPRESS, help='Limit number of results')
    args = parser.parse_args()

    result = args.__dict__

    # Validate table exists
    utils.validate_tables(env_configs, parser, result['table'])
    
    # Validate SQL query
    validate_sql_query(parser, result['query'], result['table'])
    
    # Transform query to handle table names with special characters
    table_name = result['table']
    table_alias = table_name.replace('-', '_').replace('.', '_')
    
    # Replace table references in the query
    query = result['query']
    # Handle both quoted and unquoted table references
    query = re.sub(rf'\b{re.escape(table_name)}\b', f'`{table_name}`', query, flags=re.IGNORECASE)
    query = re.sub(rf'`{re.escape(table_name)}`', table_alias, query, flags=re.IGNORECASE)
    
    result['query'] = query

    log.info(f"Running action '{result['verb']}' with arguments: {result}")

    return True, result