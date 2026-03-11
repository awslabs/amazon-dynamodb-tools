"""DynamoDB table validation for checking table existence and key schema."""

from ...shared.logger import log

class TableValidator:
    """Validates DynamoDB table state before import operations."""
    
    def __init__(self, dynamodb_client):
        """
        Initialize with a boto3 DynamoDB client.
        
        Args:
            dynamodb_client: boto3 DynamoDB client instance
        """
        self.dynamodb_client = dynamodb_client
    
    def validate_table_exists(self, table_name: str) -> dict:
        """
        Check if the table exists and return its key schema.
        
        Args:
            table_name: Name of the DynamoDB table
            
        Returns:
            dict with 'pk' (always) and optionally 'sk', each having 'name' and 'type' keys.
            Example: {'pk': {'name': 'id', 'type': 'S'}, 'sk': {'name': 'ts', 'type': 'N'}}
            
        Raises:
            ValueError: If table doesn't exist or cannot be described
        """
        try:
            response = self.dynamodb_client.describe_table(TableName=table_name)
            table_desc = response['Table']
            key_schema = table_desc['KeySchema']
            attr_defs = {a['AttributeName']: a['AttributeType'] for a in table_desc['AttributeDefinitions']}

            result = {}
            for key in key_schema:
                name = key['AttributeName']
                key_type = 'pk' if key['KeyType'] == 'HASH' else 'sk'
                result[key_type] = {'name': name, 'type': attr_defs[name]}

            log.info(f"Table '{table_name}' validation successful: {result}")
            return result

        except Exception as e:
            if e.__class__.__name__ == 'ResourceNotFoundException':
                error_msg = f"Table '{table_name}' does not exist"
                log.error(error_msg)
                raise ValueError(error_msg)
            error_msg = f"Error validating table '{table_name}': {str(e)}"
            log.error(error_msg)
            raise ValueError(error_msg)
