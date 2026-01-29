"""DynamoDB table validation for checking table existence and emptiness."""

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
    
    def validate_table_empty(self, table_name: str) -> bool:
        """
        Check if the table exists and is empty.
        
        Args:
            table_name: Name of the DynamoDB table
            
        Returns:
            True if table exists and is empty
            
        Raises:
            ValueError: If table doesn't exist or contains items
        """
        try:
            # Use scan with limit=1 for efficiency - we only need to know if any items exist
            response = self.dynamodb_client.scan(
                TableName=table_name,
                Limit=1,
                Select='COUNT'
            )
            
            item_count = response.get('Count', 0)
            
            if item_count > 0:
                error_msg = f"Table '{table_name}' is not empty. Contains {item_count} or more items."
                log.error(error_msg)
                raise ValueError(error_msg)
            
            # Table is empty
            success_msg = f"Table '{table_name}' validation successful: table is empty"
            log.info(success_msg)
            return True
            
        except ValueError:
            # Re-raise ValueError from our own checks
            raise
        except Exception as e:
            # Check if it's a ResourceNotFoundException
            if e.__class__.__name__ == 'ResourceNotFoundException':
                error_msg = f"Table '{table_name}' does not exist"
                log.error(error_msg)
                raise ValueError(error_msg)
            
            # Handle other unexpected errors
            error_msg = f"Error validating table '{table_name}': {str(e)}"
            log.error(error_msg)
            raise ValueError(error_msg)
    

