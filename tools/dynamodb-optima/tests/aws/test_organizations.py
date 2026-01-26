"""
Tests for AWS Organizations integration.
"""

import pytest
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch

from metrics_collector.aws.organizations import OrganizationsManager, AWSAccount


@pytest.mark.unit
@pytest.mark.aws
class TestOrganizationsManager:
    """Test AWS Organizations manager."""
    
    @pytest.mark.asyncio
    async def test_list_accounts(self, mock_organizations_client):
        """Test listing accounts from Organizations."""
        manager = OrganizationsManager()
        
        with patch.object(manager.session, 'client') as mock_client:
            mock_client.return_value.__aenter__.return_value = mock_organizations_client
            
            # Mock management account detection
            mock_organizations_client.describe_organization.return_value = {
                "Organization": {"MasterAccountId": "123456789012"}
            }
            
            accounts = await manager.list_accounts()
            
            assert len(accounts) == 2
            assert accounts[0].account_id == "123456789012"
            assert accounts[0].is_management_account is True
            assert accounts[1].account_id == "123456789013"
            assert accounts[1].is_management_account is False
    
    @pytest.mark.asyncio
    async def test_get_account_credentials(self):
        """Test assuming role in member account."""
        manager = OrganizationsManager()
        
        mock_sts = AsyncMock()
        mock_sts.assume_role.return_value = {
            "Credentials": {
                "AccessKeyId": "ASIA...",
                "SecretAccessKey": "secret",
                "SessionToken": "token"
            }
        }
        
        with patch.object(manager.session, 'client') as mock_client:
            mock_client.return_value.__aenter__.return_value = mock_sts
            
            creds = await manager.get_account_credentials("123456789013")
            
            assert creds["aws_access_key_id"] == "ASIA..."
            assert "aws_secret_access_key" in creds
            assert "aws_session_token" in creds
    
    @pytest.mark.asyncio
    async def test_store_accounts_in_database(self, db_connection):
        """Test storing accounts in database."""
        manager = OrganizationsManager()
        
        accounts = [
            AWSAccount(
                account_id="123456789012",
                account_name="Test Account",
                account_email="test@example.com",
                account_status="ACTIVE",
                joined_method="CREATED",
                joined_timestamp=datetime.now(),
                is_management_account=True
            )
        ]
        
        await manager.store_accounts_in_database(accounts, db_connection)
        
        result = db_connection.execute(
            "SELECT * FROM aws_accounts WHERE account_id = ?",
            ("123456789012",)
        ).fetchone()
        
        assert result is not None
        assert result[1] == "Test Account"  # account_name
        assert result[8] is True  # is_management_account
