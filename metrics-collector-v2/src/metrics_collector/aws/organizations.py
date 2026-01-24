"""
AWS Organizations integration for multi-account discovery.

Provides account discovery and cross-account role assumption for
analyzing DynamoDB tables across an entire AWS Organization.
"""

import asyncio
from datetime import datetime
from typing import Dict, List, Optional
from dataclasses import dataclass

import aioboto3
from botocore.exceptions import ClientError

from ..config import get_settings
from ..logging import get_logger

logger = get_logger(__name__)


@dataclass
class AWSAccount:
    """AWS account information from Organizations."""
    
    account_id: str
    account_name: str
    account_email: str
    account_status: str
    joined_method: str
    joined_timestamp: datetime
    organizational_unit_id: Optional[str] = None
    organizational_unit_name: Optional[str] = None
    is_management_account: bool = False


class OrganizationsManager:
    """Manager for AWS Organizations operations."""
    
    def __init__(self):
        self.settings = get_settings()
        self.session = aioboto3.Session()
        self._management_account_id: Optional[str] = None
    
    async def get_management_account_id(self) -> str:
        """Get the management account ID for the organization."""
        if self._management_account_id:
            return self._management_account_id
        
        # Use configured value if provided
        if self.settings.organizations_management_account_id:
            self._management_account_id = self.settings.organizations_management_account_id
            return self._management_account_id
        
        # Auto-detect by calling DescribeOrganization
        try:
            async with self.session.client("organizations") as org_client:
                response = await org_client.describe_organization()
                self._management_account_id = response["Organization"]["MasterAccountId"]
                logger.info(
                    "Detected management account",
                    account_id=self._management_account_id
                )
                return self._management_account_id
        except ClientError as e:
            logger.error("Failed to detect management account", error=str(e))
            raise
    
    async def list_accounts(self) -> List[AWSAccount]:
        """
        List all accounts in the AWS Organization.
        
        Returns:
            List of AWSAccount objects for all active accounts
        """
        accounts = []
        management_account_id = await self.get_management_account_id()
        
        try:
            async with self.session.client("organizations") as org_client:
                paginator = org_client.get_paginator("list_accounts")
                
                async for page in paginator.paginate():
                    for account in page.get("Accounts", []):
                        # Only include ACTIVE accounts
                        if account["Status"] != "ACTIVE":
                            logger.debug(
                                "Skipping non-active account",
                                account_id=account["Id"],
                                status=account["Status"]
                            )
                            continue
                        
                        aws_account = AWSAccount(
                            account_id=account["Id"],
                            account_name=account["Name"],
                            account_email=account["Email"],
                            account_status=account["Status"],
                            joined_method=account["JoinedMethod"],
                            joined_timestamp=account["JoinedTimestamp"],
                            is_management_account=(account["Id"] == management_account_id)
                        )
                        accounts.append(aws_account)
                        
                        logger.debug(
                            "Discovered account",
                            account_id=aws_account.account_id,
                            account_name=aws_account.account_name,
                            is_management=aws_account.is_management_account
                        )
        
        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code", "Unknown")
            if error_code == "AccessDeniedException":
                logger.error(
                    "Access denied to Organizations API. "
                    "Ensure the role has organizations:ListAccounts permission."
                )
            else:
                logger.error("Failed to list accounts", error=str(e))
            raise
        
        logger.info("Account discovery complete", total_accounts=len(accounts))
        return accounts
    
    async def get_account_credentials(
        self,
        account_id: str,
        role_name: Optional[str] = None
    ) -> Dict[str, str]:
        """
        Get temporary credentials for a member account.
        
        Args:
            account_id: The AWS account ID to assume role in
            role_name: IAM role name to assume (defaults to settings)
        
        Returns:
            Dictionary with AccessKeyId, SecretAccessKey, SessionToken
        """
        role_name = role_name or self.settings.organizations_role_name
        role_arn = f"arn:aws:iam::{account_id}:role/{role_name}"
        
        try:
            async with self.session.client("sts") as sts_client:
                response = await sts_client.assume_role(
                    RoleArn=role_arn,
                    RoleSessionName=f"metrics-collector-{account_id}",
                    DurationSeconds=3600  # 1 hour
                )
                
                credentials = response["Credentials"]
                logger.debug(
                    "Assumed role successfully",
                    account_id=account_id,
                    role_name=role_name
                )
                
                return {
                    "aws_access_key_id": credentials["AccessKeyId"],
                    "aws_secret_access_key": credentials["SecretAccessKey"],
                    "aws_session_token": credentials["SessionToken"]
                }
        
        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code", "Unknown")
            if error_code == "AccessDenied":
                logger.error(
                    "Failed to assume role in member account. "
                    "Ensure the role exists and has the correct trust policy.",
                    account_id=account_id,
                    role_arn=role_arn
                )
            else:
                logger.error(
                    "Failed to assume role",
                    account_id=account_id,
                    role_arn=role_arn,
                    error=str(e)
                )
            raise
    
    async def store_accounts_in_database(
        self,
        accounts: List[AWSAccount],
        connection
    ) -> None:
        """
        Store discovered accounts in the database.
        
        Args:
            accounts: List of AWSAccount objects to store
            connection: DuckDB connection
        """
        logger.info("Storing accounts in database", count=len(accounts))
        
        for account in accounts:
            connection.execute(
                """
                INSERT OR REPLACE INTO aws_accounts (
                    account_id,
                    account_name,
                    account_email,
                    account_status,
                    joined_method,
                    joined_timestamp,
                    organizational_unit_id,
                    organizational_unit_name,
                    is_management_account,
                    discovered_at,
                    last_updated
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    account.account_id,
                    account.account_name,
                    account.account_email,
                    account.account_status,
                    account.joined_method,
                    account.joined_timestamp,
                    account.organizational_unit_id,
                    account.organizational_unit_name,
                    account.is_management_account,
                    datetime.now(),
                    datetime.now()
                )
            )
        
        connection.commit()
        logger.info("Accounts stored successfully")
    
    async def discover_and_store_accounts(self, connection) -> List[AWSAccount]:
        """
        Discover accounts from Organizations and store them in the database.
        
        Args:
            connection: DuckDB connection
        
        Returns:
            List of discovered AWSAccount objects
        """
        logger.info("Starting AWS Organizations account discovery")
        
        accounts = await self.list_accounts()
        await self.store_accounts_in_database(accounts, connection)
        
        logger.info(
            "Account discovery and storage complete",
            total_accounts=len(accounts),
            management_accounts=sum(1 for a in accounts if a.is_management_account),
            member_accounts=sum(1 for a in accounts if not a.is_management_account)
        )
        
        return accounts
