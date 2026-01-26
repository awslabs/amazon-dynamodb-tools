"""
AWS client factory with profile and credential support.

Provides unified AWS client creation with automatic credential resolution
from profiles, environment variables, or explicit configuration.
Enhanced with connection pooling, retry logic, and comprehensive validation.
"""

import asyncio
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Set

import aioboto3
import boto3
from botocore.config import Config
from botocore.exceptions import (
    BotoCoreError,
    ClientError,
    EndpointConnectionError,
    NoCredentialsError,
    ProfileNotFound,
)
from botocore.retries import adaptive

from ..config import get_settings
from ..logging import get_logger

logger = get_logger("dynamodb_optima.aws.client")


@dataclass
class CredentialValidationResult:
    """Result of credential validation across regions."""

    is_valid: bool
    valid_regions: List[str]
    invalid_regions: List[str]
    account_id: Optional[str]
    user_arn: Optional[str]
    error_messages: Dict[str, str]
    validation_time: datetime


@dataclass
class RegionValidationResult:
    """Result of validating a specific region."""

    region: str
    is_valid: bool
    error_message: Optional[str]
    response_time_ms: Optional[float]


class AWSClientManager:
    """
    Manages AWS client creation with flexible authentication, connection pooling,
    and comprehensive validation across all regions.
    """

    def __init__(
        self,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None
    ):
        """
        Initialize AWS client manager with enhanced configuration.
        
        Args:
            aws_access_key_id: Optional AWS access key ID (for cross-account credentials)
            aws_secret_access_key: Optional AWS secret access key (for cross-account credentials)
            aws_session_token: Optional AWS session token (for cross-account credentials)
        """
        self.settings = get_settings()
        self._session_cache: Dict[str, boto3.Session] = {}
        self._async_session_cache: Dict[str, aioboto3.Session] = {}
        self._validated_regions: Set[str] = set()
        self._validation_cache_expiry: Optional[datetime] = None
        self._validation_cache_duration = timedelta(
            hours=1
        )  # Cache validation for 1 hour
        
        # Store override credentials for cross-account access
        self._override_credentials: Optional[Dict[str, str]] = None
        if aws_access_key_id and aws_secret_access_key:
            self._override_credentials = {
                'aws_access_key_id': aws_access_key_id,
                'aws_secret_access_key': aws_secret_access_key,
                'aws_session_token': aws_session_token
            }

        # Enhanced boto3 configuration with retry logic and connection pooling
        self._boto_config = Config(
            retries={"mode": "adaptive", "max_attempts": 5},
            max_pool_connections=50,  # Connection pooling
            region_name=self.settings.aws_region,
            connect_timeout=10,
            read_timeout=30,
            parameter_validation=True,
        )

    def _get_session_kwargs(self, region: Optional[str] = None) -> Dict[str, Any]:
        """Get session configuration based on settings priority."""
        kwargs = {}

        # Set region
        target_region = region or self.settings.aws_region
        kwargs["region_name"] = target_region

        # Priority: override credentials > explicit credentials > profile > environment/IAM
        if self._override_credentials:
            # Use override credentials (for cross-account access)
            kwargs.update(self._override_credentials)
            logger.info("Using override credentials (cross-account)", region=target_region)

        elif self.settings.aws_access_key_id and self.settings.aws_secret_access_key:
            # Use explicit credentials from settings
            kwargs.update(
                {
                    "aws_access_key_id": self.settings.aws_access_key_id,
                    "aws_secret_access_key": self.settings.aws_secret_access_key,
                }
            )
            if self.settings.aws_session_token:
                kwargs["aws_session_token"] = self.settings.aws_session_token

            logger.info("Using explicit AWS credentials", region=target_region)

        elif self.settings.aws_profile:
            # Use named profile
            kwargs["profile_name"] = self.settings.aws_profile
            logger.info(
                "Using AWS profile",
                profile=self.settings.aws_profile,
                region=target_region,
            )

        else:
            # Use default credential chain (environment, IAM role, etc.)
            logger.info("Using default AWS credential chain", region=target_region)

        return kwargs

    def _get_boto_config(self, region: Optional[str] = None) -> Config:
        """Get boto3 configuration with region-specific settings."""
        target_region = region or self.settings.aws_region
        return Config(
            retries={"mode": "adaptive", "max_attempts": 5},
            max_pool_connections=50,
            region_name=target_region,
            connect_timeout=10,
            read_timeout=30,
            parameter_validation=True,
        )

    def get_session(self, region: Optional[str] = None) -> boto3.Session:
        """Get boto3 session for specified region with enhanced error handling."""
        target_region = region or self.settings.aws_region
        cache_key = f"{self.settings.aws_profile or 'default'}:{target_region}"

        if cache_key not in self._session_cache:
            try:
                kwargs = self._get_session_kwargs(target_region)
                session = boto3.Session(**kwargs)

                # Test the session by getting caller identity with retry logic
                sts = session.client("sts", config=self._get_boto_config(target_region))
                identity = sts.get_caller_identity()

                logger.info(
                    "AWS session created successfully",
                    region=target_region,
                    account_id=identity.get("Account"),
                    user_arn=identity.get("Arn"),
                )

                self._session_cache[cache_key] = session

            except ProfileNotFound as e:
                available_profiles = self.list_available_profiles()
                profiles_str = ", ".join(available_profiles) or "None"
                error_msg = (
                    f"AWS profile '{self.settings.aws_profile}' not found. "
                    f"Available profiles: {profiles_str}"
                )
                logger.error(
                    "AWS profile not found",
                    profile=self.settings.aws_profile,
                    error=str(e),
                )
                raise ValueError(error_msg) from e

            except NoCredentialsError as e:
                error_msg = (
                    "No AWS credentials found. Please configure credentials:\n"
                    "1. Environment variables: AWS_ACCESS_KEY_ID, "
                    "AWS_SECRET_ACCESS_KEY\n"
                    "2. AWS profile: Set AWS_PROFILE environment variable\n"
                    "3. IAM role: Ensure EC2 instance has appropriate IAM role\n"
                    "4. AWS credentials file: ~/.aws/credentials"
                )
                logger.error("No AWS credentials found", error=str(e))
                raise ValueError(error_msg) from e

            except ClientError as e:
                error_code = e.response.get("Error", {}).get("Code", "Unknown")
                error_message = e.response.get("Error", {}).get("Message", str(e))
                error_msg = f"AWS API error ({error_code}): {error_message}"
                logger.error(
                    "AWS API error during session creation",
                    region=target_region,
                    error=error_msg,
                )
                raise ValueError(error_msg) from e

            except EndpointConnectionError as e:
                error_msg = (
                    f"Cannot connect to AWS region '{target_region}'. "
                    "Check internet connectivity and region name."
                )
                logger.error(
                    "AWS endpoint connection error", region=target_region, error=str(e)
                )
                raise ValueError(error_msg) from e

            except Exception as e:
                error_msg = (
                    f"Failed to create AWS session for region '{target_region}': "
                    f"{str(e)}"
                )
                logger.error(
                    "Unexpected error creating AWS session",
                    region=target_region,
                    error=str(e),
                )
                raise ValueError(error_msg) from e

        return self._session_cache[cache_key]

    def get_client(self, service: str, region: Optional[str] = None):
        """Get boto3 client for specified service and region."""
        session = self.get_session(region)
        target_region = region or self.settings.aws_region
        return session.client(service, config=self._get_boto_config(target_region))

    def get_async_session(self, region: Optional[str] = None) -> aioboto3.Session:
        """Get aioboto3 session for async operations with caching."""
        target_region = region or self.settings.aws_region
        cache_key = f"{self.settings.aws_profile or 'default'}:{target_region}"

        if cache_key not in self._async_session_cache:
            kwargs = self._get_session_kwargs(target_region)
            session = aioboto3.Session(**kwargs)
            self._async_session_cache[cache_key] = session

        return self._async_session_cache[cache_key]

    async def get_async_client(self, service: str, region: Optional[str] = None):
        """Get aioboto3 client for async operations with enhanced configuration."""
        session = self.get_async_session(region)
        target_region = region or self.settings.aws_region

        # Create async client with enhanced configuration
        return session.client(service, config=self._get_boto_config(target_region))

    def list_available_profiles(self) -> List[str]:
        """List available AWS profiles from credentials file."""
        try:
            session = boto3.Session()
            return session.available_profiles
        except Exception as e:
            logger.warning("Could not list AWS profiles", error=str(e))
            return []

    async def validate_region_async(self, region: str) -> RegionValidationResult:
        """Validate a single region asynchronously with detailed error reporting."""
        start_time = datetime.now()

        try:
            # Use STS to validate credentials and region access (lightweight call)
            async with await self.get_async_client("sts", region) as sts_client:
                await sts_client.get_caller_identity()

            response_time = (datetime.now() - start_time).total_seconds() * 1000

            logger.debug(
                "Region validated successfully",
                region=region,
                response_time_ms=response_time,
            )
            return RegionValidationResult(
                region=region,
                is_valid=True,
                error_message=None,
                response_time_ms=response_time,
            )

        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code", "Unknown")
            error_message = e.response.get("Error", {}).get("Message", str(e))
            full_error = f"AWS API error ({error_code}): {error_message}"

            logger.warning("Region validation failed", region=region, error=full_error)
            return RegionValidationResult(
                region=region,
                is_valid=False,
                error_message=full_error,
                response_time_ms=None,
            )

        except EndpointConnectionError as e:
            error_message = f"Cannot connect to region '{region}': {str(e)}"
            logger.warning(
                "Region connection failed", region=region, error=error_message
            )
            return RegionValidationResult(
                region=region,
                is_valid=False,
                error_message=error_message,
                response_time_ms=None,
            )

        except Exception as e:
            error_message = f"Unexpected error validating region '{region}': {str(e)}"
            logger.warning(
                "Region validation error", region=region, error=error_message
            )
            return RegionValidationResult(
                region=region,
                is_valid=False,
                error_message=error_message,
                response_time_ms=None,
            )

    async def validate_credentials_comprehensive(
        self, regions: Optional[List[str]] = None
    ) -> CredentialValidationResult:
        """
        Comprehensive credential validation across all specified regions.

        Args:
            regions: List of regions to validate. If None, uses settings.aws_regions

        Returns:
            CredentialValidationResult with detailed validation information
        """
        validation_start = datetime.now()
        target_regions = regions or self.settings.aws_regions

        logger.info(
            "Starting comprehensive credential validation", regions=target_regions
        )

        # Check if we have cached validation results that are still valid
        if (
            self._validation_cache_expiry
            and datetime.now() < self._validation_cache_expiry
            and all(region in self._validated_regions for region in target_regions)
        ):

            logger.info("Using cached validation results")
            return CredentialValidationResult(
                is_valid=True,
                valid_regions=list(
                    self._validated_regions.intersection(target_regions)
                ),
                invalid_regions=[],
                account_id=None,
                user_arn=None,
                error_messages={},
                validation_time=validation_start,
            )

        # Validate all regions concurrently
        validation_tasks = [
            self.validate_region_async(region) for region in target_regions
        ]

        try:
            # Use asyncio.gather with timeout to prevent hanging
            region_results = await asyncio.wait_for(
                asyncio.gather(*validation_tasks, return_exceptions=True),
                timeout=60.0,  # 60 second timeout for all validations
            )
        except asyncio.TimeoutError:
            logger.error("Credential validation timed out after 60 seconds")
            return CredentialValidationResult(
                is_valid=False,
                valid_regions=[],
                invalid_regions=target_regions,
                account_id=None,
                user_arn=None,
                error_messages={
                    region: "Validation timed out" for region in target_regions
                },
                validation_time=validation_start,
            )

        # Process results
        valid_regions = []
        invalid_regions = []
        error_messages = {}
        account_id = None
        user_arn = None

        for i, result in enumerate(region_results):
            if isinstance(result, Exception):
                region = target_regions[i]
                invalid_regions.append(region)
                error_messages[region] = f"Validation exception: {str(result)}"
                continue

            if result.is_valid:
                valid_regions.append(result.region)
                self._validated_regions.add(result.region)
            else:
                invalid_regions.append(result.region)
                error_messages[result.region] = result.error_message or "Unknown error"

        # Get account information from the first valid region
        if valid_regions:
            try:
                async with await self.get_async_client(
                    "sts", valid_regions[0]
                ) as sts_client:
                    identity = await sts_client.get_caller_identity()
                    account_id = identity.get("Account")
                    user_arn = identity.get("Arn")
            except Exception as e:
                logger.warning("Could not get caller identity", error=str(e))

        # Update cache
        self._validation_cache_expiry = datetime.now() + self._validation_cache_duration

        is_valid = len(valid_regions) > 0

        logger.info(
            "Credential validation completed",
            valid_regions=len(valid_regions),
            invalid_regions=len(invalid_regions),
            account_id=account_id,
            duration_seconds=(datetime.now() - validation_start).total_seconds(),
        )

        return CredentialValidationResult(
            is_valid=is_valid,
            valid_regions=valid_regions,
            invalid_regions=invalid_regions,
            account_id=account_id,
            user_arn=user_arn,
            error_messages=error_messages,
            validation_time=validation_start,
        )

    def validate_regions(self, regions: List[str]) -> List[str]:
        """
        Synchronous region validation (legacy method for backward compatibility).
        For new code, use validate_credentials_comprehensive() instead.
        """
        valid_regions = []

        for region in regions:
            try:
                # Test region by getting caller identity (lightweight call)
                sts = self.get_client("sts", region)
                sts.get_caller_identity()
                valid_regions.append(region)
                logger.debug("Region validated", region=region)

            except Exception as e:
                logger.warning("Region not accessible", region=region, error=str(e))

        return valid_regions

    def clear_cache(self):
        """Clear all caches (useful for credential rotation)."""
        self._session_cache.clear()
        self._async_session_cache.clear()
        self._validated_regions.clear()
        self._validation_cache_expiry = None
        logger.info("AWS client caches cleared")

    async def test_service_access(self, service: str, region: str) -> bool:
        """
        Test access to a specific AWS service in a region.

        Args:
            service: AWS service name (e.g., 'dynamodb', 'cloudwatch')
            region: AWS region to test

        Returns:
            True if service is accessible, False otherwise
        """
        try:
            if service == "dynamodb":
                async with await self.get_async_client("dynamodb", region) as client:
                    # List tables with limit to minimize cost/impact
                    await client.list_tables(Limit=1)

            elif service == "cloudwatch":
                async with await self.get_async_client("cloudwatch", region) as client:
                    # List metrics with minimal parameters - no MaxRecords
                    await client.list_metrics()

            else:
                # Generic test using STS
                async with await self.get_async_client("sts", region) as client:
                    await client.get_caller_identity()

            logger.debug("Service access validated", service=service, region=region)
            return True

        except Exception as e:
            logger.warning(
                "Service access failed", service=service, region=region, error=str(e)
            )
            return False


# Global client manager instance
aws_client_manager = AWSClientManager()


def get_aws_client(service: str, region: Optional[str] = None):
    """Get AWS client for dependency injection."""
    return aws_client_manager.get_client(service, region)


async def get_async_aws_client(service: str, region: Optional[str] = None):
    """Get async AWS client for dependency injection."""
    return await aws_client_manager.get_async_client(service, region)


async def validate_aws_credentials(
    regions: Optional[List[str]] = None,
) -> CredentialValidationResult:
    """
    Validate AWS credentials across specified regions.

    Args:
        regions: List of regions to validate. If None, uses configured regions.

    Returns:
        CredentialValidationResult with validation details
    """
    return await aws_client_manager.validate_credentials_comprehensive(regions)


def get_available_aws_profiles() -> List[str]:
    """Get list of available AWS profiles."""
    return aws_client_manager.list_available_profiles()


def clear_aws_cache():
    """Clear AWS client caches (useful for credential rotation)."""
    aws_client_manager.clear_cache()
