"""
CUR (Cost and Usage Reports) discovery and validation.

Discovers CUR S3 locations from AWS management account and validates access.
"""

from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Dict, Tuple
import boto3
from botocore.exceptions import ClientError

from ..logging import get_logger
from ..config import get_settings

logger = get_logger(__name__)
settings = get_settings()


@dataclass
class CURLocation:
    """CUR report location information."""
    
    report_name: str
    s3_bucket: str
    s3_prefix: str
    format: str  # Should be 'Parquet'
    management_account_id: str
    compression: str
    versioning: str
    granularity: str  # 'HOURLY', 'DAILY', or 'MONTHLY' (must be HOURLY)
    has_resource_ids: bool = False  # INCLUDE_RESOURCES in AdditionalSchemaElements
    
    @property
    def s3_uri(self) -> str:
        """Get full S3 URI."""
        if self.s3_prefix:
            return f"s3://{self.s3_bucket}/{self.s3_prefix}"
        return f"s3://{self.s3_bucket}"


class CURDiscovery:
    """Discover and validate CUR report locations."""
    
    def __init__(self):
        """Initialize CUR discovery."""
        self.cur_api_region = 'us-east-1'  # CUR API only available in us-east-1
    
    async def discover_all_cur_reports(
        self,
        management_account_id: str,
        credentials: Optional[Dict] = None,
        hourly_only: bool = True
    ) -> list[CURLocation]:
        """
        Discover ALL CUR reports in the account.
        
        Args:
            management_account_id: Management account ID
            credentials: Optional temporary credentials from STS
            hourly_only: If True, only return HOURLY granularity reports (default: True)
        
        Returns:
            List of CURLocation objects (empty list if none found)
        """
        cur_locations = []
        
        try:
            cur_client = self._get_cur_client(credentials)
            response = cur_client.describe_report_definitions()
            
            for report in response.get('ReportDefinitions', []):
                report_format = report.get('Format', '')
                granularity = report.get('TimeUnit', 'UNKNOWN')
                
                # Skip non-Parquet reports
                if report_format != 'Parquet':
                    logger.debug(
                        "Skipping non-Parquet CUR report",
                        report_name=report['ReportName'],
                        format=report_format
                    )
                    continue
                
                # Skip non-HOURLY reports if hourly_only is True
                if hourly_only and granularity != 'HOURLY':
                    logger.debug(
                        "Skipping non-HOURLY CUR report",
                        report_name=report['ReportName'],
                        granularity=granularity
                    )
                    continue
                
                # Check if INCLUDE_RESOURCES is enabled
                has_resources = 'RESOURCES' in report.get('AdditionalSchemaElements', [])
                
                location = CURLocation(
                    report_name=report['ReportName'],
                    s3_bucket=report['S3Bucket'],
                    s3_prefix=report.get('S3Prefix', ''),
                    format=report_format,
                    management_account_id=management_account_id,
                    compression=report.get('Compression', 'Parquet'),
                    versioning=report.get('Versioning', 'CREATE_NEW_REPORT'),
                    granularity=granularity,
                    has_resource_ids=has_resources
                )
                
                cur_locations.append(location)
                logger.info(
                    "Found valid CUR report",
                    report_name=location.report_name,
                    bucket=location.s3_bucket,
                    granularity=location.granularity,
                    has_resource_ids=location.has_resource_ids
                )
            
            if not cur_locations:
                logger.warning(
                    "No HOURLY Parquet CUR reports found" if hourly_only else "No Parquet CUR reports found",
                    management_account_id=management_account_id,
                    hint="Ensure CUR is enabled with HOURLY granularity and Parquet format"
                )
            
            return cur_locations
            
        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', 'Unknown')
            error_message = e.response.get('Error', {}).get('Message', str(e))
            
            logger.warning(
                "CUR discovery failed",
                error_code=error_code,
                error_message=error_message,
                management_account_id=management_account_id,
                hint="Ensure IAM permissions allow cur:DescribeReportDefinitions"
            )
            return []
            
        except Exception as e:
            logger.warning(
                "Unexpected error during CUR discovery",
                error=str(e),
                management_account_id=management_account_id
            )
            return []
    
    def _get_cur_client(self, credentials: Optional[Dict]):
        """Get CUR API client with credentials."""
        client_kwargs = {
            'service_name': 'cur',
            'region_name': self.cur_api_region
        }
        
        if credentials:
            client_kwargs.update({
                'aws_access_key_id': credentials['AccessKeyId'],
                'aws_secret_access_key': credentials['SecretAccessKey'],
                'aws_session_token': credentials['SessionToken']
            })
        
        return boto3.client(**client_kwargs)
    
    def _parse_override_location(
        self,
        s3_uri: str,
        account_id: str
    ) -> CURLocation:
        """
        Parse manual S3 URI override.
        
        Args:
            s3_uri: S3 URI in format s3://bucket/prefix
            account_id: AWS account ID
        
        Returns:
            CURLocation object
        """
        # Remove s3:// prefix and split
        path = s3_uri.replace('s3://', '')
        parts = path.split('/', 1)
        
        bucket = parts[0]
        prefix = parts[1] if len(parts) > 1 else ''
        
        return CURLocation(
            report_name='manual-override',
            s3_bucket=bucket,
            s3_prefix=prefix,
            format='Parquet',
            management_account_id=account_id,
            compression='Parquet',
            versioning='OVERWRITE_REPORT',
            granularity='HOURLY'  # Assume HOURLY for manual override
        )
    
    async def validate_cur_access(
        self,
        location: CURLocation,
        credentials: Dict
    ) -> Tuple[bool, Optional[str]]:
        """
        Validate S3 access to CUR location.
        
        Args:
            location: CUR location information
            credentials: Temporary AWS credentials
        
        Returns:
            Tuple of (is_accessible, error_message)
        """
        try:
            s3_client = boto3.client(
                's3',
                aws_access_key_id=credentials['AccessKeyId'],
                aws_secret_access_key=credentials['SecretAccessKey'],
                aws_session_token=credentials['SessionToken']
            )
            
            # Try to list objects with limit 1
            response = s3_client.list_objects_v2(
                Bucket=location.s3_bucket,
                Prefix=location.s3_prefix,
                MaxKeys=1
            )
            
            # Check if we got any objects
            if 'Contents' in response and len(response['Contents']) > 0:
                logger.info(
                    "CUR S3 access validated",
                    bucket=location.s3_bucket,
                    prefix=location.s3_prefix
                )
                return True, None
            else:
                logger.warning(
                    "CUR S3 location is empty",
                    bucket=location.s3_bucket,
                    prefix=location.s3_prefix,
                    hint="CUR data may not be generated yet. Wait 24 hours after enabling CUR."
                )
                return False, "CUR S3 location is empty"
            
        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', 'Unknown')
            error_message = e.response.get('Error', {}).get('Message', str(e))
            
            logger.error(
                "CUR S3 access validation failed",
                error_code=error_code,
                error_message=error_message,
                bucket=location.s3_bucket,
                hint="Check S3 bucket permissions and IAM role"
            )
            return False, f"{error_code}: {error_message}"
            
        except Exception as e:
            logger.error(
                "Unexpected error validating CUR access",
                error=str(e),
                bucket=location.s3_bucket
            )
            return False, str(e)
    
    async def get_cur_date_range(
        self,
        location: CURLocation,
        credentials: Dict
    ) -> Tuple[Optional[datetime], Optional[datetime]]:
        """
        Determine available date range in CUR data.
        
        Args:
            location: CUR location
            credentials: AWS credentials
        
        Returns:
            Tuple of (earliest_date, latest_date) or (None, None) if unavailable
        """
        try:
            s3_client = boto3.client(
                's3',
                aws_access_key_id=credentials['AccessKeyId'],
                aws_secret_access_key=credentials['SecretAccessKey'],
                aws_session_token=credentials['SessionToken']
            )
            
            # List objects to find date folders
            # CUR structure: bucket/prefix/report-name/YYYYMMDD-YYYYMMDD/...
            paginator = s3_client.get_paginator('list_objects_v2')
            pages = paginator.paginate(
                Bucket=location.s3_bucket,
                Prefix=location.s3_prefix,
                Delimiter='/'
            )
            
            dates = []
            for page in pages:
                for prefix_info in page.get('CommonPrefixes', []):
                    prefix = prefix_info['Prefix']
                    # Extract date folder name (e.g., 20260101-20260131)
                    folder_name = prefix.rstrip('/').split('/')[-1]
                    
                    # Try to parse start date
                    if '-' in folder_name:
                        start_str = folder_name.split('-')[0]
                        try:
                            date = datetime.strptime(start_str, '%Y%m%d')
                            dates.append(date)
                        except ValueError:
                            continue
            
            if dates:
                earliest = min(dates)
                latest = max(dates)
                logger.info(
                    "CUR date range determined",
                    earliest=earliest.strftime('%Y-%m-%d'),
                    latest=latest.strftime('%Y-%m-%d')
                )
                return earliest, latest
            else:
                logger.warning("Could not determine CUR date range")
                return None, None
                
        except Exception as e:
            logger.error("Failed to determine CUR date range", error=str(e))
            return None, None
