"""
CUR (Cost and Usage Reports) data collection from S3 into DuckDB.

Uses DuckDB's httpfs extension to query Parquet files directly from S3,
filtering for DynamoDB usage data only.
"""

from datetime import datetime, timedelta
from typing import Optional, List, Tuple, Dict, Any
import duckdb
import boto3
import json

from ..logging import get_logger
from ..config import get_settings
from .cur_discovery import CURLocation

logger = get_logger(__name__)
settings = get_settings()


class CURCollector:
    """Collect CUR data from S3 into DuckDB."""
    
    def __init__(self, connection: duckdb.DuckDBPyConnection):
        """
        Initialize CUR collector.
        
        Args:
            connection: DuckDB connection
        """
        self.connection = connection
    
    async def collect_cur_data(
        self,
        location: CURLocation,
        credentials: dict,
        months: int = 3,
        force_refresh: bool = False
    ) -> Tuple[int, str]:
        """
        Collect CUR data from S3.
        
        Args:
            location: CUR location info
            credentials: Temporary AWS credentials from STS
            months: Number of months to collect (default: 3)
            force_refresh: If True, delete existing data and re-collect
        
        Returns:
            Tuple of (rows_collected, status_message)
        
        Raises:
            Exception if collection fails (fail hard as per requirements)
        """
        logger.info(
            "Starting CUR collection",
            bucket=location.s3_bucket,
            prefix=location.s3_prefix,
            months=months,
            force_refresh=force_refresh
        )
        
        try:
            # Step 1: Set up DuckDB for S3 access with correct region
            self._setup_duckdb_s3(credentials, location.s3_bucket)
            
            # Step 2: Clear existing data if force refresh
            if force_refresh:
                logger.info("Force refresh enabled - clearing existing CUR data")
                self.connection.execute("DELETE FROM cur_data")
                self.connection.commit()
            
            # Step 3: Build S3 paths for last N months
            s3_paths = self._build_s3_paths(location, months)
            logger.info("CUR S3 paths to query", paths=s3_paths)
            
            # Step 4: Load CUR data with filtering
            rows_collected = self._load_cur_data(s3_paths, location)
            
            logger.info("CUR collection complete", rows=rows_collected)
            return rows_collected, "complete"
            
        except Exception as e:
            logger.error("CUR collection failed", error=str(e), error_type=type(e).__name__)
            # Fail hard as per requirements
            raise Exception(f"CUR collection failed: {str(e)}") from e
    
    def _setup_duckdb_s3(self, credentials: dict, bucket_name: str) -> None:
        """
        Set up DuckDB httpfs extension and AWS credentials.
        
        Args:
            credentials: AWS credentials dict with AccessKeyId, SecretAccessKey, SessionToken
            bucket_name: S3 bucket name (to detect region)
        
        Raises:
            Exception if setup fails
        """
        try:
            # Detect bucket region
            bucket_region = self._get_bucket_region(bucket_name)
            logger.info(f"Detected S3 bucket region: {bucket_region}")
            
            # Install and load httpfs extension
            logger.debug("Installing DuckDB httpfs extension")
            self.connection.execute("INSTALL httpfs;")
            self.connection.execute("LOAD httpfs;")
            
            # Create or replace AWS secret for S3 access with correct region
            logger.debug("Configuring AWS credentials in DuckDB", region=bucket_region)
            self.connection.execute(f"""
                CREATE OR REPLACE SECRET aws_credentials (
                    TYPE S3,
                    KEY_ID '{credentials['AccessKeyId']}',
                    SECRET '{credentials['SecretAccessKey']}',
                    SESSION_TOKEN '{credentials['SessionToken']}',
                    REGION '{bucket_region}'
                );
            """)
            
            logger.info("DuckDB S3 access configured successfully", region=bucket_region)
            
        except Exception as e:
            logger.error("Failed to set up DuckDB S3 access", error=str(e))
            raise Exception(f"DuckDB S3 setup failed: {str(e)}") from e
    
    def _get_bucket_region(self, bucket_name: str) -> str:
        """
        Detect the region of an S3 bucket.
        
        Args:
            bucket_name: Name of the S3 bucket
        
        Returns:
            AWS region string (e.g., 'us-west-2')
        """
        try:
            s3_client = boto3.client('s3')
            
            # Get bucket location
            response = s3_client.get_bucket_location(Bucket=bucket_name)
            
            # LocationConstraint is None for us-east-1
            location = response.get('LocationConstraint')
            if location is None:
                return 'us-east-1'
            
            return location
            
        except Exception as e:
            logger.warning(
                f"Failed to detect bucket region for {bucket_name}: {e}. "
                "Falling back to us-east-1"
            )
            return 'us-east-1'
    
    def _build_s3_paths(
        self,
        location: CURLocation,
        months: int
    ) -> List[str]:
        """
        Build S3 paths by discovering and parsing CUR manifest files.
        
        Uses manifest-driven approach:
        1. Find manifest.json files for last N months
        2. Parse each manifest to get reportKeys
        3. Return exact S3 paths from manifests
        
        Args:
            location: CUR location info
            months: Number of months to include
        
        Returns:
            List of exact S3 paths to Parquet files
        """
        logger.info("Discovering CUR manifests", months=months)
        
        # Find manifest files
        manifests = self._find_manifests(location, months)
        
        if not manifests:
            logger.warning("No manifests found, falling back to pattern-based search")
            # Fallback: use glob pattern if no manifests found
            base_path = f"s3://{location.s3_bucket}"
            if location.s3_prefix:
                base_path = f"{base_path}/{location.s3_prefix}"
            return [f"{base_path}/{location.report_name}/**/*.parquet"]
        
        # Extract reportKeys from all manifests
        all_paths = []
        for manifest_data in manifests:
            report_keys = manifest_data.get('reportKeys', [])
            for key in report_keys:
                # Build full S3 path
                full_path = f"s3://{location.s3_bucket}/{key}"
                all_paths.append(full_path)
                logger.debug("Found CUR file from manifest", path=full_path)
        
        logger.info("Discovered CUR files from manifests", count=len(all_paths))
        return all_paths
    
    def _find_manifests(
        self,
        location: CURLocation,
        months: int
    ) -> List[Dict[str, Any]]:
        """
        Find and parse CUR manifest files for last N months.
        
        Manifest path pattern: 
        s3://bucket/prefix/report-name/YYYYMMDD-YYYYMMDD/report-name-Manifest.json
        
        Args:
            location: CUR location info
            months: Number of months to look back
        
        Returns:
            List of parsed manifest dictionaries
        """
        s3_client = boto3.client('s3')
        manifests = []
        
        # Build base prefix for manifests
        prefix = location.s3_prefix if location.s3_prefix else ""
        if prefix and not prefix.endswith('/'):
            prefix += '/'
        prefix += f"{location.report_name}/"
        
        try:
            # List objects under the report prefix
            logger.debug("Listing S3 objects", bucket=location.s3_bucket, prefix=prefix)
            
            paginator = s3_client.get_paginator('list_objects_v2')
            page_iterator = paginator.paginate(
                Bucket=location.s3_bucket,
                Prefix=prefix
            )
            
            # Find all manifest files
            manifest_keys = []
            for page in page_iterator:
                if 'Contents' not in page:
                    continue
                    
                for obj in page['Contents']:
                    key = obj['Key']
                    # Look for manifest files
                    if key.endswith('-Manifest.json') or key.endswith('/Manifest.json'):
                        manifest_keys.append(key)
            
            logger.info("Found manifest files", count=len(manifest_keys))
            
            # Parse each manifest (limit to most recent based on months)
            # Sort by key (date-based folders come first when sorted)
            manifest_keys.sort(reverse=True)
            
            for manifest_key in manifest_keys[:months]:  # Limit to N most recent
                try:
                    logger.debug("Parsing manifest", key=manifest_key)
                    
                    # Download and parse manifest
                    response = s3_client.get_object(
                        Bucket=location.s3_bucket,
                        Key=manifest_key
                    )
                    
                    manifest_content = response['Body'].read().decode('utf-8')
                    manifest_data = json.loads(manifest_content)
                    
                    # Validate it's a Parquet manifest
                    if manifest_data.get('compression') == 'Parquet':
                        manifests.append(manifest_data)
                        logger.debug(
                            "Parsed Parquet manifest",
                            report_keys_count=len(manifest_data.get('reportKeys', []))
                        )
                    else:
                        logger.warning(
                            "Skipping non-Parquet manifest",
                            compression=manifest_data.get('compression')
                        )
                        
                except Exception as e:
                    logger.warning(f"Failed to parse manifest {manifest_key}: {e}")
                    continue
            
            return manifests
            
        except Exception as e:
            logger.error("Failed to find manifests", error=str(e))
            return []
    
    def _detect_available_columns(self, s3_paths: List[str]) -> set:
        """
        Detect which columns are available in the CUR Parquet files.
        
        Args:
            s3_paths: List of S3 paths to Parquet files
        
        Returns:
            Set of available column names (lowercase)
        """
        try:
            # Query just the schema of the first file
            first_file = s3_paths[0] if s3_paths else None
            if not first_file:
                return set()
            
            result = self.connection.execute(f"""
                SELECT * FROM read_parquet('{first_file}', union_by_name=true)
                LIMIT 0
            """).description
            
            columns = {col[0].lower() for col in result}
            logger.info(f"Detected {len(columns)} columns in CUR Parquet files")
            
            return columns
            
        except Exception as e:
            logger.warning(f"Could not detect CUR columns: {e}")
            return set()
    
    def _load_cur_data(
        self,
        s3_paths: List[str],
        location: CURLocation
    ) -> int:
        """
        Load CUR data from S3 Parquet files into DuckDB.
        
        Filters for DynamoDB usage only and extracts required columns.
        Conditionally extracts resource_name if INCLUDE_RESOURCES is enabled.
        
        Args:
            s3_paths: List of S3 glob patterns
            location: CUR location info
        
        Returns:
            Number of rows inserted
        """
        # Detect available columns first
        available_columns = self._detect_available_columns(s3_paths)
        
        # Check which optional columns exist
        has_resource_id = 'line_item_resource_id' in available_columns
        has_net_unblended_cost = 'line_item_net_unblended_cost' in available_columns
        
        logger.info(
            "CUR column availability",
            has_resource_id=has_resource_id,
            has_net_unblended_cost=has_net_unblended_cost,
            has_resource_ids_config=location.has_resource_ids
        )
        
        # Build file list for read_parquet
        file_list = ", ".join([f"'{path}'" for path in s3_paths])
        
        # Build conditional resource_name extraction
        # Only if BOTH: (1) CUR config has INCLUDE_RESOURCES AND (2) column actually exists
        if location.has_resource_ids and has_resource_id:
            resource_name_expr = """
            CASE 
                WHEN line_item_resource_id LIKE 'arn:aws:dynamodb:%table/%' 
                THEN regexp_extract(line_item_resource_id, 'table/(.+)$', 1)
                ELSE NULL
            END as resource_name"""
            
            where_clause = """
            WHERE product_product_name = 'Amazon DynamoDB'
                AND line_item_line_item_type = 'Usage'
                AND line_item_product_code = 'AmazonDynamoDB'
                AND line_item_resource_id IS NOT NULL"""
            
            group_by_resource = "line_item_resource_id,"
            
            logger.info("CUR has INCLUDE_RESOURCES enabled - extracting resource names from ARNs")
        else:
            resource_name_expr = "NULL as resource_name"
            
            where_clause = """
            WHERE product_product_name = 'Amazon DynamoDB'
                AND line_item_line_item_type = 'Usage'
                AND line_item_product_code = 'AmazonDynamoDB'"""
            
            group_by_resource = ""
            
            logger.info("CUR does not have INCLUDE_RESOURCES - resource names will be NULL")
        
        # Build cost expressions with fallbacks (no aggregation - raw line items)
        if has_net_unblended_cost:
            net_cost_expr = "line_item_net_unblended_cost"
            logger.info("Using line_item_net_unblended_cost column")
        else:
            net_cost_expr = "line_item_unblended_cost"
            logger.info("Falling back to line_item_unblended_cost for net_unblended_cost (column not in CUR)")
        
        # SQL query for DynamoDB CUR data - raw line items (no aggregation)
        # Uses ON CONFLICT to handle duplicates gracefully (upsert)
        query = f"""
        INSERT INTO cur_data (
            identity_line_item_id,
            identity_time_interval,
            account_id,
            region,
            resource_name,
            usage_month,
            operation,
            usage_type,
            line_item_type,
            usage_start_date,
            usage_end_date,
            usage_amount,
            unblended_cost,
            net_unblended_cost,
            blended_cost,
            line_item_description
        )
        SELECT 
            identity_line_item_id,
            identity_time_interval,
            line_item_usage_account_id as account_id,
            product_region as region,
            {resource_name_expr},
            date_trunc('month', line_item_usage_start_date) as usage_month,
            line_item_operation as operation,
            line_item_usage_type as usage_type,
            line_item_line_item_type as line_item_type,
            line_item_usage_start_date,
            line_item_usage_end_date,
            line_item_usage_amount as usage_amount,
            line_item_unblended_cost as unblended_cost,
            {net_cost_expr} as net_unblended_cost,
            line_item_blended_cost as blended_cost,
            line_item_line_item_description as line_item_description
        FROM read_parquet([{file_list}], hive_partitioning=true, union_by_name=true)
        {where_clause}
        ON CONFLICT (identity_line_item_id, identity_time_interval) DO UPDATE SET
            usage_amount = EXCLUDED.usage_amount,
            unblended_cost = EXCLUDED.unblended_cost,
            net_unblended_cost = EXCLUDED.net_unblended_cost,
            blended_cost = EXCLUDED.blended_cost
        """
        
        logger.info("Executing CUR data load query")
        logger.debug("Query", query=query[:500] + "...")  # Log first 500 chars
        
        try:
            # Execute query
            result = self.connection.execute(query)
            self.connection.commit()
            
            # Get row count
            count_result = self.connection.execute(
                "SELECT COUNT(*) FROM cur_data"
            ).fetchone()
            
            row_count = count_result[0] if count_result else 0
            
            logger.info(
                "CUR data loaded successfully",
                rows=row_count,
                s3_paths_count=len(s3_paths)
            )
            
            return row_count
            
        except Exception as e:
            logger.error(
                "Failed to load CUR data",
                error=str(e),
                error_type=type(e).__name__
            )
            raise Exception(f"CUR data load failed: {str(e)}") from e
    
    def get_collection_summary(self) -> dict:
        """
        Get summary of collected CUR data.
        
        Returns:
            Dictionary with collection statistics
        """
        try:
            summary = {}
            
            # Total rows
            total_result = self.connection.execute(
                "SELECT COUNT(*) FROM cur_data"
            ).fetchone()
            summary['total_rows'] = total_result[0] if total_result else 0
            
            # Unique resources (tables, GSIs, etc.)
            resources_result = self.connection.execute(
                "SELECT COUNT(DISTINCT resource_name) FROM cur_data WHERE resource_name IS NOT NULL"
            ).fetchone()
            summary['unique_resources'] = resources_result[0] if resources_result else 0
            
            # Unique accounts
            accounts_result = self.connection.execute(
                "SELECT COUNT(DISTINCT account_id) FROM cur_data"
            ).fetchone()
            summary['unique_accounts'] = accounts_result[0] if accounts_result else 0
            
            # Date range
            date_range_result = self.connection.execute("""
                SELECT 
                    MIN(usage_month) as earliest,
                    MAX(usage_month) as latest
                FROM cur_data
            """).fetchone()
            
            if date_range_result and date_range_result[0]:
                summary['earliest_month'] = date_range_result[0]
                summary['latest_month'] = date_range_result[1]
            
            # Total costs (prefer net_unblended_cost if available)
            cost_result = self.connection.execute("""
                SELECT 
                    SUM(unblended_cost) as unblended,
                    SUM(net_unblended_cost) as net_unblended,
                    SUM(blended_cost) as blended
                FROM cur_data
            """).fetchone()
            
            if cost_result:
                summary['total_unblended_cost_usd'] = float(cost_result[0]) if cost_result[0] else 0.0
                summary['total_net_unblended_cost_usd'] = float(cost_result[1]) if cost_result[1] else 0.0
                summary['total_blended_cost_usd'] = float(cost_result[2]) if cost_result[2] else 0.0
            
            return summary
            
        except Exception as e:
            logger.error("Failed to get collection summary", error=str(e))
            return {'error': str(e)}
    
    def validate_cur_data(self) -> Tuple[bool, List[str]]:
        """
        Validate collected CUR data.
        
        Returns:
            Tuple of (is_valid, list of issues)
        """
        issues = []
        
        try:
            # Check if data exists
            count_result = self.connection.execute(
                "SELECT COUNT(*) FROM cur_data"
            ).fetchone()
            
            if not count_result or count_result[0] == 0:
                issues.append("No CUR data collected")
                return False, issues
            
            # Check for NULL resource names (informational, not an error if INCLUDE_RESOURCES not enabled)
            null_resources = self.connection.execute("""
                SELECT COUNT(*) 
                FROM cur_data 
                WHERE resource_name IS NULL
            """).fetchone()
            
            if null_resources and null_resources[0] > 0:
                pct = (null_resources[0] / count_result[0]) * 100
                if pct == 100:
                    issues.append("No resource names available - CUR may not have INCLUDE_RESOURCES enabled")
            
            # Check for zero costs
            zero_cost = self.connection.execute("""
                SELECT COUNT(*) 
                FROM cur_data 
                WHERE unblended_cost = 0
            """).fetchone()
            
            if zero_cost and zero_cost[0] > 0:
                pct = (zero_cost[0] / count_result[0]) * 100
                if pct > 50:
                    issues.append(f"{pct:.1f}% of rows have zero cost")
            
            # Check date coverage
            months_result = self.connection.execute("""
                SELECT COUNT(DISTINCT usage_month) 
                FROM cur_data
            """).fetchone()
            
            if months_result and months_result[0] < 2:
                issues.append(f"Only {months_result[0]} month(s) of data - recommend 3+ months")
            
            is_valid = len(issues) == 0
            return is_valid, issues
            
        except Exception as e:
            logger.error("CUR data validation failed", error=str(e))
            return False, [f"Validation error: {str(e)}"]
