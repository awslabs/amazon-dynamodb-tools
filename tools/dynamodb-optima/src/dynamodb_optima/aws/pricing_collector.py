"""
AWS Pricing API integration for comprehensive DynamoDB pricing data.

Dynamically discovers available attributes and collects ALL DynamoDB SKUs
for all regions, storing complete pricing information in DuckDB.
"""

import json
import uuid
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Dict, List, Optional, Set

import aioboto3
from botocore.exceptions import ClientError

from ..config import get_settings
from ..logging import get_logger

logger = get_logger(__name__)

# Region groupings for pricing API endpoint selection
# Pricing API is only available in us-east-1 and ap-south-1
AMERICAN_REGIONS = [
    "us-east-1", "us-east-2", "us-west-1", "us-west-2",
    "us-gov-west-1", "us-gov-east-1",
    "ca-central-1", "ca-west-1", "sa-east-1"
]


class PricingCollector:
    """
    Comprehensive collector for DynamoDB pricing data from AWS Pricing API.
    
    Discovers available attributes dynamically and collects all SKUs for all regions.
    """
    
    def __init__(self):
        """Initialize pricing collector."""
        self.settings = get_settings()
        self.session = aioboto3.Session()
        self._available_attributes: Optional[List[str]] = None
    
    def _get_pricing_api_region(self, target_region: str) -> str:
        """
        Determine which Pricing API endpoint to use.
        
        Args:
            target_region: The region we're collecting pricing for
        
        Returns:
            Pricing API region ('us-east-1' or 'ap-south-1')
        """
        if target_region in AMERICAN_REGIONS:
            return "us-east-1"
        return "ap-south-1"
    
    async def discover_available_attributes(self) -> List[str]:
        """
        Discover available attributes for DynamoDB service.
        
        Calls describe-services to get the list of attributes we can query.
        
        Returns:
            List of attribute names available for DynamoDB
        """
        if self._available_attributes is not None:
            return self._available_attributes
        
        logger.info("Discovering available DynamoDB pricing attributes")
        
        # Use us-east-1 for service discovery
        async with self.session.client("pricing", region_name="us-east-1") as pricing_client:
            try:
                response = await pricing_client.describe_services(
                    ServiceCode="AmazonDynamoDB"
                )
                
                if not response.get("Services"):
                    raise ValueError("No services returned from describe_services")
                
                self._available_attributes = response["Services"][0]["AttributeNames"]
                
                logger.info(
                    "Discovered DynamoDB pricing attributes",
                    count=len(self._available_attributes),
                    attributes=self._available_attributes
                )
                
                return self._available_attributes
                
            except ClientError as e:
                logger.error("Failed to discover pricing attributes", error=str(e))
                raise
    
    async def collect_region_pricing(
        self,
        region_code: str,
        available_attributes: List[str]
    ) -> List[Dict]:
        """
        Collect all DynamoDB pricing data for a specific region.
        
        Args:
            region_code: AWS region code (e.g., 'us-east-1')
            available_attributes: List of attributes discovered from describe-services
        
        Returns:
            List of pricing records with all available attributes
        """
        logger.info("Collecting comprehensive pricing data", region=region_code)
        
        pricing_api_region = self._get_pricing_api_region(region_code)
        products = []
        
        async with self.session.client("pricing", region_name=pricing_api_region) as pricing_client:
            try:
                # Collect with pagination
                next_token = None
                page_count = 0
                
                while True:
                    page_count += 1
                    
                    params = {
                        "ServiceCode": "AmazonDynamoDB",
                        "Filters": [
                            {
                                "Type": "TERM_MATCH",
                                "Field": "regionCode",
                                "Value": region_code
                            }
                        ],
                        "FormatVersion": "aws_v1",
                        "MaxResults": 100
                    }
                    
                    if next_token:
                        params["NextToken"] = next_token
                    
                    logger.debug(
                        f"Fetching pricing page {page_count}",
                        region=region_code
                    )
                    
                    response = await pricing_client.get_products(**params)
                    
                    # Parse each product
                    for price_list_item in response.get("PriceList", []):
                        product = json.loads(price_list_item)
                        parsed_records = self._parse_product(product, region_code, available_attributes)
                        # _parse_product now returns a list of records (one per price dimension)
                        if parsed_records:
                            products.extend(parsed_records)
                    
                    # Check for more pages
                    next_token = response.get("NextToken")
                    if not next_token:
                        break
                
                logger.info(
                    "Pricing data collected",
                    region=region_code,
                    products=len(products),
                    pages=page_count
                )
                
                return products
                
            except ClientError as e:
                logger.error(
                    "Failed to collect pricing",
                    region=region_code,
                    error=str(e)
                )
                raise
    
    def _parse_product(
        self,
        product: dict,
        region_code: str,
        available_attributes: List[str]
    ) -> List[Dict]:
        """
        Parse a product from the pricing API response.
        
        Returns ALL price dimensions (including free tier) as separate records.
        
        Args:
            product: Raw product JSON from AWS
            region_code: Region code for this product
            available_attributes: List of available attributes
        
        Returns:
            List of parsed pricing records (one per price dimension)
        """
        try:
            # Extract product attributes
            attrs = product.get("product", {}).get("attributes", {})
            sku = product.get("product", {}).get("sku")
            
            # Extract pricing from terms
            # We focus on OnDemand pricing (Reserved can be added later if needed)
            terms = product.get("terms", {})
            on_demand_terms = terms.get("OnDemand", {})
            
            if not on_demand_terms:
                # No pricing terms available
                return []
            
            # Get the first term (usually only one)
            term_key = list(on_demand_terms.keys())[0]
            term_data = on_demand_terms[term_key]
            
            # Extract ALL price dimensions (not just first one!)
            price_dims = term_data.get("priceDimensions", {})
            if not price_dims:
                return []
            
            # Process EACH price dimension as a separate record
            records = []
            for dim_key, dim_data in price_dims.items():
                # Extract price and unit
                price_str = dim_data.get("pricePerUnit", {}).get("USD", "0")
                price = Decimal(price_str)
                unit = dim_data.get("unit", "")
                
                # Extract tiered pricing ranges
                begin_range = dim_data.get("beginRange", "")
                end_range = dim_data.get("endRange", "")
                description = dim_data.get("description", "")
                
                # Build record with all available attributes
                record = {
                    "pricing_id": str(uuid.uuid4()),
                    "sku": sku,
                    "region_code": region_code,
                    "location": attrs.get("location"),
                    "collected_at": datetime.now(),
                    "price_per_unit": float(price),
                    "unit": unit,
                    "currency": "USD",
                    "begin_range": begin_range,
                    "end_range": end_range,
                    "description": description,
                    "term_type": "OnDemand",
                    "product_family": attrs.get("productFamily"),
                    "service_code": attrs.get("servicecode"),
                    "service_name": attrs.get("servicename"),
                    "location_type": attrs.get("locationType"),
                    "volume_type": attrs.get("volumeType"),
                    "usage_type": attrs.get("usagetype"),
                    "region": region_code,  # AWS region code (fixed from friendly name)
                    "group_name": attrs.get("group"),  # Maps from 'group'
                    "group_description": attrs.get("groupDescription"),
                    "operation": attrs.get("operation"),
                    "lease_contract_length": None,  # NULL for OnDemand
                    "purchase_option": None,  # NULL for OnDemand
                    "offering_class": None,  # NULL for OnDemand
                }
                
                records.append(record)
            
            return records
            
        except (KeyError, ValueError, IndexError) as e:
            logger.warning(
                "Failed to parse product",
                sku=product.get("product", {}).get("sku"),
                error=str(e)
            )
            return []
    
    async def store_pricing_in_database(
        self,
        region_code: str,
        pricing_records: List[Dict],
        connection
    ) -> None:
        """
        Store pricing records in the database.
        
        Args:
            region_code: AWS region code
            pricing_records: List of pricing records to store
            connection: DuckDB connection
        """
        if not pricing_records:
            logger.warning("No pricing records to store", region=region_code)
            return
        
        logger.info(
            "Storing pricing data",
            region=region_code,
            records=len(pricing_records)
        )
        
        # Delete existing pricing for this region
        connection.execute(
            "DELETE FROM pricing_data WHERE region_code = ?",
            (region_code,)
        )
        
        # Insert new pricing records
        for record in pricing_records:
            connection.execute(
                """
                INSERT INTO pricing_data (
                    pricing_id, sku, region_code, location, collected_at,
                    price_per_unit, unit, currency, begin_range, end_range, description,
                    term_type, product_family, service_code, service_name, location_type,
                    volume_type, usage_type, region, group_name, group_description,
                    operation, lease_contract_length, purchase_option, offering_class
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    record["pricing_id"],
                    record["sku"],
                    record["region_code"],
                    record["location"],
                    record["collected_at"],
                    record["price_per_unit"],
                    record["unit"],
                    record["currency"],
                    record["begin_range"],
                    record["end_range"],
                    record["description"],
                    record["term_type"],
                    record["product_family"],
                    record["service_code"],
                    record["service_name"],
                    record["location_type"],
                    record["volume_type"],
                    record["usage_type"],
                    record["region"],
                    record["group_name"],
                    record["group_description"],
                    record["operation"],
                    record["lease_contract_length"],
                    record["purchase_option"],
                    record["offering_class"],
                )
            )
        
        connection.commit()
        
        logger.info(
            "Pricing data stored successfully",
            region=region_code,
            records=len(pricing_records)
        )
    
    async def is_pricing_stale(
        self,
        region_code: str,
        connection,
        refresh_days: int = 30
    ) -> bool:
        """
        Check if pricing data needs refresh.
        
        Args:
            region_code: AWS region code
            connection: DuckDB connection
            refresh_days: Days before pricing is considered stale
        
        Returns:
            True if pricing needs refresh, False otherwise
        """
        try:
            result = connection.execute(
                """
                SELECT MAX(collected_at) as last_collected
                FROM pricing_data
                WHERE region_code = ?
                """,
                (region_code,)
            ).fetchone()
            
            if not result or not result[0]:
                logger.debug("No pricing data found", region=region_code)
                return True  # No data, needs collection
            
            last_collected = result[0]
            if isinstance(last_collected, str):
                last_collected = datetime.fromisoformat(last_collected)
            
            refresh_threshold = datetime.now() - timedelta(days=refresh_days)
            is_stale = last_collected < refresh_threshold
            
            logger.debug(
                "Pricing staleness check",
                region=region_code,
                last_collected=last_collected,
                is_stale=is_stale
            )
            
            return is_stale
            
        except Exception as e:
            logger.error(
                "Error checking pricing staleness",
                region=region_code,
                error=str(e)
            )
            return True  # Assume stale on error
    
    async def collect_and_store_pricing(
        self,
        region_code: str,
        connection,
        force_refresh: bool = False
    ) -> None:
        """
        Collect and store comprehensive pricing data for a region.
        
        Args:
            region_code: AWS region code
            connection: DuckDB connection
            force_refresh: Force refresh even if not stale
        """
        # Check if refresh needed
        if not force_refresh:
            is_stale = await self.is_pricing_stale(region_code, connection)
            if not is_stale:
                logger.info(
                    "Pricing data is fresh, skipping collection",
                    region=region_code
                )
                return
        
        logger.info("Refreshing pricing data", region=region_code)
        
        # Discover available attributes (cached after first call)
        available_attributes = await self.discover_available_attributes()
        
        # Collect all pricing for region
        pricing_records = await self.collect_region_pricing(
            region_code,
            available_attributes
        )
        
        # Store in database
        await self.store_pricing_in_database(
            region_code,
            pricing_records,
            connection
        )
        
        logger.info(
            "Pricing refresh complete",
            region=region_code,
            records=len(pricing_records)
        )
    
    async def collect_all_regions(
        self,
        regions: List[str],
        connection,
        force_refresh: bool = False
    ) -> None:
        """
        Collect pricing data for multiple regions.
        
        Args:
            regions: List of AWS region codes
            connection: DuckDB connection
            force_refresh: Force refresh even if not stale
        """
        logger.info(
            "Collecting pricing for multiple regions",
            regions=len(regions)
        )
        
        # Discover attributes once
        await self.discover_available_attributes()
        
        # Collect for each region
        for region in regions:
            try:
                await self.collect_and_store_pricing(
                    region,
                    connection,
                    force_refresh
                )
            except Exception as e:
                logger.error(
                    "Failed to collect pricing for region",
                    region=region,
                    error=str(e)
                )
                # Continue with other regions
        
        logger.info("Pricing collection complete for all regions")
