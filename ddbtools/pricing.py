import boto3
import json

from ddbtools import constants
from decimal import Decimal

class PricingUtility(object):
    def __init__(self, region_name):
        closest_api_region = 'us-east-1'

        # the pricing API is only available in us-east-1 and ap-south-1 
        # pick the closest endpoint to the supplied region
        if region_name not in constants.AMERICAN_REGIONS:
            closest_api_region = 'ap-south-1'

        self.pricing_client = boto3.client('pricing', region_name=closest_api_region)

    def get_replicated_write_pricing(self, region_code: str) -> dict:
        """Get DynamoDB replicated write (for global tables) pricing for a given region."""
        replicated_writes_pricing = {} 

        response = self.pricing_client.get_products(
            ServiceCode='AmazonDynamoDB',
            Filters=[{'Type': 'TERM_MATCH',
                      'Field': 'productFamily',
                      'Value': 'DDB-Operation-ReplicatedWrite'},
                     {'Type': 'TERM_MATCH',
                      'Field': 'regionCode',
                      'Value': region_code}
            ],
            FormatVersion='aws_v1',
            MaxResults=100
        )
        price_list = response['PriceList']

        for entry in price_list:
            product = json.loads(entry)
            product_group = product['product']['attributes']['group']
            offer = product['terms']['OnDemand'].popitem()
            offer_terms = offer[1]
            price_dimensions = offer_terms['priceDimensions']

            for price_dimension_code in price_dimensions:
                price_terms = price_dimensions[price_dimension_code]
                price_per_unit = price_terms['pricePerUnit']['USD']
                price = Decimal(price_per_unit)

                # Regions with free tier pricing will have an initial entry set to zero; skip this
                if price != 0:
                    if product_group == 'DDB-ReplicatedWriteUnits':
                        replicated_writes_pricing[constants.REPLICATED_STD_WCU_PRICING] = price
                    elif product_group == 'DDB-ReplicatedWriteUnitsIA':
                        replicated_writes_pricing[constants.REPLICATED_IA_WCU_PRICING] = price

        return replicated_writes_pricing
     

    def get_storage_pricing(self, region_code: str) -> dict:
        """Get pricing for all DynamoDB storage classes in this region."""
        storage_pricing = {}
        storage_pricing[constants.STD_VOLUME_TYPE] = self.get_storage_class_pricing(region_code, 
                                                                                    constants.STD_VOLUME_TYPE)
        storage_pricing[constants.IA_VOLUME_TYPE] = self.get_storage_class_pricing(region_code, 
                                                                                   constants.IA_VOLUME_TYPE)
        return storage_pricing

    
    def get_storage_class_pricing(self, region_code: str, volume_type: str) -> Decimal:
        """Get table class pricing by looking for a specific volume type in the specified region."""
        response = self.pricing_client.get_products(
            ServiceCode=constants.DDB_RESOURCE_CODE,
            Filters=[{'Type': 'TERM_MATCH',
                      'Field': 'volumeType',
                      'Value': volume_type},
                     {'Type': 'TERM_MATCH',
                      'Field': 'regionCode',
                      'Value': region_code}
            ],
            FormatVersion='aws_v1',
            MaxResults=1
        )

        price_list = response['PriceList']
        product = json.loads(price_list[0])
        offer = product['terms']['OnDemand'].popitem()
        offer_terms = offer[1]
        price_dimensions = offer_terms['priceDimensions']

        for price_dimension_code in price_dimensions:
            price_terms = price_dimensions[price_dimension_code]
            price_per_unit = price_terms['pricePerUnit']['USD']
            storage_pricing = Decimal(price_per_unit)

            # Regions with free tier pricing will have an initial entry set to zero; skip this
            if storage_pricing != 0:
                return storage_pricing

        return None


    def get_provisioned_capacity_pricing(self, region_code: str) -> dict:
        """Get DynamoDB provisioned capacity pricing for a given region."""
        throughput_pricing = {} 

        response = self.pricing_client.get_products(
            ServiceCode='AmazonDynamoDB',
            Filters=[{'Type': 'TERM_MATCH',
                      'Field': 'productFamily',
                      'Value': 'Provisioned IOPS'},
                     {'Type': 'TERM_MATCH',
                      'Field': 'regionCode',
                      'Value': region_code}
            ],
            FormatVersion='aws_v1',
            MaxResults=100
        )
        price_list = response['PriceList']

        for entry in price_list:
            product = json.loads(entry)
            product_group = product['product']['attributes']['group']
            offer = product['terms']['OnDemand'].popitem()
            offer_terms = offer[1]
            price_dimensions = offer_terms['priceDimensions']

            for price_dimension_code in price_dimensions:
                price_terms = price_dimensions[price_dimension_code]
                price_per_unit = price_terms['pricePerUnit']['USD']
                price = Decimal(price_per_unit)

                # Regions with free tier pricing will have an initial entry set to zero; skip this
                if price != 0:
                    if product_group == 'DDB-ReadUnits':
                        throughput_pricing[constants.STD_RCU_PRICING] = price
                    elif product_group == 'DDB-WriteUnits':
                        throughput_pricing[constants.STD_WCU_PRICING] = price
                    elif product_group == 'DDB-ReadUnitsIA':
                        throughput_pricing[constants.IA_RCU_PRICING] = price
                    elif product_group == 'DDB-WriteUnitsIA':
                        throughput_pricing[constants.IA_WCU_PRICING] = price

        return throughput_pricing