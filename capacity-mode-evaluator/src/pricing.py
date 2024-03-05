import json
from decimal import Decimal

import boto3


class PricingUtility(object):
    def __init__(self, region_name, profile_name='default'):

        closest_api_region = 'us-east-1'

        AMERICAN_REGIONS = ['us-east-1', 'us-east-2',
                            'us-west-1', 'us-west-2',
                            'us-gov-west-1', 'us-gov-west-2',
                            'ca-central-1', 'sa-east-1']
        # the pricing API is only available in us-east-1 and ap-south-1
        # pick the closest endpoint to the supplied region
        if region_name not in AMERICAN_REGIONS:
            closest_api_region = 'ap-south-1'

        self.session = boto3.session.Session(profile_name=profile_name)
        self.pricing_client = self.session.client(
            'pricing', region_name=closest_api_region)

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
                        throughput_pricing['std_rcu_pricing'] = price
                    elif product_group == 'DDB-WriteUnits':
                        throughput_pricing['std_wcu_pricing'] = price
                    elif product_group == 'DDB-ReadUnitsIA':
                        throughput_pricing['ia_rcu_pricing'] = price
                    elif product_group == 'DDB-WriteUnitsIA':
                        throughput_pricing['ia_wcu_pricing'] = price

        return throughput_pricing

    def get_on_demand_capacity_pricing(self, region_code: str) -> dict:
        """Get DynamoDB On-demand capacity pricing for a given region."""
        throughput_pricing = {}

        response = self.pricing_client.get_products(
            ServiceCode='AmazonDynamoDB',
            Filters=[{'Type': 'TERM_MATCH',
                      'Field': 'productFamily',
                      'Value': 'Amazon DynamoDB PayPerRequest Throughput'},
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
                        throughput_pricing['std_rcu_pricing'] = price
                    elif product_group == 'DDB-WriteUnits':
                        throughput_pricing['std_wcu_pricing'] = price
                    elif product_group == 'DDB-ReadUnitsIA':
                        throughput_pricing['ia_rcu_pricing'] = price
                    elif product_group == 'DDB-WriteUnitsIA':
                        throughput_pricing['ia_wcu_pricing'] = price

        return throughput_pricing
