import boto3

from ddbtools import constants
from decimal import Decimal

from ddbtools.pricing import PricingUtility


class TableUtility(object):
    def __init__(self, region_name, profile_name=None):
        self.session = boto3.Session(profile_name=profile_name)
        self.dynamodb_client = self.session.client('dynamodb', region_name=region_name)
        self.pricing_utility = PricingUtility(region_name=region_name, profile_name=profile_name)


    def add_tags_to_table(self, table_arn:str, tags:list) -> None:
        """Given a list of {key: value} pairs, apply each as a tag to the table ARN provided."""
        self.dynamodb_client.tag_resource(ResourceArn=table_arn, Tags=tags)
        

    def estimate_current_table_costs(self, provisioned_capacity_pricing: dict,
                                           replicated_write_pricing: dict,
                                           storage_pricing: dict,
                                           table_pricing_data: dict) -> dict:
        """Calculate monthly storage and throughput costs for a table for all storage classes"""
        # storage costs
        ia_monthly_storage_cost = table_pricing_data[constants.SIZE_IN_GB] * storage_pricing[constants.IA_VOLUME_TYPE]
        std_monthly_storage_cost = table_pricing_data[constants.SIZE_IN_GB] * storage_pricing[constants.STD_VOLUME_TYPE]
        
        # read capacity costs
        ia_monthly_rcu_cost = (table_pricing_data[constants.PROVISIONED_RCUS] 
                               * provisioned_capacity_pricing[constants.IA_RCU_PRICING] 
                               * constants.HOURS_IN_MONTH)
        std_monthly_rcu_cost = (table_pricing_data[constants.PROVISIONED_RCUS] 
                                * provisioned_capacity_pricing[constants.STD_RCU_PRICING]
                                * constants.HOURS_IN_MONTH)
        
        # write capacity costs
        ia_monthly_wcu_cost = None
        std_monthly_wcu_cost = None

        # If the table has replicas, estimate rWCUs instead of WCUs
        if constants.REPLICAS in table_pricing_data:
            ia_monthly_wcu_cost = (table_pricing_data[constants.PROVISIONED_RCUS] 
                                   * replicated_write_pricing[constants.REPLICATED_IA_WCU_PRICING]
                                   * constants.HOURS_IN_MONTH)
            std_monthly_wcu_cost = (table_pricing_data[constants.PROVISIONED_RCUS] 
                                    * replicated_write_pricing[constants.REPLICATED_STD_WCU_PRICING]
                                    * constants.HOURS_IN_MONTH)
        else:
            ia_monthly_wcu_cost = (table_pricing_data[constants.PROVISIONED_WCUS] 
                                  * provisioned_capacity_pricing[constants.IA_WCU_PRICING] 
                                  * constants.HOURS_IN_MONTH)
            std_monthly_wcu_cost = (table_pricing_data[constants.PROVISIONED_WCUS] 
                                    * provisioned_capacity_pricing[constants.STD_WCU_PRICING]
                                    * constants.HOURS_IN_MONTH)

        # total costs
        ia_monthly_total_cost = ia_monthly_storage_cost + ia_monthly_rcu_cost + ia_monthly_wcu_cost
        std_monthly_total_cost = std_monthly_storage_cost + std_monthly_rcu_cost + std_monthly_wcu_cost

        estimated_table_costs = {constants.STD_MO_STORAGE_COST: std_monthly_storage_cost,
                                 constants.STD_MO_RCU_COST: std_monthly_rcu_cost,
                                 constants.STD_MO_WCU_COST: std_monthly_wcu_cost,
                                 constants.STD_MO_TOTAL_COST: std_monthly_total_cost,
                                 constants.IA_MO_STORAGE_COST: ia_monthly_storage_cost,
                                 constants.IA_MO_RCU_COST: ia_monthly_rcu_cost,
                                 constants.IA_MO_WCU_COST: ia_monthly_wcu_cost,
                                 constants.IA_MO_TOTAL_COST: ia_monthly_total_cost}

        estimated_table_costs[constants.TOTAL_STD_MO_COSTS] = std_monthly_total_cost
        estimated_table_costs[constants.TOTAL_IA_MO_COSTS] = ia_monthly_total_cost

        # GSI costs
        # TODO: refactor GSI and table cost estimating out into a single method
        estimated_gsi_costs = None

        if constants.GSIS in table_pricing_data:
            estimated_gsi_costs = []
            total_gsi_std_costs = Decimal(0)
            total_gsi_ia_costs = Decimal(0)

            for gsi_data in table_pricing_data[constants.GSIS]:
                gsi_ia_mo_storage_cost = gsi_data[constants.SIZE_IN_GB] * storage_pricing[constants.IA_VOLUME_TYPE]
                gsi_std_mo_storage_cost = gsi_data[constants.SIZE_IN_GB] * storage_pricing[constants.STD_VOLUME_TYPE]
                
                gsi_ia_mo_rcu_cost = (gsi_data[constants.PROVISIONED_RCUS] 
                                      * provisioned_capacity_pricing[constants.IA_RCU_PRICING] 
                                      * constants.HOURS_IN_MONTH)
                gsi_std_mo_rcu_cost = (gsi_data[constants.PROVISIONED_RCUS] 
                                       * provisioned_capacity_pricing[constants.STD_RCU_PRICING]
                                       * constants.HOURS_IN_MONTH)

                gsi_ia_mo_wcu_cost = (gsi_data[constants.PROVISIONED_WCUS] 
                                      * provisioned_capacity_pricing[constants.IA_WCU_PRICING] 
                                      * constants.HOURS_IN_MONTH)
                gsi_std_mo_wcu_cost = (gsi_data[constants.PROVISIONED_WCUS] 
                                        * provisioned_capacity_pricing[constants.STD_WCU_PRICING]
                                        * constants.HOURS_IN_MONTH)

                gsi_ia_mo_total_cost = gsi_ia_mo_storage_cost + gsi_ia_mo_rcu_cost + gsi_ia_mo_wcu_cost
                total_gsi_ia_costs += gsi_ia_mo_total_cost

                gsi_std_mo_total_cost = gsi_std_mo_storage_cost + gsi_std_mo_rcu_cost + gsi_std_mo_wcu_cost
                total_gsi_std_costs += gsi_std_mo_total_cost

                gsi_costs = {constants.INDEX_NAME: gsi_data[constants.INDEX_NAME],
                             constants.INDEX_ARN: gsi_data[constants.INDEX_ARN],
                             constants.STD_MO_STORAGE_COST: gsi_std_mo_storage_cost,
                             constants.STD_MO_RCU_COST: gsi_std_mo_rcu_cost,
                             constants.STD_MO_WCU_COST: gsi_std_mo_wcu_cost,
                             constants.STD_MO_TOTAL_COST: gsi_std_mo_total_cost,
                             constants.IA_MO_STORAGE_COST: gsi_ia_mo_storage_cost,
                             constants.IA_MO_RCU_COST: gsi_ia_mo_rcu_cost,
                             constants.IA_MO_WCU_COST: gsi_ia_mo_wcu_cost,
                             constants.IA_MO_TOTAL_COST: gsi_ia_mo_total_cost}

                estimated_gsi_costs.append(gsi_costs)

            if estimated_gsi_costs is not None:
                estimated_table_costs[constants.GSI_MONTHLY_COSTS] = estimated_gsi_costs
                estimated_table_costs[constants.TOTAL_STD_MO_COSTS] += total_gsi_std_costs
                estimated_table_costs[constants.TOTAL_IA_MO_COSTS] += total_gsi_ia_costs

        return estimated_table_costs


    def estimate_table_costs_for_region(self, table_names: list, region_code: str) -> dict:
        """For a list of tables in a region, estimate the monthly costs for each"""
        table_results = []
        storage_pricing = self.pricing_utility.get_storage_pricing(region_code)
        provisioned_capacity_pricing = self.pricing_utility.get_provisioned_capacity_pricing(region_code)
        replicated_write_pricing = self.pricing_utility.get_replicated_write_pricing(region_code)
        
        for table_name in table_names:
            table_data = {}
            table_data[constants.TABLE_NAME] = table_name
            table_pricing_data = self.get_table_pricing_data(table_name)
            table_data[constants.PRICING_DATA] = table_pricing_data

            # We cannot yet calculate estimated costs for On-Demand billing mode
            if table_pricing_data[constants.BILLING_MODE] == constants.ON_DEMAND_BILLING:
                table_results.append(table_data)
                continue

            monthly_costs = self.estimate_current_table_costs(provisioned_capacity_pricing,
                                                              replicated_write_pricing,
                                                              storage_pricing,
                                                              table_pricing_data)
            table_data[constants.ESTIMATED_MONTHLY_COSTS] = monthly_costs
            table_results.append(table_data)

        return table_results


    def get_table_arn(self, table_name:str) -> str:
        """Given a table's name, return its ARN"""
        try:
            response = self.dynamodb_client.describe_table(TableName=table_name)
        except Exception as e:
            raise Exception(f"Failed to describe table {table_name}: {e}.") from None

        table_data = response['Table']
        table_arn = table_data['TableArn']

        return table_arn


    def get_table_names(self, start_table_name: str=None, table_names: list=None) -> list:
        """Get a complete list of DynamoDB tables in this region"""
        if table_names is None:
            table_names = []

        response = None

        if not start_table_name:
            response = self.dynamodb_client.list_tables()

        else:
            # if there are more than 100 table names returned, recurse to get a new page of table names
            response = self.dynamodb_client.list_tables(ExclusiveStartTableName=start_table_name)
        
        paginated_table_names = response['TableNames']
        table_names.extend(paginated_table_names)

        if 'LastEvaluatedTableName' in response:
            self.get_table_names(start_table_name=response['LastEvaluatedTableName'], table_names=table_names)

        return table_names


    def get_table_pricing_data(self, table_name: str) -> dict:
        """Return table data useful for determining pricing"""
        try:
            response = self.dynamodb_client.describe_table(TableName=table_name)
        except Exception as e:
            raise Exception(f"Failed to describe table {table_name}: {e}.") from None

        table_data = response['Table']
        table_arn = table_data['TableArn']

        if 'BillingModeSummary' in table_data:
            billing_mode = table_data['BillingModeSummary']['BillingMode']
        else:
            billing_mode = constants.PROVISIONED_BILLING

        table_bytes = table_data['TableSizeBytes']
        table_gb = Decimal(table_bytes / constants.GB_IN_BYTES)

        throughput_data = table_data['ProvisionedThroughput']
        provisioned_rcus = throughput_data['ReadCapacityUnits']
        provisioned_wcus = throughput_data['WriteCapacityUnits']

        table_pricing_data = {constants.BILLING_MODE: billing_mode,
                              constants.SIZE_IN_GB: table_gb,
                              constants.PROVISIONED_RCUS: provisioned_rcus,
                              constants.PROVISIONED_WCUS: provisioned_wcus,
                              constants.TABLE_ARN: table_arn}

        if 'GlobalSecondaryIndexes' in table_data:
            gsi_list = table_data['GlobalSecondaryIndexes']
            gsi_pricing_data = []

            for gsi_data in gsi_list:
                if gsi_data['IndexStatus'] == 'ACTIVE':
                    gsi = {}
                    gsi[constants.INDEX_ARN] = gsi_data['IndexArn']
                    gsi[constants.INDEX_NAME] = gsi_data['IndexName']

                    gsi_throughput = gsi_data['ProvisionedThroughput']
                    gsi[constants.PROVISIONED_RCUS] = gsi_throughput['ReadCapacityUnits']
                    gsi[constants.PROVISIONED_WCUS] = gsi_throughput['WriteCapacityUnits']
                    
                    gsi_bytes = gsi_data['IndexSizeBytes']
                    gsi_gb = Decimal(gsi_bytes / constants.GB_IN_BYTES)
                    gsi[constants.SIZE_IN_GB] = gsi_gb

                    gsi_pricing_data.append(gsi)

            table_pricing_data[constants.GSIS] = gsi_pricing_data

        if 'Replicas' in table_data:
            for replica in table_data['Replicas']:
                if replica['ReplicaStatus'] == 'ACTIVE':
                    if constants.REPLICAS in table_pricing_data:
                        table_pricing_data[constants.REPLICAS].append(replica['RegionName'])
                    else:
                        table_pricing_data[constants.REPLICAS]= [replica['RegionName']]

        if 'TableClassSummary' in table_data:
            table_pricing_data[constants.TABLE_CLASS] = table_data['TableClassSummary']['TableClass']
        else:
            table_pricing_data[constants.TABLE_CLASS] = constants.STD_TABLE_CLASS

        return table_pricing_data


    def get_table_tags(self, table_arn:str, next_tag_token:str=None, table_tags:dict=None) -> dict:
        """Given a table's ARN, return all tags key:value pairs assigned to that table"""
        if table_tags is None:
            table_tags = {}

        response = None

        if not next_tag_token:
            response = self.dynamodb_client.list_tags_of_resource(ResourceArn=table_arn)

        else:
            response = self.dynamodb_client.get_table_tags(ResourceArn=table_arn, 
                                                           NextToken=next_tag_token)
        
        tag_list = response['Tags']

        for tag_dict in tag_list:
            tag_key = tag_dict['Key']
            table_tags[tag_key] = tag_dict['Value']

        # recurse if there are more tags to retrieve
        if 'NextToken' in response:
            self.get_table_tags(table_arn=table_arn,
                                next_tag_token=response['NextToken'], 
                                table_tags=table_tags)

        return table_tags
