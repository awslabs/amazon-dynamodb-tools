"""bill calculation constants"""
GB_IN_BYTES = 1024*1024*1024
HOURS_IN_MONTH = 730

# table data constants
ESTIMATED_MONTHLY_COSTS = 'table_mo_costs'
GSI_MONTHLY_COSTS = 'gsi_mo_costs'
PRICING_DATA = 'pricing_data'
REPLICAS = 'replicas'
TABLE_CLASS = 'table_class'
TABLE_NAME = 'table_name'

# table pricing data
BILLING_MODE = 'billing_mode'
GSIS = 'global_secondary_indexes'
INDEX_NAME = 'index_name'
INDEX_ARN = 'index_arn'
IA_RCU_PRICING = 'ia_rcu_pricing'
IA_TABLE_CLASS = 'STANDARD_INFREQUENT_ACCESS'
IA_WCU_PRICING = 'ia_wcu_pricing'
PROVISIONED_RCUS = 'provisioned_rcus'
PROVISIONED_WCUS = 'provisioned_wcus'
REPLICATED_IA_WCU_PRICING = 'replicated_ia_wcu_pricing'
REPLICATED_STD_WCU_PRICING = 'replicated_std_wcu_pricing'
SIZE_IN_GB = 'size_in_gb'
STD_RCU_PRICING = 'std_rcu_pricing'
STD_TABLE_CLASS = 'STANDARD'
STD_WCU_PRICING = 'std_wcu_pricing'
TABLE_ARN = 'table_arn'

# Pricing API constants
DDB_RESOURCE_CODE = 'AmazonDynamoDB'
PROVISIONED_BILLING = 'PROVISIONED'
ON_DEMAND_BILLING = 'PAY_PER_REQUEST'
STD_VOLUME_TYPE = 'Amazon DynamoDB - Indexed DataStore'
IA_VOLUME_TYPE = 'Amazon DynamoDB - Indexed DataStore - IA'

# calculated table costs
IA_MO_COST_DIFFERENCE = 'ia_mo_cost_difference'
IA_MO_STORAGE_COST = 'ia_mo_storage_cost'
IA_MO_RCU_COST = 'ia_mo_rcu_cost'
IA_MO_WCU_COST = 'ia_mo_wcu_cost'
IA_MO_TOTAL_COST = 'ia_mo_total_cost'
STD_MO_STORAGE_COST = 'std_storage_cost'
STD_MO_RCU_COST = 'std_mo_rcu_cost'
STD_MO_WCU_COST = 'std_mo_wcu_cost'
STD_MO_TOTAL_COST = 'std_mo_total_cost'
STD_MO_STORAGE_FACTOR = 'std_mo_storage_factor'

# recommendation constants
ESTIMATED_MO_SAVINGS = 'estimated_monthly_savings'
RECOMMENDATION_TYPE = 'recommendation_type'
RECOMMENDED_TABLE_CLASS = 'recommended_table_class'
TABLE_CLASS_CHANGE_RECOMMENDATION = 'CHANGE_TABLE_CLASS'
ESTIMATE_DETAIL = 'estimate_detail'
TOTAL_IA_MO_COSTS = 'total_ia_mo_costs'
TOTAL_STD_MO_COSTS = 'total_std_mo_costs'

# region constants
AMERICAN_REGIONS = ['us-east-1', 'us-east-2', 
                    'us-west-1', 'us-west-2', 
                    'us-gov-west-1', 'us-gov-west-2',
                    'ca-central-1', 'sa-east-1']
                    