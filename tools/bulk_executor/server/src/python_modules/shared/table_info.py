import math
import sys

import boto3
import botocore.exceptions

# Custom Library Imports
sys.path.append('/server/src')
from python_modules.shared.logger import log
from python_modules.shared.pricing import PricingUtility

MIN_RECOMMENDED_READ_RATE = 100
MIN_RECOMMENDED_WRITE_RATE = 100

def get_quota_value(quota_name, region_name):
    """
    Get the value of a specific DynamoDB quota from Service Quotas API.
    Returns None if the quota cannot be found or if there's an error.
    """
    try:
        # Map of quota names to their codes
        quota_codes = {
            "Table-level read throughput limit": "L-CF0CBE56",
            "Table-level write throughput limit": "L-AB614373"
        }

        if quota_name not in quota_codes:
            log.info(f"Warning: Unknown quota name: {quota_name}")
            return None

        quota_code = quota_codes[quota_name]

        # Get the quota value
        service_quotas = boto3.client('service-quotas', region_name=region_name)
        response = service_quotas.get_service_quota(
            ServiceCode='dynamodb',
            QuotaCode=quota_code
        )

        return int(response['Quota']['Value'])
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchResourceException':
            # Try to get the default value if the quota is not found
            try:
                response = service_quotas.get_aws_default_service_quota(
                    ServiceCode='dynamodb',
                    QuotaCode=quota_codes[quota_name]
                )
                return int(response['Quota']['Value'])
            except Exception as inner_e:
                log.info(f"Warning: Could not retrieve default quota: {str(inner_e)}")
                return None
        else:
            log.info(f"Warning: Error retrieving quota: {str(e)}")
            return None
    except Exception as e:
        log.info(f"Warning: Could not retrieve quota: {str(e)}")
        return None

def get_and_print_dynamodb_table_info(table_name, index_name=None):
    region_name = _region_from_table_ref(table_name) or _default_region()
    if not region_name:
        raise ValueError("Unable to determine region_name for DynamoDB call.")

    autoscaling = boto3.client('application-autoscaling', region_name=region_name)
    dynamodb = boto3.client('dynamodb', region_name=region_name)

    # Get table description
    response = dynamodb.describe_table(TableName=table_name)
    table_desc = response['Table']

    # Find the specific GSI if index_name is provided
    gsi = None
    if index_name:
        for index in table_desc.get('GlobalSecondaryIndexes', []):
            if index['IndexName'] == index_name:
                gsi = index
                break
        if not gsi:
            log.info(f"Index {index_name} not found")
            return

    # find the relevant pricing categories based on the table class
    table_class = table_desc.get('TableClassSummary', {}).get('TableClass', 'STANDARD')
    if table_class == 'STANDARD_INFREQUENT_ACCESS':
        write_pricing_category = 'ia_wcu_pricing'
        read_pricing_category = 'ia_rcu_pricing'
    else:
        write_pricing_category = 'std_wcu_pricing'
        read_pricing_category = 'std_rcu_pricing'

    # Get billing mode
    billing_mode = table_desc.get('BillingModeSummary', {}).get('BillingMode', 'PROVISIONED')

    # Handle provisioned throughput based on whether we're looking at table or index
    if billing_mode == 'PROVISIONED':
        if index_name:
            log.info(f"Table: '{table_name}'")
            log.info(f"Index: '{index_name}'")
            log.info("Billing mode: Provisioned")
            rcu = gsi['ProvisionedThroughput']['ReadCapacityUnits']
            wcu = gsi['ProvisionedThroughput']['WriteCapacityUnits']
        else:
            log.info(f"Table: '{table_name}'")
            log.info("Billing mode: Provisioned")
            rcu = table_desc['ProvisionedThroughput']['ReadCapacityUnits']
            wcu = table_desc['ProvisionedThroughput']['WriteCapacityUnits']

        log.info(f"Read Capacity Units (RCU): {rcu:,}")
        log.info(f"Write Capacity Units (WCU): {wcu:,}")

        # Get auto-scaling settings
        log.info("\nAuto Scaling Settings:")
        if index_name:
            resource_id = f'table/{table_name}/index/{index_name}'
            scalable_dimensions = [
                'dynamodb:index:ReadCapacityUnits',
                'dynamodb:index:WriteCapacityUnits'
            ]
        else:
            resource_id = f'table/{table_name}'
            scalable_dimensions = [
                'dynamodb:table:ReadCapacityUnits',
                'dynamodb:table:WriteCapacityUnits'
            ]

        for dimension in scalable_dimensions:
            scalable_target = autoscaling.describe_scalable_targets(
                ServiceNamespace='dynamodb',
                ResourceIds=[resource_id],
                ScalableDimension=dimension
            )

            if scalable_target['ScalableTargets']:
                target = scalable_target['ScalableTargets'][0]
                min_capacity = target['MinCapacity']
                max_capacity = target['MaxCapacity']
                log.info(f"- {dimension.split(':')[-1]}:")
                log.info(f"  Auto Scaling Enabled: Yes")
                log.info(f"  Min Capacity: {min_capacity:,}")
                log.info(f"  Max Capacity: {max_capacity:,}")

                # Get scaling policies
                policies = autoscaling.describe_scaling_policies(
                    ServiceNamespace='dynamodb',
                    ResourceId=resource_id,
                    ScalableDimension=dimension
                )
                for policy in policies['ScalingPolicies']:
                    target_value = policy['TargetTrackingScalingPolicyConfiguration']['TargetValue']
                    log.info(f"  Target Value: {target_value}")
            else:
                log.info(f"- {dimension.split(':')[-1]}:")
                log.info(f"  Auto Scaling Enabled: No")

    else:
        if index_name:
            log.info(f"Table: '{table_name}'")
            log.info(f"Index: '{index_name}'")
            log.info("Billing mode: On-demand")
            capacity = gsi.get('OnDemandThroughput', {})
        else:
            log.info(f"Table: '{table_name}'")
            log.info("Billing mode: On-demand")
            capacity = table_desc.get('OnDemandThroughput', {})

        max_rru = capacity.get('MaxReadRequestUnits', None)
        max_wru = capacity.get('MaxWriteRequestUnits', None)

        if max_rru is not None and max_wru is not None:
            log.info(f"Max Read Request Units: {int(max_rru):,}")
            log.info(f"Max Write Request Units: {int(max_wru):,}")

    # Get item count and size
    if index_name:
        item_count = gsi.get('ItemCount', 0)
        size_bytes = gsi.get('IndexSizeBytes', 0)
        log.info(f"\nIndex Item Count (approx): {item_count:,}")
        log.info(f"Index Size (approx): {size_bytes:,} bytes")
    else:
        item_count = table_desc.get('ItemCount', 0)
        size_bytes = table_desc.get('TableSizeBytes', 0)
        log.info(f"\nTable Item Count (approx): {item_count:,}")
        log.info(f"Table Size (approx): {size_bytes:,} bytes")
    log.info("")

    return {
        'table_name': table_name,
        'region_name': region_name,
        'billing_mode': billing_mode,
        'write_pricing_category': write_pricing_category,
        'read_pricing_category': read_pricing_category,
        'item_count': item_count,
        'size_bytes': size_bytes
    }

def get_and_print_table_scan_cost(table_info, region_name=None, fraction=1.0, numberOfScans=1):
    region_name = (
        region_name
        or table_info.get("region_name")
        or _default_region()
    )

    read_units = math.ceil(int(table_info['size_bytes']) / 8096)

    log.info(f"DynamoDB read costs depend on the table size.")
    log.info(f"DynamoDB read units required for a full scan (approx): {read_units:,}")
    if fraction < 1.0:
        read_units = math.ceil(read_units * fraction)
        log.info(f"DynamoDB read units required for this partial scan (approx): {read_units:,}")

    if numberOfScans > 1:
        log.info(f"Scans required: {numberOfScans}")
        read_units *= numberOfScans

    pricing_utility = PricingUtility()
    ondemand_pricing = pricing_utility.get_on_demand_capacity_pricing(region_name)
    rru_cost = float(ondemand_pricing[table_info['read_pricing_category']])
    od_cost = read_units * rru_cost
    prov_cost = od_cost / 1.5  # very rough, look into updating this
    if table_info['billing_mode'] == "PROVISIONED":
        log.info(f"Approx DynamoDB cost for provisioned scan consuming {read_units:,} RCUs (using {region_name} prices): ${prov_cost:,.2f}\n")
        return prov_cost
    elif table_info['billing_mode'] == "PAY_PER_REQUEST":
        log.info(f"Approx DynamoDB cost for on-demand scan consuming {read_units:,} RRUs (using {region_name} prices): ${od_cost:,.2f}\n")
        return od_cost
    return 0

def get_and_print_table_copy_write_cost(source_info, target_info):
    region_name = (
        target_info.get("region_name") # it's the target we price writes in
        or _default_region()
    )

    item_count = source_info['item_count']
    if item_count == 0:
        avg_write_units = 0
        write_units = 0
    else:
        item_size = source_info['size_bytes'] / item_count
        avg_write_units = math.ceil(item_size / 1024)
        write_units = item_count * avg_write_units

    log.info(f"DynamoDB write costs depend on the number and size of items written.")
    log.info(f"DynamoDB write units required to write {item_count:,} items of {avg_write_units} average write units (approx): {write_units:,}")

    pricing_utility = PricingUtility()
    ondemand_pricing = pricing_utility.get_on_demand_capacity_pricing(region_name)
    wru_cost = float(ondemand_pricing[target_info["write_pricing_category"]])
    od_cost = write_units * wru_cost
    prov_cost = od_cost / 1.5  # very rough, look into updating this
    if target_info['billing_mode'] == "PROVISIONED":
        log.info(f"Approx DynamoDB cost for provisioned writes consuming {write_units:,} WCUs (using {region_name} prices): ${prov_cost:,.2f}\n")
        return prov_cost
    elif target_info['billing_mode'] == "PAY_PER_REQUEST":
        log.info(f"Approx DynamoDB cost for on-demand writes consuming {write_units:,} WRUs (using {region_name} prices): ${od_cost:,.2f}\n")
        return od_cost
    return 0

def get_dynamodb_throughput_configs(args, table_name, modes=None, format="connector"):
    region_name = _region_from_table_ref(table_name) or _default_region()
    if not region_name:
        raise ValueError("Unable to determine region_name for DynamoDB call.")
    dynamodb = boto3.client('dynamodb', region_name=region_name)

    if modes is None:
        modes = ("read", "write")

    DEFAULT_ON_DEMAND_CAPACITY = 40000

    # Get table description to determine if it's on-demand or provisioned
    read_rate = args.get('XMaxReadRate', None)   # User set read rate
    write_rate = args.get('XMaxWriteRate', None) # User set write rate

    try:
        response = dynamodb.describe_table(TableName=table_name)
        table_desc = response['Table']
        billing_mode = table_desc.get('BillingModeSummary', {}).get('BillingMode', 'PROVISIONED')
        is_on_demand_table = billing_mode == 'PAY_PER_REQUEST'
    except Exception as e:
        log.info(f"Warning: Could not retrieve table information: {str(e)}")
        is_on_demand_table = False
        table_desc = {}

    # Handle read throughput
    if "read" in modes:
        if read_rate:
            log.info(f"Max read rate set to specified limit: {read_rate}")
        elif is_on_demand_table:
            # Check for table-specific limit
            on_demand_throughput = table_desc.get('OnDemandThroughput', {})
            table_read_limit = on_demand_throughput.get('MaxReadRequestUnits')

            if table_read_limit is None:
                # Try to get account-level quota
                quota_read_limit = get_quota_value("Table-level read throughput limit", region_name)
                if quota_read_limit is not None:
                    read_rate = quota_read_limit
                    log.info(f"Max read rate set to account quota limit: {read_rate}")
                else:
                    # Default for on-demand tables
                    read_rate = DEFAULT_ON_DEMAND_CAPACITY
                    log.info(f"Max read rate set to default on-demand limit: {read_rate}")
            else:
                read_rate = table_read_limit
                log.info(f"Max read rate set to table-specific on-demand limit: {read_rate}")
        else:
            provisioned_read = table_desc.get('ProvisionedThroughput', {}).get('ReadCapacityUnits')
            if provisioned_read:
                read_rate = provisioned_read
                log.info(f"Max read rate set to {read_rate} RCUs (based on provisioned capacity)")
            else:
                log.info(f"Max read rate set internally by Glue (no provisioned level found)") # shouldn't happen

        if int(read_rate) < MIN_RECOMMENDED_READ_RATE:
            log.warn(f"Read rate {read_rate} less than recommended value of {MIN_RECOMMENDED_READ_RATE}.")

    # Handle write throughput
    if "write" in modes:
        if write_rate:
            log.info(f"Max write rate set to specified limit: {write_rate}")
        elif is_on_demand_table:
            # Check for table-specific limit
            on_demand_throughput = table_desc.get('OnDemandThroughput', {})
            table_write_limit = on_demand_throughput.get('MaxWriteRequestUnits')

            if table_write_limit is None:
                # Try to get account-level quota
                quota_write_limit = get_quota_value("Table-level write throughput limit", region_name)
                if quota_write_limit is not None:
                    write_rate = quota_write_limit
                    log.info(f"Max write rate set to account quota limit: {write_rate}")
                else:
                    # Default for on-demand tables
                    write_rate = DEFAULT_ON_DEMAND_CAPACITY
                    log.info(f"Max write rate set to default on-demand limit: {write_rate}")
            else:
                write_rate = table_write_limit
                log.info(f"Max write rate set to table-specific on-demand limit: {write_rate}")
        else:
            # For provisioned tables, use the percentage approach
            provisioned_write = table_desc.get('ProvisionedThroughput', {}).get('WriteCapacityUnits')
            if provisioned_write:
                write_rate = provisioned_write
                log.info(f"Max write rate set to {write_rate} WCUs (based on provisioned capacity)")
            else:
                log.info(f"Max write rate set internally by Glue (no provisioned level found)") # shouldn't happen

        if int(write_rate) < MIN_RECOMMENDED_WRITE_RATE:
            log.warn(f"Write rate {write_rate} less than recommended value of {MIN_RECOMMENDED_WRITE_RATE}.")

    if format == "connector":
        # Now let's convert the read_rate and write_rate into connection_options
        connection_options = {}

        if "read" in modes:
            # For reads, we set dynamodb.throughput.read to the value we want
            # We set dynamodb.throughput.read.percent to 1.0 to say use 100% of it
            if read_rate:
                connection_options["dynamodb.throughput.read"] = str(read_rate)
            connection_options["dynamodb.throughput.read.percent"] = "1.0"

        if "write" in modes:
            # For writes, there's no dynamodb.throughput.write
            # Our only control is dynamodb.throughput.write.percent
            # If we know the user's desired write rate and what the Connector thinks the table's level is,
            # then we set the percent as write_rate / table_level.
            # For example, if the user wants 10,000 and the table is on-demand so the Connector determines it's 40,000
            # then a percent of 0.25 gets the job done.

            if write_rate is None: # if we couldn't find a rate, we'll go with 1.0
                actual_percent = 1.0
            else:
                provisioned_write = table_desc.get('ProvisionedThroughput', {}).get('WriteCapacityUnits')
                if provisioned_write and int(provisioned_write) > 0:
                    table_level = int(provisioned_write)
                else:
                    table_level = DEFAULT_ON_DEMAND_CAPACITY # Connector sees all on-demand as 40,000
                desired_percent = int(write_rate) / table_level
                actual_percent = min(desired_percent, 1.5)
            connection_options["dynamodb.throughput.write.percent"] = str(actual_percent)

            # For now let's be verbose and explain the way we're doing write limiting
            pct = actual_percent * 100
            formatted = f"{pct:.0f}%" if pct.is_integer() else f"{pct:.1f}%"
            log.info(f"Write rate achieved by requesting {formatted} of table capacity {table_level}")

            # If we couldn't achieve the user's goal, be verbose about that and why
            if desired_percent > actual_percent:
                if provisioned_write and int(provisioned_write) > 0:
                    log.info("Note: Rate is limited as it cannot be more than 150% of current provisioned capacity")
                else:
                    log.info("Note: Rate is limited as it cannot be above 60000 with on-demand tables")

        log.info(f"\n") # Separate throughput configs on CLI for clarity
        return connection_options

    elif format == "monitor":
        result = {}
        if read_rate is not None:
            result["aggregate_max_read_rate"] = int(read_rate)
        if write_rate is not None:
            result["aggregate_max_write_rate"] = int(write_rate)
        return result

    else:
        raise ValueError(f"Unrecognized value for 'format': {format}")

# If they pass an ARN we want to read the components out

def _default_region():
    return (
        boto3.Session().region_name
        or os.environ.get("AWS_REGION")
        or os.environ.get("AWS_DEFAULT_REGION")
    )

def _parse_arn(arn: str) -> dict:
    """
    Minimal ARN parser.
    Returns dict with: partition, service, region, account, resource.
    Raises ValueError if malformed.
    """
    parts = arn.split(":", 5)

    if len(parts) != 6 or parts[0] != "arn":
        raise ValueError(f"Invalid ARN: {arn}")

    _, partition, service, region, account, resource = parts

    return {
        "partition": partition,
        "service": service,
        "region": region,
        "account": account,
        "resource": resource,
    }

def _region_from_table_ref(table_ref: str) -> str | None:
    if not table_ref or not table_ref.startswith("arn:"):
        return None
    arn = _parse_arn(table_ref)
    if arn.get("service") != "dynamodb":
        return None
    # Expect resource like "table/MyTable"
    if not arn.get("resource", "").startswith("table/"):
        return None
    return arn.get("region")

