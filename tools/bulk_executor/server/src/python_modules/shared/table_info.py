import math
import os
import time

import boto3
import botocore.exceptions

# Custom Library Imports
from .logger import log
from .pricing import PricingUtility

MIN_RECOMMENDED_READ_RATE = 100
MIN_RECOMMENDED_WRITE_RATE = 100

# Glue's default job timeout (minutes). Mirrors client GlueJobDefaults.Timeout;
# used only as a fallback for the duration estimate when --XTimeout is unset.
DEFAULT_JOB_TIMEOUT_MINUTES = 60

# Monotonic reference captured when this module is first imported, which on a
# Glue worker is at job startup (root.py imports the verb, which imports this).
# The #89 check-1 timeout estimate races the next phase against the time
# *remaining* before the job timeout, not the raw timeout: multi-phase verbs
# (e.g. delete = scan then write) resolve a later phase's rate only after an
# earlier phase has already consumed part of the budget, so that phase must
# look at what's left. Using elapsed here needs no extra IAM (no glue:GetJobRun)
# and no per-verb plumbing — every verb resolves its rate when the phase begins,
# so a late phase automatically sees a shrunken budget.
_JOB_START_MONOTONIC = time.monotonic()


def _job_elapsed_minutes():
    """Minutes elapsed since the job started (module import), never negative."""
    return max(0.0, (time.monotonic() - _JOB_START_MONOTONIC) / 60.0)

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

def get_and_print_dynamodb_table_info(table_name, index_name=None, quiet=False):
    region_name = _region_from_table_ref(table_name) or _default_region()
    if not region_name:
        raise ValueError("Unable to determine region_name for DynamoDB call.")

    autoscaling = boto3.client('application-autoscaling', region_name=region_name)
    dynamodb = boto3.client('dynamodb', region_name=region_name)

    # Get table description
    try:
        response = dynamodb.describe_table(TableName=table_name)
    except dynamodb.exceptions.ResourceNotFoundException:
        raise ValueError(f"Table '{table_name}' does not exist")
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
            if not quiet: log.info(f"Table: '{table_name}'")
            if not quiet: log.info(f"Index: '{index_name}'")
            if not quiet: log.info("Billing mode: Provisioned")
            rcu = gsi['ProvisionedThroughput']['ReadCapacityUnits']
            wcu = gsi['ProvisionedThroughput']['WriteCapacityUnits']
        else:
            if not quiet: log.info(f"Table: '{table_name}'")
            if not quiet: log.info("Billing mode: Provisioned")
            rcu = table_desc['ProvisionedThroughput']['ReadCapacityUnits']
            wcu = table_desc['ProvisionedThroughput']['WriteCapacityUnits']

        if not quiet: log.info(f"Read Capacity Units (RCU): {rcu:,}")
        if not quiet: log.info(f"Write Capacity Units (WCU): {wcu:,}")

        # Get auto-scaling settings
        if not quiet: log.info("\nAuto Scaling Settings:")
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

        if not quiet:
            # This is a best-effort DIAGNOSTIC print. The autoscaling lookup
            # must never crash the job (issue #89): if the Glue role lacks
            # application-autoscaling:DescribeScalableTargets, note that we
            # couldn't read the settings and move on — the actual load/read
            # proceeds and the capacity check in get_dynamodb_throughput_configs
            # degrades separately. An unguarded call here previously took the
            # whole job down at info-print time, before any data moved.
            try:
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
            except Exception as e:
                log.info(
                    f"- Could not read autoscaling settings ({str(e)}); skipping "
                    f"this diagnostic. Grant application-autoscaling:"
                    f"DescribeScalableTargets to the Glue role to see them."
                )

    else:
        if index_name:
            if not quiet: log.info(f"Table: '{table_name}'")
            if not quiet: log.info(f"Index: '{index_name}'")
            if not quiet: log.info("Billing mode: On-demand")
            capacity = gsi.get('OnDemandThroughput', {})
        else:
            if not quiet: log.info(f"Table: '{table_name}'")
            if not quiet: log.info("Billing mode: On-demand")
            capacity = table_desc.get('OnDemandThroughput', {})

        max_rru = capacity.get('MaxReadRequestUnits', None)
        max_wru = capacity.get('MaxWriteRequestUnits', None)

        if not quiet and max_rru is not None and max_wru is not None:
            log.info(f"Max Read Request Units: {int(max_rru):,}")
            log.info(f"Max Write Request Units: {int(max_wru):,}")

    # Get item count and size
    if index_name:
        item_count = gsi.get('ItemCount', 0)
        size_bytes = gsi.get('IndexSizeBytes', 0)
        if not quiet: log.info(f"\nIndex Item Count (approx): {item_count:,}")
        if not quiet: log.info(f"Index Size (approx): {size_bytes:,} bytes")
    else:
        item_count = table_desc.get('ItemCount', 0)
        size_bytes = table_desc.get('TableSizeBytes', 0)
        if not quiet: log.info(f"\nTable Item Count (approx): {item_count:,}")
        if not quiet: log.info(f"Table Size (approx): {size_bytes:,} bytes")
    if not quiet: log.info("")

    return {
        'table_name': table_name,
        'region_name': region_name,
        'billing_mode': billing_mode,
        'write_pricing_category': write_pricing_category,
        'read_pricing_category': read_pricing_category,
        'item_count': item_count,
        'size_bytes': size_bytes,
        'key_schema': {
            ('pk' if k['KeyType'] == 'HASH' else 'sk'): {
                'name': k['AttributeName'],
                'type': {a['AttributeName']: a['AttributeType'] for a in table_desc['AttributeDefinitions']}[k['AttributeName']]
            }
            for k in table_desc['KeySchema']
        }
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

def get_and_print_table_write_cost(table_info, item_count, size_bytes):
    region_name = table_info.get("region_name") or _default_region()

    if item_count == 0:
        log.info("No items to write, skipping cost estimate.")
        return 0

    avg_size = size_bytes / item_count
    avg_write_units_per_item = math.ceil(avg_size / 1024)
    write_units = item_count * avg_write_units_per_item

    pricing_utility = PricingUtility()
    ondemand_pricing = pricing_utility.get_on_demand_capacity_pricing(region_name)
    wru_cost = float(ondemand_pricing.get(table_info['write_pricing_category']))
    od_cost = write_units * wru_cost
    prov_cost = od_cost / 1.5  # very rough, look into updating this

    log.info("DynamoDB write costs depend on how many items are being written and the size of the items.")
    log.info(f"Here we estimate the command will write {item_count:,} items")
    log.info(f" with average size {int(avg_size):,} bytes;")
    log.info(f" each write incurs an average of {avg_write_units_per_item} write units")
    log.info(f"Write units required (approx): {write_units:,}")
    log.info("This does not include costs for secondary indexes!")
    if table_info['billing_mode'] == "PROVISIONED":
        log.info(f"Approx DynamoDB cost for provisioned writes consuming {write_units:,} WCUs (using {region_name} prices): ${prov_cost:,.2f}")
        return prov_cost
    elif table_info['billing_mode'] == "PAY_PER_REQUEST":
        log.info(f"Approx DynamoDB cost for on-demand writes consuming {write_units:,} WRUs (using {region_name} prices): ${od_cost:,.2f}")
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

def _bare_table_name(table_ref):
    """Return the plain table name for a table ref that may be an ARN.

    Autoscaling resource IDs need the bare name (`table/<name>`), whereas the
    caller may hand us a full ARN (from which we also derive the region).
    """
    if table_ref and table_ref.startswith("arn:"):
        try:
            resource = _parse_arn(table_ref).get("resource", "")
        except ValueError:
            return table_ref
        if resource.startswith("table/"):
            # resource may carry a trailing /index/... or :stream:...; the
            # table name is the first path segment after "table/".
            return resource[len("table/"):].split("/")[0].split(":")[0]
        return table_ref
    return table_ref


def _autoscaling_max_capacity(table_name, region_name, dimension):
    """Return the autoscaling MaxCapacity for a provisioned table dimension.

    `dimension` is 'read' or 'write'. Returns None when the table genuinely
    has no autoscaling target on that dimension. Raises on API failure so the
    caller can distinguish "no autoscaling" (safe to treat as hard-provisioned)
    from "unknown" (must skip the capacity warning to avoid a false positive).
    """
    scalable_dimension = (
        'dynamodb:table:ReadCapacityUnits' if dimension == 'read'
        else 'dynamodb:table:WriteCapacityUnits'
    )
    autoscaling = boto3.client('application-autoscaling', region_name=region_name)
    resource_id = f'table/{_bare_table_name(table_name)}'
    response = autoscaling.describe_scalable_targets(
        ServiceNamespace='dynamodb',
        ResourceIds=[resource_id],
        ScalableDimension=scalable_dimension,
    )
    targets = response.get('ScalableTargets', [])
    if targets:
        return int(targets[0]['MaxCapacity'])
    return None


def _effective_capacity_ceiling(table_desc, is_on_demand, region_name, table_name, dimension):
    """Resolve the effective throughput ceiling for a user-requested rate.

    Returns a (ceiling, source, soft_floor) tuple describing the most the
    table can actually deliver on `dimension` ('read' or 'write'):

    - Provisioned, no autoscaling  → (provisioned CU, "provisioned capacity", None)
    - Provisioned with autoscaling → (autoscaling max, "autoscaling maximum", provisioned CU)
    - On-demand with a table max    → (table max, "table's on-demand maximum", None)
    - On-demand, no table max       → (account quota, "account quota", None)

    `soft_floor` is the current provisioned level when autoscaling can climb
    above it — a request between the floor and ceiling gets a gentler note
    rather than a hard warning. `ceiling` is None when it can't be determined
    (e.g. quota lookup failed), signalling the caller to skip the warning.
    """
    if is_on_demand:
        on_demand_throughput = table_desc.get('OnDemandThroughput', {})
        key = 'MaxReadRequestUnits' if dimension == 'read' else 'MaxWriteRequestUnits'
        table_max = on_demand_throughput.get(key)
        if isinstance(table_max, (int, float)) and table_max:
            return int(table_max), "table's on-demand maximum", None
        quota_name = (
            "Table-level read throughput limit" if dimension == 'read'
            else "Table-level write throughput limit"
        )
        quota = get_quota_value(quota_name, region_name)
        if quota is not None:
            return int(quota), "account quota", None
        return None, None, None

    key = 'ReadCapacityUnits' if dimension == 'read' else 'WriteCapacityUnits'
    provisioned = table_desc.get('ProvisionedThroughput', {}).get(key)
    if not (isinstance(provisioned, (int, float)) and provisioned):
        return None, None, None
    provisioned = int(provisioned)
    try:
        autoscaling_max = _autoscaling_max_capacity(table_name, region_name, dimension)
    except Exception as e:
        # Can't tell whether autoscaling would lift the ceiling; skip the
        # capacity warning rather than emit a false "exceeds provisioned" for a
        # table that may well autoscale above the request. This is the expected
        # path when the Glue role lacks application-autoscaling:DescribeScalableTargets
        # (issue #89) — proceed, but surface that we're doing so without
        # visibility into the table's autoscaling settings.
        log.warning(
            f"[{table_name}] Could not read autoscaling settings for the {dimension} "
            f"dimension ({str(e)}); proceeding without knowledge of the table's "
            f"autoscaling metrics, so the requested-rate capacity check is skipped. "
            f"Grant application-autoscaling:DescribeScalableTargets to enable it."
        )
        return None, None, None
    if autoscaling_max is not None:
        return autoscaling_max, "autoscaling maximum", provisioned
    return provisioned, "provisioned capacity", None


def _warn_if_rate_exceeds_capacity(table_name, dimension, user_rate, table_desc,
                                    is_on_demand, region_name):
    """Warn when a user-specified rate exceeds what the table can deliver.

    Implements issue #89 checks 2-5: a hard warning when the request is above
    the effective ceiling (provisioned/autoscaling-max/on-demand-max/quota),
    and a softer note when autoscaling can scale up from the current
    provisioned level to meet the request.
    """
    ceiling, source, soft_floor = _effective_capacity_ceiling(
        table_desc, is_on_demand, region_name, table_name, dimension
    )
    if ceiling is None:
        return
    user_rate = int(user_rate)
    if user_rate > ceiling:
        log.warning(
            f"[{table_name}] Requested {dimension} rate {user_rate} exceeds the "
            f"table's {source} of {ceiling}; the table cannot deliver this rate."
        )
    elif soft_floor is not None and user_rate > soft_floor:
        log.warning(
            f"[{table_name}] Requested {dimension} rate {user_rate} is above the "
            f"current provisioned {dimension} capacity of {soft_floor}; autoscaling "
            f"will need to scale up (toward its maximum of {ceiling}) to meet it."
        )


def _warn_if_job_may_timeout(table_name, dimension, rate, table_desc, remaining_minutes):
    """Warn when the effective rate is too low to finish in the time remaining.

    Implements issue #89 check 1: estimate how long moving the table's data will
    take at `rate` capacity units/second and, if that exceeds the time *remaining*
    before the Glue job times out, warn that the job will likely time out before
    the work completes. The rate may be user-specified or table-derived — either
    way a rate that is technically valid but too small for the table's size is
    worth surfacing.

    `remaining_minutes` is the budget for THIS phase — the job timeout minus the
    time already elapsed — not the raw timeout. Multi-phase verbs (e.g. delete's
    scan phase then write phase) call this as each phase begins, so a later phase
    is measured against the time actually left, per Jason's PR #231 review. We
    deliberately do not predict what future phases will need.

    Uses the same unit formulas as the cost estimators:
    - read  → ceil(size_bytes / 8096) read units for a full scan
    - write → item_count * ceil(avg_item_size / 1024) write units

    Estimation is best-effort; missing/zero metadata simply skips the check.
    """
    try:
        rate = int(rate)
    except (TypeError, ValueError):
        return
    if rate <= 0:
        return

    size_bytes = int(table_desc.get('TableSizeBytes', 0) or 0)
    item_count = int(table_desc.get('ItemCount', 0) or 0)

    if dimension == "read":
        total_units = math.ceil(size_bytes / 8096) if size_bytes else 0
    else:
        if item_count <= 0 or size_bytes <= 0:
            total_units = 0
        else:
            avg_units_per_item = math.ceil((size_bytes / item_count) / 1024)
            total_units = item_count * avg_units_per_item

    if total_units <= 0:
        return

    estimated_seconds = total_units / rate
    remaining_seconds = max(0.0, remaining_minutes * 60)
    if estimated_seconds > remaining_seconds:
        estimated_minutes = estimated_seconds / 60
        log.warning(
            f"[{table_name}] Estimated {dimension} time of ~{estimated_minutes:,.0f} min "
            f"at {rate:,} units/sec for ~{total_units:,} {dimension} units exceeds the "
            f"~{remaining_minutes:,.0f} min remaining before the job timeout; the job will "
            f"likely time out before finishing. Increase the rate "
            f"(e.g. --XMax{dimension.capitalize()}Rate) or the timeout (--XTimeout)."
        )


def get_dynamodb_throughput_configs(args, table_name, modes=None, format="connector"):
    region_name = _region_from_table_ref(table_name) or _default_region()
    if not region_name:
        raise ValueError("Unable to determine region_name for DynamoDB call.")
    dynamodb = boto3.client('dynamodb', region_name=region_name)

    if modes is None:
        modes = ("read", "write")

    DEFAULT_ON_DEMAND_CAPACITY = 40000

    # Job timeout drives the "will this finish in time?" estimate (#89 check 1).
    # We race the next phase against the time REMAINING (timeout minus elapsed),
    # not the raw timeout: this function is called when each phase begins, so a
    # late phase (e.g. delete's write after its scan) sees a shrunken budget.
    timeout_minutes = args.get('XTimeout', DEFAULT_JOB_TIMEOUT_MINUTES)
    try:
        timeout_minutes = int(timeout_minutes)
    except (TypeError, ValueError):
        timeout_minutes = DEFAULT_JOB_TIMEOUT_MINUTES
    remaining_minutes = timeout_minutes - _job_elapsed_minutes()

    # Get table description to determine if it's on-demand or provisioned
    read_rate = args.get('XMaxReadRate', None)   # User set read rate
    write_rate = args.get('XMaxWriteRate', None) # User set write rate

    # Preserve the raw user-specified rates before the branches below may
    # overwrite read_rate/write_rate with table-derived values; only these
    # user requests can meaningfully exceed the table's capacity (#89).
    user_read_rate = read_rate
    user_write_rate = write_rate

    try:
        response = dynamodb.describe_table(TableName=table_name)
        table_desc = response['Table']
        billing_mode = table_desc.get('BillingModeSummary', {}).get('BillingMode', 'PROVISIONED')
        is_on_demand_table = billing_mode == 'PAY_PER_REQUEST'
    except Exception as e:
        log.info(f"[{table_name}] Warning: Could not retrieve table information: {str(e)}")
        is_on_demand_table = False
        table_desc = {}

    # Handle read throughput
    if "read" in modes:
        if read_rate:
            log.info(f"[{table_name}] Max read rate set to specified limit: {read_rate}")
        elif is_on_demand_table:
            # Check for table-specific limit
            on_demand_throughput = table_desc.get('OnDemandThroughput', {})
            table_read_limit = on_demand_throughput.get('MaxReadRequestUnits')

            if table_read_limit is None:
                # Try to get account-level quota
                quota_read_limit = get_quota_value("Table-level read throughput limit", region_name)
                if quota_read_limit is not None:
                    read_rate = quota_read_limit
                    log.info(f"[{table_name}] Max read rate set to account quota limit: {read_rate}")
                else:
                    # Default for on-demand tables
                    read_rate = DEFAULT_ON_DEMAND_CAPACITY
                    log.info(f"[{table_name}] Max read rate set to default on-demand limit: {read_rate}")
            else:
                read_rate = table_read_limit
                log.info(f"[{table_name}] Max read rate set to table-specific on-demand limit: {read_rate}")
        else:
            provisioned_read = table_desc.get('ProvisionedThroughput', {}).get('ReadCapacityUnits')
            if provisioned_read:
                read_rate = provisioned_read
                log.info(f"[{table_name}] Max read rate set to {read_rate} RCUs (based on provisioned capacity)")
            else:
                log.info(f"[{table_name}] Max read rate set internally by Glue (no provisioned level found)") # shouldn't happen

        if read_rate is not None and int(read_rate) < MIN_RECOMMENDED_READ_RATE:
            log.warning(f"[{table_name}] Read rate {read_rate} less than recommended value of {MIN_RECOMMENDED_READ_RATE}.")

        if user_read_rate:
            _warn_if_rate_exceeds_capacity(
                table_name, "read", user_read_rate, table_desc,
                is_on_demand_table, region_name,
            )

        if read_rate is not None:
            _warn_if_job_may_timeout(
                table_name, "read", read_rate, table_desc, remaining_minutes,
            )

    # Handle write throughput
    if "write" in modes:
        if write_rate:
            log.info(f"[{table_name}] Max write rate set to specified limit: {write_rate}")
        elif is_on_demand_table:
            # Check for table-specific limit
            on_demand_throughput = table_desc.get('OnDemandThroughput', {})
            table_write_limit = on_demand_throughput.get('MaxWriteRequestUnits')

            if table_write_limit is None:
                # Try to get account-level quota
                quota_write_limit = get_quota_value("Table-level write throughput limit", region_name)
                if quota_write_limit is not None:
                    write_rate = quota_write_limit
                    log.info(f"[{table_name}] Max write rate set to account quota limit: {write_rate}")
                else:
                    # Default for on-demand tables
                    write_rate = DEFAULT_ON_DEMAND_CAPACITY
                    log.info(f"[{table_name}] Max write rate set to default on-demand limit: {write_rate}")
            else:
                write_rate = table_write_limit
                log.info(f"[{table_name}] Max write rate set to table-specific on-demand limit: {write_rate}")
        else:
            provisioned_write = table_desc.get('ProvisionedThroughput', {}).get('WriteCapacityUnits')
            if provisioned_write:
                write_rate = provisioned_write
                log.info(f"[{table_name}] Max write rate set to {write_rate} WCUs (based on provisioned capacity)")
            else:
                log.info(f"[{table_name}] Max write rate set internally by Glue (no provisioned level found)")

        if write_rate is not None and int(write_rate) < MIN_RECOMMENDED_WRITE_RATE:
            log.warning(f"[{table_name}] Write rate {write_rate} less than recommended value of {MIN_RECOMMENDED_WRITE_RATE}.")

        if user_write_rate:
            _warn_if_rate_exceeds_capacity(
                table_name, "write", user_write_rate, table_desc,
                is_on_demand_table, region_name,
            )

        if write_rate is not None:
            _warn_if_job_may_timeout(
                table_name, "write", write_rate, table_desc, remaining_minutes,
            )

    if format == "connector":
        connection_options = {}

        if "read" in modes:
            if read_rate:
                connection_options["dynamodb.throughput.read"] = str(read_rate)

        if "write" in modes:
            if write_rate is not None:
                connection_options["dynamodb.throughput.write"] = str(write_rate)

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

