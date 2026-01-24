"""
Extended database schema for Metrics Collector.

Includes tables for:
- AWS account discovery (Organizations)
- Pricing data (AWS Pricing API)
- Capacity mode recommendations
- Table class recommendations
- Utilization recommendations
"""

# DynamoDB tables schema (table_metadata is the actual table name used by collector)
DYNAMODB_TABLES_SCHEMA = """
CREATE TABLE IF NOT EXISTS table_metadata (
    account_id VARCHAR,
    region VARCHAR,
    table_name VARCHAR,
    table_arn VARCHAR,
    table_status VARCHAR,
    creation_datetime TIMESTAMP,
    billing_mode VARCHAR,
    table_class VARCHAR,
    table_size_bytes BIGINT,
    item_count BIGINT,
    provisioned_read_capacity BIGINT,
    provisioned_write_capacity BIGINT,
    global_secondary_indexes JSON,
    local_secondary_indexes JSON,
    stream_enabled BOOLEAN,
    point_in_time_recovery_enabled BOOLEAN,
    encryption_type VARCHAR,
    deletion_protection_enabled BOOLEAN,
    tags JSON,
    discovered_at TIMESTAMP,
    last_updated TIMESTAMP,
    configuration JSON,
    PRIMARY KEY (table_name, region)
);
"""

# GSI metadata schema
GSI_METADATA_SCHEMA = """
CREATE TABLE IF NOT EXISTS gsi_metadata (
    account_id VARCHAR,
    region VARCHAR,
    table_name VARCHAR,
    gsi_name VARCHAR,
    resource_name VARCHAR,
    provisioned_read_capacity INTEGER,
    provisioned_write_capacity INTEGER,
    projection_type VARCHAR,
    discovered_at TIMESTAMP,
    last_updated TIMESTAMP,
    PRIMARY KEY (account_id, region, table_name, gsi_name)
);

-- Index for resource_name lookups (used in metrics collection)
CREATE INDEX IF NOT EXISTS idx_gsi_resource_name ON gsi_metadata(resource_name, region);
"""

# AWS Accounts schema (for Organizations support)
AWS_ACCOUNTS_SCHEMA = """
CREATE TABLE IF NOT EXISTS aws_accounts (
    account_id VARCHAR PRIMARY KEY,
    account_name VARCHAR,
    account_email VARCHAR,
    account_status VARCHAR,
    joined_method VARCHAR,
    joined_timestamp TIMESTAMP,
    organizational_unit_id VARCHAR,
    organizational_unit_name VARCHAR,
    is_management_account BOOLEAN,
    discovered_at TIMESTAMP,
    last_updated TIMESTAMP
);
"""

# Comprehensive pricing data schema (from AWS Pricing API)
# Stores ALL DynamoDB SKUs with all available attributes
PRICING_DATA_SCHEMA = """
CREATE TABLE IF NOT EXISTS pricing_data (
    -- Primary identifiers
    pricing_id VARCHAR PRIMARY KEY,  -- Generated unique ID
    sku VARCHAR NOT NULL,
    
    -- Always present core fields
    region_code VARCHAR NOT NULL,
    location VARCHAR,
    collected_at TIMESTAMP NOT NULL,
    
    -- Core pricing information
    price_per_unit DECIMAL(20, 10) NOT NULL,
    unit VARCHAR,  -- 'Hrs', 'GB-Mo', 'requests', etc.
    currency VARCHAR DEFAULT 'USD',
    
    -- Tiered pricing support (for free tier and other tiers)
    begin_range VARCHAR,  -- e.g., "0", "18600", etc.
    end_range VARCHAR,    -- e.g., "18600", "Inf", etc.
    description TEXT,     -- e.g., "free tier", "beyond free tier"
    
    -- Term information
    term_type VARCHAR,  -- 'OnDemand', 'Reserved'
    
    -- DynamoDB service attributes (may be NULL if not available)
    product_family VARCHAR,
    service_code VARCHAR,
    service_name VARCHAR,
    location_type VARCHAR,
    volume_type VARCHAR,
    usage_type VARCHAR,
    region VARCHAR,  -- Different from region_code, full name like 'US East (N. Virginia)'
    group_name VARCHAR,  -- Maps to 'group' attribute in AWS
    group_description VARCHAR,
    operation VARCHAR,
    
    -- Reserved instance attributes (NULL for On-Demand)
    lease_contract_length VARCHAR,
    purchase_option VARCHAR,
    offering_class VARCHAR
);

-- Indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_pricing_region_family ON pricing_data(region_code, product_family);
CREATE INDEX IF NOT EXISTS idx_pricing_usage_type ON pricing_data(usage_type);
CREATE INDEX IF NOT EXISTS idx_pricing_group ON pricing_data(group_name);
CREATE INDEX IF NOT EXISTS idx_pricing_operation ON pricing_data(operation);
CREATE INDEX IF NOT EXISTS idx_pricing_collected ON pricing_data(collected_at);
"""

# CloudWatch metrics schema (metrics is the actual table name used by collector)
CLOUDWATCH_METRICS_SCHEMA = """
CREATE TABLE IF NOT EXISTS metrics (
    table_name VARCHAR,
    resource_name VARCHAR,
    resource_type VARCHAR,
    metric_name VARCHAR,
    operation VARCHAR,
    operation_type VARCHAR,
    statistic VARCHAR,
    period_seconds INTEGER,
    timestamp TIMESTAMP,
    value DOUBLE,
    unit VARCHAR,
    region VARCHAR,
    dimensions JSON,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (resource_name, metric_name, timestamp, statistic, period_seconds)
);
"""

# Capacity mode recommendations schema
CAPACITY_MODE_RECOMMENDATIONS_SCHEMA = """
CREATE TABLE IF NOT EXISTS capacity_mode_recommendations (
    recommendation_id VARCHAR PRIMARY KEY,
    account_id VARCHAR,
    region VARCHAR,
    table_name VARCHAR,
    current_billing_mode VARCHAR,
    recommended_billing_mode VARCHAR,
    analysis_start_date TIMESTAMP,
    analysis_end_date TIMESTAMP,
    analysis_days INTEGER,
    
    -- Current costs
    current_monthly_cost_usd DECIMAL(18, 2),
    current_read_cost_usd DECIMAL(18, 2),
    current_write_cost_usd DECIMAL(18, 2),
    
    -- Projected costs with recommendation
    projected_monthly_cost_usd DECIMAL(18, 2),
    projected_read_cost_usd DECIMAL(18, 2),
    projected_write_cost_usd DECIMAL(18, 2),
    
    -- Savings
    monthly_savings_usd DECIMAL(18, 2),
    annual_savings_usd DECIMAL(18, 2),
    savings_percentage DECIMAL(5, 2),
    
    -- Autoscaling simulation details (for Provisioned mode)
    avg_provisioned_rcu INTEGER,
    avg_provisioned_wcu INTEGER,
    max_provisioned_rcu INTEGER,
    max_provisioned_wcu INTEGER,
    min_provisioned_rcu INTEGER,
    min_provisioned_wcu INTEGER,
    
    -- Utilization statistics
    avg_read_utilization DECIMAL(5, 2),
    avg_write_utilization DECIMAL(5, 2),
    peak_read_utilization DECIMAL(5, 2),
    peak_write_utilization DECIMAL(5, 2),
    
    -- Recommendation metadata
    confidence_score DECIMAL(5, 2),
    risk_level VARCHAR,  -- 'low', 'medium', 'high'
    recommendation_reason TEXT,
    created_at TIMESTAMP,
    status VARCHAR  -- 'pending', 'accepted', 'rejected', 'implemented'
);
"""

# Table class recommendations schema
TABLE_CLASS_RECOMMENDATIONS_SCHEMA = """
CREATE TABLE IF NOT EXISTS table_class_recommendations (
    recommendation_id VARCHAR PRIMARY KEY,
    account_id VARCHAR,
    region VARCHAR,
    table_name VARCHAR,
    current_table_class VARCHAR,
    recommended_table_class VARCHAR,
    analysis_start_date TIMESTAMP,
    analysis_end_date TIMESTAMP,
    analysis_months INTEGER,
    
    -- Current costs from CUR
    current_monthly_storage_cost_usd DECIMAL(18, 2),
    current_monthly_throughput_cost_usd DECIMAL(18, 2),
    current_monthly_total_cost_usd DECIMAL(18, 2),
    
    -- Projected costs with recommendation
    projected_monthly_storage_cost_usd DECIMAL(18, 2),
    projected_monthly_throughput_cost_usd DECIMAL(18, 2),
    projected_monthly_total_cost_usd DECIMAL(18, 2),
    
    -- Savings
    monthly_savings_usd DECIMAL(18, 2),
    annual_savings_usd DECIMAL(18, 2),
    savings_percentage DECIMAL(5, 2),
    
    -- Storage details
    avg_table_size_gb DECIMAL(18, 2),
    storage_to_throughput_ratio DECIMAL(10, 4),
    
    -- Breakeven analysis
    breakeven_ratio DECIMAL(10, 4),
    is_above_breakeven BOOLEAN,
    
    -- Recommendation metadata
    confidence_score DECIMAL(5, 2),
    has_reserved_capacity BOOLEAN,
    recommendation_reason TEXT,
    created_at TIMESTAMP,
    status VARCHAR  -- 'pending', 'accepted', 'rejected', 'implemented'
);
"""

# Utilization recommendations schema
UTILIZATION_RECOMMENDATIONS_SCHEMA = """
CREATE TABLE IF NOT EXISTS utilization_recommendations (
    recommendation_id VARCHAR PRIMARY KEY,
    account_id VARCHAR,
    region VARCHAR,
    table_name VARCHAR,
    resource_type VARCHAR,  -- 'table', 'gsi'
    resource_name VARCHAR,  -- table name or GSI name
    analysis_start_date TIMESTAMP,
    analysis_end_date TIMESTAMP,
    analysis_days INTEGER,
    
    -- Current provisioned capacity
    current_provisioned_rcu INTEGER,
    current_provisioned_wcu INTEGER,
    
    -- Recommended capacity
    recommended_provisioned_rcu INTEGER,
    recommended_provisioned_wcu INTEGER,
    
    -- Current costs
    current_monthly_cost_usd DECIMAL(18, 2),
    
    -- Projected costs with recommendation
    projected_monthly_cost_usd DECIMAL(18, 2),
    
    -- Savings
    monthly_savings_usd DECIMAL(18, 2),
    annual_savings_usd DECIMAL(18, 2),
    savings_percentage DECIMAL(5, 2),
    
    -- Utilization statistics
    avg_read_utilization DECIMAL(5, 2),
    avg_write_utilization DECIMAL(5, 2),
    max_read_utilization DECIMAL(5, 2),
    max_write_utilization DECIMAL(5, 2),
    p99_read_utilization DECIMAL(5, 2),
    p99_write_utilization DECIMAL(5, 2),
    
    -- Recommendation metadata
    confidence_score DECIMAL(5, 2),
    risk_level VARCHAR,  -- 'low', 'medium', 'high'
    recommendation_reason TEXT,
    created_at TIMESTAMP,
    status VARCHAR  -- 'pending', 'accepted', 'rejected', 'implemented'
);
"""

# State management schema (from dmetrics)
STATE_SCHEMA = """
CREATE TABLE IF NOT EXISTS collection_state (
    state_id VARCHAR PRIMARY KEY,
    operation_type VARCHAR,
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    status VARCHAR,
    progress_percent INTEGER,
    current_step VARCHAR,
    total_steps INTEGER,
    error_message TEXT,
    metadata JSON
);
"""

# Checkpoints schema (from dmetrics)
CHECKPOINTS_SCHEMA = """
CREATE TABLE IF NOT EXISTS checkpoints (
    checkpoint_id VARCHAR PRIMARY KEY,
    operation_type VARCHAR,
    operation_id VARCHAR,
    checkpoint_data JSON,
    created_at TIMESTAMP,
    description TEXT
);
"""

# CUR metadata schema (for CUR discovery and tracking)
CUR_METADATA_SCHEMA = """
CREATE TABLE IF NOT EXISTS cur_metadata (
    management_account_id VARCHAR PRIMARY KEY,
    cur_report_name VARCHAR NOT NULL,
    cur_s3_bucket VARCHAR NOT NULL,
    cur_s3_prefix VARCHAR,
    cur_format VARCHAR DEFAULT 'Parquet',
    cur_compression VARCHAR,
    cur_versioning VARCHAR,
    cur_granularity VARCHAR NOT NULL,  -- 'HOURLY', 'DAILY', 'MONTHLY' (must be HOURLY)
    has_resource_ids BOOLEAN DEFAULT FALSE,  -- INCLUDE_RESOURCES enabled in CUR config
    last_collected_date DATE,
    last_discovered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    collection_status VARCHAR,  -- 'discovered', 'collecting', 'complete', 'error'
    rows_collected BIGINT DEFAULT 0,
    error_message TEXT
);
"""

# CUR data schema (filtered DynamoDB usage data from S3)
# Raw line items - no aggregation, using AWS's official composite unique identifier
CUR_DATA_SCHEMA = """
CREATE TABLE IF NOT EXISTS cur_data (
    identity_line_item_id VARCHAR NOT NULL,  -- AWS's official line item ID (unique within partition)
    identity_time_interval VARCHAR NOT NULL,  -- Time interval in format YYYY-MM-DDTHH:mm:ssZ/YYYY-MM-DDTHH:mm:ssZ
    account_id VARCHAR,
    region VARCHAR,
    resource_name VARCHAR,  -- Table name/GSI/etc from line_item_resource_id (when INCLUDE_RESOURCES enabled)
    usage_month DATE,
    operation VARCHAR,
    usage_type VARCHAR,
    line_item_type VARCHAR,  -- Usage, Discount, Tax, Credit, etc.
    usage_start_date TIMESTAMP,
    usage_end_date TIMESTAMP,
    usage_amount DECIMAL(18, 6),
    unblended_cost DECIMAL(18, 4),
    net_unblended_cost DECIMAL(18, 4),  -- After-discount cost
    blended_cost DECIMAL(18, 4),  -- Consolidated billing cost
    line_item_description TEXT,
    collected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (identity_line_item_id, identity_time_interval)
);

-- Indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_cur_account ON cur_data(account_id);
CREATE INDEX IF NOT EXISTS idx_cur_region ON cur_data(region);
CREATE INDEX IF NOT EXISTS idx_cur_resource ON cur_data(resource_name);
CREATE INDEX IF NOT EXISTS idx_cur_month ON cur_data(usage_month);
CREATE INDEX IF NOT EXISTS idx_cur_operation ON cur_data(operation);

-- Indexes for fast querying
CREATE INDEX IF NOT EXISTS idx_cur_resource ON cur_data(resource_name, usage_month);
CREATE INDEX IF NOT EXISTS idx_cur_account_region ON cur_data(account_id, region, usage_month);
CREATE INDEX IF NOT EXISTS idx_cur_cost ON cur_data(unblended_cost DESC);
CREATE INDEX IF NOT EXISTS idx_cur_net_cost ON cur_data(net_unblended_cost DESC);
CREATE INDEX IF NOT EXISTS idx_cur_operation ON cur_data(operation, usage_type);
CREATE INDEX IF NOT EXISTS idx_cur_line_item_type ON cur_data(line_item_type);
"""

# All schemas
ALL_SCHEMAS = [
    DYNAMODB_TABLES_SCHEMA,
    GSI_METADATA_SCHEMA,
    AWS_ACCOUNTS_SCHEMA,
    PRICING_DATA_SCHEMA,
    CLOUDWATCH_METRICS_SCHEMA,
    CAPACITY_MODE_RECOMMENDATIONS_SCHEMA,
    TABLE_CLASS_RECOMMENDATIONS_SCHEMA,
    UTILIZATION_RECOMMENDATIONS_SCHEMA,
    STATE_SCHEMA,
    CHECKPOINTS_SCHEMA,
    CUR_METADATA_SCHEMA,
    CUR_DATA_SCHEMA,
]


def initialize_database(connection) -> None:
    """Initialize all database tables."""
    for schema in ALL_SCHEMAS:
        connection.execute(schema)
    connection.commit()


def get_table_schemas() -> dict[str, str]:
    """Get all table schemas as a dictionary."""
    return {
        "table_metadata": DYNAMODB_TABLES_SCHEMA,
        "gsi_metadata": GSI_METADATA_SCHEMA,
        "aws_accounts": AWS_ACCOUNTS_SCHEMA,
        "pricing_data": PRICING_DATA_SCHEMA,
        "metrics": CLOUDWATCH_METRICS_SCHEMA,
        "capacity_mode_recommendations": CAPACITY_MODE_RECOMMENDATIONS_SCHEMA,
        "table_class_recommendations": TABLE_CLASS_RECOMMENDATIONS_SCHEMA,
        "utilization_recommendations": UTILIZATION_RECOMMENDATIONS_SCHEMA,
        "collection_state": STATE_SCHEMA,
        "checkpoints": CHECKPOINTS_SCHEMA,
        "cur_metadata": CUR_METADATA_SCHEMA,
        "cur_data": CUR_DATA_SCHEMA,
    }
