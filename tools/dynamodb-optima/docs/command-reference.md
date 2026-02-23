
# Command Reference

← [Documentation Index](README.md) | [Main README](../README.md)

---

## Core Commands

> **⚠️ Note on Checkpointing (Work In Progress):**
> The checkpoint and resume functionality (`--resume`, `--operation-id`, `status`, `checkpoints` commands) is currently under development and **untested as of February 2026**. While these options are documented and available in the CLI, they may not function as expected. Use at your own discretion.

#### Multi-organization with isolated data (use --project-root for all commands)
```sh
mkdir org-a
dynamodb-optima --project-root org-a COMMANDHERE
```

#### `discover`
Discover DynamoDB tables across AWS accounts and regions.

```bash
# Single account, specific regions
dynamodb-optima discover --regions us-east-1,us-west-2

# AWS Organizations (all accounts)
dynamodb-optima discover --use-org

# AWS Organizations with custom role
dynamodb-optima discover --use-org --org-role CustomRole

# Resume from checkpoint
dynamodb-optima discover --resume --operation-id <id>

# Skip specific accounts
dynamodb-optima discover --use-org --skip-accounts 111122223333,444455556666

# Manual CUR location override
dynamodb-optima discover --cur-override s3://my-bucket/cur-prefix
```

**Options:**
- `--regions TEXT` - Comma-separated list of AWS regions (e.g., us-east-1,us-west-2)
- `--profile TEXT` - AWS profile name to use
- `--use-org` - Use AWS Organizations to discover accounts across entire organization
- `--org-role TEXT` - IAM role name to assume in member accounts (default: OrganizationAccountAccessRole)
- `--skip-accounts TEXT` - Comma-separated list of account IDs to skip during discovery
- `--resume` - Resume from last checkpoint
- `--operation-id TEXT` - Operation ID for resuming or tracking
- `--cur-override TEXT` - Manual CUR S3 location override (s3://bucket/prefix)

#### `collect`
Collect CloudWatch metrics for discovered tables.

```bash
# Collect 14 days of metrics (default)
dynamodb-optima collect --days 14

# Collect specific tables
dynamodb-optima collect --tables table1,table2 --days 7

# Collect from specific regions
dynamodb-optima collect --regions us-east-1,us-west-2

# Resume interrupted collection
dynamodb-optima collect --resume --operation-id <id>

# Comprehensive metrics (includes all operations)
dynamodb-optima collect --comprehensive --days 7

# Force full re-collection (truncate first)
dynamodb-optima collect --truncate --days 14
```

**Options:**
- `--regions TEXT` - Comma-separated list of AWS regions (leave empty to use discovered tables)
- `--tables TEXT` - Comma-separated list of table names (leave empty for all discovered)
- `--days INTEGER` - Number of days of metrics to collect (default: 14)
- `--profile TEXT` - AWS profile name to use
- `--resume` - Resume from last checkpoint
- `--operation-id TEXT` - Operation ID for resuming or tracking
- `--comprehensive` - Collect comprehensive metrics (more detailed, slower)
- `--truncate` - Truncate metrics table before collection (forces full re-collection)

#### `collect-cur`
Collect Cost & Usage Report data for table class analysis.

```bash
# Collect default 3 months of CUR data
dynamodb-optima collect-cur

# Collect 6 months with force refresh
dynamodb-optima collect-cur --months 6 --force

# Use specific AWS profile
dynamodb-optima collect-cur --profile production
```

**Options:**
- `--months INTEGER` - Number of months to collect (default: 3 from config)
- `--force` - Force full refresh - delete and re-collect all data
- `--profile TEXT` - AWS profile name to use

**Prerequisites:**
- Run `dynamodb-optima discover` first to find CUR location
- CUR must be enabled in AWS Billing Console
- CUR format must be Parquet
- IAM permissions for S3 access required

### Analysis Commands

#### `analyze-capacity`
Analyze capacity mode recommendations (On-Demand vs Provisioned).

```bash
# Analyze all tables with 14 days of metrics
dynamodb-optima analyze-capacity

# Analyze with 30 days of metrics
dynamodb-optima analyze-capacity --days 30

# Analyze specific table
dynamodb-optima analyze-capacity --table 123456:us-east-1:my-table

# Show only recommendations with >$100 savings
dynamodb-optima analyze-capacity --min-savings 100
```

**Options:**
- `--days INTEGER` - Number of days of metrics to analyze (default: 14)
- `--table TEXT` - Analyze specific table (format: account_id:region:table_name)
- `--min-savings FLOAT` - Minimum savings threshold in USD (default: 10.0)
- `--format [table|csv|json]` - Output format

**How It Works:**
1. Analyzes consumed vs provisioned capacity patterns
2. Simulates autoscaling for Provisioned mode
3. Calculates costs using real AWS pricing (with free tier)
4. Recommends mode with lower monthly cost

#### `analyze-table-class`
Analyze table class recommendations (Standard vs Standard-IA).

```bash
# Analyze all tables with default settings (3 months, $50 min savings)
dynamodb-optima analyze-table-class

# Analyze specific tables in us-east-1
dynamodb-optima analyze-table-class --region us-east-1 --table MyTable

# Analyze with higher savings threshold
dynamodb-optima analyze-table-class --min-savings 100 --months 6

# Output as JSON
dynamodb-optima analyze-table-class --format json
```

**Options:**
- `--months INTEGER` - Number of months of CUR data to analyze (default: 3)
- `--min-savings FLOAT` - Minimum monthly savings threshold in USD (default: 50)
- `--account TEXT` - Filter by AWS account ID (can specify multiple times)
- `--region TEXT` - Filter by AWS region (can specify multiple times)
- `--table TEXT` - Filter by table name (can specify multiple times)
- `--save / --no-save` - Save recommendations to database (default: True)
- `--format [table|json|csv]` - Output format (default: table)

**Requirements:**
- CUR data must be collected first via `collect-cur`
- CUR must have `INCLUDE_RESOURCES` enabled to identify tables

**How It Works:**
1. Calculates storage-to-throughput cost ratio from CUR data
2. Compares to Standard-IA breakeven ratio (2.67:1)
3. Tables above ratio = candidates for Standard-IA (50% storage savings)
4. Accounts for 50% throughput price increase in Standard-IA

#### `analyze-utilization`
Identify over-provisioned capacity opportunities.

```bash
# Analyze all provisioned tables
dynamodb-optima analyze-utilization

# Analyze with 30 days of metrics
dynamodb-optima analyze-utilization --days 30

# Analyze specific table
dynamodb-optima analyze-utilization --table us-east-1:my-table

# Show only recommendations with >$50 savings
dynamodb-optima analyze-utilization --min-savings 50

# Use 30% utilization threshold
dynamodb-optima analyze-utilization --threshold 30
```

**Options:**
- `--days INTEGER` - Number of days of metrics to analyze (default: 14)
- `--table TEXT` - Analyze specific table (format: region:table_name)
- `--threshold FLOAT` - Utilization threshold percentage (default: 45.0)
- `--min-savings FLOAT` - Minimum savings threshold in USD (default: 10.0)
- `--format [table|csv|json]` - Output format

**How It Works:**
1. Analyzes average utilization over analysis period
2. Identifies resources with <30% average utilization
3. Recommends capacity reduction to 80% of peak usage
4. Calculates savings from reduced provisioned capacity

### Utility Commands

#### `status`
Show current operation status and checkpoint information.

```bash
# Basic status
dynamodb-optima status

# Show specific operation
dynamodb-optima status <operation-id>

# Detailed status information
dynamodb-optima status --detailed

# Show throughput metrics
dynamodb-optima status --throughput
```

**Options:**
- `--detailed` - Show detailed status information
- `--throughput` - Show throughput metrics

#### `health`
Check system health and operational status.

```bash
# Basic health check
dynamodb-optima health

# Show detailed health information
dynamodb-optima health --detailed

# Output as JSON
dynamodb-optima health --json
```

**Options:**
- `--detailed` - Show detailed health information
- `--json` - Output results as JSON

#### `checkpoints`
Manage operation checkpoints.

```bash
# View checkpoints
dynamodb-optima checkpoints

# Clean up old checkpoint files
dynamodb-optima checkpoints --cleanup

# Clean up checkpoints older than 7 days
dynamodb-optima checkpoints --cleanup --max-age 7
```

**Options:**
- `--cleanup` - Clean up old checkpoint files
- `--max-age INTEGER` - Maximum age in days for cleanup

#### `gui`
Launch interactive Streamlit dashboard.

```bash
# Default port 8501
dynamodb-optima gui

# Custom port
dynamodb-optima gui --port 8080

# Custom theme
dynamodb-optima gui --theme dark
```

**Options:**
- `--port INTEGER` - Port for Streamlit GUI (default: 8501)
- `--theme [light|dark]` - Theme selection

**GUI Features:**
- Dashboard with summary metrics
- Interactive filtering (region, table, min savings)
- Visualization charts (pie charts, bar charts, trend lines)
- CSV export for all recommendation types
- Drill-down into individual table details
