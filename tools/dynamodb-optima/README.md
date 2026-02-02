# DynamoDB Optima

**Unified DynamoDB Cost Optimization Platform**

A comprehensive CLI and GUI tool that analyzes DynamoDB usage patterns across AWS Organizations to identify cost optimization opportunities. Consolidates the functionality of `capacity-mode-evaluator`, `table_class_optimizer`, and `metrics-collector` into a single, enterprise-ready solution.

[![Python 3.12+](https://img.shields.io/badge/python-3.12+-blue.svg)](https://www.python.org/downloads/)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![AWS](https://img.shields.io/badge/AWS-DynamoDB-orange.svg)](https://aws.amazon.com/dynamodb/)

## 🎯 Overview

DynamoDB Optima provides three types of cost optimization analysis for DynamoDB:

| Analysis Type | Use Case | Data Source | Potential Savings |
|--------------|----------|-------------|-------------------|
| **Capacity Mode** | Switch between On-Demand ↔ Provisioned | CloudWatch Metrics | 30-70% on capacity costs |
| **Table Class** | Switch between Standard ↔ Standard-IA | Cost & Usage Reports | 30-60% on storage costs |
| **Utilization** | Right-size over-provisioned capacity | CloudWatch Metrics | 20-50% on wasted capacity |

### Key Capabilities

- 🏢 **Enterprise Scale** - Handles 1000+ AWS accounts via Organizations
- 📊 **3 Analysis Types** - Comprehensive cost optimization coverage
- 🔄 **Resumable Operations** - Checkpoint/resume for multi-day collections
- 💾 **OLAP Performance** - DuckDB backend for fast analytical queries
- 💰 **Real Pricing** - AWS Pricing API with free tier support
- 🎨 **Interactive GUI** - Streamlit dashboard for visualizations, support for RE2-format regex searches on tablename
- 🔐 **IAM Ready** - Minimal permissions, cross-account role support

## 📋 Prerequisites

### Required

- **Python 3.12+**
- **AWS Credentials** - Configure via `aws configure` or environment variables
- **IAM Permissions**:
  ```json
  {
    "Version": "2012-10-17",
    "Statement": [
      {
        "Effect": "Allow",
        "Action": [
          "dynamodb:ListTables",
          "dynamodb:DescribeTable",
          "dynamodb:ListTagsOfResource",
          "cloudwatch:GetMetricData",
          "cloudwatch:GetMetricStatistics",
          "pricing:GetProducts"
        ],
        "Resource": "*"
      }
    ]
  }
  ```

### Optional (For AWS Organizations)

- **Organizations Read Access**:
  ```json
  {
    "Effect": "Allow",
    "Action": [
      "organizations:ListAccounts",
      "organizations:DescribeAccount",
      "organizations:ListOrganizationalUnitsForParent"
    ],
    "Resource": "*"
  }
  ```

- **Cross-Account Role** - Deploy to member accounts:
  ```json
  {
    "Effect": "Allow",
    "Action": ["sts:AssumeRole"],
    "Resource": "arn:aws:iam::*:role/MetricsCollectorRole"
  }
  ```

### Optional (For Table Class Analysis)

- **Cost & Usage Report (CUR)** - Must have `INCLUDE_RESOURCES` enabled
- **S3 Read Access** - To CUR export bucket
- **Athena Query Permission** (if using Athena-based collection)

## 🚀 Installation

```bash
cd beta/dynamodb-optima

# Install with development dependencies
pip install -e ".[dev]"

# Verify installation
dynamodb-optima version
```

## 💨 Quick Start

### Step 1: Discover Tables (2-5 minutes)

Discover DynamoDB tables across your AWS environment:

```bash
# Single account, specific regions
dynamodb-optima discover --regions us-east-1,us-west-2

# AWS Organizations (all accounts, all regions)
dynamodb-optima discover --use-org
```

**Output:**
```
Discovered 45 DynamoDB tables across 3 accounts
  us-east-1: 23 tables
  us-west-2: 18 tables
  eu-west-1: 4 tables
```

### Step 2: Collect Metrics (10-60 minutes)

Collect CloudWatch metrics for analysis:

```bash
# Collect 14 days of metrics (recommended for capacity analysis)
dynamodb-optima collect --days 14

# Or collect specific time range
dynamodb-optima collect --start-date 2026-01-01 --end-date 2026-01-14
```

**Progress:**
```
Collecting metrics: ████████████████████ 100% | 976,543/976,543 datapoints | 12.5m elapsed
✓ Collected metrics for 45 tables across 3 regions
```

### Step 3: Collect Pricing Data (1-2 minutes)

Download AWS pricing information:

```bash
# Collect pricing for discovered regions
dynamodb-optima collect-pricing
```

**Output:**
```
✓ Collected 1,247 DynamoDB SKUs across 3 regions
✓ Including free tier pricing data
```

### Step 4: Run Analysis (1-5 minutes)

Generate cost optimization recommendations:

```bash
# Capacity mode analysis (On-Demand vs Provisioned)
dynamodb-optima analyze-capacity --days 14

# Utilization analysis (over-provisioned capacity)
dynamodb-optima analyze-utilization --days 7

# Table class analysis (requires CUR data)
dynamodb-optima analyze-table-class --months 1
```

**Sample Output:**
```
Capacity Mode Analysis Results
==============================
Analyzed: 45 tables | Recommendations: 12 tables
  
Switch to On-Demand:  8 tables → Save $2,340/month
Switch to Provisioned: 4 tables → Save $890/month

Total Potential Savings: $3,230/month ($38,760/year)
```

### Step 5: View Recommendations

```bash
# List all recommendations
dynamodb-optima list-recommendations

# Filter by type and minimum savings
dynamodb-optima list-recommendations --type capacity --min-savings 100

# Export to CSV
dynamodb-optima list-recommendations --format csv --output recs.csv
```

### Step 6: Launch Interactive GUI

```bash
# Start Streamlit dashboard
dynamodb-optima gui

# Opens browser to http://localhost:8501
```

## 📚 Command Reference

### Core Commands

#### Multi-organization with isolated data (use --project-root for all commands)
dynamodb-optima --project-root /data/org-a COMMANDHERE


#### `discover`
Discover DynamoDB tables across AWS accounts and regions.

```bash
# Single account
dynamodb-optima discover --regions us-east-1,us-west-2

# AWS Organizations
dynamodb-optima discover --use-org [--role-name MetricsCollectorRole]

# Resume from checkpoint
dynamodb-optima discover --resume
```

**Options:**
- `--regions` - Comma-separated list of regions (default: all available)
- `--use-org` - Enable AWS Organizations multi-account discovery
- `--role-name` - Cross-account role name (default: MetricsCollectorRole)
- `--resume` - Resume from last checkpoint
- `--skip-tags` - Skip tag collection for faster discovery

#### `collect`
Collect CloudWatch metrics for discovered tables.

```bash
# Collect by days
dynamodb-optima collect --days 14

# Collect by date range
dynamodb-optima collect --start-date 2026-01-01 --end-date 2026-01-14

# Collect specific tables
dynamodb-optima collect --tables table1,table2 --days 7

# Resume interrupted collection
dynamodb-optima collect --resume
```

**Options:**
- `--days` - Number of days to collect (default: 14)
- `--start-date` - Start date (YYYY-MM-DD)
- `--end-date` - End date (YYYY-MM-DD)
- `--tables` - Specific table names (comma-separated)
- `--regions` - Specific regions (comma-separated)
- `--resume` - Resume from checkpoint
- `--batch-size` - Metrics per batch (default: 1000)

#### `collect-pricing`
Collect AWS Pricing API data for DynamoDB.

```bash
# Collect for all discovered regions
dynamodb-optima collect-pricing

# Force refresh (skip cache)
dynamodb-optima collect-pricing --force-refresh
```

**Options:**
- `--regions` - Specific regions (default: all discovered)
- `--force-refresh` - Ignore cached pricing data

#### `collect-cur`
Collect Cost & Usage Report data for table class analysis.

```bash
# Auto-discover CUR from management account
dynamodb-optima collect-cur

# Specify CUR details
dynamodb-optima collect-cur \
  --bucket my-cur-bucket \
  --prefix cur-reports/hourly \
  --months 1

# Parquet format (recommended)
dynamodb-optima collect-cur --format parquet
```

**Options:**
- `--bucket` - S3 bucket with CUR exports
- `--prefix` - S3 prefix for CUR data
- `--months` - Months of data to collect (default: 1)
- `--format` - CUR format: parquet or csv (default: parquet)

### Analysis Commands

#### `analyze-capacity`
Analyze capacity mode recommendations (On-Demand vs Provisioned).

```bash
# Standard analysis
dynamodb-optima analyze-capacity --days 14

# Custom autoscaling parameters
dynamodb-optima analyze-capacity \
  --days 14 \
  --target-utilization 0.7 \
  --min-savings 50

# Specific tables
dynamodb-optima analyze-capacity \
  --tables my-table-1,my-table-2 \
  --days 7
```

**Options:**
- `--days` - Days of metrics to analyze (default: 14)
- `--target-utilization` - Autoscaling target (default: 0.7)
- `--min-savings` - Minimum monthly savings threshold (default: 10.0)
- `--tables` - Specific tables to analyze

**How It Works:**
1. Analyzes consumed vs provisioned capacity patterns
2. Simulates autoscaling for Provisioned mode
3. Calculates costs using real AWS pricing (with free tier)
4. Recommends mode with lower monthly cost

#### `analyze-table-class`
Analyze table class recommendations (Standard vs Standard-IA).

```bash
# Standard analysis
dynamodb-optima analyze-table-class --months 1

# Custom breakeven ratio
dynamodb-optima analyze-table-class \
  --months 3 \
  --breakeven-ratio 2.5 \
  --min-savings 100
```

**Options:**
- `--months` - Months of CUR data to analyze (default: 1)
- `--breakeven-ratio` - Storage/throughput ratio threshold (default: 2.67)
- `--min-savings` - Minimum monthly savings threshold (default: 10.0)
- `--tables` - Specific tables to analyze

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
# Standard analysis
dynamodb-optima analyze-utilization --days 7

# Custom thresholds
dynamodb-optima analyze-utilization \
  --days 7 \
  --low-utilization-threshold 30 \
  --min-savings 25
```

**Options:**
- `--days` - Days of metrics to analyze (default: 7)
- `--low-utilization-threshold` - Threshold percentage (default: 30)
- `--min-savings` - Minimum monthly savings threshold (default: 10.0)
- `--tables` - Specific tables to analyze

**How It Works:**
1. Analyzes average utilization over analysis period
2. Identifies resources with <30% average utilization
3. Recommends capacity reduction to 80% of peak usage
4. Calculates savings from reduced provisioned capacity

### Utility Commands

#### `list-recommendations`
Display generated recommendations.

```bash
# All recommendations
dynamodb-optima list-recommendations

# Filter by type
dynamodb-optima list-recommendations --type capacity

# Filter by savings
dynamodb-optima list-recommendations --min-savings 100

# Export to CSV
dynamodb-optima list-recommendations --format csv --output recs.csv

# Show only actionable recommendations
dynamodb-optima list-recommendations --status pending
```

**Options:**
- `--type` - Filter by type: capacity, table_class, utilization
- `--min-savings` - Minimum monthly savings (USD)
- `--status` - Filter by status: pending, accepted, rejected, implemented
- `--format` - Output format: table, csv, json
- `--output` - Output file path

#### `status`
Show collection and analysis status.

```bash
# Overall status
dynamodb-optima status

# Detailed metrics breakdown
dynamodb-optima status --verbose
```

#### `health`
Run system health checks.

```bash
# Basic health check
dynamodb-optima health

# Detailed diagnostics
dynamodb-optima health --verbose
```

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

**GUI Features:**
- Dashboard with summary metrics
- Interactive filtering (region, table, min savings)
- Visualization charts (pie charts, bar charts, trend lines)
- CSV export for all recommendation types
- Drill-down into individual table details

## 🔬 Analysis Deep Dive

### Capacity Mode Analysis

**Objective:** Determine if On-Demand or Provisioned with autoscaling is more cost-effective.

**Process:**
1. **Collect** 14 days of `ConsumedReadCapacityUnits` and `ConsumedWriteCapacityUnits` metrics
2. **Simulate** autoscaling behavior:
   - Target utilization: 70%
   - Scale-out: When >70% for 2 consecutive minutes
   - Scale-in: When <50% for 15 consecutive minutes
   - Min capacity: 1, Max capacity: 40,000
3. **Calculate** costs:
   - On-Demand: $1.25/million write requests, $0.25/million read requests
   - Provisioned: $0.00065/hour per WCU, $0.00013/hour per RCU
   - Includes free tier: 25 WCU, 25 RCU if eligible
4. **Recommend** mode with lower monthly cost

**Best For:**
- Tables with unpredictable traffic patterns → On-Demand
- Tables with steady, predictable traffic → Provisioned
- Tables with high utilization (>70%) → Provisioned

**Example Savings:**
```
Table: prod-users-table
Current: Provisioned (500 RCU, 500 WCU) → $280/month
Recommended: On-Demand → $95/month
Monthly Savings: $185 (66% reduction)
Reason: Highly variable traffic, low average utilization
```

### Table Class Analysis

**Objective:** Determine if Standard-IA table class provides cost savings.

**Process:**
1. **Collect** CUR data with resource IDs (requires `INCLUDE_RESOURCES`)
2. **Calculate** storage-to-throughput cost ratio:
   ```
   Ratio = Monthly Storage Cost / Monthly Throughput Cost
   ```
3. **Compare** to breakeven ratio (2.67:1):
   - Standard-IA saves 50% on storage
   - Standard-IA costs 50% more on throughput
   - Breakeven when storage is 2.67x throughput costs
4. **Recommend** Standard-IA if ratio > 2.67

**Best For:**
- Tables with high storage, low throughput
- Infrequently accessed data (archives, logs)
- Tables with read-heavy workloads

**Example Savings:**
```
Table: prod-audit-logs
Current: Standard → $450/month
  Storage: $400 (10 TB)
  Throughput: $50
Ratio: 8.0:1 (well above 2.67 breakeven)

Recommended: Standard-IA → $300/month
  Storage: $200 (50% savings)
  Throughput: $75 (50% increase)
Monthly Savings: $150 (33% reduction)
```

### Utilization Analysis

**Objective:** Identify over-provisioned capacity in Provisioned mode tables.

**Process:**
1. **Analyze** average utilization over 7-14 days
2. **Identify** resources with <30% average utilization
3. **Recommend** capacity = 1.25x peak observed consumption (80% target)
4. **Calculate** savings from reduced capacity

**Best For:**
- Right-sizing after traffic decrease
- Cleaning up over-provisioned capacity
- Post-migration optimization

**Example Savings:**
```
Table: staging-test-data
Current: Provisioned (1000 RCU, 1000 WCU) → $560/month
Average Utilization: 15% read, 8% write

Recommended: Provisioned (200 RCU, 150 WCU) → $100/month
Monthly Savings: $460 (82% reduction)
Reason: Significantly over-provisioned for actual load
```

## 🎨 GUI Usage

The Streamlit dashboard provides an interactive interface for exploring recommendations.

### Dashboard Page

The main dashboard shows:
- Total potential monthly/annual savings
- Breakdown by analysis type (pie chart)
- Top 10 tables by savings (bar chart)
- Regional distribution
- Summary statistics

### Analysis Pages

Each analysis type has a dedicated page:

**Capacity Analysis Page:**
- Table with all capacity recommendations
- Filters: region, min savings, current mode
- Current vs recommended billing mode comparison
- Cost breakdown charts
- Autoscaling simulation results
- Export to CSV

**Table Class Analysis Page:**
- Table with all table class recommendations
- Storage-to-throughput ratio visualization
- Current vs recommended cost comparison
- Breakeven analysis charts
- Export to CSV

**Utilization Analysis Page:**
- Table with all utilization recommendations
- Utilization percentage charts
- Current vs recommended capacity comparison
- Savings by resource type (table vs GSI)
- Export to CSV

### Navigation

Use the sidebar to:
- Switch between pages
- Apply global filters (region, min savings)
- View summary statistics
- Access documentation

## 🔧 Configuration

### Environment Variables

Create a `.env` file or set environment variables:

```bash
# AWS Configuration
AWS_REGION=us-east-1
AWS_PROFILE=default

# Database
DATABASE_URL=data/dynamodb_optima.db
DATABASE_POOL_SIZE=10

# AWS Organizations
USE_ORGANIZATIONS=false
ORGANIZATIONS_ROLE_NAME=MetricsCollectorRole

# Analysis Settings
CAPACITY_ANALYSIS_DAYS=14
TABLE_CLASS_ANALYSIS_MONTHS=1
UTILIZATION_ANALYSIS_DAYS=7
AUTOSCALING_TARGET_UTILIZATION=0.7
MIN_SAVINGS_THRESHOLD_USD=10.0

# CUR Configuration
CUR_S3_BUCKET=my-cur-bucket
CUR_S3_PREFIX=cur-reports/hourly
CUR_FORMAT=parquet

# GUI
STREAMLIT_SERVER_PORT=8501
STREAMLIT_THEME=light
```

### Advanced Configuration

Edit `src/dynamodb_optima/config.py` for programmatic configuration or create a custom config file:

```python
from dynamodb_optima.config import Settings

settings = Settings(
    autoscaling_target_utilization=0.75,
    min_savings_threshold_usd=50.0,
    database_pool_size=20
)
```

## 🏗️ Architecture

### System Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                      CLI / GUI Layer                         │
│  (commands, Streamlit dashboard, interactive analysis)      │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                    Business Logic Layer                      │
│  • Discovery (multi-account, cross-region)                  │
│  • Collection (metrics, pricing, CUR)                       │
│  • Analysis (capacity, table class, utilization)            │
│  • State Management (checkpoints, resume)                   │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                   Data Access Layer                          │
│  • DuckDB Connection Pool                                    │
│  • OLAP-Optimized Queries                                    │
│  • Schema Management & Migrations                            │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                   External Services                          │
│  AWS Organizations • CloudWatch • Pricing API • S3 (CUR)    │
└─────────────────────────────────────────────────────────────┘
```

### Database Schema

**Core Tables:**
- `table_metadata` - Discovered DynamoDB tables
- `gsi_metadata` - Global Secondary Indexes
- `metrics` - CloudWatch metrics (time-series)
- `pricing_data` - AWS Pricing API data (with free tier)

**Analysis Tables:**
- `capacity_mode_recommendations`
- `table_class_recommendations`
- `utilization_recommendations`

**Operational Tables:**
- `collection_state` - Operation tracking
- `checkpoints` - Resume points
- `aws_accounts` - Organizations accounts
- `cur_metadata` - CUR configuration
- `cur_data` - Cost & Usage Report data

### Performance Characteristics

- **Discovery:** ~100 accounts/minute
- **Metrics Collection:** ~50,000 datapoints/minute
- **Analysis:** ~1,000 tables/minute
- **Database:** DuckDB optimized for OLAP queries
- **Memory:** Scales with dataset size (~100 MB per 1M metrics)

## 🩺 Troubleshooting

### Common Issues

#### No Recommendations Generated

**Symptom:** Analysis completes but shows 0 recommendations

**Causes:**
1. Insufficient metrics collected
2. All tables already optimized
3. Savings below minimum threshold

**Solutions:**
```bash
# Check metrics collection
dynamodb-optima status --verbose

# Lower savings threshold
dynamodb-optima analyze-capacity --min-savings 1

# Verify metrics in database
dynamodb-optima health
```

#### Collection Timeout

**Symptom:** Metrics collection times out or hangs

**Causes:**
1. Too many tables/metrics
2. AWS API rate limiting
3. Network connectivity issues

**Solutions:**
```bash
# Collect specific regions first
dynamodb-optima collect --regions us-east-1 --days 7

# Reduce batch size
dynamodb-optima collect --days 14 --batch-size 500

# Resume from checkpoint
dynamodb-optima collect --resume
```

#### CUR Data Not Found

**Symptom:** Table class analysis fails with "No CUR data"

**Causes:**
1. CUR not configured
2. INCLUDE_RESOURCES not enabled
3. Wrong S3 bucket/prefix

**Solutions:**
```bash
# Verify CUR configuration
aws cur describe-report-definitions

# Check S3 access
aws s3 ls s3://my-cur-bucket/cur-prefix/

# Collect CUR with correct parameters
dynamodb-optima collect-cur --bucket my-cur-bucket --prefix reports/
```

#### Permission Errors

**Symptom:** "Access Denied" errors during operations

**Solutions:**
1. Verify IAM permissions (see Prerequisites)
2. Check cross-account role trust relationships
3. Verify S3 bucket policies for CUR access

### Debug Mode

Enable verbose logging:

```bash
# Set environment variable
export LOG_LEVEL=DEBUG

# Or use --verbose flag
dynamodb-optima collect --days 7 --verbose
```

### Log Locations

- Application logs: `logs/dynamodb_optima.log`
- Database: `data/dynamodb_optima.db`
- Checkpoints: `checkpoints/`

### Support

For issues not covered here:
1. Check existing GitHub issues: [github.com/awslabs/amazon-dynamodb-tools/issues](https://github.com/awslabs/amazon-dynamodb-tools/issues)
2. Review phase completion documents in repo
3. Open a new issue with:
   - Command used
   - Error message
   - Log excerpt
   - `dynamodb-optima version` output

## 🚀 Advanced Topics

### Multi-Account Strategy

**Option 1: Organizations Discovery (Recommended)**
```bash
# Single command discovers all accounts
dynamodb-optima discover --use-org
dynamodb-optima collect --days 14
```

**Option 2: Manual Account Iteration**
```bash
# Iterate through account profiles
for profile in prod-account-1 prod-account-2; do
  AWS_PROFILE=$profile dynamodb-optima discover
  AWS_PROFILE=$profile dynamodb-optima collect --days 14
done
```

### Automation with Cron

```bash
# Daily metrics collection
0 2 * * * cd /path/to/dynamodb-optima-v2 && dynamodb-optima collect --days 1

# Weekly analysis
0 3 * * 0 cd /path/to/dynamodb-optima-v2 && dynamodb-optima analyze-capacity --days 7

# Monthly table class analysis
0 4 1 * * cd /path/to/dynamodb-optima-v2 && dynamodb-optima analyze-table-class --months 1
```

### Custom Analysis Queries

Access the DuckDB database directly for custom analysis:

```python
from dynamodb_optima.database import get_connection

conn = get_connection()

# Custom query
results = conn.execute("""
    SELECT 
        table_name,
        AVG(value) as avg_consumed_rcu
    FROM metrics
    WHERE metric_name = 'ConsumedReadCapacityUnits'
      AND timestamp >= CURRENT_DATE - INTERVAL '7 days'
    GROUP BY table_name
    ORDER BY avg_consumed_rcu DESC
    LIMIT 10
""").fetchall()

for row in results:
    print(f"{row[0]}: {row[1]:.2f} RCU")
```

### Integration with Other Tools

**Export to CloudWatch Dashboard:**
```bash
# Export recommendations as JSON
dynamodb-optima list-recommendations --format json > recs.json

# Process with jq and create dashboard
cat recs.json | jq '...' | aws cloudwatch put-dashboard
```

**Integration with Cost Explorer:**
```bash
# Export recommendations with current costs
dynamodb-optima list-recommendations --format csv --output recs.csv

# Import to Cost Explorer for validation
```

## 🧪 Development

All implementation phases are complete:

- ✅ **Phase 1** - Foundation (core infrastructure, database, CLI framework)
- ✅ **Phase 2** - AWS Integration (Organizations, Pricing API, multi-account)
- ✅ **Phase 3** - Capacity Analysis (autoscaling simulation, On-Demand vs Provisioned)
- ✅ **Phase 4** - CUR Integration (table class analysis, S3 collection)
- ✅ **Phase 5** - Utilization Analysis (over-provisioning detection)
- ✅ **Phase 6** - GUI (Streamlit dashboard, visualizations, CSV export)

### Testing

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=dynamodb_optima --cov-report=html

# Run specific test types
pytest -m unit
pytest -m integration
pytest -m aws

# Run specific test file
pytest tests/aws/test_organizations.py
```

### Code Quality

```bash
# Format code
black src/ tests/

# Lint code
ruff check src/ tests/

# Type checking
mypy src/
```

## 📄 License

This project is licensed under the Apache License 2.0. See [LICENSE](../LICENSE) file.

## 🤝 Contributing

See [CONTRIBUTING.md](../CONTRIBUTING.md) in the repository root for guidelines.

## 📚 Related Projects

- **capacity-mode-evaluator** - Original capacity mode analysis tool (archived)
- **table_class_optimizer** - Original table class analysis tool (archived)
- **metrics-collector** - Original metrics collection tool (archived)
- **dmetrics** - Reference implementation for core architecture

## 🔗 Related Issues

- [#118](https://github.com/awslabs/amazon-dynamodb-tools/issues/118) - Tool consolidation and v2 development
- [#24](https://github.com/awslabs/amazon-dynamodb-tools/issues/24) - Capacity mode analysis improvements
- [#52](https://github.com/awslabs/amazon-dynamodb-tools/issues/52) - Table class optimization enhancements

## ✨ Credits

Built by AWS Labs with contributions from the DynamoDB community.

Special thanks to the authors of the original tools that were consolidated into this project.
