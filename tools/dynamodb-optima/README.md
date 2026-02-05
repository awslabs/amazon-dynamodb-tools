# DynamoDB Optima

**Unified DynamoDB Cost Optimization Platform**

A comprehensive CLI and GUI tool that analyzes DynamoDB usage patterns across AWS Organizations to identify cost optimization opportunities. Utilizes AWS CUR and Amazon CloudWatch data across accounts to create recommendations.

[![Active Development](https://img.shields.io/badge/status-active%20development-green.svg)](https://github.com/awslabs/amazon-dynamodb-tools)
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
- 🔄 **Resumable Operations** (*in development*) - Checkpoint/resume for multi-day collections
- 💾 **OLAP Performance** - DuckDB backend for fast analytical queries
- 💰 **Real Pricing** - AWS Pricing API with free tier support
- 🎨 **Interactive GUI** - Streamlit dashboard for visualizations, support for RE2-format regex searches on tablename
- 🔐 **IAM Ready** - Minimal permissions, cross-account role support

## 📋 Prerequisites

### Required

- **Python 3.12+**
- **AWS Credentials** - Configure via `aws configure` or environment variables
- **RAM** - ~2GB of free memory
- **IAM Permissions** (Management Account):
  ```json
  {
    "Version": "2012-10-17",
    "Statement": [
      {
        "Sid": "DynamoDBOptimaAccess",
        "Effect": "Allow",
        "Action": [
          "dynamodb:ListTables",
          "dynamodb:DescribeTable",
          "dynamodb:ListTagsOfResource",
          "cloudwatch:GetMetricData",
          "cloudwatch:GetMetricStatistics",
          "pricing:GetProducts",
          "sts:AssumeRole",
          "organizations:DescribeOrganization",
          "organizations:ListAccounts",
          "cur:DescribeReportDefinitions"
        ],
        "Resource": "*"
      },
      {
        "Sid": "CURDataAccess",
        "Effect": "Allow",
        "Action": [
          "s3:ListBucket",
          "s3:GetObject"
        ],
        "Resource": [
          "arn:aws:s3:::YOUR-CUR-BUCKET-NAME",
          "arn:aws:s3:::YOUR-CUR-BUCKET-NAME/*"
        ]
      }
    ]
  }
  ```

  **Note:** Replace `YOUR-CUR-BUCKET-NAME` with your actual CUR S3 bucket name. The Organizations permissions are only required for `--use-org` mode. AWS CUR and S3 permissions are required for table class analysis with data collected via `collect-cur`. `ListTagsOfResource` is added for future expansion plans, but not required.

### Optional

**For AWS Organizations (Multi-Account Discovery):**
- See **[AWS Organizations Setup Guide](docs/aws-organizations-setup.md)** for detailed instructions on configuring cross-account access

**For Table Class Analysis:**
- **Cost & Usage Report (CUR)** - Must have `INCLUDE_RESOURCES` enabled
- **S3 Read Access** - To CUR export bucket

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

Discover DynamoDB tables across your AWS environment, and store pricing data:

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
```

**Progress:**
```
Collecting metrics: ████████████████████ 100% | 976,543/976,543 datapoints | 12.5m elapsed
✓ Collected metrics for 45 tables across 3 regions
```

### Step 3: Run Analysis (1-5 minutes)

Generate cost optimization recommendations:

```bash
# Capacity mode analysis (On-Demand vs Provisioned)
dynamodb-optima analyze-capacity

# Utilization analysis (over-provisioned capacity)
dynamodb-optima analyze-utilization

# Table class analysis (requires CUR data)
dynamodb-optima analyze-table-class
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

### Step 4: Launch Interactive GUI

```bash
# Start Streamlit dashboard
dynamodb-optima gui

# Open your browser to http://localhost:8501
```

## 📚 Documentation

For detailed documentation, see the **[Documentation Index](docs/README.md)**.

### Quick Links

**User Guides:**
- [Command Reference](docs/command-reference.md) - Complete CLI command documentation
- [GUI Usage](docs/gui-usage.md) - Interactive dashboard guide
- [Configuration](docs/configuration.md) - Environment variables and settings

**Technical Documentation:**
- [Architecture](docs/architecture.md) - System design and database schema
- [Analysis Deep Dive](docs/analysis-deep-dive.md) - How each analysis type works

**Operations:**
- [Advanced Topics](docs/advanced-topics.md) - Multi-account, automation, custom queries

### Support

1. Check existing GitHub issues: [github.com/awslabs/amazon-dynamodb-tools/issues](https://github.com/awslabs/amazon-dynamodb-tools/issues)
2. Open a new issue with:
   - Command used
   - Error message
   - Log excerpt
   - `dynamodb-optima version` output

### Code Quality / Testing

In the future, we desire to implement ruff and tests/. For now, the tests are unwritten.

## 📄 License

This project is licensed under the Apache License 2.0. See [LICENSE](../LICENSE) file.

## 🤝 Contributing

See [CONTRIBUTING.md](../CONTRIBUTING.md) in the repository root for guidelines.

## ✨ Credits

Built by AWS Labs with contributions from the DynamoDB community.

Special thanks to the authors of the original tools that were consolidated into this project.
