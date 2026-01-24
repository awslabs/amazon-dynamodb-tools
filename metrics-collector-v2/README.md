# Metrics Collector v2.0

**DynamoDB Cost Optimization and Analysis Platform**

A unified tool that consolidates the functionality of `capacity-mode-evaluator`, `table_class_optimizer`, and the original `metrics-collector` into a single, comprehensive solution for optimizing DynamoDB costs across AWS Organizations.

## Overview

Metrics Collector v2.0 provides three types of cost optimization analysis:

1. **Capacity Mode Analysis** - Compare On-Demand vs Provisioned with autoscaling simulation
2. **Table Class Analysis** - Evaluate Standard vs Standard-IA based on CUR data
3. **Utilization Analysis** - Identify over-provisioned capacity opportunities

## Features

- ✅ **AWS Organizations Support** - Discover and analyze tables across thousands of accounts
- ✅ **Multi-Account Discovery** - Automatic cross-account role assumption
- ✅ **Autoscaling Simulation** - Accurate cost projections with scale-out/scale-in logic
- ✅ **CUR Integration** - Leverage Cost & Usage Reports for table class analysis
- ✅ **AWS Pricing API** - Real-time pricing data (auto-refreshed monthly)
- ✅ **DuckDB Backend** - Fast OLAP queries on collected metrics
- ✅ **Checkpoint & Resume** - Resilient multi-day collection workflows
- ✅ **Streamlit GUI** - Interactive visualization and analysis
- ✅ **Fortune 50 Ready** - Designed for large-scale enterprise deployments

## Installation

```bash
cd metrics-collector-v2
pip install -e ".[dev]"
```

## Quick Start

### 1. Discover Tables

```bash
# Single account, specific regions
metrics-collector discover --regions us-east-1,us-west-2

# AWS Organizations (all accounts)
metrics-collector discover --use-org
```

### 2. Collect Metrics

```bash
# Collect 14 days of CloudWatch metrics
metrics-collector collect --days 14
```

### 3. Run Analysis

```bash
# Capacity mode recommendations
metrics-collector analyze-capacity --days 14

# Table class recommendations (requires CUR setup)
metrics-collector analyze-table-class --months 1

# Utilization recommendations
metrics-collector analyze-utilization --days 7
```

### 4. View Recommendations

```bash
# List all recommendations
metrics-collector list-recommendations

# Filter by type and minimum savings
metrics-collector list-recommendations --type capacity --min-savings 100
```

### 5. Launch GUI

```bash
# Interactive dashboard
metrics-collector gui
```

## Configuration

Create `.env` file or set environment variables:

```bash
# AWS Configuration
AWS_REGION=us-east-1
AWS_PROFILE=default

# AWS Organizations (optional)
USE_ORGANIZATIONS=false
ORGANIZATIONS_ROLE_NAME=MetricsCollectorRole

# CUR Configuration (for table class analysis)
CUR_DATABASE_NAME=athenacurcfn_cur_report
CUR_TABLE_NAME=cur_report
CUR_S3_BUCKET=my-cur-bucket
CUR_REGION=us-east-1

# Analysis Settings
CAPACITY_ANALYSIS_DAYS=14
TABLE_CLASS_ANALYSIS_MONTHS=1
AUTOSCALING_TARGET_UTILIZATION=0.7
MIN_SAVINGS_THRESHOLD_USD=10.0
```

## Architecture

```
metrics-collector-v2/
├── src/metrics_collector/
│   ├── cli.py                 # Command-line interface
│   ├── config.py              # Configuration management
│   ├── logging.py             # Structured logging
│   ├── core/
│   │   └── state.py           # State management
│   ├── aws/
│   │   ├── client.py          # AWS client factory
│   │   ├── discovery.py       # Table discovery
│   │   ├── collector.py       # Metrics collection
│   │   ├── organizations.py   # Organizations support (Phase 2)
│   │   ├── athena.py          # CUR queries (Phase 4)
│   │   └── pricing_collector.py  # Pricing API (Phase 2)
│   ├── database/
│   │   ├── connection.py      # DuckDB connection
│   │   ├── schema.py          # Extended schema
│   │   └── view_manager.py    # Views and queries
│   ├── analysis/
│   │   ├── capacity_mode.py   # Capacity analysis (Phase 3)
│   │   ├── table_class.py     # Table class analysis (Phase 4)
│   │   ├── utilization.py     # Utilization analysis (Phase 5)
│   │   └── autoscaling_sim.py # Autoscaling simulation (Phase 3)
│   ├── commands/
│   │   ├── core/              # Core commands (status, health, etc.)
│   │   └── analysis/          # Analysis commands (Phases 3-5)
│   ├── gui/
│   │   ├── app.py             # Streamlit app (Phase 6)
│   │   └── components/        # GUI components (Phase 6)
│   └── utils/
│       ├── error_handling.py  # Error handling
│       └── progress.py        # Progress tracking
└── tests/                     # Comprehensive test suite
```

## Database Schema

The tool uses DuckDB with the following tables:

- `dynamodb_tables` - Discovered table metadata
- `aws_accounts` - Organization accounts
- `pricing_data` - AWS Pricing API data (auto-refreshed)
- `cloudwatch_metrics` - Collected metrics
- `capacity_mode_recommendations` - On-Demand vs Provisioned analysis
- `table_class_recommendations` - Standard vs Standard-IA analysis
- `utilization_recommendations` - Over-provisioning analysis
- `collection_state` - Operation state tracking
- `checkpoints` - Resume points for long-running operations

## Development Status

**Phase 1 - Foundation** ✅ COMPLETE
- Core infrastructure and schema
- Configuration management
- CLI framework
- Test infrastructure

**Phase 2-8** 🚧 IN PROGRESS
- See `implementation_plan.md` for detailed roadmap

## Testing

```bash
# Run all tests
pytest

# Run specific test types
pytest -m unit
pytest -m integration
pytest -m aws

# With coverage
pytest --cov=metrics_collector --cov-report=html
```

## Contributing

This project follows AWS Labs standards. See `CONTRIBUTING.md` in the repository root.

## License

This project is licensed under the Apache License 2.0. See `LICENSE` in the repository root.

## Related Issues

- #118 - Consolidate tools into unified metrics-collector
- #24 - Capacity mode analysis improvements
- #52 - Table class optimization enhancements
- #60 - Multi-account support via Organizations

## Credits

Built upon the foundation of:
- `dmetrics` - Core async architecture and state management
- `capacity-mode-evaluator` - Autoscaling simulation logic
- `table_class_optimizer` - CUR-based table class analysis
- `archived/ddbtools` - AWS Pricing API integration
