# Configuration

← [Documentation Index](README.md) | [Main README](../README.md)

---

DynamoDB Optima uses [Pydantic Settings](https://docs.pydantic.dev/latest/concepts/pydantic_settings/) for configuration management with automatic environment variable support.

**Source Code:** All configuration settings are defined in [`src/dynamodb_optima/config.py`](../src/dynamodb_optima/config.py)

**Environment Variable Mapping:** Pydantic automatically maps field names to uppercase environment variables. For example, `aws_region` becomes `AWS_REGION`.

> **⚠️ Important:**
> This is an exhaustive list of all configurable settings as of February 2026. Not all settings are safe to change - many are internal tuning parameters for AWS Organizations throttling, autoscaling simulation, and other advanced features. **Modify settings at your own risk and test thoroughly.** 

## Configuration Methods

### 1. Environment Variables

Create a `.env` file in the project root or set environment variables directly:

```bash
# Example .env file
AWS_REGION=us-east-1
AWS_PROFILE=my-profile
DATABASE_POOL_SIZE=20
```

### 2. Programmatic Configuration

```python
from dynamodb_optima.config import Settings

settings = Settings(
    aws_region="us-west-2",
    database_pool_size=20,
    min_savings_threshold_usd=50.0
)
```

## Available Settings

### Application Settings

| Environment Variable | Default | Description |
|---------------------|---------|-------------|
| `APP_NAME` | `"DynamoDB Optima"` | Application name |
| `DEBUG` | `False` | Enable debug mode |
| `LOG_LEVEL` | `"INFO"` | Logging level (DEBUG, INFO, WARNING, ERROR) |

### Database Settings

| Environment Variable | Default | Description |
|---------------------|---------|-------------|
| `DATABASE_POOL_SIZE` | `10` | DuckDB connection pool size |

**Note:** `DATABASE_URL` is computed automatically based on `--project-root` CLI option and cannot be set via environment variable.

### AWS Settings

| Environment Variable | Default | Description |
|---------------------|---------|-------------|
| `AWS_REGION` | `"us-east-1"` | Default AWS region |
| `AWS_PROFILE` | `None` | AWS profile name from ~/.aws/credentials |
| `AWS_ACCESS_KEY_ID` | `None` | AWS Access Key (overrides profile) |
| `AWS_SECRET_ACCESS_KEY` | `None` | AWS Secret Key (overrides profile) |
| `AWS_SESSION_TOKEN` | `None` | AWS Session Token (overrides profile) |

### AWS Organizations Settings

| Environment Variable | Default | Description |
|---------------------|---------|-------------|
| `USE_ORGANIZATIONS` | `False` | Enable AWS Organizations integration |
| `ORGANIZATIONS_ROLE_NAME` | `"OrganizationAccountAccessRole"` | IAM role to assume in member accounts |
| `ORGANIZATIONS_ROLE_SESSION_NAME` | `"dynamodb-optima"` | Session name for STS AssumeRole |
| `ORGANIZATIONS_MAX_CREDENTIAL_CACHE_SIZE` | `10000` | Max credentials to cache (prevents OOM) |
| `ORGANIZATIONS_MANAGEMENT_ACCOUNT_ID` | `None` | Management account ID (auto-detected if not set) |

**Advanced Organizations Settings:** See [`config.py`](../src/dynamodb_optima/config.py) for throttling and retry configuration.

### Cost & Usage Reports (CUR) Settings

| Environment Variable | Default | Description |
|---------------------|---------|-------------|
| `CUR_S3_LOCATION_OVERRIDE` | `None` | Manual CUR S3 path (s3://bucket/prefix). Auto-discovered if not set. |
| `CUR_MANAGEMENT_ACCOUNT_ROLE` | `"MetricsCollectorRole"` | IAM role for CUR access in management account |
| `CUR_COLLECTION_MONTHS` | `3` | Number of months of CUR data to collect |
| `CUR_COLLECTION_BATCH_SIZE` | `1000000` | Rows per checkpoint during CUR collection |
| `CUR_REGION` | `"us-east-1"` | AWS region for CUR API |

### AWS Pricing API Settings

| Environment Variable | Default | Description |
|---------------------|---------|-------------|
| `PRICING_API_REGION` | `"us-east-1"` | Region for Pricing API (us-east-1 or ap-south-1) |
| `PRICING_REFRESH_DAYS` | `30` | Days before refreshing pricing data |

### Metrics Collection Settings

| Environment Variable | Default | Description |
|---------------------|---------|-------------|
| `METRICS_COLLECTION_INTERVAL_HOURS` | `24` | Collection interval in hours |
| `METRICS_RETENTION_DAYS` | `1095` | Retention period (3 years) |
| `CHECKPOINT_SAVE_INTERVAL` | `25` | Save checkpoint every N operations |

### Capacity Mode Analysis Settings

| Environment Variable | Default | Description |
|---------------------|---------|-------------|
| `CAPACITY_ANALYSIS_DAYS` | `14` | Days to analyze for capacity recommendations |
| `AUTOSCALING_TARGET_UTILIZATION` | `0.7` | Target utilization for autoscaling (70%) |
| `AUTOSCALING_SCALE_OUT_COOLDOWN` | `2` | Minutes before scale-out |
| `AUTOSCALING_SCALE_IN_COOLDOWN` | `15` | Minutes before scale-in |
| `AUTOSCALING_MIN_CAPACITY` | `1` | Minimum capacity units |
| `AUTOSCALING_MAX_CAPACITY` | `40000` | Maximum capacity units |

### Table Class Analysis Settings

| Environment Variable | Default | Description |
|---------------------|---------|-------------|
| `TABLE_CLASS_ANALYSIS_MONTHS` | `1` | Months to analyze for table class recommendations |
| `TABLE_CLASS_STANDARD_STORAGE_GB_COST` | `0.25` | Standard storage $/GB-month (updated from API) |
| `TABLE_CLASS_IA_STORAGE_GB_COST` | `0.10` | Standard-IA storage $/GB-month (updated from API) |

### Utilization Analysis Settings

| Environment Variable | Default | Description |
|---------------------|---------|-------------|
| `UTILIZATION_THRESHOLD` | `0.4` | Utilization threshold (40%) |
| `UTILIZATION_ANALYSIS_DAYS` | `7` | Days to analyze for utilization recommendations |

### General Recommendation Settings

| Environment Variable | Default | Description |
|---------------------|---------|-------------|
| `CONFIDENCE_THRESHOLD` | `0.8` | Minimum confidence score (80%) |
| `MIN_SAVINGS_THRESHOLD_USD` | `10.0` | Minimum monthly savings to generate recommendation |

### GUI Settings

| Environment Variable | Default | Description |
|---------------------|---------|-------------|
| `GUI_PORT` | `8501` | Port for Streamlit dashboard |
| `GUI_THEME` | `"light"` | GUI theme (light or dark) |

## Example Configurations

### Production Environment

```bash
# .env
LOG_LEVEL=WARNING
DATABASE_POOL_SIZE=20
AWS_PROFILE=production
USE_ORGANIZATIONS=true
ORGANIZATIONS_ROLE_NAME=DynamoDBOptimaRole
CUR_COLLECTION_MONTHS=6
MIN_SAVINGS_THRESHOLD_USD=100.0
```

### Development Environment

```bash
# .env
LOG_LEVEL=DEBUG
DATABASE_POOL_SIZE=5
AWS_PROFILE=dev
CAPACITY_ANALYSIS_DAYS=7
MIN_SAVINGS_THRESHOLD_USD=1.0
GUI_THEME=dark
```

### Multi-Account Analysis

```bash
# .env
USE_ORGANIZATIONS=true
ORGANIZATIONS_ROLE_NAME=MetricsCollectorRole
ORGANIZATIONS_MAX_CREDENTIAL_CACHE_SIZE=20000
CUR_S3_LOCATION_OVERRIDE=s3://my-cur-bucket/reports/
```

## Notes

- **Case Insensitive:** Pydantic Settings accepts both `AWS_REGION` and `aws_region`
- **Type Validation:** Invalid values will raise validation errors at startup
- **`.env` File:** Automatically loaded from project root if present
- **CLI Override:** Some settings (like `--profile`, `--regions`) can be overridden via CLI flags
- **Full Reference:** See [`config.py`](../src/dynamodb_optima/config.py) for all available settings and their types
