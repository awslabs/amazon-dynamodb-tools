"""
Configuration management for Metrics Collector platform.

Handles environment variables, AWS credentials, and application settings
using Pydantic Settings for type safety and validation.
"""

from typing import List, Optional

from pydantic import Field
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Application settings with environment variable support."""

    # Application settings
    app_name: str = Field(default="Metrics Collector", description="Application name")
    debug: bool = Field(default=False, description="Enable debug mode")
    log_level: str = Field(default="INFO", description="Logging level")

    # Database settings
    database_url: str = Field(
        default="duckdb://./data/metrics_collector.db", description="DuckDB database URL"
    )
    database_pool_size: int = Field(
        default=10, description="Database connection pool size"
    )

    # AWS settings
    aws_region: str = Field(default="us-east-1", description="Default AWS region")
    aws_regions: List[str] = Field(
        default_factory=lambda: [
            "us-east-1",
            "us-east-2",
            "us-west-1",
            "us-west-2",
            "eu-west-1",
            "eu-west-2",
            "eu-west-3",
            "eu-central-1",
            "ap-southeast-1",
            "ap-southeast-2",
            "ap-northeast-1",
            "ap-northeast-2",
        ],
        description="AWS regions to collect metrics from",
    )
    aws_profile: Optional[str] = Field(
        default=None, description="AWS profile name from ~/.aws/credentials"
    )
    aws_access_key_id: Optional[str] = Field(
        default=None, description="AWS Access Key ID (overrides profile)"
    )
    aws_secret_access_key: Optional[str] = Field(
        default=None, description="AWS Secret Access Key (overrides profile)"
    )
    aws_session_token: Optional[str] = Field(
        default=None, description="AWS Session Token (overrides profile)"
    )

    # AWS Organizations settings
    use_organizations: bool = Field(
        default=False,
        description="Use AWS Organizations to discover accounts"
    )
    organizations_role_name: str = Field(
        default="MetricsCollectorRole",
        description="IAM role name to assume in member accounts"
    )
    organizations_management_account_id: Optional[str] = Field(
        default=None,
        description="AWS Organizations management account ID (auto-detected if not provided)"
    )

    # Cost & Usage Reports (CUR) settings - DuckDB S3 Direct Access
    cur_s3_location_override: Optional[str] = Field(
        default=None,
        description="Manual override for CUR S3 location (s3://bucket/prefix). Auto-discovered if not set."
    )
    cur_management_account_role: str = Field(
        default="MetricsCollectorRole",
        description="IAM role name to assume in management account for CUR access"
    )
    cur_collection_months: int = Field(
        default=3,
        description="Number of months of CUR data to collect (default: 3 months)"
    )
    cur_collection_batch_size: int = Field(
        default=1000000,
        description="Number of rows per checkpoint when collecting CUR data"
    )
    cur_region: str = Field(
        default="us-east-1",
        description="AWS region for CUR API (always us-east-1)"
    )

    # AWS Pricing API settings
    pricing_api_region: str = Field(
        default="us-east-1",
        description="AWS region for Pricing API (us-east-1 for Americas, ap-south-1 for APAC/Europe)"
    )
    pricing_refresh_days: int = Field(
        default=30,
        description="Days before refreshing pricing data from AWS Pricing API"
    )

    # API settings
    api_host: str = Field(default="0.0.0.0", description="API host")
    api_port: int = Field(default=8000, description="API port")
    api_workers: int = Field(default=1, description="Number of API workers")

    # Security settings
    secret_key: str = Field(
        default="your-secret-key-change-in-production",
        description="Secret key for JWT tokens",
    )
    access_token_expire_minutes: int = Field(
        default=30, description="Access token expiration time in minutes"
    )

    # Metrics collection settings
    metrics_collection_interval_hours: int = Field(
        default=24, description="Metrics collection interval in hours"
    )
    metrics_retention_days: int = Field(
        default=1095,
        description="Metrics retention period in days",  # 3 years
    )
    checkpoint_save_interval: int = Field(
        default=25, description="Save checkpoint every N operations"
    )

    # Capacity mode analysis settings
    capacity_analysis_days: int = Field(
        default=14,
        description="Number of days to analyze for capacity mode recommendations"
    )
    autoscaling_target_utilization: float = Field(
        default=0.7,
        description="Target utilization for autoscaling simulation (70%)"
    )
    autoscaling_scale_out_cooldown: int = Field(
        default=2,
        description="Minutes of consecutive high utilization before scale-out"
    )
    autoscaling_scale_in_cooldown: int = Field(
        default=15,
        description="Minutes of consecutive low utilization before scale-in"
    )
    autoscaling_min_capacity: int = Field(
        default=1,
        description="Minimum capacity units for autoscaling"
    )
    autoscaling_max_capacity: int = Field(
        default=40000,
        description="Maximum capacity units for autoscaling"
    )

    # Table class analysis settings
    table_class_analysis_months: int = Field(
        default=1,
        description="Number of months to analyze for table class recommendations"
    )
    table_class_standard_storage_gb_cost: float = Field(
        default=0.25,
        description="Standard storage cost per GB-month (updated from pricing API)"
    )
    table_class_ia_storage_gb_cost: float = Field(
        default=0.10,
        description="Standard-IA storage cost per GB-month (updated from pricing API)"
    )

    # Utilization analysis settings
    utilization_threshold: float = Field(
        default=0.4,
        description=(
            "Utilization threshold for provisioned vs on-demand recommendations"
        ),
    )
    utilization_analysis_days: int = Field(
        default=7,
        description="Number of days to analyze for utilization recommendations"
    )

    # General recommendation settings
    confidence_threshold: float = Field(
        default=0.8, description="Minimum confidence score for recommendations"
    )
    min_savings_threshold_usd: float = Field(
        default=10.0,
        description="Minimum monthly savings in USD to generate a recommendation"
    )

    # GUI settings
    gui_port: int = Field(
        default=8501,
        description="Port for Streamlit GUI"
    )
    gui_theme: str = Field(
        default="light",
        description="GUI theme (light or dark)"
    )

    model_config = {
        "env_file": ".env",
        "env_file_encoding": "utf-8",
        "case_sensitive": False,
    }


# Global settings instance
settings = Settings()


def get_settings() -> Settings:
    """Get application settings instance."""
    return settings
