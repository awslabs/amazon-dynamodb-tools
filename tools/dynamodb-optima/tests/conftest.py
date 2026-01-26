"""
Pytest configuration and fixtures for DynamoDB Optima tests.

Provides common fixtures for database connections, AWS mocking,
and test data generation.
"""

import pytest
from unittest.mock import Mock, MagicMock
from datetime import datetime, timedelta
import duckdb
from pathlib import Path
import tempfile


@pytest.fixture
def temp_db_path():
    """Create a temporary database path for testing."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    yield db_path
    # Cleanup
    Path(db_path).unlink(missing_ok=True)


@pytest.fixture
def db_connection(temp_db_path):
    """Create a DuckDB connection for testing."""
    conn = duckdb.connect(temp_db_path)
    
    # Initialize schema
    from dynamodb_optima.database.schema import initialize_database
    initialize_database(conn)
    
    yield conn
    
    conn.close()


@pytest.fixture
def mock_settings():
    """Mock settings configuration."""
    settings = Mock()
    settings.aws_region = "us-east-1"
    settings.aws_regions = ["us-east-1", "us-west-2"]
    settings.database_url = "duckdb:///:memory:"
    settings.log_level = "INFO"
    settings.debug = False
    settings.use_organizations = False
    settings.organizations_role_name = "MetricsCollectorRole"
    settings.cur_database_name = "test_cur_db"
    settings.cur_table_name = "test_cur_table"
    settings.pricing_api_region = "us-east-1"
    settings.pricing_refresh_days = 30
    settings.capacity_analysis_days = 14
    settings.autoscaling_target_utilization = 0.7
    settings.autoscaling_scale_out_cooldown = 2
    settings.autoscaling_scale_in_cooldown = 15
    settings.autoscaling_min_capacity = 1
    settings.autoscaling_max_capacity = 40000
    settings.table_class_analysis_months = 1
    settings.utilization_threshold = 0.4
    settings.confidence_threshold = 0.8
    settings.min_savings_threshold_usd = 10.0
    return settings


@pytest.fixture
def mock_aws_client():
    """Mock AWS client for testing."""
    client = MagicMock()
    return client


@pytest.fixture
def mock_dynamodb_client():
    """Mock DynamoDB client."""
    client = MagicMock()
    
    # Mock table description
    client.describe_table.return_value = {
        "Table": {
            "TableName": "test-table",
            "TableArn": "arn:aws:dynamodb:us-east-1:123456789012:table/test-table",
            "TableStatus": "ACTIVE",
            "CreationDateTime": datetime(2024, 1, 1),
            "BillingModeSummary": {"BillingMode": "PAY_PER_REQUEST"},
            "TableClassSummary": {"TableClass": "STANDARD"},
            "TableSizeBytes": 1024000,
            "ItemCount": 1000,
        }
    }
    
    # Mock list tables
    client.list_tables.return_value = {
        "TableNames": ["test-table", "test-table-2"]
    }
    
    return client


@pytest.fixture
def mock_cloudwatch_client():
    """Mock CloudWatch client."""
    client = MagicMock()
    
    # Mock metric statistics
    client.get_metric_statistics.return_value = {
        "Datapoints": [
            {
                "Timestamp": datetime.now() - timedelta(minutes=i),
                "Sum": 100.0 * (1 + i % 10),
                "Unit": "Count",
            }
            for i in range(60)
        ]
    }
    
    return client


@pytest.fixture
def mock_organizations_client():
    """Mock AWS Organizations client."""
    client = MagicMock()
    
    # Mock list accounts
    client.list_accounts.return_value = {
        "Accounts": [
            {
                "Id": "123456789012",
                "Name": "Management Account",
                "Email": "management@example.com",
                "Status": "ACTIVE",
                "JoinedMethod": "CREATED",
                "JoinedTimestamp": datetime(2020, 1, 1),
            },
            {
                "Id": "123456789013",
                "Name": "Member Account 1",
                "Email": "member1@example.com",
                "Status": "ACTIVE",
                "JoinedMethod": "INVITED",
                "JoinedTimestamp": datetime(2021, 1, 1),
            },
        ]
    }
    
    return client


@pytest.fixture
def mock_athena_client():
    """Mock Athena client for CUR queries."""
    client = MagicMock()
    
    # Mock start query execution
    client.start_query_execution.return_value = {
        "QueryExecutionId": "test-query-id-123"
    }
    
    # Mock get query execution (successful)
    client.get_query_execution.return_value = {
        "QueryExecution": {
            "QueryExecutionId": "test-query-id-123",
            "Status": {"State": "SUCCEEDED"},
            "ResultConfiguration": {
                "OutputLocation": "s3://test-bucket/results/"
            },
        }
    }
    
    # Mock get query results
    client.get_query_results.return_value = {
        "ResultSet": {
            "Rows": [
                # Header row
                {"Data": [{"VarCharValue": "table_name"}, {"VarCharValue": "storage_cost"}]},
                # Data rows
                {"Data": [{"VarCharValue": "test-table"}, {"VarCharValue": "100.50"}]},
            ]
        }
    }
    
    return client


@pytest.fixture
def mock_pricing_client():
    """Mock AWS Pricing client."""
    client = MagicMock()
    
    # Mock get products (DynamoDB pricing)
    client.get_products.return_value = {
        "PriceList": [
            '{"product": {"attributes": {"location": "US East (N. Virginia)", "group": "DDB-ReadUnits"}}, "terms": {"OnDemand": {"JRTCKXETXF.JRTCKXETXF": {"priceDimensions": {"JRTCKXETXF.JRTCKXETXF.6YS6EN2CT7": {"pricePerUnit": {"USD": "0.00000025"}}}}}}}'
        ]
    }
    
    return client


@pytest.fixture
def sample_table_data():
    """Sample DynamoDB table data for testing."""
    return {
        "account_id": "123456789012",
        "region": "us-east-1",
        "table_name": "test-table",
        "table_arn": "arn:aws:dynamodb:us-east-1:123456789012:table/test-table",
        "table_status": "ACTIVE",
        "billing_mode": "PAY_PER_REQUEST",
        "table_class": "STANDARD",
        "table_size_bytes": 1024000,
        "item_count": 1000,
        "discovered_at": datetime.now(),
    }


@pytest.fixture
def sample_metrics_data():
    """Sample CloudWatch metrics data for testing."""
    now = datetime.now()
    return [
        {
            "account_id": "123456789012",
            "region": "us-east-1",
            "table_name": "test-table",
            "metric_name": "ConsumedReadCapacityUnits",
            "timestamp": now - timedelta(minutes=i),
            "value": 100.0 + (i % 10) * 10,
            "unit": "Count",
            "statistic": "Sum",
        }
        for i in range(60)
    ]


@pytest.fixture
def sample_pricing_data():
    """Sample pricing data for testing."""
    return [
        {
            "region": "us-east-1",
            "pricing_type": "on_demand_read",
            "price_per_unit": 0.00000025,
            "unit": "per_RRU",
            "collected_at": datetime.now(),
        },
        {
            "region": "us-east-1",
            "pricing_type": "on_demand_write",
            "price_per_unit": 0.00000125,
            "unit": "per_WRU",
            "collected_at": datetime.now(),
        },
        {
            "region": "us-east-1",
            "pricing_type": "storage_standard",
            "price_per_unit": 0.25,
            "unit": "per_GB_month",
            "collected_at": datetime.now(),
        },
        {
            "region": "us-east-1",
            "pricing_type": "storage_ia",
            "price_per_unit": 0.10,
            "unit": "per_GB_month",
            "collected_at": datetime.now(),
        },
    ]


@pytest.fixture
def mock_logger():
    """Mock logger for testing."""
    logger = MagicMock()
    return logger


# Pytest configuration
def pytest_configure(config):
    """Configure pytest with custom markers."""
    config.addinivalue_line("markers", "unit: Unit tests")
    config.addinivalue_line("markers", "integration: Integration tests")
    config.addinivalue_line("markers", "aws: Tests requiring AWS API mocking")
    config.addinivalue_line("markers", "slow: Slow running tests")
