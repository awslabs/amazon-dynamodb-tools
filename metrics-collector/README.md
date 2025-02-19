# DynamoDB Metrics Collector

## 📊 Uncover Hidden Insights in Your DynamoDB Tables

Are your DynamoDB tables optimized for performance and cost? The DynamoDB Metrics Collector is here to help you find out! This powerful tool scans your AWS regions, identifies provisioned DynamoDB tables, and provides detailed utilization metrics to help you optimize your database infrastructure.

![DynamoDB Metrics Collector Demo](./documentation/metrics.gif)

## 🚀 Features

- 🌎 **Multi-Region Support**: Scans all your AWS regions automatically
- 🔍 **Smart Table Detection**: Identifies provisioned DynamoDB tables
- 📈 **Comprehensive Metrics**: Collects read and write utilization data
- 💡 **Utilization Insights**: Highlights tables with low utilization (below 45%)
- 📊 **CSV Exports**: Generates easy-to-analyze CSV reports

## 🛠 Installation

This project is built with Poetry for dependency management. To get started:

1. Clone the repository:

`git clone https://github.com/awslabs/amazon-dynamodb-tools.git cd metrics-collector`


2. Install dependencies with Poetry:
`poetry install`

3. Alternatively, you can use pip with the provided `requirements.txt`:
`pip install -r requirements.txt`

The install might take a couple of minutes because of the dependencies. 

## 🏃‍♂️ Usage

Run the metrics collector with a single command:

```bash
python -m metrics_collector.utilization_example --start-time 2025-02-19

Options:

--start-time: Specify the start time for metric collection (ISO8601 format)
--end-time: Specify the end time (defaults to current time if not provided)
--config: Path to a custom configuration file
--output: Custom name for the output CSV file
```

## 📊 Output

The tool generates two CSV files:

```bash
dynamodb_utilization_YYYYMMDD_HHMMSS.csv: Lists tables with utilization below 45%
dynamodb_raw_metrics_YYYYMMDD_HHMMSS.csv: Contains raw metric data for all tables
```

### 🖥 Sample Output

```
❯ pip install -r requirements.txt
Collecting aioboto3==13.4.0
  Downloading aioboto3-13.4.0-py3-none-any.whl (34 kB)
Collecting aiobotocore==2.18.0
  Downloading aiobotocore-2.18.0-py3-none-any.whl (77 kB)
     ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 77.6/77.6 kB 3.5 MB/s eta 0:00:00
...
Successfully installed aioboto3-13.4.0 aiobotocore-2.18.0 aiofiles-24.1.0 aiohappyeyeballs-2.4.6 aiohttp-3.11.12 aioitertools-0.12.0 aiosignal-1.3.2 async-timeout-5.0.1 asyncio-3.4.3 attrs-25.1.0 boto3-1.36.1 botocore-1.36.1 colorama-0.4.6 frozenlist-1.5.0 idna-3.10 multidict-6.1.0 propcache-0.2.1 python-dateutil-2.9.0.post0 s3transfer-0.11.2 simpleeval-1.0.3 six-1.17.0 tqdm-4.67.1 typing-extensions-4.12.2 urllib3-2.3.0 wrapt-1.17.2 yarl-1.18.3

❯ python -m metrics_collector.utilization_example --start-time 2025-02-19
2025-02-19T15:40:04.456749 - INFO - Initializing DynamoDBMetricsCollector
2025-02-19T15:40:04.456851 - INFO - Collecting metrics from 2025-02-19 00:00:00+00:00 to 2025-02-19 23:59:59.999999+00:00
2025-02-19T15:40:04.456873 - INFO - Fetching all AWS regions...
2025-02-19T15:40:05.461387 - INFO - Found 17 regions.
2025-02-19T15:40:05.461463 - INFO - Identifying provisioned tables in each region...
Scanning regions: 100%|████████████████████████████████████████████████████████████████████████████████████████████████████████████████| 17/17 [00:13<00:00,  1.23it/s]
2025-02-19T15:40:19.284727 - INFO - Found 14 provisioned tables across all regions.
2025-02-19T15:40:19.285119 - INFO - Collecting metrics for provisioned tables...
Collecting metrics: 100%|██████████████████████████████████████████████████████████████████████████████████████████████████████████████| 14/14 [00:04<00:00,  3.46it/s]
2025-02-19T15:40:23.335240 - INFO - Metrics collected and stored successfully.
2025-02-19T15:40:23.335333 - INFO - Found 13 tables with utilization below 45%
2025-02-19T15:40:23.335774 - INFO - Tables with low utilization are written to dynamodb_utilization_20250219_154023.csv
2025-02-19T15:40:23.349258 - INFO - Raw metrics data written to dynamodb_raw_metrics_20250219_154023.csv
2025-02-19T15:40:23.361457 - INFO - Raw metrics data written to dynamodb_raw_metrics_20250219_154023.csv
```

## 🔧 Configuration

The metrics collection is driven by a configuration file named `metrics_config.json`. This file allows you to customize which metrics are collected and how they are calculated.

### Sample `metrics_config.json`:

```json
{
  "metrics": [
    {
      "Id": "consumed_read",
      "MetricName": "ConsumedReadCapacityUnits",
      "Stat": "Sum"
    },
    {
      "Id": "provisioned_read",
      "MetricName": "ProvisionedReadCapacityUnits",
      "Stat": "Average"
    },
    {
      "Id": "consumed_write",
      "MetricName": "ConsumedWriteCapacityUnits",
      "Stat": "Sum"
    },
    {
      "Id": "provisioned_write",
      "MetricName": "ProvisionedWriteCapacityUnits",
      "Stat": "Average"
    }
  ],
  "period": 300,
  "calculations": [
    {
      "id": "read_utilization",
      "formula": "(consumed_read / period) / provisioned_read",
      "required_vars": ["consumed_read", "provisioned_read"]
    },
    {
      "id": "write_utilization",
      "formula": "(consumed_write / period) / provisioned_write",
      "required_vars": ["consumed_write", "provisioned_write"]
    }
  ]
}
```

### Configuration Breakdown:

1. **metrics**: An array of metrics to collect from CloudWatch.
   - `Id`: A unique identifier for the metric.
   - `MetricName`: The name of the CloudWatch metric.
   - `Stat`: The statistic to retrieve (e.g., "Sum", "Average").

2. **period**: The time period (in seconds) for each data point.

3. **calculations**: An array of custom calculations to perform on the collected metrics.
   - `id`: A unique identifier for the calculation.
   - `formula`: The mathematical formula to calculate the metric.
   - `required_vars`: Variables required for the calculation.

### Customizing Metrics

You can modify this file to collect different metrics or perform custom calculations:

1. To add a new metric:
   - Append to the `metrics` array with the appropriate CloudWatch metric details.

2. To create a new calculation:
   - Add to the `calculations` array with your custom formula.

This configuration flexibility allows you to tailor the metrics collection to your specific needs and focus on the DynamoDB performance aspects most relevant to your use case.


## 📈 Visualize Your Data

There is a companion project in the making where we will simplify metric visualization. Stay tuned for future updates!

Want to turn your CSV data into stunning visualizations? Check out our companion project DynamoDB Metrics Visualizer to create interactive dashboards and charts! 

The current report will present data in csv 

| Region | Table Name | Read Utilization | Write Utilization |
|--------|------------|------------------|-------------------|
| us-east-1 | Table-Acccount-A | 0.00 | 0.00 |
| us-east-1 | my_handler_table | 0.00 | 0.00 |
| us-east-1 | my_table | 0.00 | 0.00 |
| us-east-1 | vpc-test-table-01 | 0.00 | 0.06 |
| us-east-1 | vpc-test-table-02 | 0.00 | 0.12 |
| us-east-1 | vpc-test-table-03 | 0.00 | 0.18 |
| us-east-1 | vpc-test-table-04 | 0.00 | 0.24 |
| us-east-1 | vpc-test-table-05 | 0.00 | 0.30 |
| us-east-1 | vpc-test-table-06 | 0.00 | 0.36 |
| us-east-1 | vpc-test-table-07 | 0.00 | 0.41 |
| us-east-1 | vpc-test-table-09 | 0.00 | 0.00 |
| us-east-1 | vpc-test-table-10 | 0.00 | 0.00 |
| ap-southeast-2 | ddbeventstable-StreamsSampleDDBTable-5W08OVKQE1PN | 0.00 | 0.00 |

## Compatibility

This project has been tested with:

- Python 3.10.6
- Python 3.13.1

## 🤝 Contributing

We welcome contributions! Please see our Contributing Guide for more details.

## 📜 License

This project is licensed under the MIT License - see the LICENSE file for details.

Built with ❤️ by DynamoDB Specialist Solutions Architects. 