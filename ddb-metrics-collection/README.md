# DynamoDB Metrics Collector

## 📊 Uncover Hidden Insights in Your DynamoDB Tables

Are your DynamoDB tables optimized for performance and cost? The DynamoDB Metrics Collector is here to help you find out! This powerful tool scans your AWS regions, identifies provisioned DynamoDB tables, and provides detailed utilization metrics to help you optimize your database infrastructure.

![DynamoDB Metrics Collector Demo](link_to_demo_gif_or_image)

## 🚀 Features

- 🌎 **Multi-Region Support**: Scans all your AWS regions automatically
- 🔍 **Smart Table Detection**: Identifies provisioned DynamoDB tables
- 📈 **Comprehensive Metrics**: Collects read and write utilization data
- 💡 **Utilization Insights**: Highlights tables with low utilization (below 45%)
- 📊 **CSV Exports**: Generates easy-to-analyze CSV reports

## 🛠 Installation

This project is built with Poetry for dependency management. To get started:

1. Clone the repository:

`git clone https://github.com/awslabs/amazon-dynamodb-tools.git cd ddb-metrics-collection`


2. Install dependencies with Poetry:
poetry install


Alternatively, you can use pip with the provided `requirements.txt`:
pip install -r requirements.txt


## 🏃‍♂️ Usage

Run the metrics collection with a single command:

```bash
poetry run python ddb_metrics_collection/utilization_example.py --start-time 2025-02-19

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
Collecting metrics from 2025-02-19 00:00:00+00:00 to 2025-02-19 23:59:59.999999+00:00
Fetching all AWS regions...
Found 17 regions.
Identifying provisioned tables in each region...
Scanning regions: 100%|██████████████████| 17/17 [00:12<00:00,  1.33it/s]
Found 14 provisioned tables across all regions.
Collecting metrics for provisioned tables...
Collecting metrics: 100%|██████████████████| 14/14 [00:03<00:00,  3.76it/s]
Metrics collected and stored successfully.

Found 11 tables with utilization below 45%
Tables with low utilization are written to dynamodb_utilization_20250219_102746.csv
Raw metrics data written to dynamodb_raw_metrics_20250219_102746.csv
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

Want to turn your CSV data into stunning visualizations? Check out our companion project DynamoDB Metrics Visualizer to create interactive dashboards and charts!

## 🤝 Contributing

We welcome contributions! Please see our Contributing Guide for more details.

## 📜 License

This project is licensed under the MIT License - see the LICENSE file for details.

Built with ❤️ by DynamoDB Specialist Solutions Architects. 