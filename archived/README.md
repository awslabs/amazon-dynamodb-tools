# 🗃️ Archived DynamoDB Tools and Code

This folder contains deprecated tools or code that are no longer actively supported. These resources are kept for historical reference, but should be used with caution as they may not reflect the latest best practices or be compatible with current AWS services and SDKs.

## 🕰️ Blast from the Past

The artifacts stored in this folder represent earlier stages of the Amazon DynamoDB ecosystem. While these samples may no longer be the preferred approach, they can still offer valuable insights into the evolution of DynamoDB and cloud-native development.

## 🔍 Explore with Caution

Before utilizing any of the code or configurations in this folder, please be aware that they may:

    - Rely on outdated AWS services or SDK versions
    - Contain security vulnerabilities or antipatterns
    - Lack comprehensive documentation and support

## 🌱 Nurturing the Future

While these archived resources are preserved for reference, we encourage you to focus your efforts on the actively maintained examples and solutions in the rest of this DynamoDB repository. The community is continuously working to expand and improve the available DynamoDB content to better serve your needs.


## Archived resources.

- October 2024: [Python script to help choose the right DynamoDB table class](./table_class_evaluator/README.md)
- January 2026: [Archive ddbtools, reco, and table_tagger.py](https://github.com/awslabs/amazon-dynamodb-tools/issues/114)
- January 2026: [DynamoDB Cost Tool](./ddb_cost_tool/README.MD) - Tool is not functional due to closed, deprecated webservice

## Recently Archived (January 2026) - Consolidated into metrics-collector-v2

The following tools have been **merged into a unified platform** at `/metrics-collector-v2/`:

- **[metrics-collector](./metrics-collector/README.md)** - CloudWatch metrics collection and utilization analysis
  - Replaced by: `metrics-collector` commands in the new platform
  - Migration: Compatible database schema, use new CLI commands

- **[capacity-mode-evaluator](./capacity-mode-evaluator/README.md)** - On-Demand vs Provisioned capacity mode optimization
  - Replaced by: `metrics-collector analyze-capacity` command
  - Migration: Same autoscaling simulation logic, enhanced implementation

- **[table_class_optimizer](./table_class_optimizer/README.md)** - Standard vs Standard-IA table class recommendations
  - Replaced by: `metrics-collector analyze-table-class` command
  - Migration: No more Athena queries needed - DuckDB reads Parquet files directly

**Why consolidated?** These three tools had overlapping functionality (metrics collection, cost analysis, recommendations). The new unified platform provides:
- ✅ Single installation and configuration
- ✅ Shared database for all analysis types
- ✅ Multi-account AWS Organizations support
- ✅ Integrated Streamlit GUI
- ✅ Comprehensive recommendations in one place

See [refactor/migration-guide.md](../refactor/migration-guide.md) for migration details.

## Migrated Tools

**Note:** Some tools have been migrated to [aws-dynamodb-examples](https://github.com/aws-samples/aws-dynamodb-examples) including the Excel cost template, tester framework, and ddb-migration. See [issue 207 in the aws-samples repo for more information](https://github.com/aws-samples/aws-dynamodb-examples/issues/207).
