# Amazon DynamoDB Tools

A collection of tools and utilities for working with Amazon DynamoDB.

## Repository Structure

- **[/tools](tools/)** - Our main tools
- **[/beta](beta/)** - Experimental tools and tutorials  
- **[/snippets](snippets/)** - Simple utilities
- **[/archived](archived/)** - Deprecated tools

## Available Tools

### Main Tools

- [Bulk Executor](tools/bulk_executor/README.md) - Serverless Glue-based solution for bulk DynamoDB operations
- [DAX Calculator](tools/dax_calculator/README.md) - DAX cluster sizing and cost estimation
- **[DynamoDB Optima](tools/dynamodb-optima/README.md)** - **[NEW]** Unified cost optimization and analysis platform
  - Multi-account discovery via AWS Organizations
  - CloudWatch metrics collection with incremental updates
  - CUR-based cost analysis  
  - Capacity mode optimization (On-Demand vs Provisioned) with autoscaling simulation
  - Table class optimization (Standard vs Standard-IA)
  - Utilization analysis
  - Integrated Streamlit GUI for visualization
  - CLI: `dynamodb-optima`

### Other Tools

- [Item Size Calculator](#item-size-calculator) - NPM package for calculating DynamoDB item sizes

While we make efforts to test and verify the functionality of these tools, you are encouraged to read and understand the code, and use them at your own risk.

Each tool has been developed independent from one another, please make sure to read the installation requirements for each one of them.


#### Archived Tools

The [DDB Cost Tool](archived/ddb_cost_tool/README.MD) and other deprecated tools have been moved to the [archived/](archived/) directory. For more details, migration guides, and alternatives, see the README files in each archived tool's directory.


**Three tools were consolidated** into the new [DynamoDB Optima](tools/dynamodb-optima/) platform in February 2026:
- metrics-collector → Now: `dynamodb-optima collect` CLI
- capacity-mode-evaluator → Now: `dynamodb-optima analyze-capacity`
- table_class_optimizer → Now: `dynamodb-optima analyze-table-class`


## Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

## License

This project is licensed under the Apache-2.0 License.
