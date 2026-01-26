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
- [Metrics Collector](tools/metrics-collector/README.md) - Captures Amazon DynamoDB table metrics in your account for all regions

### Other Tools

- [Table Class Optimizer](#table-class-optimizer) - Provides manual and automated solutions to optimize your DynamoDB table classes
- [Table Capacity Mode Evaluator](#table-capacity-mode-evaluator) - Generate capacity mode recommendations by analyzing DynamoDB table usage
- [Item Size Calculator](#item-size-calculator) - NPM package for calculating DynamoDB item sizes

While we make efforts to test and verify the functionality of these tools, you are encouraged to read and understand the code, and use them at your own risk.

Each tool has been developed independent from one another, please make sure to read the installation requirements for each one of them.


## Table Class Optimizer
### There are two solutions available for Table Class Optimization:
1. [Manual Query Tool](table_class_optimizer/README.md) - [2024] - An Athena CUR query that allows you to manually analyze and optimize your DynamoDB table classes.
2. [Automated Optimization System](table_class_optimizer/AUTOMATED_SOLUTION.md) - [2025] - A fully automated, serverless solution that continuously monitors and optimizes your DynamoDB table classes across all accounts and regions in your AWS organization.

The above solution replaces the deprecated Python table class evaluator. 


## Item Size Calculator

NPM package for calculating DynamoDB item sizes. See the [README](snippets/item_size_calculator/README.md).

## Table Capacity Mode Evaluator

See the separate [README](capacity-mode-evaluator/README.md)

## Archived Tools

The [DDB Cost Tool](archived/ddb_cost_tool/README.MD) and other deprecated tools have been moved to the [archived/](archived/) directory.


For more details, migration guides, and alternatives, see the README files in each archived tool's directory.

## Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

## License

This project is licensed under the Apache-2.0 License.
