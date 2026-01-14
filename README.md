# Amazon DynamoDB Tools

These tools are intended to make using Amazon DynamoDB effectively and easier. The following tools are available:

- [Cost Template](#cost-template) - Model read, write, and storage costs for a DynamoDB table in Excel
- [Table Class Optimizer](#table-class-optimizer) [2025] - Provides manual and automated solutions to optimize your DynamoDB table classes
- [Table Capacity Mode Evaluator](#table-capacity-mode-evaluator) - Generate capacity mode recommendations by analyzing DynamoDB table usage
- [DynamoDB Cost Tool](#ddb-cost-tool) - Captures table metadata and metrics to generate cost savings recommendations
- [Metrics Collector](metrics-collector/README.md) - Captures Amazon DynamoDB table metrics in your account for all regions. Use this to identify over-provisioned tables
- [Item Size Calculator](#item-size-calculator-nodejs) - Utility to calculate DynamoDB item sizes

**Archived Tools:** See [Archived Tools](#archived-tools) section below for deprecated utilities.

While we make efforts to test and verify the functionality of these tools, you are encouraged to read and understand the code, and use them at your own risk.

Each tool has been developed independent from one another, please make sure to read the installation requirements for each one of them.


## Cost Template

Before creating a new DynamoDB table, you may want to estimate
what its core costs will be, measured not in capacity units but dollars.
Or, you may have a table in On Demand mode and be wondering if Provisioned Capacity would be cheaper.

[DynamoDB+Cost+Template.xlsx](Excel/DynamoDB+Cost+Template.xlsx)

This worksheet will help you estimate a table's cost of ownership, for a given time period.
The first step is to decide the table's average storage, read and write velocity levels
and then adjust the green values in cells C16-C18, and review the calculated costs in rows E, F, and G.
Both On Demand and Provisioned Capacity costs are shown side by side, along with storage costs.

![Cost Template Screenshot](https://dynamodb-images.s3.amazonaws.com/img/pricing_template_screenshot_sm_2025.jpg "DynamoDB Cost Template")

While Provisioned Capacity is generally less expensive, it is unrealistic to assume
you will ever be 100% efficient in using the capacity you pay for.
Even if using Auto Scaling, overhead is required to account for bumps and spikes in traffic.
Achieving 50% efficiency is good, but very spiky traffic patterns
may use less than 30%. In these scenarios, On Demand mode will be less expensive.
You may adjust the efficiency level and other model parameters via the green cells in column C.

For specific jobs, such as a large data import, you may want to know just the write costs.
Imagine a job that performs 2500 writes per second and takes three hours. You can adjust
the time period in C9 and C10 and WCU per second velocity in C17 to show the write costs
for a specific workload like this.

An existing table in DynamoDB can be promoted to a Global Table by adding a new region to the table.
When moving to a two-region Global Table, storage costs and write costs will double. 
Multi Region Strongly Consistent tables use three regions.
These prices will be modeled by choosing a Global Table type in cell C12.

The unit prices shown on rows 4-7 are the current list prices for a table in us-east-1.
Because prices may change in the future, you can adjust these as needed, or for a specific region.

The tool helps you model the core costs of a table,
please refer to the [DynamoDB Pricing Page](https://aws.amazon.com/dynamodb/pricing/)
for a full list of DynamoDB features, options and prices.


## Table Class Optimizer
### There are two solutions available for Table Class Optimization:
1. [Manual Query Tool](table_class_optimizer/README.md) - [2024] - An Athena CUR query that allows you to manually analyze and optimize your DynamoDB table classes.
2. [Automated Optimization System](table_class_optimizer/AUTOMATED_SOLUTION.md) - [2025] - A fully automated, serverless solution that continuously monitors and optimizes your DynamoDB table classes across all accounts and regions in your AWS organization.

The above solution replaces the deprecated Python table class evaluator. 


## Item Size Calculator NodeJS

### Overview

Utility tool to gain item size information for DynamoDB JSON items to understand capacity consumption and ensure items are under the 400KB DynamoDB limit.

### Using the Item Size Calculator

Please see the [README](item_size_calculator/README.md).

## Table Capacity Mode Evaluator

See the separate [README](capacity-mode-evaluator/README.md)

## DDB Cost Tool

See the separate [README](ddb_cost_tool/README.MD)

## Archived Tools

The following tools have been deprecated and moved to the `archived/` directory:

- **[reco](archived/reco/)** - DynamoDB reserved capacity recommendations
  - **Deprecated:** January 2026
  - **Reason:** AWS Cost Explorer now provides native reserved capacity recommendations
  - **Alternative:** AWS Console → Cost Explorer → Reservations → Recommendations

- **[table_tagger](archived/table_tagger/)** - Eponymous table tagger
  - **Deprecated:** January 2026  
  - **Reason:** Better alternatives available (tag during table creation via IaC, AWS Cost Explorer improvements)
  - **Alternative:** Tag tables during creation using CloudFormation, CDK, or Terraform

- **[ddbtools](archived/ddbtools/)** - Python utility library
  - **Deprecated:** January 2026
  - **Reason:** Only used by archived tools
  - **Alternative:** Use boto3 (AWS SDK for Python) directly

For more details, migration guides, and alternatives, see the README files in each archived tool's directory.

## Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

## License

This project is licensed under the Apache-2.0 License.
