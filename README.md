# Amazon DynamoDB Tools

These tools are intended to make using Amazon DynamoDB effectively and easier. The following tools are available:

- [DynamoDB reserved capacity recommendations](reco) - Generate reserved capacity purchase recommendations using existing AWS Cost and Usage Reports data
- [Cost Template](#cost-template) - Model read, write, and storage costs for a DynamoDB table in Excel
- [MySQL to S3 Migrator](#mysql-to-s3-migrator) - Bring your relational data into Amazon S3 to prepare for a DynamoDB migration
- [Table Class Optimizer](#table-class-optimizer) [2024] - Recommend Amazon DynamoDB table class changes to optimize costs
- [Eponymous Table Tagger](#eponymous-table-tagger-tool) - Tag tables with their own name to make per-table cost analysis easier
- [Table Capacity Mode Evaluator](capacity-mode-evaluator) - Generate capacity mode recommendations by analyizing DynamoDB table usage
- [DynamoDB cost optimization tool](#cost-optimization-tool) - Captures table metadata and metrics to generate cost savings recommendations.

While we make efforts to test and verify the functionality of these tools, you are encouraged to read and understand the code, and use them at your own risk.

Each tool has been developed independent from one another, please make sure to read the installation requirements for each one of them.

## DynamoDB reserved capacity recommendations

[See the separate README](reco)

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

## MySQL to S3 Migrator

When moving your SQL database to DynamoDB, you can leverage Amazon S3 as a staging area
for data. This Python script connects to your MySQL host, executes a SQL SELECT,
and writes the results to your S3 bucket.

The [DynamoDB Import from S3](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/S3DataImport.HowItWorks.html)
feature can then automatically load your DynamoDB table.

### Shaping Data in SQL

The tool is simple, it converts your relational dataset into standard [DynamoDB JSON format](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Programming.LowLevelAPI.html)
before writing to S3. If a row has any NULL values, the tool will skip the column
altogether and only write non-null columns to the JSON document.

There is no additional shaping, modeling or formatting done.
However, a DynamoDB solution can may require data in certain formats to optimize
for expected access patterns. When customizing the SQL statement the tool runs,
take advantage of the SQL language to craft an optimal data set for your DynamoDB table.

Your relational application likely uses many tables.
The NoSQL "single table design" philosophy says that combining multiple data sets
into a single table is valuable. Done right, item collections will emerge from the data,
optimized for fast, efficient querying.

A SQL view can do much of the work to convert relational data into this format.
The view can use either JOIN or UNION ALL to combine tables.
A JOIN can be used to denormalize, or duplicate some data so that each single row
is more complete; while UNION ALL is used to stack tables vertically into one set.
The full set of SQL expressions can be leveraged, for example to generate unique IDs,
rename columns, combine columns, duplicate columns, calculate expiration dates,
decorate data with labels, and more. The goal is to make a well formatted data set
that matches your DynamoDB table and index strategy.

### Limitations

The tool is single threaded and designed to move modest amounts of data to S3 for demonstration purposes.

### Pre-requisites:

- Python 3
- Amazon S3 bucket with write permissions
- [AWS SDK for Python](https://aws.amazon.com/sdk-for-python/)
- [MySQL Connector/Python](https://dev.mysql.com/doc/connector-python/en/)

### Using the Migrator

1. Open [ddbtools/mysql_s3.py](./ddbtools/mysql_s3.py)

2. Update the hostname, credentials, target bucket, path, region, and sql statement you want to run.

3. Run `python3 mysql_s3.py`

Expected output:

```
HTTP 200 for object s3://s3-export-demo/demo/data_upto_5.json
HTTP 200 for object s3://s3-export-demo/demo/data_upto_10.json
HTTP 200 for object s3://s3-export-demo/demo/data_upto_15.json
...
```

## Table Class Optimizer

See the [Table Class Optimizer](table_class_optimizer/README.md) [2024]. This Athena CUR query replaces the deprecated Python table class evaluator. 

## Eponymous Table Tagger Tool

### Overview

[AWS Cost Explorer](https://aws.amazon.com/aws-cost-management/aws-cost-explorer/) will group all Amazon DynamoDB table cost categories in a region by default. In order to view table-level cost breakdowns in Cost Explorer (for example, storage costs for a specific table), tables must be tagged so costs can be grouped by that tag. Tagging each DynamoDB table with its own name enables this table-level cost analysis. This tool automatically tags each table in a region with its own name, if it is not already thus tagged.

### Using the Eponymous Table Tagger tool

The Eponymous Table Tagger is a command-line tool written in Python 3, and requires the AWS Python SDK (Boto3). The tool can be run directly from the cloned repository without installation.

The tool is invoked from the command line like so:

```console
user@host$ python3 table_tagger.py --help
usage: table_tagger.py [-h] [--dry-run] [--region REGION] [--table-name TABLE_NAME] [--tag-name TAG_NAME] [--profile PROFILE]

Tag all DynamoDB tables in a region with their own name.

optional arguments:
  -h, --help            show this help message and exit
  --dry-run             output results but do not actually tag tables
  --region REGION       tag tables in REGION (default: us-east-1)
  --table-name TABLE_NAME
                        tag only TABLE_NAME (defaults to all tables in region)
  --tag-name TAG_NAME   tag table with tag TAG_NAME (default is "table_name")
  --profile PROFILE     set a custom profile name to perform the operation under
```

With no arguments, the tool will tag each table in the default region (us-east-1) that is not already correctly tagged, and returns a list of JSON objects, each containing applied tag and table details:

```console
user@host$ python3 table_tagger.py
[
 {
    "table_arn": "arn:aws:dynamodb:us-east-1:123456789012:table/customers",
    "tag_key": "table_name",
    "tag_value": "customers"
  },
  {
    "table_arn": "arn:aws:dynamodb:us-east-1:123456789012:table/datasource",
    "tag_key": "table_name",
    "tag_value": "datasource"
  },
]
```

You can choose to run the tool in a different region, and use a different tag name than the default:

```console
user@host$ python3 table_tagger.py --region us-east-2 --tag-name dynamodb_table
[
 {
    "table_arn": "arn:aws:dynamodb:us-east-2:123456789012:table/moviefacts",
    "tag_key": "dynamodb_table",
    "tag_value": "moviefacts"
  },
  {
    "table_arn": "arn:aws:dynamodb:us-east-2:123456789012:table/topsongs",
    "tag_key": "dynamodb_table",
    "tag_value": "topsongs"
  },
]
```

If the tool does not tag any tables (usually because they are already tagged), the tool returns an empty list:

```console
user@host$ python3 table_tagger.py
[]
```

## Item Size Calculator NodeJS

### Overview

Utility tool to gain item size information for DynamoDB JSON items to understand capacity consumption and ensure items are under the 400KB DynamoDB limit.

### Using the Item Size Calculator

Please see the [README](item_size_calculator/README.md).

## Table Capacity Mode Evaluator

See the separate [README](capacity-mode-evaluator/README.md)

## Cost optimization tool

See the separate [README](ddb_cost_tool/README.MD)

## Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

## License

This project is licensed under the Apache-2.0 License.
