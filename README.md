# Amazon DynamoDB Tools

 These tools are intended to make using Amazon DynamoDB effectively easier. The following tools are available:

 * [DynamoDB reserved capacity recommendations](reco) - Generate reserved capacity purchase recommendations using existing AWS Cost and Usage Reports data
 * [Cost Template](#cost-template) - Model read, write, and storage costs for a DynamoDB table in Excel
 * [MySQL to S3 Migrator](#mysql-to-s3-migrator) - Bring your relational data into Amazon S3 to prepare for a DynamoDB migration
 * [Table Class Evaluator](#table-class-evaluator-tool) - Recommend Amazon DynamoDB table class changes to optimize costs
 * [Eponymous Table Tagger](#eponymous-table-tagger-tool)  - Tag tables with their own name to make per-table cost analysis easier

While we make efforts to test and verify the functionality of these tools, you are encouraged to read and understand the code, and use them at your own risk.


## DynamoDB reserved capacity recommendations

[See the separate README](reco)

## Cost Template
Before creating a new DynamoDB table, you may want to estimate 
what its core costs will be, measured not in capacity units but dollars.
Or, you may have a table in On Demand mode and be wondering if Provisioned Capacity would be cheaper.

[DynamoDB+Cost+Template.xlsx](Excel/DynamoDB+Cost+Template.xlsx) 

This worksheet will help you estimate a table's cost of ownership, for a given time period.
Both On Demand and Provisioned Capacity costs are shown side by side, along with storage costs. 
While Provisioned Capacity is generally less expensive, it is unrealistic to assume 
you will ever be 100% efficient in using the capacity you pay for. 
Even if using Auto Scaling, overhead is required to account for bumps and spikes in traffic.
Achieving 50% efficiency is good, but very spiky traffic patterns 
may use less than 15%.  In these scenarios, On Demand mode will be less expensive.
You may adjust the efficiency level and other model parameters via the green cells in column C.


![Cost Template Screenshot](https://dynamodb-images.s3.amazonaws.com/img/pricing_template_screenshot_sm.jpg "DynamoDB Cost Template")

For specific jobs, such as a large data import, you may want to know just the write costs.
Imagine a job that performs 2500 writes per second and takes three hours. You can adjust 
the time period in C9 and C10 and WCU per second velocity in C17 to show the write costs 
for a specific workload like this.

An existing table in DynamoDB can be promoted to a Global Table by adding a new region to the table.
For a two-region Global Table, storage costs will double while write costs approximately triple.
These prices will be modeled by choosing a Global Table in cell C12.

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
before writing to S3.  If a row has any NULL values, the tool will skip the column
altogether and only write non-null columns to the JSON document.

There is no additional shaping, modeling or formatting done.
However, a DynamoDB solution can may require data in certain formats to optimize
for expected access patterns.  When customizing the SQL statement the tool runs,
take advantage of the SQL language to craft an optimal data set for your DynamoDB table.

Your relational application likely uses many tables.
The NoSQL "single table design" philosophy says that combining multiple data sets
into a single table is valuable.  Done right, item collections will emerge from the data,
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
 * Python 3
 * Amazon S3 bucket with write permissions
 * [AWS SDK for Python](https://aws.amazon.com/sdk-for-python/)
 * [MySQL Connector/Python](https://dev.mysql.com/doc/connector-python/en/)


### Using the Migrator

1. Open [ddbtools/mysql_s3.py](./ddbtools/mysql_s3.py)

2. Update the hostname, credentials, target bucket, path, region, and sql statement you want to run.

3. Run  ```python3 mysql_s3.py```

Expected output:
```
HTTP 200 for object s3://s3-export-demo/demo/data_upto_5.json
HTTP 200 for object s3://s3-export-demo/demo/data_upto_10.json
HTTP 200 for object s3://s3-export-demo/demo/data_upto_15.json
...
```


## Table Class Evaluator Tool

### Overview
Amazon DynamoDB supports two [table classes](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/HowItWorks.TableClasses.html):
* Standard: The default for new tables, this table class balances storage costs and provisioned throughput.  

* Standard Infrequent Access (Standard-IA): This table class offers lower storage pricing and  higher throughput pricing comapred to the Standard table class. The Standard-IA table class is a good fit for tables where data is not queried frequently, and can be a good choice for tables using the Standard table class where storage costs exceed 50% of total throughput costs.

The Table Class Evaluator tool evaluates one or more tables in an AWS region for suitability for the Infrequent Access table class. The tool accomplishes this by calculating costs for both table classes for the following cost dimensions:

* AWS Region
* Table storage utilization
* Instantaneous provisioned throughput
* Global Tables replicated writes
* Global Secondary Indexes (GSIs)

The tool will will return recommendations for tables that may benefit from a change in table class.


### Limitations

The Table Class Evaluator tool has the following limitations:

* Estimated costs are calculated from the current (instantaneous) provisioned throughput. If the provisioned capacity of the table being evaluated changes frequently due to Auto Scaling activity, the recommendation could be incorrect.
* Tables using On-Demand pricing are not supported.
* Local Secondary Index costs are not calculated.


### Using the Table Class Evaluator tool
The Table Class Evaluator is a command-line tool written in Python 3, and requires the AWS Python SDK (Boto3) >= 1.23.18. You can find instructions for installing the AWS Python SDK at  https://aws.amazon.com/sdk-for-python/. The tool can be run directly from the cloned repository without installation.

The tool is invoked from the command line like so:
```console
user@host$ python3 table_class_evaluator.py --help
usage: table_class_evaluator.py [-h] [--estimates-only] [--region REGION] [--table-name TABLE_NAME]

Recommend Amazon DynamoDB table class changes to optimize costs.

optional arguments:
  -h, --help            show this help message and exit
  --estimates-only      print table cost estimates instead of change recommendations
  --region REGION       evaluate tables in REGION (default: us-east-1)
  --table-name TABLE_NAME
                        evaluate TABLE_NAME (defaults to all tables in region)
```

With no arguments, the tool will evaluate costs for all tables in the default region (us-east-1), and returns a list of JSON objects, each containing details for a change recommendation:
```console
user@host$ python3 table_class_evaluator.py
[{
    "recommendation_type": "CHANGE_TABLE_CLASS",
    "recommended_table_class": "STANDARD_INFREQUENT_ACCESS",
    "estimated_monthly_savings": "1.35",
    "estimate_detail": {
        "table_name": "test",
        "pricing_data": {
        "billing_mode": "PROVISIONED",
        "size_in_gb": "13.61",
        "provisioned_rcus": 5,
        "provisioned_wcus": 5,
        "table_arn": "arn:aws:dynamodb:us-east-1:123456789012:table/test",
        "table_class": "STANDARD"
        },
        "table_mo_costs": {
        "std_storage_cost": "3.40",
        "std_mo_rcu_cost": "0.47",
        "std_mo_wcu_cost": "2.37",
        "std_mo_total_cost": "6.25",
        "ia_mo_storage_cost": "1.36",
        "ia_mo_rcu_cost": "0.58",
        "ia_mo_wcu_cost": "2.96",
        "ia_mo_total_cost": "4.90",
        "total_std_mo_costs": "6.25",
        "total_ia_mo_costs": "4.90"
        }
    }
}]
```

If cost calculations don't reveal any change recommendations, the tool returns an empty list:
```console
user@host$ python3 table_class_evaluator.py
[]
```


## Eponymous Table Tagger Tool

### Overview
[AWS Cost Explorer](https://aws.amazon.com/aws-cost-management/aws-cost-explorer/) will group all Amazon DynamoDB table cost categories in a region by default. In order to view table-level cost breakdowns in Cost Explorer (for example, storage costs for a specific table), tables must be tagged so costs can be grouped by that tag. Tagging each DynamoDB table with its own name enables this table-level cost analysis. This tool automatically tags each table in a region with its own name, if it is not already thus tagged.


### Using the Eponymous Table Tagger tool
The Eponymous Table Tagger is a command-line tool written in Python 3, and requires the AWS Python SDK (Boto3). The tool can be run directly from the cloned repository without installation.

The tool is invoked from the command line like so:
```console
user@host$ python3 table_tagger.py --help
usage: table_tagger.py [-h] [--dry-run] [--region REGION] [--table-name TABLE_NAME] [--tag-name TAG_NAME]

Tag all DynamoDB tables in a region with their own name.

optional arguments:
  -h, --help            show this help message and exit
  --dry-run             output results but do not actually tag tables
  --region REGION       tag tables in REGION (default: us-east-1)
  --table-name TABLE_NAME
                        tag only TABLE_NAME (defaults to all tables in region)
  --tag-name TAG_NAME   tag table with tag TAG_NAME (default is "table_name")
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

## Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

## License

This project is licensed under the Apache-2.0 License.
