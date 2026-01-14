# ddbtools - ARCHIVED

**Status:** Deprecated and Archived  
**Date Archived:** January 2026  
**Reason:** Only used by archived tools

## What was ddbtools?

A Python utility library that provided helper functions for DynamoDB operations. It included:
- `TableUtility` - DynamoDB table operations (describe, list, tagging)
- `PricingUtility` - Cost calculations
- `DecimalEncoder` - JSON serialization for DynamoDB Decimal types
- Constants and utilities

## Why was it archived?

This library was only used by `table_tagger.py` (see `archived/table_tagger/`), which has also been archived. With no other consumers, there's no reason to maintain it.

## Need similar functionality?

Use `boto3` (the official AWS SDK for Python) directly - it provides comprehensive DynamoDB APIs with better support and documentation.

## Related

- `archived/table_tagger/` - The primary (and only active) consumer of this library
- [#114](https://github.com/awslabs/amazon-dynamodb-tools/issues/114) - Archive deprecated utilities

## Original mysql_s3 documentation
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
