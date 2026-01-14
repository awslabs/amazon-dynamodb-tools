# table_tagger - ARCHIVED

**Status:** Deprecated and Archived  
**Date Archived:** January 2026  
**Reason:** Superseded by better alternatives

## What was table_tagger?

A Python utility that automatically tagged DynamoDB tables with their own table names. This was useful for cost analysis and AWS Cost Explorer filtering.

## Why was it archived?

1. **AWS Cost Explorer Improvements**: Modern AWS Cost Explorer provides native, sophisticated filtering capabilities by resource tags
2. **Better Approaches Available**: Tags should be added during table creation (via IaC tools like CloudFormation, CDK, Terraform)
3. **Limited Use Case**: Most organizations now follow tag-on-create practices
4. **Known Issues**: Rate limiting problems (#32) were never fully resolved

## Migration Guide

### Alternative 1: Tag During Table Creation (Recommended)

**CloudFormation:**
```yaml
MyTable:
  Type: AWS::DynamoDB::Table
  Properties:
    TableName: my-table
    Tags:
      - Key: table-name
        Value: my-table
      - Key: cost-center
        Value: "4414"
```

**AWS CDK (Python):**
```python
from aws_cdk import aws_dynamodb as dynamodb
from aws_cdk import Tags

table = dynamodb.Table(self, "MyTable",
    table_name="my-table",
    partition_key=dynamodb.Attribute(
        name="id",
        type=dynamodb.AttributeType.STRING
    )
)

Tags.of(table).add("table-name", "my-table")
Tags.of(table).add("cost-center", "4414")
```


### Alternative 2: AWS CLI for Existing Tables

For existing tables, use AWS CLI:

```bash
# Tag a single table
aws dynamodb tag-resource \
  --resource-arn arn:aws:dynamodb:us-east-1:123456789012:table/my-table \
  --tags Key=table-name,Value=my-table Key=cost-center,Value=4414

# Script to tag multiple tables
for table in $(aws dynamodb list-tables --query 'TableNames[]' --output text); do
  arn=$(aws dynamodb describe-table --table-name "$table" \
    --query 'Table.TableArn' --output text)
  aws dynamodb tag-resource --resource-arn "$arn" \
    --tags Key=table-name,Value="$table" Key=cost-center,Value=4414
  echo "Tagged: $table"
done
```

## Original Documentation

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

## Original Dependencies

If you need to reference the original code, it depended on:
- `ddbtools` (also archived) - utility library for DynamoDB operations

## Related Issues

- [#32](https://github.com/awslabs/amazon-dynamodb-tools/issues/32): Rate limiting problems with table tagging
- [#114](https://github.com/awslabs/amazon-dynamodb-tools/issues/114): Archive deprecated utilities (this archival)

## Last Known Version

The final working version is preserved in this archived state. The code is no longer maintained or supported.

## Questions?

For questions about alternatives or migration, please open a new issue in the main repository.
