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
