
# Advanced Topics

← [Documentation Index](README.md) | [Main README](../README.md)

---

## Multi-Account Strategy

### Organizations Setup with CloudFormation StackSets

For deploying the MetricsCollectorRole across many accounts, use CloudFormation StackSets:

**CloudFormation Template:**

```yaml
AWSTemplateFormatVersion: '2010-09-09'
Description: 'DynamoDB Optima - Cross-Account Metrics Collector Role'

Parameters:
  ManagementAccountId:
    Type: String
    Description: 'AWS Account ID of the management account'
    AllowedPattern: '^\d{12}$'

Resources:
  MetricsCollectorRole:
    Type: AWS::IAM::Role
    Properties:
      RoleName: MetricsCollectorRole
      Description: 'Allows DynamoDB Optima in management account to collect metrics'
      AssumeRolePolicyDocument:
        Version: '2012-10-17'
        Statement:
          - Effect: Allow
            Principal:
              AWS: !Sub 'arn:aws:iam::${ManagementAccountId}:root'
            Action: 'sts:AssumeRole'
      ManagedPolicyArns: []
      Policies:
        - PolicyName: DynamoDBMetricsAccess
          PolicyDocument:
            Version: '2012-10-17'
            Statement:
              - Effect: Allow
                Action:
                  - 'dynamodb:ListTables'
                  - 'dynamodb:DescribeTable'
                  - 'dynamodb:ListTagsOfResource'
                  - 'cloudwatch:GetMetricData'
                  - 'cloudwatch:GetMetricStatistics'
                  - 'pricing:GetProducts'
                Resource: '*'
      Tags:
        - Key: Purpose
          Value: DynamoDBOptimaMetricsCollection
        - Key: ManagedBy
          Value: CloudFormation

Outputs:
  RoleArn:
    Description: 'ARN of the MetricsCollectorRole'
    Value: !GetAtt MetricsCollectorRole.Arn
    Export:
      Name: !Sub '${AWS::StackName}-RoleArn'
```

**Deploy with StackSets:**

```bash
# Create the StackSet in management account
aws cloudformation create-stack-set \
  --stack-set-name DynamoDBOptimaMetricsRole \
  --template-body file://metrics-collector-role.yaml \
  --parameters ParameterKey=ManagementAccountId,ParameterValue=111122223333 \
  --capabilities CAPABILITY_NAMED_IAM \
  --permission-model SERVICE_MANAGED \
  --auto-deployment Enabled=true,RetainStacksOnAccountRemoval=false

# Deploy to all accounts in your organization
aws cloudformation create-stack-instances \
  --stack-set-name DynamoDBOptimaMetricsRole \
  --deployment-targets OrganizationalUnitIds=ou-xxxx-xxxxxxxx \
  --regions us-east-1
```

**Option 1: Organizations Discovery (Recommended)**
```bash
# Single command discovers all accounts
dynamodb-optima discover --use-org
dynamodb-optima collect --days 14
```

**Option 2: Manual Account Iteration**
```bash

# Iterate through account profiles with isolated data folders
for profile in prod-account-1 prod-account-2; do
  # Create a separate folder for each profile
  mkdir $profile
  dynamodb-optima --project-root $profile --profile $profile discover
  # Collect doesn't support --profile
  AWS_PROFILE=$profile dynamodb-optima --project-root $profile collect
done
```

**Note:** Using `--project-root` keeps each account's data (database, logs, checkpoints) completely separate, which is essential for maintaining separate recommendations.

### Automation with Cron

```bash
# Daily metrics collection
0 2 * * * cd /path/to/dynamodb-optima-v2 && dynamodb-optima collect --days 1

# Weekly analysis
0 3 * * 0 cd /path/to/dynamodb-optima-v2 && dynamodb-optima analyze-capacity --days 7

# Monthly table class analysis
0 4 1 * * cd /path/to/dynamodb-optima-v2 && dynamodb-optima analyze-table-class --months 1
```

### Custom Analysis Queries

Access the DuckDB database directly for custom analysis:

```python
from dynamodb_optima.database import get_connection

conn = get_connection()

# Custom query
results = conn.execute("""
    SELECT 
        table_name,
        AVG(value) as avg_consumed_rcu
    FROM metrics
    WHERE metric_name = 'ConsumedReadCapacityUnits'
      AND timestamp >= CURRENT_DATE - INTERVAL '7 days'
    GROUP BY table_name
    ORDER BY avg_consumed_rcu DESC
    LIMIT 10
""").fetchall()

for row in results:
    print(f"{row[0]}: {row[1]:.2f} RCU")
```
