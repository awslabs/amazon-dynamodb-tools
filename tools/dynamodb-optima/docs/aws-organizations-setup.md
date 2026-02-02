# AWS Organizations Setup

← [Documentation Index](README.md) | [Main README](../README.md)

---

This guide covers how to set up DynamoDB Optima for multi-account discovery using AWS Organizations.

## Overview

AWS Organizations integration allows DynamoDB Optima to automatically discover and analyze DynamoDB tables across all accounts in your organization with a single command.

## Prerequisites

- Access to the AWS Organizations management account
- Permission to create IAM roles in member accounts
- Basic understanding of AWS cross-account access patterns

## Setup Steps

### Step 1: Management Account Setup

Add Organizations read permissions to your current IAM user/role to discover accounts:

```json
{
  "Effect": "Allow",
  "Action": [
    "organizations:ListAccounts",
    "organizations:DescribeAccount",
    "organizations:ListOrganizationalUnitsForParent"
  ],
  "Resource": "*"
}
```

Also add permission to assume the cross-account role:

```json
{
  "Effect": "Allow",
  "Action": ["sts:AssumeRole"],
  "Resource": "arn:aws:iam::*:role/MetricsCollectorRole"
}
```

### Step 2: Member Account Setup

Deploy a cross-account IAM role to each member account with:

**Role Name:** `MetricsCollectorRole` (or custom name via `--org-role` CLI option)

**Permissions Policy:**
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "dynamodb:ListTables",
        "dynamodb:DescribeTable",
        "dynamodb:ListTagsOfResource",
        "cloudwatch:GetMetricData",
        "cloudwatch:GetMetricStatistics",
        "pricing:GetProducts"
      ],
      "Resource": "*"
    }
  ]
}
```

**Trust Policy:** (replace `111122223333` with your management account ID)
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "AWS": "arn:aws:iam::111122223333:root"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
```

### Quick Setup Commands

**Option 1: Manual Setup (Run in each member account)**

```bash
# Create role in member account
aws iam create-role \
  --role-name MetricsCollectorRole \
  --assume-role-policy-document '{
    "Version": "2012-10-17",
    "Statement": [{
      "Effect": "Allow",
      "Principal": {"AWS": "arn:aws:iam::MANAGEMENT-ACCOUNT-ID:root"},
      "Action": "sts:AssumeRole"
    }]
  }'

# Attach permissions policy
aws iam put-role-policy \
  --role-name MetricsCollectorRole \
  --policy-name DynamoDBMetricsAccess \
  --policy-document '{
    "Version": "2012-10-17",
    "Statement": [{
      "Effect": "Allow",
      "Action": [
        "dynamodb:ListTables",
        "dynamodb:DescribeTable",
        "dynamodb:ListTagsOfResource",
        "cloudwatch:GetMetricData",
        "cloudwatch:GetMetricStatistics",
        "pricing:GetProducts"
      ],
      "Resource": "*"
    }]
  }'
```

**Note:** Replace `MANAGEMENT-ACCOUNT-ID` with your actual management account ID.

**Option 2: CloudFormation StackSets (Recommended for many accounts)**

Deploy the role across all accounts using CloudFormation StackSets from the management account. See [Advanced Topics](advanced-topics.md) for automation strategies.

## Usage

Once setup is complete, use the `--use-org` flag:

```bash
# Discover all tables across all accounts
dynamodb-optima discover --use-org

# Use custom role name
dynamodb-optima discover --use-org --org-role CustomRoleName

# Skip specific accounts
dynamodb-optima discover --use-org --skip-accounts 111122223333,444455556666
```

## Troubleshooting Organizations

### Access Denied Errors

**Symptom:** `AssumeRole` failures when accessing member accounts

**Solutions:**
1. Verify the trust policy in member account roles includes your management account ID
2. Ensure the management account has permission to assume the role
3. Check that the role name matches what you're specifying (default: `MetricsCollectorRole`)

### Missing Accounts

**Symptom:** Not all accounts are being discovered

**Solutions:**
1. Verify Organizations permissions in management account
2. Check if accounts are in a suspended state
3. Use `--skip-accounts` to explicitly exclude problematic accounts

### Permission Issues in Member Accounts

**Symptom:** Can assume role but can't list tables or get metrics

**Solutions:**
1. Verify the IAM policy attached to the role includes all required DynamoDB and CloudWatch permissions
2. Check for any service control policies (SCPs) that might be blocking access
3. Ensure the role has permissions in all required regions

## Security Best Practices
These are common best practices that are worth mentioning when making changes to access policies. These don't all necessarily apply to Optima:

1. **Least Privilege:** Only grant the minimum required permissions
2. **External ID:** Consider adding an ExternalId condition to the trust policy for additional security (not yet passed by DynamoDB Optima, however)
3. **Session Duration:** Configure appropriate session duration limits
4. **Audit:** Regularly audit which accounts and principals are assuming the role
5. **MFA:** Consider requiring MFA for sensitive operations

## Related Documentation

- [Command Reference](command-reference.md) - See `discover` command options
- [Advanced Topics](advanced-topics.md) - Multi-account strategies and automation
