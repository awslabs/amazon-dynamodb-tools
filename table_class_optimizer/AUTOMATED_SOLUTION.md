# Automated Optimization System - User Guide

## Table of Contents
- [Overview](#overview)
- [Solution Architecture](#solution-architecture)
- [Prerequisites](#prerequisites)
- [Deployment Options](#deployment-options)
- [Configuration Parameters](#configuration-parameters)
- [Cost Estimation](#cost-estimation)
- [Troubleshooting](#troubleshooting)
- [Changelog](#changelog)

## Overview
Note: Actual costs may vary based on your AWS usage and the scale of your DynamoDB deployments.

The automated solution utilizes customer usage data from Athena CUR queries and operates as a serverless solution using Lambda, Step Functions, EventBridge, and Amazon SES. The solution is designed to scale and can be implemented by any customer directly from the DynamoDB Tools in the AWS Labs repository.

## Solution Architecture
<img width="608" alt="image" src="https://github.com/user-attachments/assets/f56677cb-dc0d-4f5d-83cd-9f3b957b239e" />

The automated system workflow:

1. EventBridge triggers the workflow monthly (configurable schedule)
2. Step Function initiates the optimization process:
   - Retrieves the Athena named query
   - Executes the query against CUR data
   - Processes results through Lambda
3. Athena pulls data from the CUR database
4. Lambda function:
   - Analyzes query results for optimization opportunities
   - Updates table classes (if ExecuteRecommendations enabled)
   - Generates detailed CSV report
5. SES delivers the report to specified recipients

## Prerequisites

Before deploying the automated solution, ensure you have:

- AWS Organizations enabled
  - Required for multi-account management
  - Organization root or OU access

- Cost and Usage Report (CUR) configured
  - Properly set up and generating data
  - Accessible via Athena
  - At least one month of historical data

- Athena setup complete
  - CUR database created
  - Tables properly configured
  - Query access verified

- SES configuration
  - Verified sender identity
  - Required email addresses verified
  - Appropriate sending limits

- IAM permissions
  - Cross-account access configured
  - Necessary service roles available
  - Permission boundaries set (if required)

## Deployment Options

### Option 1: AWS CloudFormation Console

1. Navigate to CloudFormation in your AWS Console
2. Create new stack with template
3. Upload the template.yaml file
4. Fill in required parameters
5. Review and create stack

Example of CloudFormation deployment:

<img width="1890" alt="SCR-20250527-nxik" src="https://github.com/user-attachments/assets/50d24d3a-d87c-437b-a457-9d0cc473a5ba" />

### Option 2: AWS CLI

Use the following commands to deploy via CLI:

    # Generate CloudFormation template
    python generate_cloud_formation.py

    # Deploy the stack
    aws cloudformation deploy \
        --template-file ./template.yaml \
        --stack-name DynamoDB-Storage-Class-Optimizer \
        --capabilities CAPABILITY_NAMED_IAM \
        --no-fail-on-empty-changeset \
        --parameter-overrides \
            AthenaCURDatabase=<your-database> \
            AthenaCURTable=<your-table> \
            SesSenderIdentity=<domain> \
            SesSenderEmailAddress=<email> \
            NotificationEmailRecipients=<emails> \
            OrganizationlUnitIds=<ou-id> 

            
## Configuration Parameters

Example of parameters screen in CloudFormation: 
<img width="941" alt="image" src="https://github.com/user-attachments/assets/ba1f2e1f-be12-4ada-afb0-3f81118bcc00" />

### Execution Configuration

**AthenaWorkgroup**
- Type: String
- Default: "primary"
- Description: The Athena workgroup to use for queries.

**AthenaCURDatabase**
- Type: String
- Description: The name of your Athena database containing CUR data.

**AthenaCURTable**
- Type: String
- Description: The name of your Athena table containing CUR data.

**OrganizationlUnitIds**
- Type: CommaDelimitedList
- Format: Root ID (r-xxxx) or Organizational unit (OU) ID (ou-xxxx-xxxxxxxx)
- Description: List of Organization Units to include in the optimization.

**CronDayOfMonth**
- Type: String
- Default: "10"
- Format: 1-31, L (last day), or W (weekday)
- Description: Day of the month to run the optimization.

**CronHour**
- Type: Number
- Range: 0-23
- Default: 0
- Description: Hour of the day to run the optimization (UTC).

**CronMinute**
- Type: Number
- Allowed Values: 0, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55
- Default: 0
- Description: Minute of the hour to run the optimization.

**SesSenderIdentity**
- Type: String
- Description: SES Sender Identity. Can be the same as the SesSenderEmailAddress.

**SesSenderEmailAddress**
- Type: String
- Format: Valid email address
- Description: SES Sender Email Address. Must be a Verified Identity or belong to a Verified Domain in SES.

**NotificationEmailRecipients**
- Type: CommaDelimitedList
- Format: Valid email addresses
- Description: Email addresses that will receive the optimization report.

**ExecutionMode**
- Type: String
- Allowed Values: 
  - ReportAndExecute
  - ReportOnly
- Default: ReportOnly
- Description: Determines whether the system will automatically apply recommended changes.

### Query Configuration

**PricingTerms**
- Type: String
- Allowed Values: 
  - NET
  - GROSS
- Default: NET
- Description: Determines whether to use net pricing (after discounts) or list pricing.

**AccountIds**
- Type: String
- Default: 'ALL'
- Format: 'ALL' or comma-separated list of AWS Account IDs in single quotes
- Example: '111111111111','222222222222'
- Description: Specifies which AWS accounts to analyze.

**MinimumSavings**
- Type: Number
- Default: 50
- Range: 0-1000000
- Description: Minimum monthly savings threshold in dollars.
**PayerIds**
- Type: String
- Default: 'ALL'
- Format: 'ALL' or comma-separated list of Payer AWS Account IDs in single quotes
- Example: '111122223333' for specific payer or leave it as 'ALL' to all include all payers
- Description: Specifies which Payer accounts to analyze.

**TableNames**
- Type: String
- Default: 'ALL'
- Format: 'ALL' or comma-separated list of DynamoDB Table names in single quotes
- Example: 'Table1','my_other_table'
- Description: Specifies which DynamoDB tables to analyze.

**RegionNames**
- Type: String
- Default: 'ALL'
- Format: 'ALL' or comma-separated list of AWS Region names in single quotes
- Example: 'us-east-1','eu-west-1'
- Description: Specifies which AWS Regions to analyze.

### Notes on Parameters
- When specifying lists (e.g., AccountIds, TableNames), always use single quotes around each item.
- The 'ALL' option for AccountIds, PayerIds, TableNames, and RegionNames allows for comprehensive analysis across your entire organization.
- MinimumSavings helps filter out minor optimizations, focusing on more impactful changes.
- ExecutionMode should be set carefully. Start with 'ReportOnly' to review recommendations before enabling automatic changes.
- Ensure all email addresses (SesSenderEmailAddress and NotificationEmailRecipients) are properly verified in SES to avoid delivery issues.

## Cost Estimation

Monthly operational costs for the automated system (approximate):

- Lambda execution: < $0.10
  - One execution per month
  - Typical runtime < 1 minute
- Step Functions: < $0.05
  - One state machine execution per month
- Athena query: $0.20-1.00
  - Depends on CUR data volume
  - Typically processes 1-6 months of data
- SES: Free tier eligible
  - One email per month with CSV attachment
- Total estimated cost: < $2.00 per month

Note: Actual costs may vary based on your AWS usage and the scale of your DynamoDB deployments.

## Design Considerations

The automated solution is distributed as a single CloudFormation Template file to allow for easy distribution, such as via a [quick-create link](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/cfn-console-create-stacks-quick-create-links.html). The SQL query and Lambda Function code are inlined into the Template, while the query [DDB_TableClassReco.sql](DDB_TableClassReco.sql) remains as a separate file for independent use or modification.

The project contains a [raw_template.yaml](raw_template.yaml) template file as the foundation. The Python script [generate_cloud_formation.py](generate_cloud_formation.py) takes the [lambda_handler.py](lambda_handler.py) and [DDB_TableClassReco.sql](DDB_TableClassReco.sql) files and inlines them into the raw_template.yaml to produce `template.yaml`.

The resulting `template.yaml` can be deployed to CloudFormation in a management account that has access to query the Cost and Usage Report (CUR) files and also has permission to create a CloudFormation StackSet (containing an IAM Role for performing the Table Class updates) in the target accounts that have the DynamoDB tables.

## Troubleshooting

### Q1: Why am I not seeing any CUR data in my Athena queries?
A1: This could be due to several reasons:
- CUR may not be properly configured in your AWS account.
- The Athena table might not exist or is not up to date.
- The AthenaCURDatabase and AthenaCURTable parameters in your configuration might be incorrect.

To resolve:
1. Verify CUR is set up correctly in your AWS account.
2. Check if the Athena table exists and contains recent data.
3. Ensure the database and table names in your configuration match those in Athena.

### Q2: I'm encountering permission errors. What should I check?
A2: Permission issues often stem from incorrect IAM configurations:
1. Review the IAM roles and policies for the Lambda function and Step Functions.
2. Verify that AWS Organizations access is properly set up.
3. If using multiple accounts, ensure cross-account roles are correctly configured.
4. Check that the executing role has necessary permissions for Athena, S3, and DynamoDB.

### Q3: Why aren't my optimization report emails being delivered?
A3: Email delivery issues can be caused by:
- Incorrect SES configuration
- Unverified sender or recipient email addresses
- SES sending limits

To troubleshoot:
1. Confirm SES is set up correctly in your account.
2. Verify that the sender email address is verified in SES.
3. Check if recipient email addresses are correct and verified (if required).
4. Review your SES sending limits and request increases if necessary.

### Q4: The optimizer isn't generating any recommendations. What could be wrong?
A4: This could be due to:
- A MinimumSavings threshold that's set too high
- Incorrect specification of AccountIds, PayerIds, TableNames, or RegionNames
- Insufficient historical data in the CUR for meaningful analysis

Try the following:
1. Lower the MinimumSavings threshold in your configuration.
2. Verify that the AccountIds, PayerIds, TableNames, and RegionNames parameters are correct.
3. Ensure you have at least 1-3 months of data in your CUR.

### Q5: My Athena query is timing out. How can I resolve this?
A5: For large datasets, Athena queries might time out. To address this:
1. Reduce the `months_to_scan` parameter in the SQL query.
2. Optimize your Athena table (e.g., partition the table, convert to Parquet format).
3. Consider breaking down the analysis into smaller chunks (e.g., by account or region).

### Q6: I see unexpected table class changes in my DynamoDB tables. What's happening?
A6: This could occur if:
- The ExecutionMode is set to 'ReportAndExecute' instead of 'ReportOnly'
- There are other processes or people modifying table classes

To address:
1. Check the ExecutionMode parameter in your configuration. Set it to 'ReportOnly' if you don't want automatic changes.
2. Review the generated report for the rationale behind each recommendation.
3. Implement change management processes to control table class modifications.

### Q7: How can I get more detailed error information for troubleshooting?
A7: For more in-depth error details:
1. Check CloudWatch Logs for the Lambda function for specific error messages.
2. Review the Step Functions execution history for a detailed breakdown of each step.
3. Enable DEBUG level logging in the Lambda function for more verbose output.

### Q8: The optimizer recommendations don't align with my expectations. What should I do?
A8: If recommendations seem off:
1. Review the SQL query parameters (e.g., `months_to_scan`, `min_savings_per_month`) to ensure they align with your optimization goals.
2. Check if there have been recent significant changes in your DynamoDB usage patterns that might not be reflected in historical data.
3. Consider adjusting the `cost_type` parameter if you're using special pricing or discounts.

## Screenshots
Stack and Resources at the end of the deployment:
   <img width="1890" alt="SCR-20250527-nxik" src="https://github.com/user-attachments/assets/50d24d3a-d87c-437b-a457-9d0cc473a5ba" />

Step function after the first run:
   <img width="1426" alt="image" src="https://github.com/user-attachments/assets/01a7d800-670a-4125-8a7b-bdb6c639a5c9" />

Past executions history:
    <img width="1160" alt="image" src="https://github.com/user-attachments/assets/efa3405c-2bba-49dc-b5f9-514c5c170cd0" />

Report body and attachment:
<img width="1443" alt="image" src="https://github.com/user-attachments/assets/057ecbdb-a135-47eb-9438-4363fc339e18" />

## Changelog
For version history and updates, see [CHANGELOG.md](CHANGELOG.md)
