# DynamoDB Table Class Optimizer

## Table of Contents
- [Overview](#overview)
- [Solutions](#solutions)
  - [Manual Query Tool](#manual-query-tool)
  - [Automated Optimization System](#automated-optimization-system)
- [Manual Query Tool - User Guide](#manual-query-user-guide)
  - [Enabling Cost and Usage Report (CUR)](#enabling-cost-and-usage-report-cur)
  - [Execution steps](#execution-steps)
  - [Parameters](#parameters)
  - [Expected output](#expected-output)
  - [Interpretation](#interpretation)
  - [Best practices](#best-practices)
  - [Important notes](#important-notes)
  - [The Query](#the-query)
  - [Limitations](#limitations)
  - [FAQ](#faq)
- [Automated Optimization System - User Guide](#automated-optimization-system-user-guide)
  - [Prerequisites](#prerequisites)
  - [Deployment Options](#deployment-options)
  - [Configuration Parameters](#configuration-parameters)
  - [Cost Estimation](#cost-estimation)
  - [Troubleshooting](#troubleshooting)
  - [Changelog](#changelog)

## Overview

DynamoDB's Standard-IA table class lowers storage costs by 60%, but offsets these savings by increasing throughput costs by 25%. This tool helps you determine whether switching between Standard and Standard-IA table classes is cost-effective for your DynamoDB tables based on your usage in [AWS Cost & Usage Reports](https://docs.aws.amazon.com/cur/latest/userguide/what-is-cur.html) (CUR) data.

For more information about table classes and their commonalities, see our [best practice guide](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/CostOptimization_TableClass.html) in the developer documentation.

## Solutions

This project offers two approaches for optimizing your DynamoDB table classes:

### Manual Query Tool
Use the Athena query directly to analyze your tables and get recommendations that you can implement manually. This approach gives you full control over when and how to apply changes.

**Best for:**
- One-time analysis
- Learning about your usage patterns
- Organizations requiring manual approval processes
- Testing and validation scenarios

### Automated Optimization System
A fully automated, serverless solution that continuously monitors and optimizes your DynamoDB table classes across all accounts and regions in your AWS organization.

**Best for:**
- Large-scale deployments (hundreds or thousands of tables)
- Organizations wanting hands-off optimization
- Consistent, data-driven optimization decisions
- Eliminating manual monitoring overhead

**Business Case for Automation:**
In many scenarios, workload access patterns change over time, including massive deletions leading to reduced storage size, changes in read/write volume, and seasonal fluctuations in usage. Manually monitoring and adjusting table classes for optimal cost-performance balance can be challenging, especially at scale. For example, managing 4000 tables across multiple accounts and regions would be a significant operational overhead.

The automated solution provides:
- Centralized control over table classes across your entire organization
- Data-driven optimization decisions
- Significant cost savings with minimal operational overhead
- Elimination of manual monitoring and adjustment tasks

## Manual Query User Guide

### Enabling Cost and Usage Report (CUR)
Before running the Athena query, you'll need to ensure that your AWS account has the Cost and Usage Report (CUR) properly configured and up-to-date. Follow the steps in the [AWS documentation on setting up the CUR](https://docs.aws.amazon.com/cur/latest/userguide/cur-query-athena.html) to enable and configure the report for your use case.

Some key settings to consider when setting up the CUR:

- **Report name**: Use a descriptive name like "DynamoDB Cost Optimization".
- **Report name prefix**: Set a unique prefix for your organization.
- **Time unit**: Choose the "Hourly" time unit for the most granular analysis.
- **Include resource IDs**: Set this option to "Yes".
- **Compressed CSV file**: Ensure this option is enabled.
- **S3 bucket**: Choose an appropriate S3 bucket to store the CUR files.

After enabling the CUR, wait for the first report to be generated (this can take up to 24 hours) and verify that the CUR data is being delivered to the specified S3 bucket and contains the necessary information, including DynamoDB usage and costs.

### Execution steps
1. Before running the query, make sure you fully aware of the [Limitations](#limitations) list and that you've correctly enabled [Cost and Usage Reports](#enabling-cost-and-usage-report-cur).
2. Open your AWS Athena console
3. Copy the query text from [DDB_TableClassReco.sql](DDB_TableClassReco.sql) (select all query text)
4. Paste the query into a new query window
5. Rename the query's SQL code with the correct Cost and Usage Report (CUR) database details. Search for these default values:
   - `[CUR_DB]` : The CUR Database name 
   - `[CUR_TABLE]` : The CUR table name 
6. Adjust parameters in the query ([Parameters](#parameters)) as needed
7. Run the query
8. Review results in the Athena query results pane

### Parameters
At the beginning of the query, you can adjust parameters:

- `months_to_scan`: Set the number of months of data to analyze (default: 3)
- `min_savings_per_month`: Set the minimum potential monthly savings threshold in dollars (default: 50.0)
- `custom_start_date`: User can set this to the desired start date when `months_to_scan` is 0
- `custom_end_date`: User can set this to the desired end date when `months_to_scan` is 0
- `account_ids`: Specify which AWS account IDs to include in the analysis
- `payer_ids`: Specify which AWS Payer IDs to include in the analysis
- `table_names`: Specify which DynamoDB tables to include in the analysis
- `region_names`: Specify which AWS Regions to include in the analysis
- `cost_type`: 'NET' (post-discount) or 'GROSS' (pre-discount) pricing
- `report_type`: 'DETAILED' or 'SUMMARY' output

### Expected output
The query will output a detailed table-level report or a summary-level report, depending on the `report_type` parameter.

To get a summary of your total potential savings at the payer level, use the query with **SUMMARY** set in report_type parameter. To get a detailed cost optimization report on a per-table level, use the query with **DETAILED** set in report_type parameter.

### Interpretation
- In general, tables are considered candidates for Standard-IA if their storage cost is greater than 42% of their throughput cost. See the FAQ *What are the calculations behind the table class recommendations?* for an explanation of the calculation.
- Tables are considered candidates for Standard if their Standard-IA storage cost is less than 13% of their Standard-IA throughput cost.
- Positive values in `potential_savings_per_month` indicate potential cost savings by switching table classes.

### Best practices
- Start with the 'SUMMARY' report to get an overview of potential savings.
- Use the 'DETAILED' report to identify specific tables for optimization.
- Adjust the `min_savings_per_month` parameter to focus on the most impactful optimization opportunities.
- Regularly run this query to identify new optimization opportunities as usage patterns change.
- Analyze tables that have been in production for at least 3 months and have relatively stable usage patterns, as the recommendations are most accurate for such tables.

### Important notes
- This query analyzes data from the AWS Cost and Usage Report (CUR).
- Tables using reserved capacity are not considered for optimization but are included in total cost calculations.
- The difference between 'NET' and 'GROSS' results can help you understand the impact of your EDP or PPA discounts on potential savings.

### The query
Download the [DDB_TableClassReco.sql](DDB_TableClassReco.sql) file.

### Limitations

1. **Reserved Capacity**:
   - Reserved Capacity purchasing is currently not supported for the Standard-IA table class.
   - Customers who have already purchased 1-year or 3-year Reserved Instances (RIs) for the Standard table class may not be able to instantly switch their tables to the Standard-IA class.
   - This limitation applies only to tables in Provisioned Capacity mode, as there is no concept of RIs for On-Demand capacity mode.

2. **Provisioned Tables with Reservations**:
   - The Athena query automatically marks tables that are using reserved capacity as "Optimized".
   - This is done to focus the results on tables without reservations, as switching the table class for reserved capacity tables may not result in additional cost savings.

3. **New or Unstable Workloads**:
   - The query and recommendations provided by this tool are most accurate for DynamoDB tables that have at least 3 months of stable usage data. 
   - Tables with major changes in access patterns or storage/throughput needs may not show accurate recommendations, as the analysis relies on recent historical data.

4. **Table Class Update Limit**:
   - Customers are limited to no more than two table class updates on a single table within a 30-day trailing period.

5. **Query Execution Time**:
   - Using a large `months_to_scan` value (greater than 6 months) may result in long and costly query execution times.
   - It's recommended to start with a smaller time range, such as 3 months or less, to get faster results and lower query costs.
     
6. **Discount Percentages are Estimates**
   - The discount percentages used in the tool when calculating the potential savings from switching table classes (60% for storage and 25% for throughput for example) are average values.
   - The actual discounts may slightly differ across AWS regions, so the table costs and potential savings shown are estimates and may vary from the actual values.

### FAQ

#### What are the feature highlights?
New table storage class beneficial for those existing DynamoDB workloads where access to the data is very infrequent, but storage size is quite large. Particularly if storage is the dominant cost driver for a table, Standard_IA can help:
- Reduce Storage Costs by 60%
- With Throughput Cost increasing by 25%
- Same cost ratios apply irrespective of Provisioned/On-Demand Capacity Modes, Global Tables or Single region tables

#### When is it recommended to switch back to Standard?
When you switch from Standard-IA to Standard, you will pay 20% less on the throughput (read/write) cost and 150% more on the storage cost. Particularly if throughput is the clear dominant cost driver for a table. The rule of thumb is if your throughput cost is x7.5 than the storage cost, Standard will be a be a more cost-effective table class for your usage.

#### How to identify which tables are candidates for cost optimization with using Standard IA or Standard?
- If StorageCosts > 0.42 x ThroughputCosts, table is a candidate for Standard IA table class and can benefit with switching the table class for a quick cost optimization win
- If StorageCosts < 0.13 x ThroughputCosts, table is a candidate for Standard table class and can benefit with switching the table class for a quick cost optimization win.

#### What are the calculations behind the table class recommendations?
- Standard to Standard-IA:
  - Given, when change to Standard-IA, you will pay 25% more on Throughput (Read/Write) cost and 60% less on Storage cost
  - Potential saving = 0.6 *(actual_storage_cost) - 0.25 *(actual_throughput_cost)
  - It is recommended to move from Standard to Standard-IA if:
  - actual_storage_cost > (0.25/0.6) *(actual_throughput_cost)
  - Then the break-even is: actual_storage_cost / actual_throughput_cost > ~42% (41.16%)

- Standard-IA to Standard: 
  - Given, when change to Standard, you will pay 20% less on Throughput (Read/Write) cost and 150% more on Storage cost
  - Potential Saving = 0.2*(actual_throughput_cost_ia) - 1.5 *(_actual_storage_cost_ia )
  - It is recommended to switch back from Standard-IA to Standard if:
  - actual_storage_cost_ia < (0.2/1.5) *(actual_throughput_cost_ia)
  - Then the break-even: actual_storage_cost_ia / actual_throughput_cost_ia < ~13% (13.33%)

#### What is the effort on the AWS customer's side?
For the manual approach, the effort from AWS customer's side is to flip the table class of their candidate DynamoDB tables from STANDARD to STANDARD_INFREQUENT_ACCESS using either the Console, SDKs or Infrastructure as Code Tools (CFN, Terraform etc).

For the automated approach, the effort is minimal after initial setup - the system handles optimization automatically based on your configuration.

#### How do I switch the table classes manually from the console?

Here are the steps to manually switch a DynamoDB table from Standard from/to Standard-Infrequent Access (Standard-IA) class:
1. Sign in to the AWS Management Console.
2. Navigate to the DynamoDB service.
3. In the navigation pane, choose "Tables".
4. Select the table you want to modify.
5. Choose the "Additional settings" tab.
6. In the "Table class" section, choose "Edit".
7. Based on the report results, select "DynamoDB Standard-Infrequent Access" or "Standard" as the new table class.
8. Choose "Save changes".
9. Review the changes and confirm by choosing "Change table class".
10. Wait for the table status to change from "Updating" back to "Active". This process may take several minutes.
11. Once complete, verify the new table class in the table details.

##### Important notes
  - Switching table classes can take several minutes to complete.
  - You can't switch table classes more than twice in a 30-day period.
  - Switching to Standard-IA is best for tables with infrequent access patterns.
  - Monitor your costs after switching to ensure the change is beneficial for your use case.
  - Remember that Standard-IA has higher costs for data access but lower storage costs.
  - For bulk operations or automation, consider using the AWS CLI or SDKs with the UpdateTable API call, specifying the TableClass parameter. For example, in Python the Boto3 DynamoDB [update_table](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb/client/update_table.html) API can be used. 

#### How long would it take for the changes to replicate?
The time to update your table class depends on your table traffic, storage size, and other related variables. Actual switch of table classes could take up to a few minutes depending on the size of the table and above mentioned variables, however there is no performance degradation or impact to the table during or after this switch. In reality, the table class switch could be considered as simply a different billing plan for the tables.

#### What is the performance impact on using Standard IA table class?
 No performance difference between Standard and Standard-IA (S-IA) table classes. Same consistent single digit millisecond average latencies at any scale. Same integrations with other AWS Services. Same costs for every other DynamoDB feature except for Throughput and Storage (Eg: same pricing for backups, PITR, streams etc irrespective of the storage class).

#### Is it possible to switch back to Standard from Standard IA?
Yes. No more than two table class updates on your table are allowed in a 30-day trailing period. 

## Automated Optimization System User Guide

The automated solution utilizes customer usage data from Athena CUR queries and operates as a serverless solution using Lambda, Step Functions, EventBridge, and Amazon SES. The solution is designed to scale and can be implemented by any customer directly from the DynamoDB Tools in the AWS Labs repository.

### Solution Architecture
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

### Prerequisites

- AWS Organizations enabled
- Cost and Usage Report (CUR) enabled and configured
- Athena set up with CUR database
- SES configured with verified identity
- Appropriate IAM permissions for cross-account access

### Deployment Options

#### Option 1: AWS CloudFormation Console

1. Navigate to CloudFormation in your AWS Console
2. Create new stack with template
3. Upload the template.yaml file
4. Fill in required [parameters](#configuration-parameters)
5. Review and create stack  see [screenshots](#screenshots) below. 


#### Option 2: AWS CLI

Example usage (please update the values of the `--parameter-overrides` with the appropriate values):

```bash
python generate_cloud_formation.py
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
```
#### Screenshots
Stack and Resources at the end of the deployment:
   <img width="1890" alt="SCR-20250527-nxik" src="https://github.com/user-attachments/assets/50d24d3a-d87c-437b-a457-9d0cc473a5ba" />
Step function after the first run:
   <img width="1426" alt="image" src="https://github.com/user-attachments/assets/01a7d800-670a-4125-8a7b-bdb6c639a5c9" />
Past executions history:
    <img width="1160" alt="image" src="https://github.com/user-attachments/assets/efa3405c-2bba-49dc-b5f9-514c5c170cd0" />
Report body and attachment:
<img width="1443" alt="image" src="https://github.com/user-attachments/assets/057ecbdb-a135-47eb-9438-4363fc339e18" />

   
### Configuration Parameters

Example of paramenters screen in CloudFormation: 
<img width="941" alt="image" src="https://github.com/user-attachments/assets/ba1f2e1f-be12-4ada-afb0-3f81118bcc00" />


#### Execution Configuration

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

#### Query Configuration

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
- Example: '111111111111','222222222222'
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

#### Notes on Parameters
- When specifying lists (e.g., AccountIds, TableNames), always use single quotes around each item.
- The 'ALL' option for AccountIds, PayerIds, TableNames, and RegionNames allows for comprehensive analysis across your entire organization.
- MinimumSavings helps filter out minor optimizations, focusing on more impactful changes.
- ExecutionMode should be set carefully. Start with 'ReportOnly' to review recommendations before enabling automatic changes.
- Ensure all email addresses (SesSenderEmailAddress and NotificationEmailRecipients) are properly verified in SES to avoid delivery issues.

### Cost Estimation

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

### Design Considerations

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

For persistent issues or questions not covered here, please refer to the project documentation or reach out to AWS support.

### Changelog

### v2.0.0 (Automation Release - 2025-06-18)
- **NEW: Automated Optimization System**
  - Added fully automated, serverless solution for continuous DynamoDB table class optimization
  - Integrated serverless architecture using AWS Lambda, Step Functions, EventBridge, and SES
  - CloudFormation template for one-click deployment across AWS Organizations
  - Cross-account table class management with IAM role automation
  - Automated monthly reporting with CSV attachments via SES
  - Configurable execution modes: ReportOnly and ReportAndExecute
  - Support for custom scheduling (daily, weekly, monthly) with cron expressions
- **Enhanced Configuration**
  - 15+ configuration parameters for fine-tuned control
  - Support for filtering by Account IDs, Payer IDs, Table Names, and Regions
  - Configurable minimum savings thresholds and pricing terms (NET/GROSS)
  - Organization Unit (OU) support for enterprise deployments
- **Improved Documentation**
  - Comprehensive troubleshooting guide with 8 common scenarios
  - Cost estimation guide for operational expenses
  - Detailed parameter documentation with examples
  - Architecture diagrams and workflow explanations
- **Developer Experience**
  - Automated CloudFormation template generation from modular components
  - Support for both CloudFormation Console and CLI deployment
  - Integration with existing manual query workflow

#### v1.0.0 (Initial Release - 2025-06-15)
- **Initial Manual Query Tool**
  - Athena SQL query for DynamoDB table class cost analysis
  - Support for both SUMMARY and DETAILED report types
  - Configurable analysis parameters (months to scan, minimum savings, etc.)
  - Cost and Usage Report (CUR) integration
  - Support for multiple AWS accounts and regions
  - Table class recommendations based on storage vs throughput cost ratios
- **Core Features**
  - Standard to Standard-IA optimization (60% storage savings, 25% throughput increase)
  - Standard-IA to Standard optimization (20% throughput savings, 150% storage increase)
  - Reserved capacity detection and handling
  - Custom date range analysis
  - NET vs GROSS pricing support
- **Documentation**
  - Comprehensive user guide with step-by-step instructions
  - FAQ section with calculation explanations
  - Best practices and limitations documentation
  - Manual table class switching instructions
