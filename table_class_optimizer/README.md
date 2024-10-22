# DynamoDB Table Class Optimizer
## Table of Contents
- [Overview](#overview)
- [User Guide](#user-guide)
  - [Execution](#execution)
  - [Parameters](#parameters)
  - [Expected Output](#expected-output)
  - [Interpretation](#interpretation)
  - [Best Practices](#best-practices)
  - [Important Notes](#important-notes)
- [The Query](#the-query)
- [FAQ](#faq)
- [Future Work](#future-work)
- [Contact Us](#contact-us)

## Overview
DynamoDB's Standard-IA table class lowers storage costs by 60%, but offsets these savings by increasing throughput costs by 25%. This Athena query on [AWS Cost & Usage Reports](https://docs.aws.amazon.com/cur/latest/userguide/what-is-cur.html) (CUR) data help determine whether switching between Standard and Standard-IA table classes is cost-effective for your DynamoDB tables based on your usage and parameters provided. For more information about table classes and their commonalities, see our [best practice guide](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/CostOptimization_TableClass.html) in the developer documentation.

To get a summary of your total potential savings at the payer level, use the query with **SUMMARY** set in report_type parameter
To get a detailed cost optimization report on a per-table level, use the query with **DETAILED** set in report_type parameter

Use the full user guide to effectively use the query.

## User guide
### Execution steps
1. Open your AWS Athena console
2. Copy the query text from DDB_TableClassReco.sql (select all query text)
3. Paste the query into a new query window
4. Rename the query with the correct Cost and Usage Report (CUR) database details: ([CUR_DB] and [CUR_TABLE]).
5. Adjust parameters in the 'parameters' as needed
6. Run the query
7. Review results in the Athena query results pane

### Parameters
At the beginning of the query, you can adjust five parameters:

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

### Interpretation
- In general, tables are considered candidates for Standard-IA if their storage cost is greater than 42% of their throughput cost. See the FAQ *What are the calculations behind the table class recommendations?* for an explanation of the calculation.
- Tables are considered candidates for Standard if their Standard-IA storage cost is less than 13% of their Standard-IA throughput cost.
- Positive values in `potential_savings_per_month` indicate potential cost savings by switching table classes.

### Best practices
- Start with the 'SUMMARY' report to get an overview of potential savings.
- Use the 'DETAILED' report to identify specific tables for optimization.
- Adjust the `min_savings_per_month` parameter to focus on the most impactful optimization opportunities.
- Regularly run this query to identify new optimization opportunities as usage patterns change.

### Important notes
- This query analyzes data from the AWS Cost and Usage Report (CUR).
- Tables using reserved capacity are not considered for optimization but are included in total cost calculations.
- The difference between 'NET' and 'GROSS' results can help you understand the impact of your EDP or PPA discounts on potential savings.

## The query
Download the [DDB_TableClassReco.sql](DDB_TableClassReco.sql) file.

## FAQ
**What are the feature highlights?**
- New table storage class beneficial for those existing DynamoDB workloads where access to the data is very infrequent, but storage size is quite large. Particularly if storage is the dominant cost driver for a table, Standard_IA can help:
  - Reduce Storage Costs by 60%
  - With Throughput Cost increasing by 25%
  - Same cost ratios apply irrespective of Provisioned/On-Demand Capacity Modes, Global Tables or Single region tables

**When is it recommended to switch back to Standard?**
- When you switch from Standard-IA to Standard, you will pay 20% less on the throughput (read/write) cost and 150% more on the storage cost. Particularly if throughput is the clear dominant cost driver for a table. The rule of thumb is if your throughput cost is x7.5 than the storage cost, Standard will be a be a more cost-effective table class for your usage.

**How can this help AWS customers running large workloads on DynamoDB?**
- Reduce storage costs by 60% while increasing throughput costs by 25%
- Clearly not applicable to net new workloads because storage costs would not be the dominant cost driver for those brand new tables

**How to identify which tables are candidates for cost optimization with using Standard IA or Standard?**
- If StorageCosts > 0.42 x ThroughputCosts, table is a candidate for Standard IA table class and can benefit with switching the table class for a quick cost optimization win
- If StorageCosts < 0.13 x ThroughputCosts, table is a candidate for Standard table class and can benefit with switching the table class for a quick cost optimization win.

**What are the calculations behind the table class recommendations?**
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

**What is the effort on the AWS customer's side?**
- The effort from AWS customer's side is to flip the table class of their candidate DynamoDB tables from STANDARD to STANDARD_INFREQUENT_ACCESS using either the Console, SDKs or Infrastructure as Code Tools (CFN, Terraform etc)

**How do I switch the table classes manually from the console?**
- Here are the steps to manually switch a DynamoDB table from Standard from/to Standard-Infrequent Access (Standard-IA) class:  
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

- Important notes:
  - Switching table classes can take several minutes to complete.
  - You can't switch table classes more than twice in a 30-day period.
  - Switching to Standard-IA is best for tables with infrequent access patterns.
  - Monitor your costs after switching to ensure the change is beneficial for your use case.
  - Remember that Standard-IA has higher costs for data access but lower storage costs.
  - For bulk operations or automation, consider using the AWS CLI or SDKs with the UpdateTable API call, specifying the TableClass parameter. Visit the Boto3 DynamoDB Update_Table documentation for the syntax and more information.

**How long would it take for the changes to replicate?**
 - The time to update your table class depends on your table traffic, storage size, and other related variables. Actual switch of table classes could take up to a few minutes depending on the size of the table and above mentioned variables, however there is no performance degradation or impact to the table during or after this switch. In reality, the table class switch could be considered as simply a different billing plan for the tables.

**What is the performance impact on using Standard IA table class?**
 - No performance impact between Standard and Standard IA table classes. Same consistent single digit millisecond average latencies at any scale. Same integrations with other AWS Services. Same costs for every other DynamoDB feature except for Throughput and Storage (Eg: same pricing for backups, PITR, streams etc irrespective of the storage class).

**Is it possible to switch back to Standard from Standard IA?**
 - Yes. No more than two table class updates on your table are allowed in a 30-day trailing period. [Internal] Additional table class updates for a table may be allowed as one-offs.


