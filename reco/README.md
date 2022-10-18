

# DynamoDB reserved capacity recommendations

[DynamoDB reserved capacity](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/HowItWorks.ReadWriteCapacityMode.html#HowItWorks.ProvisionedThroughput.ReservedCapacity) can save you up to 77% with a three year reservation purchase. This tool helps you to find the right amount of reserved capacity to own to lower your total cost of provisioned read and write capacity.

The DynamoDB pricing [page under "Read and write requests"](https://aws.amazon.com/dynamodb/pricing/provisioned) shows the reservation offerings used by this tool, which are available in one or three year terms. Notably, reservations are not offered for [on-demand capacity](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/HowItWorks.ReadWriteCapacityMode.html#HowItWorks.OnDemand), for tables using [Standard-IA storage](https://aws.amazon.com/dynamodb/standard-ia/), or for replicated write capacity which is used by DynamoDB global tables. Read on to understand how to run the tool and how it makes a recommendation.

## Overview
The tool uses your existing [AWS Cost and Usage Reports](https://docs.aws.amazon.com/cur/latest/userguide/what-is-cur.html) (CUR) to generate reserved capacity recommendations. Here's an overview of the steps to generate a report (a detailed guide is below)

1. To begin, you should enable CUR with hour granularity
2. Then, you allow time to pass for data to accumulate in the S3 bucket you configured
3. You create a database so that Athena can query the CUR data by using the CloudFormation template provided when you create the CUR report
4. Once you're ready to get a recommendation, you issue the provided Athena query.
5. Next, you download the results in CSV format to your machine
6. Finally you clone and run the reco tool, pointing it at the CSV file you received from Athena! This generates a recommendation.

![Reserved Capacity Diagram](static/Reserved%20Capacity-Page-1.png)


## Documentation
```
$ python3 src/ddbr.py reco -h
usage: ddbr.py reco [-h] [--athena-sql] [--debug] [--disable-analytics]
                    [--file-name FILE_NAME] [--term {1,3,all}]
                    [--file-type {cur,dli,dli_rt}]
                    [--output {plain,csv,dict,all}] [--start-time START_TIME]
                    [--end-time END_TIME] [--package PACKAGE]

optional arguments:
  -h, --help            show this help message and exit
  --athena-sql          generate Athena SQL and exit
  --debug               Turn up log level
  --file-name FILE_NAME
                        File name or path to file where usage data resides.
  --term {1,3,all}      RC term length to consider.
  --file-type {cur,dli,dli_rt}
                        CUR Athena query (cur) or DBR file (dli*). Detailed
                        line items (dli). DLI with resources and tags (dli_rt)
  --output {plain,csv,dict,all}
                        Format of text to be displayed
  --start-time START_TIME
                        Start time with leading zeroes in format --start-time
                        "%m/%d/%y %H:%M:%S"
  --end-time END_TIME   End time with leading zeroes in format --end-time
                        "%m/%d/%y %H:%M:%S"
  --package PACKAGE     Should output be ZIP'd into a user-deliverable format.
                        Provide the package ZIP suffix
```


See [CHANGELOG.md](blob/main/reco/CHANGELOG.md) for recent changes.

### Simplified: three steps to generate a recommendation

1. [Create a Cost and Usage Report](https://docs.aws.amazon.com/cur/latest/userguide/creating-cur.html) (CUR) and query it using Amazon Athena
1. Clone this git repo. Use pip to install numpy.
1. Execute the tool to generate recommendations.

### Step 1: Generate usage data from CUR

You should enable CUR using [this documentation page](https://docs.aws.amazon.com/cur/latest/userguide/creating-cur.html), by first creating a S3 bucket and then by enabling the report. Keep in mind the following when enabling the report (or selecting an existing report to use for this tool:

1. When you enable CUR, you **must enable hourly granularity** because the tool needs hourly data or else it will return an error.
1. When asked to choose **Enable report data integration for** you must choose Athena. You will query the data via Athena

Once CUR is enabled, you will need to wait days for data to accumulate for proceeding. If you have an existing CUR report enabled, ensure the granularity is set to hour.

#### Query the data via Athena

1. [Follow this guide in the AWS CUR documentation](https://docs.aws.amazon.com/cur/latest/userguide/use-athena-cf.html) to setup the Athena datasource via AWS CloudFormation so you can query the data. You must create the stack in order to have a datasource to query.
1. Switch to the Athena portion of the AWS Management Console if you are not there already.
1. Open the Query editor. For Data source, choose AwsDataCatalog. For Database, choose the name of the CUR report that you made.
1. Create a new query. For the most up to date query, download this python tool and run it with `--athena-sql` in advance, or you can try using the default query supplied below, which is not guaranteed to be up to date
1. Paste in the SQL query. Change the table name to correspond to the table name for your CUR data.
1. Execute the query and download the CSV. It's best to put it in the same directory as this cloned repo.


#### Sample Athena SQL query
```
# Replace 'MyCURReport' and run this SQL script.
SELECT product_product_name as "Service",
line_item_operation as "Operation",
line_item_usage_type as "UsageType",
'' as "IntentionallyLeftBlank",
date_format(line_item_usage_start_date, '%m/%d/%y %H:%i:%S') as "StartTime",
date_format(line_item_usage_end_date, '%m/%d/%y %H:%i:%S') as "EndTime",
sum(line_item_usage_amount) as "UsageValue",
sum(line_item_unblended_cost) as "Cost",
line_item_line_item_description
FROM "MyCURReport"
WHERE product_product_name = 'Amazon DynamoDB'
GROUP BY
product_product_name,
line_item_operation,
line_item_usage_type,
line_item_usage_start_date,
line_item_usage_end_date,
line_item_line_item_description
ORDER BY  
line_item_operation,
line_item_usage_type,
line_item_usage_start_date ASC
;
```

### Step 2: Clone this package and prepare your env

You should install python3 on your laptop along with git. Then, clone this repo. The code has a dependency on numpy at least. See [requirements.txt](requirements.txt). With pip you can install from the requirements file:

`$ pip3 install -r requirements.txt `

Pre-requisites:
- Git
- Python3 environment (This package tested with Python 3.9.10) with numpy
- A usage CSV from Athena

### Step 3: Execute this script
The tool has an action named reco. Reco accepts a `--file-name`. It can output in `{plain, csv, dict}`. The last one is a raw python dict() if you want to code your own output function. The term is adjustable, but if you choose a three year term and the region does not offer a 3 year RC, a 1 year RC will be substituted.

```
$ python3 src/ddbr.py reco --file-name APN1.csv --output plain --term 1 --package test-apn1
Intepreting CUR data.
Recommendation will be generated between 05/01/19 00:00:00 and 05/31/19 23:00:00 based on source data.
Recommendation will be generated for 1 region(s)
Loading CSV into memory. Please wait.
Generating recommendations.
############################################################
DynamoDB Reserved Capacity Report
These recommendations are based on data from 05/01/19 00:00:00 to 05/31/19 23:00:00. RC term length for the report is 1 year(s)
Generated on 03/25/20 19:14:34
Please consult your account’s active reserved capacity reservations to determine the amount of capacity to own. The amounts below do not factor in what you already own. Instead, they reflect the amount you should have in your account. Please contact AWS for a final recommendation on the amount to buy if it’s not attached to this report.
############################################################
************************************************************
               Tokyo region - ap-northeast-1
************************************************************
Capacity type: Reserved Read Capacity Unit (RCU), Term: 1 year(s)
	Reserved capacity to own (not necessarily buy): 49,800 RCU(s), which is bought as 498 units of 100. Upfront cost: $17,031.60
	Effective monthly rate: $3,210.49, monthly rate after first month: $1,763.97
	Monthly savings over the public rate card: $2,249.46 (41.2%)
	min: 8,843.0 median: 48,226.0 max: 97,725.0 average: 49,451.81 std_dev: 15,766.74 sum: 36,792,145.0
Capacity type: Reserved Write Capacity Unit (WCU), Term: 1 year(s)
	Reserved capacity to own (not necessarily buy): 20,300 WCU(s), which is bought as 203 units of 100. Upfront cost: $34,794.20
	Effective monthly rate: $6,600.07, monthly rate after first month: $3,644.95
	Monthly savings over the public rate card: $4,811.92 (42.17%)
	min: 6,488.0 median: 19,643.0 max: 44,344.0 average: 20,672.1 std_dev: 6,019.0 sum: 15,380,042.0
Totals: Effective monthly rate: $9,810.57, monthly rate after first month: $5,408.92, Monthly savings over the public rate card: $7,061.38 (41.85%)
************************************************************
                          Glossary
************************************************************
Reserved capacity to own: How much RC this payer account should own. You must compare the amount to own against your current RC reservations to determine the amount to buy. See report header.
Effective monthly rate: The cost per month, factoring in the upfront RC purchase cost. This is the 'monthly rate after first month' + (upfront cost / months in the RC term). If you already own RC, this amount will be incorrect.
Monthly rate after first month: The price per month if you owned all the recommended RC, not including the upfront cost. This cost includes the usage above the recommended RC plus the RC hourly rate for the recommended number of units.
'...savings over the public rate card': The impact of owning this much RC! This is the money you'll save if you own the suggested amount. These prices are calculated using the public pricing and do not include any credits or negotiated discounts.
'XXX units of 100': DynamoDB reserved capacity is bought in batches of 100 units.
############################################################
End of report.
Totals: Effective monthly rate: $9,810.57, Total savings over the public rate card: $84,736.56 (41.85%), Total upfront purchase cost: $51,825.80
############################################################
```

## Appendix
### Explanation of methodology

This script loads the usage data into memory and then simulates AWS bill calculation with varying amounts of reserved capacity. It's capable of running over any number of billing hours. It's hard coded with the pricing data of many but not all AWS regions. It optimizes for lowest overall bill for a given usage type in a region. It doesn't use statistics or rules of thumb - it's a top down simulation of a bill run and therefore takes tens of seconds to run for a bill with 18+ regions

Here's what it does:

1. Using the CSV, it determines:
 1. The regions involved
 1. The start and end time for the billing data
 1. Whether it has the pricing data required to continue
1. Generates objects to hold the usage data for each usage type in each region.
1. Loads the CSV of usage data into these objects
1. Simulates the purchase of reserved capacity in units of 100 to see which amount lowers the bill the most
1. Generates a human readable output with the recommendation


Caveats / Warnings:
- Does not factor in exiting reserved capacity recommendations. Instead, this tool provides the amount that should be owned. While you might want it to also factor in existing RCs, this is quite complicated. Often our customers will be in the process of buying or exchanging RC reservations when they contact AWS, so the data on the existing reservations needs to be managed by hand anyways.
- `On demand` ALWAYS refers to pre-provisioned capacity using the traditional hourly model that DynamoDB launched with back in 2012. It is in no way related to the on-demand pricing model released in 2018. When we say "On demand rate", we mean that we took the number of unit hours in the period and multiplied them by the price on the public rate card for provisioned capacity.


#### How to explain this report to other people

This script finds the optimal number of reserved capacity units your account should own in a given region for a provided billing period. It does not factor in any existing reserved capacity you own, so the output should be compared against what you own to find out whether you need to buy more.

### Background

RC recommendations are complicated and time consuming. To get them right, you suffer multiple biases and complexities:

1. The number of units of RC to buy depends on whether the account owner wants to buy at 1yr or 3yr terms. The account owner might want to buy more units if they opt for a 3 year term. You always need to talk in terms of reservation duration. Provide both options to the stakeholders.
1. The bill you pull affects the number of units recommended. If you pull a busy month in usage, your recommendation will be too high. To counteract this, pull multiple bills and concatenate them in one file.
1. Even if you know how many RC units should be bought, you can't immediately recommend the number to buy. An account owner may have a combination of 1yr and 3yr RCs on their account. Imagine you determine the account owner needs 100,000 RCUs of reserved capacity at 1yr or 130,000 RCUs at 3yr. They have a 3yr RC expiring in 12 month for 30,000 RCUs and a 1yr RC for 10,000 units expiring in 6 months. Now, what do you recommend? It's not simple - the number of units to recommend are different for each term length, and the existing mixed RC durations muddy the water. Do they need to buy 90,000 units at 3yr!?!  Making RC recos is difficult! THIS is why I pity you. I don't have a simple answer for this problem.


### CSV report terms
- RegionCode - Region in format us-east-1
- UsageType - rcu or wcu
- StartTime - start time for the report
- EndTime - end time for the report
- Term - Duration of years of the RC reservation. All new AWS regions only have 1yr RCs.
- RCUnits - number of units to reserve. This number must be divided by 100 before purchase, because you buy in blocks of 100
- UpfrontCost - Upfront cost of the reserved capacity
- FullODRate - Between the start time and end time, the cost of the usage if zero reserved capacity was owned
- EffectiveRate - The total cost for the simulated bill, including the upfront cost evenly divided over the number of hours in the term. This is the cost of the RC hourly rate for the sim, plus the remaining units in each hour at the public rate, plus the RC upfront cost evenly divided over the number of hours in the term multiplied by the number of hours between StartTime and EndTime
- EffectiveRateExclUpfront - the same as above, but not including the RC upfront cost. This gives you an idea of what the monthly bill will be excluding the intial invoice for the purchase of RC
- PercentSavingsOverOD - How much money the RC reservation will save you over going 100% on demand. See note about on demand.
- Minimum - minimum hourly usage in the billing period
- Median - median hourly usage in the billing period
- Maximum - maximum hourly usage in the billing period
- Average - average hourly usage in the billing period
- StdDev - standard deviation of the hourly usage in the billing period. This is provided to give you a 5-number statistic summary in case you want to make a box plot.
- SumUsage - Sum of the hourly usage in the billing period


### Testing

**NOTE: Testing is only possible by AWS employees with access to test data.**

`PYTHONPATH=./src python -m pytest test/test_functional.py`

`PYTHONPATH=./src python -m pytest test/test_unit.py`

e.g.
```
$ PYTHONPATH=./src python -m pytest test/test_unit.py
============================ test session starts ============================
platform darwin -- Python 3.9.10, pytest-7.1.2, pluggy-0.13.1
rootdir: /workspace/reserved-capacity-reco
collected 15 items                                                          

test/test_unit.py ...............                                     [100%]

============================ 15 passed in 8.12s =============================
```
