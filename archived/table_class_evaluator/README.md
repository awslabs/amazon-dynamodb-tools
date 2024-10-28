## Table Class Evaluator [ARCHIVED]
This tool was archived in October 2024 in favor of [table class optimizer](/table_class_optimizer/README.md). **This archived tool does not provide accurate recommendations because it relies on instantaneous throughput, not historical data like the current, recommended tool.**

### Overview

Amazon DynamoDB supports two [table classes](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/HowItWorks.TableClasses.html):

- Standard: The default for new tables, this table class balances storage costs and provisioned throughput.

- Standard Infrequent Access (Standard-IA): This table class offers lower storage pricing and higher throughput pricing comapred to the Standard table class. The Standard-IA table class is a good fit for tables where data is not queried frequently, and can be a good choice for tables using the Standard table class where storage costs exceed 50% of total throughput costs.

The Table Class Evaluator tool evaluates one or more tables in an AWS region for suitability for the Infrequent Access table class. The tool accomplishes this by calculating costs for both table classes for the following cost dimensions:

- AWS Region
- Table storage utilization
- Instantaneous provisioned throughput
- Global Tables replicated writes
- Global Secondary Indexes (GSIs)

The tool will will return recommendations for tables that may benefit from a change in table class.

### Limitations

The Table Class Evaluator tool has the following limitations:

- Estimated costs are calculated from the current (instantaneous) provisioned throughput. If the provisioned capacity of the table being evaluated changes frequently due to Auto Scaling activity, the recommendation could be incorrect.
- Tables using On-Demand pricing are not supported.
- Local Secondary Index costs are not calculated.

### Using the Table Class Evaluator tool

The Table Class Evaluator is a command-line tool written in Python 3, and requires the AWS Python SDK (Boto3) >= 1.23.18. You can find instructions for installing the AWS Python SDK at https://aws.amazon.com/sdk-for-python/. The tool can be run directly from the cloned repository without installation.

The tool is invoked from the command line like so:

```console
user@host$ python3 table_class_evaluator.py --help
usage: table_class_evaluator.py [-h] [--estimates-only] [--region REGION] [--table-name TABLE_NAME] [--profile PROFILE]

Recommend Amazon DynamoDB table class changes to optimize costs.

optional arguments:
  -h, --help            show this help message and exit
  --estimates-only      print table cost estimates instead of change recommendations
  --region REGION       evaluate tables in REGION (default: us-east-1)
  --table-name TABLE_NAME
                        evaluate TABLE_NAME (defaults to all tables in region)
  --profile PROFILE     set a custom profile name to perform the operation under
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