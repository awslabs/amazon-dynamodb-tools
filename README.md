# Amazon DynamoDB Tools

 These tools are intended to make using Amazon DynamoDB effectively easier. The following tools are available:

 * [Table Class Evaluator](#table-class-evaluator-tool) - Recommend Amazon DynamoDB table class changes to optimize costs
 * [Eponymous Table Tagger](#eponymous-table-tagger-tool)  - Tag tables with their own name to make per-table cost analysis easier

While we make efforts to test and verify the functionality of these tools, you are encouraged to read and understand the code, and use them at your own risk.

## Table Class Evaluator Tool

### Overview
Amazon DynamoDB supports two [table classes](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/HowItWorks.TableClasses.html): 
* Standard: The default for new tables, this table class balances storage costs and provisioned throughput.  

* Standard Infrequent Access (Standard-IA): This table class offers lower storage pricing and  higher throughput pricing comapred to the Standard table class. The Standard-IA table class is a good fit for tables where data is not queried frequently, and can be a good choice for tables where storage costs comprise more than 50% of the total table cost.

The Table Class Evaluator tool evaluates one or more tables in an AWS region for suitability for the Infrequent Access table class. The tool accomplishes this by calculating costs for both table classes for the following cost dimensions:

* AWS Region
* Table storage utilization
* Instantaneous provisioned throughput
* Global Tables replicated writes
* Global Secondary Indexes (GSIs)

The tool will will return recommendations for tables that may benefit from a change in table class.
    
   
### Limitations

The Table Class Evaluator tool has the following limitations:

* Estimated costs are calculated from the current (instantaneous) provisioned throughput. If the provisioned capacity of the table being evaluated changes frequently due to Auto Scaling activity, the recommendation could be incorrect.
* Tables using On-Demand pricing are not supported.
* Local Secondary Index costs are not calculated.


### Using the Table Class Evaluator tool
The Table Class Evaluator is a command-line tool written in Python 3, and requires the AWS Python SDK (Boto3) >= 1.23.18. You can find instructions for installing the AWS Python SDK at  https://aws.amazon.com/sdk-for-python/. The tool can be run directly from the cloned repository without installation.

The tool is invoked from the command line like so:
```console
user@host$ python3 table_class_evaluator.py --help
usage: table_class_evaluator.py [-h] [--estimates-only] [--region REGION] [--table-name TABLE_NAME]

Recommend Amazon DynamoDB table class changes to optimize costs.

optional arguments:
  -h, --help            show this help message and exit
  --estimates-only      print table cost estimates instead of change recommendations
  --region REGION       evaluate tables in REGION (default: us-east-1)
  --table-name TABLE_NAME
                        evaluate TABLE_NAME (defaults to all tables in region)
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


## Eponymous Table Tagger Tool

### Overview
[AWS Cost Explorer](https://aws.amazon.com/aws-cost-management/aws-cost-explorer/) will group all Amazon DynamoDB table cost categories in a region by default. In order to view table-level cost breakdowns in Cost Explorer (for example, storage costs for a specific table), tables must be tagged so costs can be grouped by that tag. Tagging each DynamoDB table with its own name enables this table-level cost analysis. This tool automatically tags each table in a region with its own name, if it is not already thus tagged.


### Using the Eponymous Table Tagger tool
The Eponymous Table Tagger is a command-line tool written in Python 3, and requires the AWS Python SDK (Boto3). The tool can be run directly from the cloned repository without installation.

The tool is invoked from the command line like so:
```console
user@host$ python3 table_tagger.py --help
usage: table_tagger.py [-h] [--dry-run] [--region REGION] [--table-name TABLE_NAME] [--tag-name TAG_NAME]

Tag all DynamoDB tables in a region with their own name.

optional arguments:
  -h, --help            show this help message and exit
  --dry-run             output results but do not actually tag tables
  --region REGION       tag tables in REGION (default: us-east-1)
  --table-name TABLE_NAME
                        tag only TABLE_NAME (defaults to all tables in region)
  --tag-name TAG_NAME   tag table with tag TAG_NAME (default is "table_name")
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


## Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

## License

This project is licensed under the Apache-2.0 License.