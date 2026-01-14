"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: MIT-0
Permission is hereby granted, free of charge, to any person obtaining a copy of this
software and associated documentation files (the "Software"), to deal in the Software
without restriction, including without limitation the rights to use, copy, modify,
merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
permit persons to whom the Software is furnished to do so.
THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

ddb_table.py Contains helper functions for the ddb_cost_tool.py

"""


def prettify_describe_json(describe_ddb: dict) -> dict:
    prov_throughput = describe_ddb.get("ProvisionedThroughput")
    if prov_throughput:
        describe_ddb["throughput"] = {}
        describe_ddb["throughput"]["rcu"] = prov_throughput["ReadCapacityUnits"]
        describe_ddb["throughput"]["wcu"] = prov_throughput["WriteCapacityUnits"]

    billing_mode = describe_ddb.get("BillingModeSummary")
    if billing_mode:
        describe_ddb["billing_mode"] = (
            "on_demand"
            if billing_mode["BillingMode"] == "PAY_PER_REQUEST"
            else "provisioned"
        )
    else:
        describe_ddb["billing_mode"] = "provisioned"

    stream_spec = describe_ddb.get("StreamSpecification")
    describe_ddb["stream_spec"] = {}
    if stream_spec:
        describe_ddb["stream_spec"]["stream_enabled"] = stream_spec["StreamEnabled"]
        describe_ddb["stream_spec"]["stream_view_type"] = stream_spec["StreamViewType"]
        describe_ddb["stream_spec"]["stream_arn"] = describe_ddb["LatestStreamArn"]
    else:
        describe_ddb["stream_spec"]["stream_enabled"] = False
    return describe_ddb


def get_metric_dimensions(table):
    table_name = table["TableName"]
    base_dimension = {"Name": "TableName", "Value": f"{table_name}"}
    global_secondary_indexes = table.get("GlobalSecondaryIndexes", [])
    local_secondary_indexes = table.get("LocalSecondaryIndexes", [])
    table_dimensions = [[base_dimension]]
    if global_secondary_indexes:
        for gsi in global_secondary_indexes:
            if gsi:
                table_dimensions.append(
                    [
                        base_dimension,
                        {
                            "Name": "GlobalSecondaryIndexName",
                            "Value": gsi["IndexName"],
                        },
                    ]
                )

    if local_secondary_indexes:
        for lsi in local_secondary_indexes:
            if lsi:
                table_dimensions.append(
                    [
                        base_dimension,
                        {
                            "Name": "LocalSecondaryIndexName",
                            "Value": lsi["IndexName"],
                        },
                    ]
                )
    return table_dimensions
