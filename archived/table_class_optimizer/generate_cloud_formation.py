from textwrap import indent

REPLACEMENTS = {
    ("ARRAY['ALL'] AS account_ids", "ARRAY[${AccountIds}] AS account_ids"),
    ("ARRAY['ALL'] AS payer_ids", "ARRAY[${PayerIds}] AS payer_ids"),
    ("ARRAY['ALL'] AS table_names", "ARRAY[${TableNames}] AS table_names"),
    ("ARRAY['ALL'] AS region_names", "ARRAY[${RegionNames}] AS region_names"),
    ("50 AS min_savings_per_month", "${MinimumSavings} AS min_savings_per_month"),
    ("'NET' AS cost_type", "'${PricingTerms}' AS cost_type"),
    ("[CUR_DB]", "${AthenaCURDatabase}"),
    ("[CUR_TABLE]", "${AthenaCURTable}"),
}


def replace_query_parameters(query: str) -> str:
    for replacement in REPLACEMENTS:
        query = query.replace(replacement[0], replacement[1])
    return query


def my_indenter(num: int, text: str) -> str:
    return indent(text, " " * num)[num:]


def main():
    with open("./DDB_TableClassReco.sql") as sql, open(
        "./lambda_handler.py"
    ) as lambda_handler, open("./raw_template.yaml") as stack_template, open(
        "./template.yaml", "w"
    ) as output:
        result: str = (
            stack_template.read()
            .replace(
                "{{ athena_query_string }}",
                my_indenter(10, replace_query_parameters(sql.read())),
            )
            .replace(
                "{{ lambda_handler_code }}", my_indenter(10, lambda_handler.read())
            )
        )
        output.write(result)
    print(
        "Generated `template.yaml`. You can now use it to create a CloudFormation stack."
    )
    print(
        "For more information see https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/stacks.html"
    )


if __name__ == "__main__":
    main()
