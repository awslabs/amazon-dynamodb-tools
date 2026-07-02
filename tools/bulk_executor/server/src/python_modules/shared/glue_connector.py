"""Centralized Glue→DynamoDB connector entry points.

This module is the single seam where bulk_executor talks to Glue's
DynamoDB connector. It uses the DataFrame-based source AWS shipped in
November 2025
(https://aws.amazon.com/about-aws/whats-new/2025/11/glue-dynamodb-connector/),
which requires Glue 5.x and a Glue connection of ``ConnectionType=DYNAMODB``
attached to the job. Bootstrap handles both requirements; verbs go through
this module without caring about the underlying Spark API.
"""


def read_dynamodb_dataframe(
    glue_context,
    table_name: str,
    parsed_args: dict,
    splits: int = 200,
):
    """Read a DynamoDB table into a Spark DataFrame."""
    spark = glue_context.spark_session
    reader = (
        spark.read.format("dynamodb")
        .option("dynamodb.input.tableName", table_name)
        .option("dynamodb.splits", str(splits))
        .option("dynamodb.consistentRead", "false")
    )

    rates = _resolve_direct_rates(parsed_args, modes=["read"])
    if rates.get("read") is not None:
        reader = reader.option(
            "dynamodb.throughput.read", str(rates["read"])
        )

    return reader.load()


def write_dynamodb_dataframe(
    glue_context,
    df,
    table_name: str,
    parsed_args: dict,
    write_rate: int = None,
) -> None:
    """Write a Spark DataFrame to DynamoDB.

    Args:
        write_rate: Explicit write rate (WCU) to use. When provided, takes
            precedence over XMaxWriteRate in parsed_args. Callers should
            determine this via get_dynamodb_throughput_configs().
    """
    writer = (
        df.write.format("dynamodb")
        .mode("append")
        .option("dynamodb.output.tableName", table_name)
    )
    if write_rate is None:
        rates = _resolve_direct_rates(parsed_args, modes=["write"])
        write_rate = rates.get("write")
    if write_rate is not None:
        writer = writer.option(
            "dynamodb.throughput.write", str(write_rate)
        )
    writer.save()


def _resolve_direct_rates(parsed_args, modes):
    """Pull XMaxReadRate / XMaxWriteRate as direct integers.

    The connector takes integer rates directly — no percent math, no
    on-demand denominator inference. If neither X-flag is set, return
    an empty rate dict and let the connector fall back to its own default
    (dynamodb.throughput.read.ratio=0.5).
    """
    rates = {}
    if "read" in modes and "XMaxReadRate" in parsed_args:
        rates["read"] = int(parsed_args["XMaxReadRate"])
    if "write" in modes and "XMaxWriteRate" in parsed_args:
        rates["write"] = int(parsed_args["XMaxWriteRate"])
    return rates
