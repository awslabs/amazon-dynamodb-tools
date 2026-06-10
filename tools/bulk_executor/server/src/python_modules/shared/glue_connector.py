"""Centralized Glue→DynamoDB connector entry points.

This module is the single seam where bulk_executor talks to Glue's
DynamoDB connector. It uses the DataFrame-based source AWS shipped in
November 2025
(https://aws.amazon.com/about-aws/whats-new/2025/11/glue-dynamodb-connector/),
which requires Glue 5.0+ and a Glue connection of ``ConnectionType=DYNAMODB``
attached to the job. Bootstrap handles both requirements; verbs go through
this module without caring about the underlying Spark API.

Public entry points
-------------------
- ``read_dynamodb_dataframe(glue_context, table_name, parsed_args, splits)``
  Returns a ``pyspark.sql.DataFrame``.

- ``write_dynamodb_dataframe(glue_context, frame, table_name, parsed_args)``
  Writes a DataFrame (or DynamicFrame, transparently converted) to the
  named DynamoDB table.

- ``count_dynamodb_table(glue_context, table_name, parsed_args, splits)``
  Returns an int row count via ``DataFrame.count()``.

All entry points log table name and elapsed seconds for observability.
"""

import time
from typing import Any

from .logger import log


def read_dynamodb_dataframe(
    glue_context,
    table_name: str,
    parsed_args: dict,
    splits: int = 200,
):
    """Read a DynamoDB table into a Spark DataFrame."""
    start = time.monotonic()
    try:
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
    finally:
        elapsed = time.monotonic() - start
        log.info(
            f"[connector] read setup for '{table_name}' took "
            f"{elapsed:.3f}s (Spark execution is lazy; this is config + "
            f"reader-construction time only)"
        )


def write_dynamodb_dataframe(
    glue_context,
    frame: Any,
    table_name: str,
    parsed_args: dict,
) -> None:
    """Write a DataFrame (or DynamicFrame) to DynamoDB."""
    start = time.monotonic()
    try:
        df = _ensure_dataframe(frame)
        writer = (
            df.write.format("dynamodb")
            # Spark's default save mode is ErrorIfExists, which the Glue 5.0
            # DataFrame-based DynamoDB connector rejects ("TableProvider
            # implementation dynamodb cannot be written with ErrorIfExists
            # mode, please use Append or Overwrite modes instead"). DynamoDB
            # writes are upserts (PutItem semantics), so Append is the
            # correct mode -- Overwrite would imply truncating the table,
            # which is not the write contract. The legacy DynamicFrame
            # writer had no such mode requirement; this is a Glue 4.0->5.0
            # migration gap.
            .mode("append")
            .option("dynamodb.output.tableName", table_name)
        )
        rates = _resolve_direct_rates(parsed_args, modes=["write"])
        if rates.get("write") is not None:
            writer = writer.option(
                "dynamodb.throughput.write", str(rates["write"])
            )
        writer.save()
    finally:
        elapsed = time.monotonic() - start
        log.info(
            f"[connector] write of '{table_name}' completed in "
            f"{elapsed:.3f}s"
        )


def count_dynamodb_table(
    glue_context,
    table_name: str,
    parsed_args: dict,
    splits: int = 200,
) -> int:
    """Count items in a DynamoDB table.

    Uses ``DataFrame.count()`` as a single Spark action — the new
    connector materializes once and counts without re-scanning, so the
    ``toDF().count()`` double-scan hazard from issue #81 does not apply.
    """
    start = time.monotonic()
    try:
        df = read_dynamodb_dataframe(
            glue_context, table_name, parsed_args, splits
        )
        return df.count()
    finally:
        elapsed = time.monotonic() - start
        log.info(
            f"[connector] count of '{table_name}' completed in "
            f"{elapsed:.3f}s"
        )


def _ensure_dataframe(frame):
    """Accept a DataFrame or a DynamicFrame; return a DataFrame.

    Lets call sites pass whatever they already have without forcing each
    one to add a defensive toDF() — the wrapper owns that detail.
    """
    if _is_dynamic_frame(frame):
        return frame.toDF()
    return frame


def _is_dynamic_frame(frame) -> bool:
    """Detect a Glue DynamicFrame positively, by its distinguishing API.

    A Glue ``DynamicFrame`` exposes ``toDF()`` (the conversion to a Spark
    DataFrame) and ``write_dynamic_frame``; a Spark ``DataFrame`` has
    neither — so ``toDF`` is the clean discriminator. We detect the
    DynamicFrame rather than the DataFrame because both types carry
    ``write`` and ``schema`` attributes (a DynamicFrame's ``.write`` is a
    *method*, a DataFrame's is a property) — the earlier "has write+schema
    ⇒ DataFrame" heuristic misclassified DynamicFrames and skipped the
    needed toDF(), causing ``'function' object has no attribute 'format'``
    when ``df.write.format(...)`` ran against a DynamicFrame (issue: load
    on Glue 5.0). Using ``toDF`` avoids importing pyspark/awsglue just for
    an isinstance check, which would couple this module to a specific Spark
    version at import time."""
    return hasattr(frame, "toDF")


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
