from ...logger import log
from ..writers.writer_factory import WriterFactory


def write(spark_context, items_rdd, export_load_type, table_name, rate_limiter_shared_config, monitor_options, error_accumulator, debug_accumulator):
    """Repartition and write items to DynamoDB.

    Returns:
        written_items_accumulator
    """
    num_partitions = min(items_rdd.getNumPartitions(), spark_context.defaultParallelism * 2)

    log.debug(f"Using {num_partitions:,} partitions (based on min of #gz files {items_rdd.getNumPartitions():,} and defaultParallelism {spark_context.defaultParallelism:,})")
    items_rdd = items_rdd.repartition(num_partitions)
    log.debug(f"Repartitioned to {num_partitions:,} partitions")

    log.info("Writing items to DynamoDB...")
    writer = WriterFactory.create_writer()
    log.debug(f"Using batch writer for {export_load_type.value} load")

    written_items_accumulator = spark_context.accumulator(0)

    items_rdd.foreachPartition(
        lambda partition: writer.write_partition_to_dynamodb(
            partition,
            table_name,
            rate_limiter_shared_config,
            monitor_options,
            error_accumulator,
            debug_accumulator,
            written_items_accumulator
        )
    )

    if error_accumulator.value:
        first_error = error_accumulator.value[0]
        raise Exception(first_error) from None

    return written_items_accumulator
