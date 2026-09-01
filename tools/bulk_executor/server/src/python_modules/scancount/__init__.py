import importlib
import json
import math
import random
import sys
from decimal import Decimal

import boto3
import botocore
from awsglue.context import GlueContext
from awsglue.job import Job
from botocore.config import Config
from pyspark import AccumulatorParam
from pyspark.context import SparkContext


class DecimalEncoder(json.JSONDecoder):
    def decode(self, s):
        result = super().decode(s)
        return {k: Decimal(str(v)) if isinstance(v, float) else v
                for k, v in result.items()}

# Custom Library Imports
sys.path.append('/server/src')
from python_modules.shared.errors import *
from python_modules.shared.pricing import PricingUtility
from python_modules.shared.rate_limiter import (
    RateLimiterAggregator,
    RateLimiterSharedConfig,
    RateLimiterWorker
)
from python_modules.shared.table_info import (
    get_and_print_dynamodb_table_info, get_and_print_table_scan_cost,
    get_dynamodb_throughput_configs)
from python_modules.shared.worker_errors import (
    raise_first_worker_error,
    record_understood_failure,
    record_worker_failure
)

class ListAccumulator(AccumulatorParam):
    def zero(self, initialValue):
        return []

    def addInPlace(self, v1, v2):
        v1.extend(v2)
        return v1

DYNAMO_DB_THROTTLE_EXCEPTION = 'ProvisionedThroughputExceededException'
DYNAMO_DB_VALIDATION_EXCEPTION = 'ValidationException'

def print_dynamodb_table_info(table_name, index_name=None, fraction=1.0):
    region_name = boto3.Session().region_name
    table_info = get_and_print_dynamodb_table_info(table_name, index_name)
    _ = get_and_print_table_scan_cost(table_info, region_name, fraction=fraction)

def run(job, spark_context, glue_context, parsed_args):
    table_name = parsed_args.get('table')
    index_name = parsed_args.get('index')
    filter_expression = parsed_args.get('filter_expression')
    expression_values = parsed_args.get('expression_values')
    expression_names = parsed_args.get('expression_names')
    per_segment = parsed_args.get('per_segment', False)
    segments = int(parsed_args.get('segments', 200))
    sample_fraction = float(parsed_args.get('sample_fraction', 1.0))

    # Rate limiter configuration
    bucket_name = parsed_args.get('s3-bucket-name')
    job_run_id = parsed_args.get("JOB_RUN_ID")

    # Decide which segments to actually scan. TotalSegments stays at the full
    # count so every scanned segment still covers 1/segments of the keyspace;
    # sampling just scans a random subset of those segment indices and
    # extrapolates. Report the *true* fraction (sample_size / segments), which
    # can differ from what was requested once it is rounded to whole segments.
    segment_indices = list(range(segments))
    sampling = sample_fraction < 1.0
    true_fraction = 1.0
    if sampling:
        sample_size = max(1, int(segments * sample_fraction))
        segment_indices = sorted(random.sample(segment_indices, sample_size))
        true_fraction = sample_size / segments
        percent = _format_percent(true_fraction)
        print(f"\nSampling {percent} of segments ({sample_size} of {segments} total)\n")

    print_dynamodb_table_info(table_name, index_name, fraction=true_fraction)

    rate_limiter_shared_config = RateLimiterSharedConfig(
        bucket=bucket_name,
        job_run_id=job_run_id
    )

    rate_limiter_aggregator = RateLimiterAggregator(shared_config=rate_limiter_shared_config)

    # Get monitor options for rate limiting
    monitor_options = get_dynamodb_throughput_configs(parsed_args, table_name, modes=["read"], format="monitor")

    # Since each task might generate errors, let's accumulate them and report intelligently
    error_accumulator = spark_context.accumulator([], ListAccumulator())

    # Distribute work among partitions, each knowing what segment it's to handle.
    # A single segmented scan produces every number we report: the per-segment
    # counts are collected here and the grand total is their sum, so --per-segment
    # costs nothing beyond formatting (no second full-table scan). Each worker
    # scans its segment out of the full `segments`, so counts are comparable
    # whether we scanned all segments or only a sampled subset.
    try:
        rdd = spark_context.parallelize(segment_indices, len(segment_indices))
        segment_counts = rdd.map(
            lambda segment: (segment, _count_data(
                monitor_options, table_name, index_name, filter_expression,
                expression_values, expression_names, segment, segments,
                error_accumulator, rate_limiter_shared_config))
        ).collect()
    except Exception as e:
        raise Exception(f"Error in parallel execution: {get_error_message(e)}") from None
    finally:
        rate_limiter_aggregator.shutdown()
    raise_first_worker_error(error_accumulator)

    scanned_count = sum(count for _, count in segment_counts)

    if sampling:
        # The per-segment view (skew) is what justifies the error margin, so
        # print it first, then the extrapolated estimate it feeds into.
        if per_segment:
            _print_per_segment_counts(segment_counts, len(segment_counts))
        _print_sampled_estimate(segment_counts, segments, scanned_count,
                                per_segment_shown=per_segment)
    else:
        print(f"Total records counted: {scanned_count:,}")
        if per_segment:
            _print_per_segment_counts(segment_counts, segments)


def _format_percent(fraction):
    """Format a fraction as a percentage, trimmed to at most 3 decimals with no
    trailing zeros (e.g. 0.1 -> '10%', 4/201 -> '1.99%'). The exact segment
    count printed alongside carries the authoritative precision."""
    return f"{fraction * 100:.3f}".rstrip('0').rstrip('.') + '%'


def _print_sampled_estimate(segment_counts, total_segments, scanned_count,
                            per_segment_shown=False):
    """Extrapolate a full-table count from a sample of segments and print it
    with a 95% confidence interval.

    DynamoDB parallel-scan segments partition the keyspace by hash, so segment
    sizes are i.i.d. in expectation — a textbook finite-population sample. With
    ``k`` sampled segments out of ``N`` total, sample mean ``m`` and sample
    stddev ``s``:

        estimate = N * m
        SE       = N * (s / sqrt(k)) * sqrt(1 - k/N)   # finite-population correction
        95% CI   = estimate ± 1.96 * SE

    The relative margin is driven by ``s/m`` — the spread across segments, i.e.
    data skew — so a skewed table yields a wider interval at the same sample
    fraction, and we say so. A single sampled segment (k=1) has no measurable
    spread, so we report the point estimate without an interval.

    ``per_segment_shown`` suppresses the "Add --per-segment" hint when the
    per-segment table was already printed above this estimate.
    """
    counts = [count for _, count in segment_counts]
    k = len(counts)
    mean = sum(counts) / k if k > 0 else 0
    estimate = total_segments * mean

    print(f"\nSampled count:   {scanned_count:>15,}   (across {k} of {total_segments} segments)")

    if k < 2:
        print(f"Estimated total: {round(estimate):>15,}")
        print("Note: only one segment sampled — too small for a confidence interval. "
              "Raise --sample-fraction for an error margin.")
        return

    variance = sum((c - mean) ** 2 for c in counts) / (k - 1)
    stddev = math.sqrt(variance)
    fpc = math.sqrt(max(0.0, 1 - k / total_segments))
    std_error = total_segments * (stddev / math.sqrt(k)) * fpc
    margin = 1.96 * std_error
    low = max(0, round(estimate - margin))
    high = round(estimate + margin)

    if estimate > 0:
        rel = margin / estimate * 100
        print(f"Estimated total: {round(estimate):>15,}   ± {rel:.1f}%  "
              f"(95% CI: {low:,} – {high:,})")
    else:
        print(f"Estimated total: {round(estimate):>15,}")
        print("Note: every sampled segment was empty; no items found in the sample.")
        return

    cv = stddev / mean if mean > 0 else 0
    hint = "" if per_segment_shown else " Add --per-segment to inspect it."
    if cv > 1.0:
        print("Note: high variation across sampled segments (data skew) widens this "
              f"margin.{hint}")
    else:
        print(f"Note: estimate assumes segment sizes are representative.{hint}")


def _print_per_segment_counts(segment_counts, parallelize_count):
    """Print per-segment item counts sorted by count descending.

    ``segment_counts`` is the ``(segment, count)`` list already produced by the
    single scan in :func:`run`; this only sorts and formats it — it does not scan
    the table again.
    """
    segment_counts = sorted(segment_counts, key=lambda x: x[1], reverse=True)

    total = sum(count for _, count in segment_counts)
    mean = total / parallelize_count if parallelize_count > 0 else 0

    print(f"\n{'Segment':>8}  {'Count':>12}  {'% of Total':>10}")
    print(f"{'-------':>8}  {'-----':>12}  {'----------':>10}")
    for segment, count in segment_counts:
        pct = (count / total * 100) if total > 0 else 0
        print(f"{segment:>8}  {count:>12,}  {pct:>9.1f}%")
    print(f"{'-------':>8}  {'-----':>12}  {'----------':>10}")
    print(f"{'Total':>8}  {total:>12,}")
    print(f"{'Mean':>8}  {mean:>12,.1f}")

    if mean > 0:
        max_count = segment_counts[0][1]
        skew_ratio = max_count / mean
        print(f"\nSkew ratio (max/mean): {skew_ratio:.2f}x")
        if skew_ratio > 5:
            print("WARNING: Significant data skew detected. "
                  "The hottest segment has >5x the average item count.")


def _count_data(monitor_options, table_name, index_name, filter_expression,
                expression_values, expression_names, segment, total_segments,
                error_accumulator, rate_limiter_shared_config):
    """Run the rate-limited, segmented ``Select=COUNT`` scan for one segment and
    return its item count.

    Any exception raised while scanning is recorded on ``error_accumulator`` and
    then swallowed, so the RateLimiterWorker is always shut down and the partial
    count scanned so far is still returned. ``run`` collects these counts, sums
    them for the grand total, and reuses the same list for the --per-segment
    report — so a single scan feeds every number reported.
    """
    rate_limiter_worker = RateLimiterWorker(
        shared_config=rate_limiter_shared_config,
        **monitor_options
    )

    session = rate_limiter_worker.get_session()
    dynamodb_resource = session.resource('dynamodb', config=Config(
        connect_timeout=4.0,
        read_timeout=4.0,
        retries={
            'mode': 'standard',
            'total_max_attempts': 50
        }
    ))

    local_count = 0

    try:
        table = dynamodb_resource.Table(table_name)

        scan_kwargs = {
            "Select": "COUNT",
            "Segment": segment,
            "TotalSegments": total_segments
        }
        if index_name:
            scan_kwargs["IndexName"] = index_name
        if filter_expression:
            scan_kwargs["FilterExpression"] = filter_expression
        if expression_names:
            scan_kwargs["ExpressionAttributeNames"] = json.loads(expression_names, cls=DecimalEncoder)
        if expression_values:
            scan_kwargs["ExpressionAttributeValues"] = json.loads(expression_values, cls=DecimalEncoder)

        while True:
            response = table.scan(**scan_kwargs) # We do 50 retries within the SDK so shouldn't see a throttle response
            local_count += response.get("Count", 0)
            if "LastEvaluatedKey" not in response:
                break
            scan_kwargs["ExclusiveStartKey"] = response["LastEvaluatedKey"]
    except botocore.exceptions.ClientError as e:
        if get_error_code(e) == DYNAMO_DB_VALIDATION_EXCEPTION:
            # The user's own FilterExpression is rejected here, in a worker: the client
            # can only check that #name/:value substitutions have their maps, not that
            # the expression itself is valid, so DynamoDB is the first thing to see it.
            # A typo is not a bug of ours to dump frames for.
            record_understood_failure(
                error_accumulator,
                "Invalid scan request, most likely --filter-expression or its "
                f"--expression-names/--expression-values: {get_error_message(e)}")
        else:
            record_worker_failure(error_accumulator, e, f"Error in worker {segment}")
        # Let control drop down to exit
    except Exception as e:
        record_worker_failure(error_accumulator, e, f"Error in worker {segment}")
        # Let control drop down to exit
    finally:
        rate_limiter_worker.shutdown()

    print(f"Worker {segment}/{total_segments} counted {local_count} records.")
    return local_count
