import json
import random
import sys

import boto3
from boto3 import Session
from botocore.config import Config

sys.path.append('/server/src')
from python_modules.shared.errors import get_error_message
from python_modules.shared.table_info import (
    get_and_print_dynamodb_table_info,
    get_and_print_table_scan_cost,
    get_dynamodb_throughput_configs
)

from python_modules.shared.rate_limiter import (
    RateLimiterAggregator,  
    RateLimiterSharedConfig,
    RateLimiterWorker
)

PRINT_LIMIT = 100

class SegmentStream:
    def __init__(self, session, table_name, segment, total_segments, consistent_read, pk, sk):
        self.segment = segment
        self.total_segments = total_segments
        self.consistent_read = consistent_read
        self.last_evaluated_key = None
        self.items = []
        self.last_page = False
        self.table_name = table_name

        # use the low level Client API so that Items can be compared easily later
        # as they are built entirely of strings
        self.dynamodb = session.client('dynamodb', config=Config(
            connect_timeout=4.0,
            read_timeout=4.0,
            retries={
                'mode': 'standard',
                'total_max_attempts': 50
            }
        ))

        self.pk = pk
        self.sk = sk
        self.has_sort_key = self.sk is not None

    def _load_page(self):
        if self.last_page: return

        kwargs={
            'TableName' : self.table_name,
            'Segment' : self.segment,
            'TotalSegments' : self.total_segments,
            'ConsistentRead' : self.consistent_read
        }

        if self.last_evaluated_key:
            kwargs['ExclusiveStartKey'] = self.last_evaluated_key

        response = self.dynamodb.scan(**kwargs)

        if 'LastEvaluatedKey' in response:
            self.last_evaluated_key = response['LastEvaluatedKey']
        else:
            self.last_page = True

        self.items.extend(response['Items'])

    def head(self):
        return self.peek(0)

    def head_pk(self):
        return self.peek_pk(0)

    def head_sk(self):
        return self.peek_sk(0)

    def head_key(self):
        item = self.head()
        if not item:
            return None
        key = { self.pk : item[self.pk] }
        if self.sk:
            key[self.sk] = item[self.sk]
        return key

    def peek(self, n=0):
        self._ensure_loaded(n + 1) # because request is 0-based, we need 1 more than n
        if n < len(self.items):
            return self.items[n]
        return None

    def _ensure_loaded(self, quantity):
        while len(self.items) <= quantity and not self.last_page:
            self._load_page()

    def peek_pk(self, n):
        item = self.peek(n)
        if item:
            return next(iter(item[self.pk].values()))
        return None

    def peek_sk(self, n):
        item = self.peek(n)
        if item and self.sk:
            return next(iter(item[self.sk].values()))
        return None

    def is_finished(self):
        return self.last_page and not self.items

    def advance(self):
        if self.items:
            del self.items[0]

def item_matches(stream_a_item, stream_b_item):
    a, b = json.dumps(stream_a_item, sort_keys=True), json.dumps(stream_b_item, sort_keys=True)
    return a == b

def format_item_with_keys_first(item, pk, sk=None):
    ordered = {}

    # Add pk and sk first
    ordered[pk] = item[pk]
    if sk:
        ordered[sk] = item[sk]

    # Add remaining keys in sorted order, skipping pk and sk
    for k in sorted(item):
        if k not in ordered:
            ordered[k] = item[k]

    return ordered

def log_diff(symbol, stream, concise_format):
    item = stream.head()
    if item is None:
        return ''
    if concise_format:
        return f"{symbol} {json.dumps(stream.head_key(), separators=(',', ': '))}"
    else:
        ordered = format_item_with_keys_first(item, stream.pk, stream.sk)
        return f"{symbol} {json.dumps(ordered, separators=(',', ': '))}"


def diff_segment(stream_a_name, stream_b_name, monitor_options_a, monitor_options_b, segment, total_segments, consistent_read, concise_format, job_id, use_s3, bucket, schema_broadcast, rate_limiter_shared_config):
    rate_limiter_worker_a = RateLimiterWorker(
        shared_config=rate_limiter_shared_config,
        **monitor_options_a
    )

    rate_limiter_worker_b = RateLimiterWorker(
        shared_config=rate_limiter_shared_config,
        **monitor_options_b
    )

    schema = schema_broadcast.value

    try:
        stream_a = SegmentStream(rate_limiter_worker_a.get_session(), stream_a_name, segment, total_segments, consistent_read, pk=schema['table1']['pk'], sk=schema['table1']['sk'])
        stream_b = SegmentStream(rate_limiter_worker_b.get_session(), stream_b_name, segment, total_segments, consistent_read, pk=schema['table2']['pk'], sk=schema['table2']['sk'])

        diff = []

        while not stream_a.is_finished() and not stream_b.is_finished():
            pk_a = stream_a.head_pk()
            pk_b = stream_b.head_pk()

            if pk_a is None or pk_b is None:
                break  # One of the streams is empty; outer loop should check for is_finished()

            if pk_a == pk_b:
                pk_val = pk_a
                while stream_a.head_pk() == pk_val and stream_b.head_pk() == pk_val:
                    if not stream_a.has_sort_key:
                        # SK-less mode: just compare and advance
                        if not item_matches(stream_a.head(), stream_b.head()):
                            if concise_format:
                                diff.append(log_diff('*', stream_a, True))
                            else:
                                diff.append(log_diff('-', stream_a, False))
                                diff.append(log_diff('+', stream_b, False))
                        stream_a.advance()
                        stream_b.advance()
                    else:
                        sk_a = stream_a.head_sk()
                        sk_b = stream_b.head_sk()

                        if sk_a is None and sk_b is None:
                            break  # Can't compare further if both are exhausted

                        if sk_a == sk_b:
                            if not item_matches(stream_a.head(), stream_b.head()):
                                if concise_format:
                                    diff.append(log_diff('*', stream_a, True))
                                else:
                                    diff.append(log_diff('-', stream_a, False))
                                    diff.append(log_diff('+', stream_b, False))
                            stream_a.advance()
                            stream_b.advance()
                        elif sk_b is None or (sk_a is not None and sk_a < sk_b):
                            diff.append(log_diff('-', stream_a, concise_format))
                            stream_a.advance()
                        else:
                            diff.append(log_diff('+', stream_b, concise_format))
                            stream_b.advance()

                if stream_a.has_sort_key:
                    while stream_a.head_pk() == pk_val:
                        diff.append(log_diff('-', stream_a, concise_format))
                        stream_a.advance()

                    while stream_b.head_pk() == pk_val:
                        diff.append(log_diff('+', stream_b, concise_format))
                        stream_b.advance()

            else:
                # Streams are not aligned on pk; scan forward in both streams to find a common pk
                seen_pk_a = set()
                seen_pk_b = set()

                pk_a = stream_a.head_pk()
                pk_b = stream_b.head_pk()

                if pk_a is not None:
                    seen_pk_a.add(pk_a)
                if pk_b is not None:
                    seen_pk_b.add(pk_b)

                use_a = True
                n = 1

                # Alternate peeking ahead in both streams until we find a matching pk
                while not (seen_pk_a & seen_pk_b) and (stream_a.peek(n) or stream_b.peek(n)):
                    if use_a:
                        pk = stream_a.peek_pk(n)
                        if pk is not None:
                            seen_pk_a.add(pk)
                    else:
                        pk = stream_b.peek_pk(n)
                        if pk is not None:
                            seen_pk_b.add(pk)

                    use_a = not use_a
                    if use_a:
                        n += 1

                aligning_pks = seen_pk_a & seen_pk_b
                aligning_pk = next(iter(aligning_pks), None)

                # Emit diffs for all items before the aligning_pk
                while stream_a.head_pk() is not None and stream_a.head_pk() != aligning_pk:
                    diff.append(log_diff('-', stream_a, concise_format))
                    stream_a.advance()

                while stream_b.head_pk() is not None and stream_b.head_pk() != aligning_pk:
                    diff.append(log_diff('+', stream_b, concise_format))
                    stream_b.advance()


        # if one stream contains 0 items, we couldn't match or peek. Dump out the rest of the stream
        while not stream_a.is_finished():
            diff.append(log_diff('-', stream_a, concise_format))
            stream_a.advance()

        while not stream_b.is_finished():
            diff.append(log_diff('+', stream_b, concise_format))
            stream_b.advance()
    finally:
        rate_limiter_worker_a.shutdown()
        rate_limiter_worker_b.shutdown()

    if use_s3:
        boto3.client('s3').put_object(Body="\n".join(diff), Bucket=bucket, Key=f"{job_id}/{segment}.txt")
        return len(diff)

    return diff[0:PRINT_LIMIT]

def print_dynamodb_table_info(table_name, fraction=1.0):
    region_name = boto3.Session().region_name
    table_info = get_and_print_dynamodb_table_info(table_name)
    return get_and_print_table_scan_cost(table_info, region_name, fraction=fraction)

def run(job, spark_context, glue_context, parsed_args):
    splits = int(parsed_args.get('splits', '400'))
    sample_fraction = float(parsed_args.get('sample_fraction', '1.0'))

    table1 = parsed_args.get('table')
    table2 = parsed_args.get('table2')
    diff_type = parsed_args.get('format', 'keys') # keys or full
    use_s3 = parsed_args.get('s3')
    job_id = parsed_args.get("JOB_RUN_ID")
    bucket = parsed_args.get('s3-bucket-name')

    segment_indices = list(range(splits))
    true_fraction = 1.0
    if sample_fraction < 1.0:
        sample_size = max(1, int(splits * sample_fraction))
        segment_indices = sorted(random.sample(segment_indices, sample_size))
        true_fraction = sample_size / splits
        print()
        percent = f"{true_fraction * 100:.10f}".rstrip('0').rstrip('.') + '%' # no zeros in decimal
        print(f"Sampling {percent} of segments ({sample_size} of {splits} total): {segment_indices}")
        print()

    table1_cost = print_dynamodb_table_info(table1, fraction=true_fraction)
    print()
    table2_cost = print_dynamodb_table_info(table2, fraction=true_fraction)
    total_cost = table1_cost + table2_cost
    print()
    print(f"TOTAL DynamoDB cost for scanning both tables (approx): ${total_cost:,.2f}")
    print()

    schema1 = boto3.client("dynamodb").describe_table(TableName=table1)['Table']['KeySchema']
    schema2 = boto3.client("dynamodb").describe_table(TableName=table2)['Table']['KeySchema']

    def extract_keys(schema):
        pk = next(e['AttributeName'] for e in schema if e['KeyType'] == 'HASH')
        sk = next((e['AttributeName'] for e in schema if e['KeyType'] == 'RANGE'), None)
        return pk, sk

    pk1, sk1 = extract_keys(schema1)
    pk2, sk2 = extract_keys(schema2)

    # Bit more efficient to broadcast, so let's play with that feature
    broadcast_schema = spark_context.broadcast({
        'table1': {'pk': pk1, 'sk': sk1},
        'table2': {'pk': pk2, 'sk': sk2}
    })

    try:
        rdd = spark_context.parallelize(segment_indices, len(segment_indices))
    except Exception as e:
        raise Exception(f"Error in parallel execution: {get_error_message(e)}") from None

    rate_limiter_shared_config = RateLimiterSharedConfig(
        bucket=bucket,
        job_run_id=job_id
    )

    rate_limiter_aggregator = RateLimiterAggregator(shared_config=rate_limiter_shared_config)

    monitor_options_1 = get_dynamodb_throughput_configs(parsed_args, table1, modes=("read"), format="monitor")
    monitor_options_2 = get_dynamodb_throughput_configs(parsed_args, table2, modes=("read"), format="monitor")

    try:
        rdd2 = rdd.map(lambda worker_id: diff_segment(table1, table2, monitor_options_1, monitor_options_2, worker_id, splits, False, diff_type == 'keys', job_id, use_s3, bucket, broadcast_schema, rate_limiter_shared_config)).collect()
    except Exception as e:
        raise Exception(f"Error in parallel execution: {get_error_message(e)}") from None
    finally:
        rate_limiter_aggregator.shutdown()

    if use_s3:
        total = sum(rdd2)
        if total == 0:
            print("No differences found")
        else:
            print(f"There are {total} differences. These can be found in files at s3://{bucket}/{job_id}/")
    else:
        count = 0
        for e in rdd2:
            for r in e:
                if count < PRINT_LIMIT:
                    print(r)
                count = count + 1

        if count == 0:
            print("No differences found")
        elif count <= PRINT_LIMIT:
            print(f"There are {count} differences.")
        else:
            print(f"(output truncated). There are {count} differences, printed first {PRINT_LIMIT}. Use the --s3 flag to store them all in S3.")
    print()
