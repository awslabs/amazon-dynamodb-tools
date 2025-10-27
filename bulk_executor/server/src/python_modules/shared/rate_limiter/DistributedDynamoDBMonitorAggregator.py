import time
import json
from datetime import datetime
import threading
import sys

# Custom Library Imports
sys.path.append('/server/src')
from python_modules.shared.logger import log

class DistributedDynamoDBMonitorAggregator:
    def __init__(self, session, bucket, prefix, staleness_cutoff=15, interval=5, output_key='summary.json', autostart=True):
        """
        session: boto3.Session
        bucket: S3 bucket name
        prefix: S3 prefix for worker files (must end with /)
        staleness_cutoff: seconds; ignore workers older than this
        output_key: S3 key to write aggregated summary
        interval: seconds between aggregation runs
        autostart: if background thread should autostart, default True (else use start() later stop())
        """
        self.s3_client = session.client('s3')
        self.bucket = bucket
        self.prefix = prefix if prefix.endswith('/') else prefix + '/'
        self.staleness_cutoff = staleness_cutoff
        self.output_key = output_key
        self.interval = interval

        self._stop_event = threading.Event()
        self._thread = None

        if autostart:
            self.start()

    def aggregate_once(self):
        now = time.time()
        valid_workers = []

        # list all worker files under prefix, paginated in case > 1000 objects
        paginator = self.s3_client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.bucket, Prefix=self.prefix):

            for obj in page.get("Contents", []):
                key = obj['Key']
                if key.endswith('.json') and not key.endswith(self.output_key):
                    try:
                        file_obj = self.s3_client.get_object(Bucket=self.bucket, Key=key)
                        content = file_obj['Body'].read().decode('utf-8')
                        data = json.loads(content)

                        timestamp = data.get('timestamp')
                        if timestamp is None:
                            continue

                        age = now - timestamp
                        if age > self.staleness_cutoff:
                            log.debug(f"[Aggregator] Skipping stale worker {key} (age {age:.1f}s)")
                            continue

                        valid_workers.append(data)

                    except Exception as e:
                        log.debug(f"[Aggregator] Failed to read {key}: {e}")

        # sum rates
        aggregated_read_rate = sum(w['read_rate'] for w in valid_workers)
        aggregated_write_rate = sum(w['write_rate'] for w in valid_workers)

        summary = {
            'timestamp': now,
            'aggregated_read_rate': aggregated_read_rate,
            'aggregated_write_rate': aggregated_write_rate,
            'active_workers': len(valid_workers),
            'aggregation_time_utc': datetime.utcnow().isoformat()
        }

        # write summary back to S3
        summary_json = json.dumps(summary)
        self.s3_client.put_object(Bucket=self.bucket, Key=self.prefix + self.output_key, Body=summary_json.encode('utf-8'))

        output_full_key = self.prefix + self.output_key
        if valid_workers:
            #log.debug(f"[Aggregator] Wrote summary to s3://{self.bucket}/{output_full_key} "
            #      f"with {len(valid_workers)} workers: {aggregated_read_rate:.2f} RCU/s, {aggregated_write_rate:.2f} WCU/s")
            log.debug(f"[Aggregator] {len(valid_workers)} executor threads: {aggregated_read_rate:.2f} RCU/s, {aggregated_write_rate:.2f} WCU/s")

    def start(self):
        """Start aggregation loop in background thread (no-op if already running)."""
        if self._thread and self._thread.is_alive():
            return  # already running
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()

    def stop(self):
        """Signal thread to stop and wait for it to finish."""
        if self._thread and self._thread.is_alive():
            self._stop_event.set()
            self._thread.join()

    def cleanup(self):
        self.stop()

        # remove own S3 summary file
        upload_key = f"{self.prefix}{self.output_key}"
        try:
            self.s3_client.delete_object(Bucket=self.bucket, Key=upload_key)
        except Exception as e:
            log.warn(f"[Aggregator] Warning: failed to delete {upload_key}: {e}")

    def _loop(self):
        while not self._stop_event.is_set():
            self.aggregate_once()
            self._stop_event.wait(self.interval)
