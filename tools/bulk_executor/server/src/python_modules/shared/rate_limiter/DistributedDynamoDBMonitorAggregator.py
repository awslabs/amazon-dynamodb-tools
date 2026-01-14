import time
import json
from datetime import datetime
import threading
import sys
import random
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError
from concurrent.futures import ThreadPoolExecutor

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
        self.s3_client = session.client('s3', config=Config(max_pool_connections=50)) # more connections for parallel fetching objects
        self.bucket = bucket
        self.prefix = prefix if prefix.endswith('/') else prefix + '/'
        self.staleness_cutoff = staleness_cutoff
        self.output_key = output_key
        self.interval = interval

        self._stop_event = threading.Event()
        self._thread = None

        if autostart:
            self.start()


    def aggregate_once(self, max_workers: int = 16):
        aggregated_read_rate = 0.0
        aggregated_write_rate = 0.0
        active_workers = 0

        # 1) Collect keys (paginated)
        keys = []
        paginator = self.s3_client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.bucket, Prefix=self.prefix):
            for obj in page.get("Contents", []) or []:
                k = obj["Key"]
                if k.endswith(".json") and not k.endswith(self.output_key):
                    keys.append(k)

        # 2) Concurrently fetch + parse (skip if no keys; aggregates stay zero)
        def fetch(key: str):
            try:
                now = time.time()
                resp = self.s3_client.get_object(Bucket=self.bucket, Key=key)
                data = json.loads(resp["Body"].read().decode("utf-8"))

                timestamp = data.get("timestamp")
                if not isinstance(timestamp, (int, float)):
                    return None
                if (now - timestamp) > self.staleness_cutoff:
                    log.debug(f"[Aggregator] Skipping stale worker {key} (age {(now-timestamp):.1f}s)")
                    return None

                return float(data.get("read_rate", 0) or 0), float(data.get("write_rate", 0) or 0)
            except (BotoCoreError, ClientError, json.JSONDecodeError) as e:
                log.debug(f"[Aggregator] Failed to read {key}: {e}")
                return None

        if keys:
            with ThreadPoolExecutor(max_workers=max_workers) as ex:
                for result in ex.map(fetch, keys):
                    if result is None:
                        continue
                    r, w = result
                    aggregated_read_rate  += r
                    aggregated_write_rate += w
                    active_workers        += 1

        # 3) Write the summary out
        summary = {
            "timestamp": time.time(),
            "aggregated_read_rate": aggregated_read_rate,
            "aggregated_write_rate": aggregated_write_rate,
            "active_workers": active_workers,
            "aggregation_time_utc": datetime.utcnow().replace(microsecond=0).isoformat()
        }

        self.s3_client.put_object(
            Bucket=self.bucket,
            Key=f"{self.prefix}{self.output_key}",
            Body=json.dumps(summary).encode("utf-8"),
        )

        if active_workers:
            log.debug(
                f"[Aggregator] {active_workers} executor threads: "
                f"{aggregated_read_rate:.2f} RCU/s, {aggregated_write_rate:.2f} WCU/s"
            )

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
            loop_start = time.monotonic()

            # Do the work
            self.aggregate_once()

            # Compute a jittered target period (start-to-start)
            target_period = self.interval * random.uniform(0.9, 1.1)

            # How long did the work take?
            elapsed = time.monotonic() - loop_start

            # Sleep the remainder so next iteration starts ~target_period later
            remaining = max(0.0, target_period - elapsed)
            if self._stop_event.wait(remaining):
                break