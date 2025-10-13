import json
import threading
import time
import uuid
from collections import defaultdict
from .DynamoDBMonitor import DynamoDBMonitor

class DistributedDynamoDBMonitorWorker:
    def __init__(self, session, bucket, prefix,
                 aggregate_max_read_rate=100000,
                 aggregate_max_write_rate=50000,
                 worker_max_read_rate=1500,
                 worker_max_write_rate=500,
                 worker_initial_read_rate=None,
                 worker_initial_write_rate=None,
                 reset_interval=10,
                 sync_interval=5,
                 worker_id=None,
                 summary_key='summary.json',
                 enable_reporting=False,
                 autostart=True):

        self.session = session
        self.s3_client = session.client('s3')
        self.bucket = bucket
        self.prefix = prefix if prefix.endswith('/') else prefix + '/'
        self.summary_key = summary_key
        self.sync_interval = sync_interval
        self.stop_event = threading.Event()

        self.worker_id = worker_id or str(uuid.uuid4())

        # default initial rates
        if worker_initial_read_rate is None:
            worker_initial_read_rate = min(worker_max_read_rate, aggregate_max_read_rate / 10)
        if worker_initial_write_rate is None:
            worker_initial_write_rate = min(worker_max_write_rate, aggregate_max_write_rate / 10)

        self.aggregate_max_read_rate = aggregate_max_read_rate
        self.aggregate_max_write_rate = aggregate_max_write_rate
        self.worker_max_read_rate = worker_max_read_rate
        self.worker_max_write_rate = worker_max_write_rate

        # instantiate local monitor
        self.monitor = DynamoDBMonitor(
            session=session,
            max_read_rate=worker_initial_read_rate,
            max_write_rate=worker_initial_write_rate,
            reset_interval=reset_interval,
            enable_reporting=enable_reporting
        )

        # start background sync thread
        self._sync_thread = None
        if autostart:
            self.start()

    def _sync_loop(self):
        while not self.stop_event.is_set():
            try:
                time.sleep(self.sync_interval)

                # Upload own metrics to worker-specific S3 file
                upload_key = f"{self.prefix}worker-{self.worker_id}.json"
                timestamp = time.time()

                with self.monitor.metrics_lock, self.monitor.rate_limit_lock:
                    elapsed = time.monotonic() - self.monitor.rate_limit_state['checkpoint_time']
                    elapsed = max(elapsed, 1e-6)  # avoid division by zero

                    read_rate = self.monitor.rate_limit_state['read_so_far'] / elapsed
                    write_rate = self.monitor.rate_limit_state['write_so_far'] / elapsed

                    payload = json.dumps({
                        "worker_id": self.worker_id,
                        "timestamp": timestamp,
                        "read_rate": read_rate,
                        "write_rate": write_rate
                    })
                    #print(f"[{self.worker_id}] Writing {payload} to {upload_key}")

                self.s3_client.put_object(Bucket=self.bucket, Key=upload_key, Body=payload.encode('utf-8'))

                # Read aggregator summary (if exists)
                try:
                    resp = self.s3_client.get_object(Bucket=self.bucket, Key=f"{self.prefix}{self.summary_key}")
                    summary_data = json.loads(resp['Body'].read().decode('utf-8'))

                    current_agg_read_rate = summary_data.get("aggregated_read_rate", 0.0)
                    current_agg_write_rate = summary_data.get("aggregated_write_rate", 0.0)
                    #print(f"[{self.worker_id}] Pulled current_agg_read_rate={current_agg_read_rate} current_agg_write_rate={current_agg_write_rate}")
                    # Compute scaling factor
                    read_scale = self.aggregate_max_read_rate / current_agg_read_rate if current_agg_read_rate > 0 else 1.0
                    write_scale = self.aggregate_max_write_rate / current_agg_write_rate if current_agg_write_rate > 0 else 1.0
                    #print(f"[{self.worker_id}] Scaling read_scale={read_scale} write_scale={write_scale}")

                    # Apply scaling to what we're experiencing right now
                    worker_allowed_read_rate = read_scale * self.monitor.max_read_rate
                    worker_allowed_write_rate = write_scale * self.monitor.max_write_rate

                    # Choose new local target rate (capped at per-worker max)
                    new_read_target = min(worker_allowed_read_rate, self.worker_max_read_rate)
                    new_write_target = min(worker_allowed_write_rate, self.worker_max_write_rate)

                    # Smooth adjustment to avoid oscillation
                    #smoothing_factor = 0.2  # 0=no change, 1=jump immediately
                    smoothing_factor = 0.4  # 0=no change, 1=jump immediately
                    self.monitor.max_read_rate = (1 - smoothing_factor) * self.monitor.max_read_rate + smoothing_factor * new_read_target
                    self.monitor.max_write_rate = (1 - smoothing_factor) * self.monitor.max_write_rate + smoothing_factor * new_write_target

                    #print(f"[{self.worker_id}] Adjusted target: reads={self.monitor.max_read_rate} writes={self.monitor.max_write_rate:.2f}")

                except self.s3_client.exceptions.NoSuchKey:
                    # No summary file yet, continue using current rates
                    print(f"[{self.worker_id}] No summary file found; using current rate limits")

            except Exception as e:
                print(f"[{self.worker_id}] Error in sync loop: {e}")

    def start(self):
        """Start sync thread (no-op if already running)."""
        if self._sync_thread and self._sync_thread.is_alive():
            return  # already running
        self.stop_event.clear()
        self._sync_thread = threading.Thread(target=self._sync_loop, daemon=True)
        self._sync_thread.start()

    def stop(self):
        self.monitor.stop()
        self.stop_event.set()
        if self._sync_thread:
            self._sync_thread.join()

    def cleanup(self):
        self.stop()

        # remove own S3 metrics file
        upload_key = f"{self.prefix}worker-{self.worker_id}.json"
        try:
            self.s3_client.delete_object(Bucket=self.bucket, Key=upload_key)
        except Exception as e:
            print(f"[{self.worker_id}] Warning: failed to delete {upload_key}: {e}")
