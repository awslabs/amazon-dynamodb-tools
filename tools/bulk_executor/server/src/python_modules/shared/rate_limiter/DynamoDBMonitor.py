import threading
import time
from collections import defaultdict
from .TokenBucket import TokenBucket

class DynamoDBMonitor:
    def __init__(self, session, max_read_rate=1500, max_write_rate=500, enable_reporting=True):
        if max_read_rate < 1 or max_write_rate < 1:
            raise ValueError(
                f"Invalid rate limits: read={max_read_rate}, write={max_write_rate}. "
                "Both must be >= 1 capacity units per second."
            )
        # Private copies
        self._max_read_rate  = float(max_read_rate)
        self._max_write_rate = float(max_write_rate)
        self._capacity_multiplier = 2 # Hard-coded for now

        # Setup token buckets. Initial allotment equal to 1 second of accumulation. Max allotment equal to per-second times 2.
        self._read_bucket  = TokenBucket(rate=self._max_read_rate,  initial=self._max_read_rate,  capacity=self._max_read_rate  * self._capacity_multiplier)
        self._write_bucket = TokenBucket(rate=self._max_write_rate, initial=self._max_write_rate, capacity=self._max_write_rate * self._capacity_multiplier)

        self.metrics = defaultdict(float)
        self.metrics_lock = threading.Lock()
        self.stop_event = threading.Event()

        # Hook into botocore events
        events = session.events
        events.register('provide-client-params.dynamodb.*', self._add_return_consumed_capacity)
        events.register('before-call.dynamodb.*', self._enforce_rate_limit)
        events.register('after-call.dynamodb.*', self._track_consumed_capacity)

        # Start background reporter, if desired
        if enable_reporting:
            self._report_thread = threading.Thread(target=self._report_metrics, daemon=True)
            self._report_thread.start()

    # getters
    @property
    def max_read_rate(self) -> float:
        return self._max_read_rate

    @property
    def max_write_rate(self) -> float:
        return self._max_write_rate

    # setters that *apply* to buckets
    @max_read_rate.setter
    def max_read_rate(self, value: float):
        if value < 1:
            raise ValueError("read rate must be >= 1")
        self._max_read_rate = float(value)
        self._read_bucket.reconfigure(rate=self._max_read_rate,
                                      capacity=self._max_read_rate * self._capacity_multiplier,
                                      scale_tokens=True)

    @max_write_rate.setter
    def max_write_rate(self, value: float):
        if value < 1:
            raise ValueError("write rate must be >= 1")
        self._max_write_rate = float(value)
        self._write_bucket.reconfigure(rate=self._max_write_rate,
                                       capacity=self._max_write_rate * self._capacity_multiplier,
                                       scale_tokens=True)


    def _add_return_consumed_capacity(self, params, **kwargs):
        if 'ReturnConsumedCapacity' not in params:
            params['ReturnConsumedCapacity'] = 'TOTAL'

    def _enforce_rate_limit(self, params, model, **kwargs):
        operation = model.name
        is_read = operation in ('GetItem', 'BatchGetItem', 'Query', 'Scan', 'TransactGetItems')
        is_write = operation in ('PutItem', 'UpdateItem', 'DeleteItem', 'BatchWriteItem', 'TransactWriteItems')
        if is_read:
            self._read_bucket.wait_until_positive()
        elif is_write:
            self._write_bucket.wait_until_positive()
        else:
            return

    def _track_consumed_capacity(self, http_response, parsed, model, **kwargs):
        consumed = parsed.get('ConsumedCapacity')
        if consumed:
            if isinstance(consumed, list):
                for entry in consumed:
                    self._process_entry(entry, model.name)
            elif isinstance(consumed, dict):
                self._process_entry(consumed, model.name)

    def _process_entry(self, entry, model_name):
        read = entry.get('ReadCapacityUnits')
        write = entry.get('WriteCapacityUnits')
        if read is None and write is None:
            if model_name in ('GetItem', 'BatchGetItem', 'Query', 'Scan', 'TransactGetItems'):
                read = entry.get('CapacityUnits', 0.0)
                write = 0.0
            elif model_name in ('PutItem', 'UpdateItem', 'DeleteItem', 'BatchWriteItem', 'TransactWriteItems'):
                read = 0.0
                write = entry.get('CapacityUnits', 0.0)
            else:
                raise Exception(f"Unknown model_name {model_name} with ambiguous capacity: {entry}")
        else:
            read = read or 0.0
            write = write or 0.0

        capacity = entry.get('CapacityUnits', 0.0)
        with self.metrics_lock:
            self.metrics['read_capacity'] += read
            self.metrics['write_capacity'] += write
            self.metrics['total_capacity'] += capacity
            self.metrics['calls'] += 1

        # Deduct from buckets (OK to go negative -> future callers will wait)
        if read:
            self._read_bucket.deduct(read)
        if write:
            self._write_bucket.deduct(write)

    def _report_metrics(self):
        while not self.stop_event.is_set():
            time.sleep(1)
            with self.metrics_lock:
                m = dict(self.metrics)
            rb = self._read_bucket.snapshot()
            wb = self._write_bucket.snapshot()
            print(
                f"WCU: {m['write_capacity']:.2f}, RCU: {m['read_capacity']:.2f}, "
                f"Total CU: {m['total_capacity']:.2f}, Calls: {m['calls']} | "
                f"TokensR: {rb['tokens']:.2f}/{rb['rate']:.0f} "
                f"TokensW: {wb['tokens']:.2f}/{wb['rate']:.0f}"
            )

    def stop(self):
        self.stop_event.set()
        if hasattr(self, '_report_thread') and self._report_thread.is_alive():
            self._report_thread.join(timeout=2)
