from typing import Optional
import random
import time
import threading

class TokenBucket:
    """
    Thread-safe token bucket that:
      - Refills continuously at `rate` tokens/sec (monotonic clock).
      - Starts at `initial` with a default initial size of `rate`.
      - Caps at `capacity` with a default capacity size of `rate`.
      - Allows balance to go negative.
      - Callers can wait until balance > 0 (without knowing cost up-front).
    """
    def __init__(self, rate: float, initial: Optional[float] = None, capacity: Optional[float] = None):
        if rate <= 0:
            raise ValueError("rate must be > 0")
        self.rate = float(rate)
        self.capacity = float(capacity if capacity is not None else rate)
        init = float(initial if initial is not None else rate)
        self.tokens = max(-self.capacity, min(init, self.capacity))
        self.last = time.monotonic()

        self._lock = threading.Lock()
        self._cv = threading.Condition(self._lock)

    # Always called while holding the _lock
    def _refill_locked(self, now: float):
        dt = now - self.last
        if dt > 0:
            self.tokens = min(self.capacity, self.tokens + self.rate * dt)
            self.last = now

    def wait_until_positive(self):
        """
        Block until bucket balance > 0 (or return immediately if already > 0).
        """
        with self._cv:
            while True:
                now = time.monotonic()
                self._refill_locked(now)
                if self.tokens > 1e-9: # float noise
                    return
                # Compute time to reach just-above-zero, add a bit of jitter
                need = (0.0 - self.tokens) / self.rate  # >= 0
                extra = random.uniform(0.0, 0.003)  # up to +3 ms
                self._cv.wait(timeout=max(need + extra, 1e-6))

    def deduct(self, actual: float):
        """
        Deduct *actual* tokens (may drive balance negative).
        Wake up waiters if we remain positive after deduction (rare), but
        refilling is what normally wakes them.
        """
        amt = float(actual)
        if amt < 0:
            return
        with self._cv:
            now = time.monotonic()
            self._refill_locked(now)
            self.tokens -= amt
            # If we're still positive, nudge any waiters to recheck early.
            if self.tokens > 0:
                self._cv.notify_all()

    def snapshot(self):
        with self._lock:
            now = time.monotonic()
            self._refill_locked(now)
            return {"tokens": self.tokens, "capacity": self.capacity, "rate": self.rate, "time": now}

    def reconfigure(
        self,
        rate: Optional[float] = None,
        capacity: Optional[float] = None,
        *,
        scale_tokens: bool = False
    ) -> None:
        """
        Atomically update the bucket's rate and/or capacity.
        - If scale_tokens=True and capacity changes, tokens are scaled by the ratio.
          Otherwise tokens are preserved and clamped to [-capacity, +capacity].
        """
        with self._cv:
            now = time.monotonic()
            self._refill_locked(now)

            old_rate = self.rate
            old_cap  = self.capacity

            if rate is not None:
                if rate <= 0:
                    raise ValueError("rate must be > 0")
                self.rate = float(rate)

            if capacity is not None:
                if capacity <= 0:
                    raise ValueError("capacity must be > 0")
                new_cap = float(capacity)
                if scale_tokens and old_cap > 0:
                    scale = new_cap / old_cap
                    self.tokens *= scale
                self.capacity = new_cap
                # Clamp to new bounds (allow negative debt up to one capacity)
                self.tokens = max(-self.capacity, min(self.tokens, self.capacity))

            # wake waiters to re-evaluate with new params
            self._cv.notify_all()
