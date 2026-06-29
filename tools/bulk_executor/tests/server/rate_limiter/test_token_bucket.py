"""Unit tests for TokenBucket — refill, deduct, wait_until_positive, reconfigure, negative balance, concurrency."""
import threading
import time
from unittest.mock import patch

import pytest

from python_modules.shared.rate_limiter.TokenBucket import TokenBucket


class TestTokenBucketInit:
    def test_defaults_rate_as_initial_and_capacity(self):
        tb = TokenBucket(rate=100)
        snap = tb.snapshot()
        assert snap["rate"] == 100.0
        assert snap["capacity"] == 100.0
        assert snap["tokens"] == pytest.approx(100.0, abs=1)

    def test_custom_initial_and_capacity(self):
        tb = TokenBucket(rate=50, initial=10, capacity=200)
        snap = tb.snapshot()
        assert snap["rate"] == 50.0
        assert snap["capacity"] == 200.0
        assert snap["tokens"] == pytest.approx(10.0, abs=1)

    def test_initial_clamped_to_capacity(self):
        tb = TokenBucket(rate=10, initial=999, capacity=20)
        snap = tb.snapshot()
        assert snap["tokens"] == pytest.approx(20.0, abs=1)

    def test_zero_rate_raises(self):
        with pytest.raises(ValueError, match="rate must be > 0"):
            TokenBucket(rate=0)

    def test_negative_rate_raises(self):
        with pytest.raises(ValueError, match="rate must be > 0"):
            TokenBucket(rate=-5)


class TestRefill:
    def test_refill_adds_tokens_over_time(self):
        tb = TokenBucket(rate=1000, initial=0, capacity=2000)
        time.sleep(0.05)
        snap = tb.snapshot()
        assert snap["tokens"] > 0
        assert snap["tokens"] <= 2000

    def test_refill_capped_at_capacity(self):
        tb = TokenBucket(rate=100000, initial=100000, capacity=100000)
        time.sleep(0.01)
        snap = tb.snapshot()
        assert snap["tokens"] == pytest.approx(100000.0, abs=1)


class TestDeduct:
    def test_deduct_reduces_tokens(self):
        tb = TokenBucket(rate=100, initial=100, capacity=200)
        tb.deduct(50)
        snap = tb.snapshot()
        assert snap["tokens"] < 100

    def test_deduct_allows_negative_balance(self):
        tb = TokenBucket(rate=10, initial=10, capacity=20)
        tb.deduct(30)
        snap = tb.snapshot()
        assert snap["tokens"] < 0

    def test_deduct_negative_amount_is_noop(self):
        tb = TokenBucket(rate=100, initial=100, capacity=100)
        tb.deduct(-50)
        snap = tb.snapshot()
        assert snap["tokens"] == pytest.approx(100.0, abs=2)

    def test_deduct_zero_is_noop(self):
        tb = TokenBucket(rate=100, initial=50, capacity=100)
        tb.deduct(0)
        snap = tb.snapshot()
        assert snap["tokens"] == pytest.approx(50.0, abs=2)


class TestWaitUntilPositive:
    def test_returns_immediately_when_positive(self):
        tb = TokenBucket(rate=100, initial=50, capacity=100)
        start = time.monotonic()
        tb.wait_until_positive()
        elapsed = time.monotonic() - start
        assert elapsed < 0.01

    def test_blocks_when_negative_then_returns(self):
        tb = TokenBucket(rate=10000, initial=0, capacity=20000)
        tb.deduct(50)
        start = time.monotonic()
        tb.wait_until_positive()
        elapsed = time.monotonic() - start
        assert elapsed < 0.1  # high rate so should unblock fast

    def test_blocks_respects_refill_rate(self):
        tb = TokenBucket(rate=100, initial=0, capacity=200)
        tb.deduct(5)
        start = time.monotonic()
        tb.wait_until_positive()
        elapsed = time.monotonic() - start
        # need 5 tokens at 100/sec = 50ms minimum
        assert elapsed >= 0.04


class TestReconfigure:
    def test_change_rate(self):
        tb = TokenBucket(rate=100, initial=50, capacity=200)
        tb.reconfigure(rate=500)
        snap = tb.snapshot()
        assert snap["rate"] == 500.0
        assert snap["capacity"] == 200.0

    def test_change_capacity_clamps_tokens(self):
        tb = TokenBucket(rate=100, initial=100, capacity=200)
        tb.reconfigure(capacity=50)
        snap = tb.snapshot()
        assert snap["capacity"] == 50.0
        assert snap["tokens"] <= 50.0

    def test_scale_tokens_on_capacity_change(self):
        tb = TokenBucket(rate=100, initial=80, capacity=100)
        tb.reconfigure(capacity=200, scale_tokens=True)
        snap = tb.snapshot()
        # 80/100 * 200 = 160
        assert snap["tokens"] == pytest.approx(160.0, abs=5)

    def test_invalid_rate_raises(self):
        tb = TokenBucket(rate=100)
        with pytest.raises(ValueError, match="rate must be > 0"):
            tb.reconfigure(rate=0)

    def test_invalid_capacity_raises(self):
        tb = TokenBucket(rate=100)
        with pytest.raises(ValueError, match="capacity must be > 0"):
            tb.reconfigure(capacity=-1)

    def test_reconfigure_wakes_waiters(self):
        tb = TokenBucket(rate=1, initial=0, capacity=10)
        tb.deduct(100)  # deeply negative

        woke = threading.Event()

        def waiter():
            tb.wait_until_positive()
            woke.set()

        t = threading.Thread(target=waiter, daemon=True)
        t.start()
        time.sleep(0.02)
        assert not woke.is_set()
        # Reconfigure to very high rate so waiter unblocks
        tb.reconfigure(rate=100000)
        t.join(timeout=1.0)
        assert woke.is_set()


class TestConcurrency:
    def test_concurrent_deducts_are_consistent(self):
        tb = TokenBucket(rate=0.001, initial=1000, capacity=1000)
        n_threads = 10
        deducts_per_thread = 100

        def deductor():
            for _ in range(deducts_per_thread):
                tb.deduct(1)

        threads = [threading.Thread(target=deductor) for _ in range(n_threads)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        snap = tb.snapshot()
        expected = 1000 - (n_threads * deducts_per_thread)
        assert snap["tokens"] == pytest.approx(expected, abs=2)

    def test_concurrent_wait_and_deduct(self):
        tb = TokenBucket(rate=50000, initial=100, capacity=100000)
        results = []

        def wait_then_record():
            tb.wait_until_positive()
            results.append(True)

        def deductor():
            for _ in range(50):
                tb.deduct(1)
                time.sleep(0.001)

        threads = []
        for _ in range(5):
            threads.append(threading.Thread(target=wait_then_record))
            threads.append(threading.Thread(target=deductor))

        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=5.0)

        assert len(results) == 5


class TestNegativeBalance:
    def test_negative_initial_clamped_to_neg_capacity(self):
        tb = TokenBucket(rate=100, initial=-500, capacity=100)
        snap = tb.snapshot()
        assert snap["tokens"] == pytest.approx(-100.0, abs=1)

    def test_deep_negative_recovers_with_refill(self):
        tb = TokenBucket(rate=10000, initial=0, capacity=20000)
        tb.deduct(500)
        snap_before = tb.snapshot()
        assert snap_before["tokens"] < 0
        time.sleep(0.1)
        snap_after = tb.snapshot()
        assert snap_after["tokens"] > snap_before["tokens"]
