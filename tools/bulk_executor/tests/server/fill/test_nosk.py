"""Unit tests for the `nosk` fill generator (no-sort-key variant).

Covers `python_modules/fill/nosk.py`:
- generate(): returns a single dict (NOT a list — fill.__init__._fill_data
  wraps single dicts via isinstance(item_collection, dict) check)
- 'pknum' is a Decimal in the documented numeric range
- 'payload' is a 200-char string from ascii_lowercase + digits
- multiple invocations produce independent random output

The existing tests/server/conftest.py mocks awsglue, pyspark, and shared
modules at all resolution paths. These tests build on that.
"""

import string
from decimal import Decimal
from unittest.mock import patch

import pytest

from python_modules.fill import nosk as nosk_mod


_ALPHABET = set(string.ascii_lowercase + string.digits)


# --- generate(): top-level shape --------------------------------------------

class TestGenerateShape:
    """Tests for the structure of the dict returned by generate()."""

    def test_returns_single_dict(self):
        """Line 12: returns a single dict, not a list."""
        out = nosk_mod.generate()
        assert isinstance(out, dict), \
            "nosk.generate must return a single dict (handled by fill._fill_data)"

    def test_dict_has_only_pknum_and_payload(self):
        """Line 10: schema is {'pknum': ..., 'payload': ...} only."""
        out = nosk_mod.generate()
        assert set(out.keys()) == {'pknum', 'payload'}


# --- generate(): pknum field ------------------------------------------------

class TestPknumField:
    """Tests for the Decimal partition-key value (line 7)."""

    def test_pknum_is_decimal(self):
        """Line 7: pknum = Decimal(random.randint(1, 1_000_000_000))."""
        out = nosk_mod.generate()
        assert isinstance(out['pknum'], Decimal)

    def test_pknum_in_documented_range(self):
        """Line 7: random.randint(1, 1_000_000_000) yields inclusive bounds."""
        for _ in range(50):
            out = nosk_mod.generate()
            assert Decimal(1) <= out['pknum'] <= Decimal(1_000_000_000)

    def test_pknum_is_integer_valued(self):
        """random.randint returns ints, so the Decimal has no fractional part."""
        out = nosk_mod.generate()
        assert out['pknum'] == int(out['pknum']), \
            "pknum is built from randint, must be whole-number-valued"


# --- generate(): payload field ----------------------------------------------

class TestPayloadField:
    """Tests for the 200-char random payload (line 8)."""

    def test_payload_length_is_200(self):
        """Line 8: random.choices(..., k=200)."""
        out = nosk_mod.generate()
        assert len(out['payload']) == 200

    def test_payload_uses_lowercase_and_digits(self):
        """Line 8: ascii_lowercase + digits alphabet only."""
        out = nosk_mod.generate()
        assert set(out['payload']) <= _ALPHABET

    def test_payload_is_string(self):
        """Line 8: ''.join(random.choices(...)) → str."""
        out = nosk_mod.generate()
        assert isinstance(out['payload'], str)


# --- generate(): randomness sanity -------------------------------------------

class TestRandomness:
    """Tests that multiple invocations produce different random values."""

    def test_pknum_varies_across_calls(self):
        """Each generate() call must produce a fresh randint."""
        pknums = {nosk_mod.generate()['pknum'] for _ in range(50)}
        # 50 picks from 1..1_000_000_000 → collisions essentially impossible
        assert len(pknums) >= 49, \
            "50 calls should produce ~50 unique pknum values"

    def test_payload_varies_across_calls(self):
        """Each generate() call must produce a fresh payload."""
        payloads = {nosk_mod.generate()['payload'] for _ in range(10)}
        # 200 chars from a 36-letter alphabet → astronomically unique
        assert len(payloads) == 10


# --- generate(): determinism under seeded random ----------------------------

class TestDeterminismWithSeededRandom:
    """Tests that confirm output structure is fully driven by random module."""

    def test_seeded_random_produces_reproducible_output(self):
        """Seeded random yields identical output across runs."""
        import random as _r
        _r.seed(123)
        first = nosk_mod.generate()
        _r.seed(123)
        second = nosk_mod.generate()
        assert first == second, "same seed → identical generator output"

    def test_randint_called_once_choices_called_once(self):
        """Lines 7-8: one randint for pknum, one choices for payload."""
        import random as real_random
        with patch('python_modules.fill.nosk.random.randint',
                   wraps=real_random.randint) as randint, \
             patch('python_modules.fill.nosk.random.choices',
                   wraps=real_random.choices) as choices:
            nosk_mod.generate()
        assert randint.call_count == 1, "exactly one randint call (for pknum)"
        assert choices.call_count == 1, "exactly one choices call (for payload)"

    def test_randint_called_with_documented_bounds(self):
        """Line 7: bounds are 1, 1_000_000_000."""
        import random as real_random
        with patch('python_modules.fill.nosk.random.randint',
                   wraps=real_random.randint) as randint:
            nosk_mod.generate()
        assert randint.call_args.args == (1, 1_000_000_000)
