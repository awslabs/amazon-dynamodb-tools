"""Unit tests for the `default` fill generator.

Covers `python_modules/fill/default.py`:
- generate(): returns a 3-element list of dicts
- shared `pk` value across all three items in the same item collection
- distinct `sk` values per item
- field shape per item (item1 has '#meta', item2 has 'payload', item3 has neither)
- string lengths match the documented widths (12, 10, 24, 24, 24, 200)
- character set is restricted to ascii_lowercase + digits
- multiple invocations produce independent random output (statistical sanity)

The existing tests/server/conftest.py mocks awsglue, pyspark, and shared
modules at all resolution paths. These tests build on that.
"""

import string
from unittest.mock import patch

import pytest

from python_modules.fill import default as default_mod


_ALPHABET = set(string.ascii_lowercase + string.digits)


# --- generate(): top-level shape ---------------------------------------------

class TestGenerateShape:
    """Tests for the structure of the list returned by generate()."""

    def test_returns_list_of_three_items(self):
        """Line 18: returns [item1, item2, item3]."""
        out = default_mod.generate()
        assert isinstance(out, list)
        assert len(out) == 3, "generate() must return exactly 3 items"

    def test_each_element_is_dict(self):
        """All three items must be dicts so DynamoDB batch_writer can put_item."""
        out = default_mod.generate()
        assert all(isinstance(item, dict) for item in out)

    def test_all_items_share_same_pk(self):
        """Lines 6, 14-16: pk is generated once and reused across all items."""
        item1, item2, item3 = default_mod.generate()
        assert item1['pk'] == item2['pk'] == item3['pk'], \
            "all 3 items must share the same partition key (same item collection)"

    def test_each_item_has_distinct_sk(self):
        """Lines 8-10: sk1, sk2, sk3 are independently generated."""
        item1, item2, item3 = default_mod.generate()
        assert item1['sk'] != item2['sk']
        assert item2['sk'] != item3['sk']
        assert item1['sk'] != item3['sk']


# --- generate(): per-item field shape ---------------------------------------

class TestItemFieldShape:
    """Tests for the heterogeneous schemas across the 3 items (line 13 comment)."""

    def test_item1_has_meta_field(self):
        """Line 14: item1 has 'pk', 'sk', '#meta'."""
        item1 = default_mod.generate()[0]
        assert set(item1.keys()) == {'pk', 'sk', '#meta'}

    def test_item2_has_payload_field(self):
        """Line 15: item2 has 'pk', 'sk', 'payload'."""
        item2 = default_mod.generate()[1]
        assert set(item2.keys()) == {'pk', 'sk', 'payload'}

    def test_item3_has_only_pk_and_sk(self):
        """Line 16: item3 has 'pk', 'sk' only."""
        item3 = default_mod.generate()[2]
        assert set(item3.keys()) == {'pk', 'sk'}


# --- generate(): string lengths ---------------------------------------------

class TestStringLengths:
    """Tests that random.choices length args produce correct widths."""

    def test_pk_length_is_12(self):
        """Line 6: pk = k=12 chars."""
        item1 = default_mod.generate()[0]
        assert len(item1['pk']) == 12

    def test_meta_length_is_10(self):
        """Line 7: meta = k=10 chars."""
        item1 = default_mod.generate()[0]
        assert len(item1['#meta']) == 10

    def test_sk_lengths_are_24(self):
        """Lines 8-10: sk1, sk2, sk3 = k=24 chars each."""
        items = default_mod.generate()
        for item in items:
            assert len(item['sk']) == 24, f"sk in {item} must be 24 chars"

    def test_payload_length_is_200(self):
        """Line 11: payload = k=200 chars."""
        item2 = default_mod.generate()[1]
        assert len(item2['payload']) == 200


# --- generate(): character set ----------------------------------------------

class TestCharacterSet:
    """Tests that all generated strings only contain lowercase + digits."""

    def test_all_string_fields_use_lowercase_digits_alphabet(self):
        """Lines 6-11: random.choices(ascii_lowercase + digits, ...)."""
        items = default_mod.generate()
        # Check pk
        assert set(items[0]['pk']) <= _ALPHABET
        # Check #meta
        assert set(items[0]['#meta']) <= _ALPHABET
        # Check all sks
        for item in items:
            assert set(item['sk']) <= _ALPHABET
        # Check payload
        assert set(items[1]['payload']) <= _ALPHABET


# --- generate(): randomness sanity -------------------------------------------

class TestRandomness:
    """Tests that multiple invocations produce different random values."""

    def test_pk_varies_across_calls(self):
        """Each generate() call should produce a fresh random pk."""
        pks = {default_mod.generate()[0]['pk'] for _ in range(20)}
        # 12 chars from 36-letter alphabet → astronomically unlikely to collide
        assert len(pks) == 20, "pks across 20 calls should all be unique"

    def test_payload_varies_across_calls(self):
        """Each generate() call should produce a fresh random payload."""
        payloads = {default_mod.generate()[1]['payload'] for _ in range(10)}
        assert len(payloads) == 10


# --- generate(): determinism under seeded random ----------------------------

class TestDeterminismWithSeededRandom:
    """Tests that confirm output structure is fully driven by random module."""

    def test_seeded_random_produces_reproducible_output(self):
        """Seeded random.choices yields identical output across runs."""
        import random as _r
        _r.seed(42)
        first = default_mod.generate()
        _r.seed(42)
        second = default_mod.generate()
        assert first == second, "same seed must produce identical generator output"

    def test_random_choices_called_six_times(self):
        """Lines 6-11: random.choices is invoked exactly 6 times per generate()."""
        with patch('python_modules.fill.default.random.choices',
                   wraps=__import__('random').choices) as choices:
            default_mod.generate()
        assert choices.call_count == 6, \
            "exactly 6 random.choices calls: pk, meta, sk1, sk2, sk3, payload"
