"""Unit tests for `python_modules/update/touched.py`.

Covers the generate() function:
- Returns correct UpdateExpression kwargs with pk/sk from item
- Raises ValueError when pk or sk is missing
- Sets a Decimal timestamp via time.time()
- Includes ConditionExpression for idempotent touch
"""

import time
from decimal import Decimal
from unittest.mock import patch

import pytest

from python_modules.update import touched


class TestGenerate:
    def test_returns_update_kwargs_with_correct_key(self):
        item = {"pk": "user#123", "sk": "profile#main", "other": "data"}
        result = touched.generate(item)

        assert result["Key"] == {"pk": "user#123", "sk": "profile#main"}

    def test_update_expression_sets_touched(self):
        item = {"pk": "a", "sk": "b"}
        result = touched.generate(item)

        assert result["UpdateExpression"] == "SET #touched = :touched"
        assert result["ExpressionAttributeNames"] == {"#touched": "touched"}

    def test_condition_expression_prevents_stale_overwrite(self):
        item = {"pk": "a", "sk": "b"}
        result = touched.generate(item)

        assert "attribute_not_exists(#touched) OR #touched < :touched" in result["ConditionExpression"]

    def test_touched_value_is_decimal_timestamp(self):
        fake_time = 1700000000.123
        item = {"pk": "a", "sk": "b"}

        with patch.object(time, "time", return_value=fake_time):
            result = touched.generate(item)

        expected = Decimal(str(fake_time))
        assert result["ExpressionAttributeValues"][":touched"] == expected

    def test_raises_when_pk_missing(self):
        item = {"sk": "b", "other": "val"}

        with pytest.raises(ValueError, match="missing expected primary key"):
            touched.generate(item)

    def test_raises_when_sk_missing(self):
        item = {"pk": "a", "other": "val"}

        with pytest.raises(ValueError, match="missing expected primary key"):
            touched.generate(item)

    def test_raises_when_both_keys_missing(self):
        item = {"other": "val"}

        with pytest.raises(ValueError, match="missing expected primary key"):
            touched.generate(item)

    def test_pk_none_explicitly_raises(self):
        item = {"pk": None, "sk": "b"}

        with pytest.raises(ValueError, match="missing expected primary key"):
            touched.generate(item)

    def test_sk_none_explicitly_raises(self):
        item = {"pk": "a", "sk": None}

        with pytest.raises(ValueError, match="missing expected primary key"):
            touched.generate(item)
