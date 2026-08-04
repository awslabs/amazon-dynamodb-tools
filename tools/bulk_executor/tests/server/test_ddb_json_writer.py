"""Unit tests for python_modules.shared.ddb_json_writer.

Tests the conversion of Spark DataFrame rows to DynamoDB JSON format,
covering all DynamoDB types: S, N, B, BOOL, NULL, SS, NS, BS, L, M.
"""

import base64
import json
import sys
from unittest.mock import MagicMock

import pytest

sys.modules.setdefault('awsglue.transforms', MagicMock())
sys.modules.setdefault('pyspark.sql.functions', MagicMock())

from pyspark.sql.types import (
    ArrayType,
    BinaryType,
    BooleanType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    MapType,
    ShortType,
    StringType,
    StructField,
    StructType,
)

from python_modules.shared.ddb_json_writer import (
    _encode_binary,
    _infer_ddb_type,
    _serialize_value,
    row_to_ddb_json,
)


class TestSerializeValue:
    """Test _serialize_value with explicit dynamodb.type metadata."""

    def test_string_type(self):
        result = _serialize_value("hello", StringType(), {"dynamodb.type": "S"})
        assert result == {"S": "hello"}

    def test_number_type_int(self):
        result = _serialize_value(42, IntegerType(), {"dynamodb.type": "N"})
        assert result == {"N": "42"}

    def test_number_type_decimal(self):
        result = _serialize_value("3.14", StringType(), {"dynamodb.type": "N"})
        assert result == {"N": "3.14"}

    def test_boolean_true(self):
        result = _serialize_value(True, BooleanType(), {"dynamodb.type": "BOOL"})
        assert result == {"BOOL": True}

    def test_boolean_false(self):
        result = _serialize_value(False, BooleanType(), {"dynamodb.type": "BOOL"})
        assert result == {"BOOL": False}

    def test_null_value(self):
        result = _serialize_value(None, StringType(), {"dynamodb.type": "S"})
        assert result == {"NULL": True}

    def test_null_type_explicit(self):
        result = _serialize_value(True, BooleanType(), {"dynamodb.type": "NULL"})
        assert result == {"NULL": True}

    def test_binary_type(self):
        data = b'\xfe\xca\xbe\xba'
        result = _serialize_value(data, BinaryType(), {"dynamodb.type": "B"})
        assert result == {"B": base64.b64encode(data).decode('ascii')}

    def test_string_set(self):
        result = _serialize_value(["abc", "def"], ArrayType(StringType()),
                                  {"dynamodb.type": "SS"})
        assert result == {"SS": ["abc", "def"]}

    def test_number_set(self):
        result = _serialize_value([1, 2, 3], ArrayType(IntegerType()),
                                  {"dynamodb.type": "NS"})
        assert result == {"NS": ["1", "2", "3"]}

    def test_binary_set(self):
        data = [b'\xbe\xba', b'\xfe\xca']
        result = _serialize_value(data, ArrayType(BinaryType()),
                                  {"dynamodb.type": "BS"})
        expected = {"BS": [base64.b64encode(b).decode('ascii') for b in data]}
        assert result == expected

    def test_list_type(self):
        schema = ArrayType(StringType())
        result = _serialize_value(["x", "y"], schema, {"dynamodb.type": "L"})
        assert result == {"L": [{"S": "x"}, {"S": "y"}]}

    def test_map_type(self):
        schema = StructType([
            StructField("inner", StringType(), metadata={"dynamodb.type": "S"}),
        ])
        row = MagicMock()
        row.__getitem__ = lambda self, key: "innerval"
        result = _serialize_value(row, schema, {"dynamodb.type": "M"})
        assert result == {"M": {"inner": {"S": "innerval"}}}


class TestInferDdbType:
    """Test fallback type inference from Spark types when metadata is absent."""

    def test_string(self):
        assert _infer_ddb_type("hello", StringType()) == {"S": "hello"}

    def test_integer(self):
        assert _infer_ddb_type(5, IntegerType()) == {"N": "5"}

    def test_long(self):
        assert _infer_ddb_type(123456789, LongType()) == {"N": "123456789"}

    def test_double(self):
        assert _infer_ddb_type(3.14, DoubleType()) == {"N": "3.14"}

    def test_boolean(self):
        assert _infer_ddb_type(True, BooleanType()) == {"BOOL": True}

    def test_binary(self):
        data = b'\xfe\xca'
        result = _infer_ddb_type(data, BinaryType())
        assert result == {"B": base64.b64encode(data).decode('ascii')}

    def test_null(self):
        assert _infer_ddb_type(None, StringType()) == {"NULL": True}

    def test_array_of_strings_inferred_as_list(self):
        result = _infer_ddb_type(["a", "b"], ArrayType(StringType()))
        assert result == {"L": [{"S": "a"}, {"S": "b"}]}

    def test_array_of_numbers_inferred_as_list(self):
        result = _infer_ddb_type([1, 2], ArrayType(IntegerType()))
        assert result == {"L": [{"N": "1"}, {"N": "2"}]}


class TestRowToDdbJson:
    """Test full row-to-DDB-JSON conversion."""

    def test_simple_row(self):
        schema = StructType([
            StructField("pk", StringType(), metadata={"dynamodb.type": "S"}),
            StructField("num", IntegerType(), metadata={"dynamodb.type": "N"}),
        ])
        row = MagicMock()
        row.__getitem__ = lambda self, key: {"pk": "x", "num": 5}[key]
        result = row_to_ddb_json(row, schema)
        assert result == {"pk": {"S": "x"}, "num": {"N": "5"}}

    def test_null_attributes_excluded(self):
        schema = StructType([
            StructField("pk", StringType(), metadata={"dynamodb.type": "S"}),
            StructField("gone", StringType(), metadata={"dynamodb.type": "S"}),
        ])
        row = MagicMock()
        row.__getitem__ = lambda self, key: {"pk": "val", "gone": None}[key]
        result = row_to_ddb_json(row, schema)
        assert "gone" not in result
        assert result == {"pk": {"S": "val"}}

    def test_all_types(self):
        schema = StructType([
            StructField("s", StringType(), metadata={"dynamodb.type": "S"}),
            StructField("n", IntegerType(), metadata={"dynamodb.type": "N"}),
            StructField("b", BooleanType(), metadata={"dynamodb.type": "BOOL"}),
            StructField("ss", ArrayType(StringType()), metadata={"dynamodb.type": "SS"}),
            StructField("ns", ArrayType(IntegerType()), metadata={"dynamodb.type": "NS"}),
        ])
        values = {
            "s": "hello",
            "n": 42,
            "b": False,
            "ss": ["x", "y"],
            "ns": [1, 2],
        }
        row = MagicMock()
        row.__getitem__ = lambda self, key: values[key]
        result = row_to_ddb_json(row, schema)
        assert result == {
            "s": {"S": "hello"},
            "n": {"N": "42"},
            "b": {"BOOL": False},
            "ss": {"SS": ["x", "y"]},
            "ns": {"NS": ["1", "2"]},
        }


class TestEncodeBinary:
    """Test binary encoding helper."""

    def test_bytes(self):
        assert _encode_binary(b'\xfe\xca') == base64.b64encode(b'\xfe\xca').decode('ascii')

    def test_bytearray(self):
        data = bytearray(b'\xbe\xba')
        assert _encode_binary(data) == base64.b64encode(data).decode('ascii')

    def test_string_passthrough(self):
        assert _encode_binary("already_encoded") == "already_encoded"
