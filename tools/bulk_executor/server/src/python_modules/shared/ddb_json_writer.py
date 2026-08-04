"""Write Spark DataFrames as DynamoDB JSON (type-annotated) to S3.

The Glue DynamoDB connector (spark.read.format("dynamodb")) attaches
``dynamodb.type`` metadata to each StructField, telling us the original
DynamoDB type (S, N, B, BOOL, NULL, SS, NS, BS, L, M).  This module
uses that metadata to serialize DataFrame rows back into DynamoDB JSON,
preserving full type fidelity for round-trip with ``load``.

Output format (JSON Lines, one item per line):
    {"pk":{"S":"x"},"nums":{"NS":["1","2"]},"flag":{"BOOL":true}, ...}
"""

import base64
import json

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
    StructType,
)


def _serialize_value(value, spark_type, metadata):
    """Convert a single Spark value to its DynamoDB JSON representation."""
    if value is None:
        return {"NULL": True}

    ddb_type = metadata.get("dynamodb.type") if metadata else None

    if ddb_type == "S":
        return {"S": str(value)}
    elif ddb_type == "N":
        return {"N": str(value)}
    elif ddb_type == "B":
        return {"B": _encode_binary(value)}
    elif ddb_type == "BOOL":
        return {"BOOL": bool(value)}
    elif ddb_type == "NULL":
        return {"NULL": True}
    elif ddb_type == "SS":
        return {"SS": [str(v) for v in value]}
    elif ddb_type == "NS":
        return {"NS": [str(v) for v in value]}
    elif ddb_type == "BS":
        return {"BS": [_encode_binary(v) for v in value]}
    elif ddb_type == "L":
        return {"L": _serialize_list(value, spark_type)}
    elif ddb_type == "M":
        return {"M": _serialize_map(value, spark_type)}

    return _infer_ddb_type(value, spark_type)


def _infer_ddb_type(value, spark_type):
    """Fallback: infer DynamoDB type from Spark type when metadata is absent."""
    if value is None:
        return {"NULL": True}

    if isinstance(spark_type, StringType):
        return {"S": str(value)}
    elif isinstance(spark_type, (DecimalType, DoubleType, FloatType,
                                 IntegerType, LongType, ShortType)):
        return {"N": str(value)}
    elif isinstance(spark_type, BooleanType):
        return {"BOOL": bool(value)}
    elif isinstance(spark_type, BinaryType):
        return {"B": _encode_binary(value)}
    elif isinstance(spark_type, ArrayType):
        return {"L": _serialize_list(value, spark_type)}
    elif isinstance(spark_type, StructType):
        return {"M": _serialize_struct_as_map(value, spark_type)}
    elif isinstance(spark_type, MapType):
        return {"M": _serialize_maptype(value, spark_type)}
    else:
        return {"S": str(value)}


def _serialize_list(value, spark_type):
    """Serialize a DynamoDB List (L) — heterogeneous elements."""
    if isinstance(spark_type, ArrayType):
        elem_type = spark_type.elementType
        result = []
        for item in value:
            if item is None:
                result.append({"NULL": True})
            elif isinstance(elem_type, StructType):
                result.append(_serialize_list_element_struct(item, elem_type))
            else:
                result.append(_infer_ddb_type(item, elem_type))
        return result
    return [_infer_ddb_type(item, StringType()) for item in (value or [])]


def _serialize_list_element_struct(row, struct_type):
    """Serialize a single element inside a List.

    The connector represents heterogeneous list elements as a struct
    where exactly one field is non-null, each field representing a
    possible DynamoDB type.
    """
    for field in struct_type.fields:
        field_val = row[field.name] if hasattr(row, '__getitem__') else getattr(row, field.name, None)
        if field_val is not None:
            metadata = dict(field.metadata) if field.metadata else {}
            return _serialize_value(field_val, field.dataType, metadata)
    return {"NULL": True}


def _serialize_map(value, spark_type):
    """Serialize a DynamoDB Map (M)."""
    if isinstance(spark_type, StructType):
        return _serialize_struct_as_map(value, spark_type)
    elif isinstance(spark_type, MapType):
        return _serialize_maptype(value, spark_type)
    elif isinstance(value, dict):
        return {k: _infer_ddb_type(v, StringType()) for k, v in value.items()}
    return {}


def _serialize_struct_as_map(row, struct_type):
    """Convert a Spark StructType row into a DDB Map."""
    result = {}
    for field in struct_type.fields:
        field_val = row[field.name] if hasattr(row, '__getitem__') else getattr(row, field.name, None)
        if field_val is None:
            continue
        metadata = dict(field.metadata) if field.metadata else {}
        result[field.name] = _serialize_value(field_val, field.dataType, metadata)
    return result


def _serialize_maptype(value, spark_type):
    """Convert a Spark MapType value into a DDB Map."""
    if not value:
        return {}
    result = {}
    for k, v in value.items():
        if v is None:
            result[str(k)] = {"NULL": True}
        else:
            result[str(k)] = _infer_ddb_type(v, spark_type.valueType)
    return result


def _encode_binary(value):
    """Encode binary data as base64 string."""
    if isinstance(value, (bytes, bytearray)):
        return base64.b64encode(value).decode('ascii')
    return str(value)


def row_to_ddb_json(row, schema):
    """Convert a Spark Row to a DynamoDB JSON dict.

    Args:
        row: A pyspark.sql.Row object.
        schema: The DataFrame's StructType schema.

    Returns:
        A dict mapping attribute names to DynamoDB typed values.
    """
    item = {}
    for field in schema.fields:
        value = row[field.name]
        if value is None:
            continue
        metadata = dict(field.metadata) if field.metadata else {}
        item[field.name] = _serialize_value(value, field.dataType, metadata)
    return item


def write_ddb_json_to_s3(records_df, s3_output_location):
    """Write a Spark DataFrame as DynamoDB JSON Lines to S3.

    Each row is serialized as a single-line JSON object with DynamoDB
    type annotations, then written via Spark's text writer.

    Args:
        records_df: A Spark DataFrame read via the DynamoDB connector.
        s3_output_location: The S3 path to write to (e.g. s3://bucket/prefix).
    """
    schema = records_df.schema

    def _row_to_json_str(row):
        item = row_to_ddb_json(row, schema)
        return json.dumps(item, separators=(',', ':'), default=str)

    json_rdd = records_df.rdd.map(_row_to_json_str)
    json_df = records_df.sparkSession.createDataFrame(
        json_rdd.map(lambda x: (x,)), ["value"]
    )
    json_df.write.mode("overwrite").text(s3_output_location)
