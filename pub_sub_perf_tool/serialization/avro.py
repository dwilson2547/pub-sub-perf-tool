"""Avro serialization utilities for pub-sub messages

Provides encode/decode helpers that wrap fastavro so callers do not need
to manage schema parsing or byte-buffer handling directly.

Example usage::

    from pub_sub_perf_tool.serialization.avro import encode, decode

    schema = {
        "type": "record",
        "name": "Event",
        "fields": [
            {"name": "id", "type": "string"},
            {"name": "value", "type": "int"},
        ],
    }

    data = {"id": "abc", "value": 42}
    raw = encode(data, schema)
    decoded = decode(raw, schema)
    assert decoded == data
"""
import io
from typing import Any, Dict

import fastavro


def encode(record: Dict[str, Any], schema: Dict[str, Any]) -> bytes:
    """Serialize *record* to Avro binary format using *schema*.

    Args:
        record: The Python dict to serialize.
        schema: A parsed Avro schema dict (following the Apache Avro
            specification).

    Returns:
        Avro-encoded bytes representing the record.
    """
    parsed = fastavro.parse_schema(schema)
    buf = io.BytesIO()
    fastavro.schemaless_writer(buf, parsed, record)
    return buf.getvalue()


def decode(data: bytes, schema: Dict[str, Any]) -> Dict[str, Any]:
    """Deserialize Avro binary *data* back to a Python dict using *schema*.

    Args:
        data: Avro-encoded bytes produced by :func:`encode` (or any
            compatible schemaless Avro writer).
        schema: The Avro schema dict used when the data was encoded.

    Returns:
        The deserialized record as a Python dict.
    """
    parsed = fastavro.parse_schema(schema)
    buf = io.BytesIO(data)
    return fastavro.schemaless_reader(buf, parsed)
