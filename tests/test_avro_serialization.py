"""Unit tests for Avro serialization support"""
import pytest
from pub_sub_perf_tool.serialization.avro import encode, decode
from pub_sub_perf_tool.base import Message
from pub_sub_perf_tool.flow_engine import Validator


# ---------------------------------------------------------------------------
# Sample schema used across all tests
# ---------------------------------------------------------------------------

SAMPLE_SCHEMA = {
    "type": "record",
    "name": "SensorEvent",
    "namespace": "com.example",
    "fields": [
        {"name": "sensor_id", "type": "string"},
        {"name": "temperature", "type": "float"},
        {"name": "timestamp_ms", "type": "long"},
        {"name": "active", "type": "boolean"},
    ],
}

SAMPLE_RECORD = {
    "sensor_id": "sensor-42",
    "temperature": 23.5,
    "timestamp_ms": 1700000000000,
    "active": True,
}


# ---------------------------------------------------------------------------
# encode / decode tests
# ---------------------------------------------------------------------------

def test_avro_encode_returns_bytes():
    """encode() should return a non-empty bytes object"""
    result = encode(SAMPLE_RECORD, SAMPLE_SCHEMA)
    assert isinstance(result, bytes)
    assert len(result) > 0


def test_avro_roundtrip():
    """Encoding then decoding should produce the original record"""
    raw = encode(SAMPLE_RECORD, SAMPLE_SCHEMA)
    recovered = decode(raw, SAMPLE_SCHEMA)
    assert recovered["sensor_id"] == SAMPLE_RECORD["sensor_id"]
    assert recovered["timestamp_ms"] == SAMPLE_RECORD["timestamp_ms"]
    assert recovered["active"] == SAMPLE_RECORD["active"]
    assert abs(recovered["temperature"] - SAMPLE_RECORD["temperature"]) < 1e-4


def test_avro_encode_different_records_produce_different_bytes():
    """Two distinct records should encode to different byte sequences"""
    record_a = dict(SAMPLE_RECORD, sensor_id="a", temperature=10.0)
    record_b = dict(SAMPLE_RECORD, sensor_id="b", temperature=20.0)
    assert encode(record_a, SAMPLE_SCHEMA) != encode(record_b, SAMPLE_SCHEMA)


def test_avro_decode_invalid_bytes_raises():
    """decode() should raise an exception for garbage input"""
    with pytest.raises(Exception):
        decode(b"\x00\xff\xde\xad\xbe\xef", SAMPLE_SCHEMA)


def test_avro_encode_missing_required_field_raises():
    """encode() should raise when a required field is absent"""
    incomplete = {"sensor_id": "x", "temperature": 1.0}
    with pytest.raises(Exception):
        encode(incomplete, SAMPLE_SCHEMA)


def test_avro_encode_nullable_field():
    """encode() should handle nullable (union) fields correctly"""
    schema_with_null = {
        "type": "record",
        "name": "NullableEvent",
        "fields": [
            {"name": "id", "type": "string"},
            {"name": "note", "type": ["null", "string"], "default": None},
        ],
    }
    record_no_note = {"id": "abc", "note": None}
    record_with_note = {"id": "def", "note": "hello"}

    assert decode(encode(record_no_note, schema_with_null), schema_with_null) == record_no_note
    assert decode(encode(record_with_note, schema_with_null), schema_with_null) == record_with_note


# ---------------------------------------------------------------------------
# Validator.validate_message – avro_schema type
# ---------------------------------------------------------------------------

validator = Validator()


def test_validator_avro_schema_passes_valid_message():
    """avro_schema validation should pass for a correctly encoded message"""
    raw = encode(SAMPLE_RECORD, SAMPLE_SCHEMA)
    msg = Message(key=None, value=raw)
    result = validator.validate_message(msg, {
        "type": "avro_schema",
        "params": {"schema": SAMPLE_SCHEMA},
    })
    assert result.passed
    assert "passed" in result.message


def test_validator_avro_schema_includes_decoded_record():
    """avro_schema validation details should contain the decoded record"""
    raw = encode(SAMPLE_RECORD, SAMPLE_SCHEMA)
    msg = Message(key=None, value=raw)
    result = validator.validate_message(msg, {
        "type": "avro_schema",
        "params": {"schema": SAMPLE_SCHEMA},
    })
    assert result.passed
    assert "record" in result.details
    assert result.details["record"]["sensor_id"] == SAMPLE_RECORD["sensor_id"]


def test_validator_avro_schema_required_fields_pass():
    """avro_schema validation passes when all required_fields are present"""
    raw = encode(SAMPLE_RECORD, SAMPLE_SCHEMA)
    msg = Message(key=None, value=raw)
    result = validator.validate_message(msg, {
        "type": "avro_schema",
        "params": {
            "schema": SAMPLE_SCHEMA,
            "required_fields": ["sensor_id", "temperature"],
        },
    })
    assert result.passed


def test_validator_avro_schema_required_fields_fail():
    """avro_schema validation fails when a required_field is missing from decoded record"""
    raw = encode(SAMPLE_RECORD, SAMPLE_SCHEMA)
    msg = Message(key=None, value=raw)
    result = validator.validate_message(msg, {
        "type": "avro_schema",
        "params": {
            "schema": SAMPLE_SCHEMA,
            "required_fields": ["nonexistent_field"],
        },
    })
    assert not result.passed
    assert "nonexistent_field" in result.details.get("missing_fields", [])


def test_validator_avro_schema_fails_on_invalid_bytes():
    """avro_schema validation should fail gracefully for malformed data"""
    msg = Message(key=None, value=b"\x00\xff\xde\xad")
    result = validator.validate_message(msg, {
        "type": "avro_schema",
        "params": {"schema": SAMPLE_SCHEMA},
    })
    assert not result.passed
    assert "Avro decode error" in result.message


def test_validator_avro_schema_fails_without_schema_param():
    """avro_schema validation should fail when 'schema' param is missing"""
    raw = encode(SAMPLE_RECORD, SAMPLE_SCHEMA)
    msg = Message(key=None, value=raw)
    result = validator.validate_message(msg, {
        "type": "avro_schema",
        "params": {},
    })
    assert not result.passed
    assert "schema" in result.message
