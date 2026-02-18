"""Unit tests for validation"""
import pytest
import json
from pub_sub_perf_tool.flow_engine import Validator
from pub_sub_perf_tool.base import Message


def test_validator_exists():
    """Test exists validation"""
    validator = Validator()
    message = Message(key="test", value=b"test content")
    
    result = validator.validate_message(message, {'type': 'exists'})
    assert result.passed is True


def test_validator_contains_pass():
    """Test contains validation - pass case"""
    validator = Validator()
    message = Message(key="test", value=b"hello world")
    
    result = validator.validate_message(message, {
        'type': 'contains',
        'params': {'text': 'world'}
    })
    assert result.passed is True


def test_validator_contains_fail():
    """Test contains validation - fail case"""
    validator = Validator()
    message = Message(key="test", value=b"hello world")
    
    result = validator.validate_message(message, {
        'type': 'contains',
        'params': {'text': 'missing'}
    })
    assert result.passed is False


def test_validator_json_schema_pass():
    """Test JSON schema validation - pass case"""
    validator = Validator()
    data = {'id': '123', 'timestamp': '2024-01-01', 'extra': 'field'}
    message = Message(key="test", value=json.dumps(data).encode('utf-8'))
    
    result = validator.validate_message(message, {
        'type': 'json_schema',
        'params': {'required_fields': ['id', 'timestamp']}
    })
    assert result.passed is True


def test_validator_json_schema_fail():
    """Test JSON schema validation - fail case"""
    validator = Validator()
    data = {'id': '123'}
    message = Message(key="test", value=json.dumps(data).encode('utf-8'))
    
    result = validator.validate_message(message, {
        'type': 'json_schema',
        'params': {'required_fields': ['id', 'timestamp']}
    })
    assert result.passed is False
    assert 'timestamp' in result.message


def test_validator_size_pass():
    """Test size validation - pass case"""
    validator = Validator()
    message = Message(key="test", value=b"hello")
    
    result = validator.validate_message(message, {
        'type': 'size',
        'params': {'min_bytes': 1, 'max_bytes': 10}
    })
    assert result.passed is True


def test_validator_size_fail():
    """Test size validation - fail case"""
    validator = Validator()
    message = Message(key="test", value=b"hello world this is a long message")
    
    result = validator.validate_message(message, {
        'type': 'size',
        'params': {'min_bytes': 1, 'max_bytes': 10}
    })
    assert result.passed is False


def test_validator_no_message():
    """Test validation with no message"""
    validator = Validator()
    
    result = validator.validate_message(None, {'type': 'exists'})
    assert result.passed is False
    assert "No message" in result.message
