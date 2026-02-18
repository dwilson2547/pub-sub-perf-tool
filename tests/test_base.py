"""Unit tests for pub-sub performance tool"""
import pytest
from pub_sub_perf_tool.base import Message, PubSubClient


def test_message_creation():
    """Test message creation"""
    msg = Message(
        key="test-key",
        value=b"test-value",
        headers={"header1": "value1"}
    )
    
    assert msg.key == "test-key"
    assert msg.value == b"test-value"
    assert msg.headers == {"header1": "value1"}
    assert msg.timestamp is not None


def test_message_default_timestamp():
    """Test message with default timestamp"""
    msg = Message(key=None, value=b"test")
    assert msg.timestamp is not None
    assert isinstance(msg.timestamp, float)
