"""Unit tests for Pulsar client"""
import pytest
from pub_sub_perf_tool.clients.pulsar_client import PulsarClient
from pub_sub_perf_tool.base import Message


def test_pulsar_client_reader_mode():
    """Test that Pulsar client respects use_reader flag"""
    config = {
        'service_url': 'pulsar://localhost:6650',
        'use_reader': True
    }
    
    client = PulsarClient(config)
    assert client.use_reader is True


def test_pulsar_client_consumer_mode():
    """Test that Pulsar client defaults to consumer mode"""
    config = {
        'service_url': 'pulsar://localhost:6650'
    }
    
    client = PulsarClient(config)
    assert client.use_reader is False


def test_pulsar_client_initialization():
    """Test Pulsar client initialization"""
    config = {
        'service_url': 'pulsar://localhost:6650',
        'use_reader': True,
        'subscription_name': 'test-sub'
    }
    
    client = PulsarClient(config)
    assert client.service_url == 'pulsar://localhost:6650'
    assert client.use_reader is True
    assert client.subscription_name == 'test-sub'
    assert not client.is_connected()
