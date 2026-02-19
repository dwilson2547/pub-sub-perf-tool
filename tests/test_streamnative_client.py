"""Unit tests for StreamNative client"""
import pytest
from pub_sub_perf_tool.clients.streamnative_client import StreamNativeClient
from pub_sub_perf_tool.base import Message


def test_streamnative_client_initialization():
    """Test StreamNative client initialization"""
    config = {
        'service_url': 'pulsar+ssl://streamnative.cloud:6651'
    }
    client = StreamNativeClient(config)
    
    assert client.service_url == 'pulsar+ssl://streamnative.cloud:6651'
    assert not client.is_connected()


def test_streamnative_client_with_oauth2_auth():
    """Test StreamNative client initialization with OAuth2 auth"""
    config = {
        'service_url': 'pulsar+ssl://streamnative.cloud:6651',
        'auth_params': {
            'type': 'oauth2',
            'issuer_url': 'https://auth.streamnative.cloud',
            'client_id': 'test-client',
            'client_secret': 'test-secret',
            'audience': 'urn:sn:pulsar:test'
        }
    }
    
    client = StreamNativeClient(config)
    assert client.auth_params['type'] == 'oauth2'
    assert client.auth_params['client_id'] == 'test-client'


def test_streamnative_client_with_token_auth():
    """Test StreamNative client initialization with token auth"""
    config = {
        'service_url': 'pulsar+ssl://streamnative.cloud:6651',
        'auth_params': {
            'type': 'token',
            'token': 'test-token-123'
        }
    }
    
    client = StreamNativeClient(config)
    assert client.auth_params['type'] == 'token'
    assert client.auth_params['token'] == 'test-token-123'


def test_streamnative_client_reader_mode():
    """Test StreamNative client with reader mode"""
    config = {
        'service_url': 'pulsar+ssl://streamnative.cloud:6651',
        'use_reader': True
    }
    
    client = StreamNativeClient(config)
    assert client.use_reader is True
