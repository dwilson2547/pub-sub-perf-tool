"""Unit tests for Google Pub/Sub client"""
import pytest
from pub_sub_perf_tool.clients.googlepubsub_client import GooglePubSubClient
from pub_sub_perf_tool.base import Message


def test_googlepubsub_client_initialization():
    """Test Google Pub/Sub client initialization"""
    config = {
        'project_id': 'test-project-123'
    }
    client = GooglePubSubClient(config)
    
    assert client.project_id == 'test-project-123'
    assert not client.is_connected()


def test_googlepubsub_client_with_credentials():
    """Test Google Pub/Sub client initialization with credentials path"""
    config = {
        'project_id': 'test-project-123',
        'credentials_path': '/path/to/credentials.json'
    }
    
    client = GooglePubSubClient(config)
    assert client.project_id == 'test-project-123'
    assert client.credentials_path == '/path/to/credentials.json'


def test_googlepubsub_client_subscriptions():
    """Test Google Pub/Sub client subscription tracking"""
    config = {
        'project_id': 'test-project-123'
    }
    
    client = GooglePubSubClient(config)
    assert client.subscriptions == {}
