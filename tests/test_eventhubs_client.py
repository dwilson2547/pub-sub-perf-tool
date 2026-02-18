"""Unit tests for EventHubs client"""
import pytest
from pub_sub_perf_tool.clients.eventhubs_client import EventHubsClient
from pub_sub_perf_tool.base import Message


def test_eventhubs_client_initialization():
    """Test EventHubs client initialization"""
    config = {
        'connection_string': 'Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=test',
        'eventhub_name': 'test-hub'
    }
    client = EventHubsClient(config)
    
    assert client.connection_string == config['connection_string']
    assert client.eventhub_name == 'test-hub'
    assert not client.is_connected()


def test_eventhubs_client_default_consumer_group():
    """Test that EventHubs client uses default consumer group"""
    config = {
        'connection_string': 'Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=test',
        'eventhub_name': 'test-hub'
    }
    
    client = EventHubsClient(config)
    assert client.consumer_group == '$Default'


def test_eventhubs_client_custom_consumer_group():
    """Test that EventHubs client uses custom consumer group if provided"""
    config = {
        'connection_string': 'Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=test',
        'eventhub_name': 'test-hub',
        'consumer_group': 'my-custom-group'
    }
    
    client = EventHubsClient(config)
    assert client.consumer_group == 'my-custom-group'
