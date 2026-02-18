"""Unit tests for Kafka client"""
import pytest
import uuid
from pub_sub_perf_tool.clients.kafka_client import KafkaClient
from pub_sub_perf_tool.base import Message


def test_kafka_client_random_consumer_group():
    """Test that Kafka client generates random consumer group ID"""
    config1 = {'bootstrap_servers': ['localhost:9092']}
    config2 = {'bootstrap_servers': ['localhost:9092']}
    
    client1 = KafkaClient(config1)
    client2 = KafkaClient(config2)
    
    # Consumer groups should be different
    assert client1.consumer_group != client2.consumer_group
    
    # Consumer groups should follow the pattern
    assert client1.consumer_group.startswith('perf-tool-')
    assert client2.consumer_group.startswith('perf-tool-')


def test_kafka_client_custom_consumer_group():
    """Test that Kafka client uses custom consumer group if provided"""
    config = {
        'bootstrap_servers': ['localhost:9092'],
        'consumer_group': 'my-custom-group'
    }
    
    client = KafkaClient(config)
    assert client.consumer_group == 'my-custom-group'


def test_kafka_client_initialization():
    """Test Kafka client initialization"""
    config = {'bootstrap_servers': ['localhost:9092']}
    client = KafkaClient(config)
    
    assert client.bootstrap_servers == ['localhost:9092']
    assert not client.is_connected()
