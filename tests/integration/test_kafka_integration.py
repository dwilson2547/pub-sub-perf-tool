"""Integration tests for Kafka client using testcontainers."""
import pytest
import time
from pub_sub_perf_tool.clients.kafka_client import KafkaClient
from pub_sub_perf_tool.base import Message


def test_kafka_publish_and_consume(kafka_container):
    """Test publishing and consuming messages with real Kafka."""
    # Get connection details from container
    bootstrap_servers = kafka_container.get_bootstrap_server()
    
    # Create Kafka client
    config = {
        'bootstrap_servers': [bootstrap_servers],
        'consumer_group': 'test-group'
    }
    
    client = KafkaClient(config)
    topic = "test-topic"
    
    try:
        # Connect
        client.connect()
        assert client.is_connected()
        
        # Create a test message
        message = Message(
            key=None,
            value=b"Hello Kafka from testcontainers!",
            headers={"source": "integration-test"}
        )
        
        # Subscribe to topic
        client.subscribe([topic])
        
        # Publish message
        client.publish(topic, message)
        
        # Give Kafka some time to process
        time.sleep(2)
        
        # Consume message
        consume_result = client.consume(topic, timeout_ms=10000)
        consumed_message = consume_result.message
        
        # Verify message
        assert consumed_message is not None
        assert consumed_message.value == b"Hello Kafka from testcontainers!"
        assert consumed_message.headers["source"] == "integration-test"
        
    finally:
        # Cleanup
        client.disconnect()
        assert not client.is_connected()


def test_kafka_multiple_messages(kafka_container):
    """Test publishing and consuming multiple messages."""
    bootstrap_servers = kafka_container.get_bootstrap_server()
    
    config = {
        'bootstrap_servers': [bootstrap_servers],
        'consumer_group': 'test-group-multi'
    }
    
    client = KafkaClient(config)
    topic = "test-topic-multi"
    
    try:
        client.connect()
        client.subscribe([topic])
        
        # Publish multiple messages
        num_messages = 5
        for i in range(num_messages):
            message = Message(
                key=None,
                value=f"Message {i}".encode('utf-8'),
                headers={"index": str(i)}
            )
            client.publish(topic, message)
        
        # Give Kafka time to process
        time.sleep(2)
        
        # Consume all messages
        messages = []
        for _ in range(num_messages):
            result = client.consume(topic, timeout_ms=10000)
            if result.message:
                messages.append(result.message)
        
        # Verify
        assert len(messages) == num_messages
        for i, msg in enumerate(messages):
            assert f"Message {i}".encode('utf-8') == msg.value
            
    finally:
        client.disconnect()


def test_kafka_message_key(kafka_container):
    """Test publishing with message key."""
    bootstrap_servers = kafka_container.get_bootstrap_server()
    
    config = {
        'bootstrap_servers': [bootstrap_servers],
        'consumer_group': 'test-group-key'
    }
    
    client = KafkaClient(config)
    topic = "test-topic-key"
    
    try:
        client.connect()
        client.subscribe([topic])
        
        # Publish with key
        message = Message(
            key="test-key-123",
            value=b"Message with key"
        )
        client.publish(topic, message)
        
        time.sleep(2)
        
        # Consume and verify
        result = client.consume(topic, timeout_ms=10000)
        consumed = result.message
        assert consumed is not None
        assert consumed.value == b"Message with key"
        assert consumed.key == "test-key-123"
        
    finally:
        client.disconnect()
