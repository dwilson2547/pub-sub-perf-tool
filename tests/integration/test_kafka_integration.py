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
            content="Hello Kafka from testcontainers!",
            headers={"source": "integration-test"}
        )
        
        # Subscribe to topic
        client.subscribe(topic)
        
        # Publish message
        client.publish(topic, message)
        
        # Give Kafka some time to process
        time.sleep(2)
        
        # Consume message
        consumed_message = client.consume(timeout=10)
        
        # Verify message
        assert consumed_message is not None
        assert consumed_message.content == "Hello Kafka from testcontainers!"
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
        client.subscribe(topic)
        
        # Publish multiple messages
        num_messages = 5
        for i in range(num_messages):
            message = Message(
                content=f"Message {i}",
                headers={"index": str(i)}
            )
            client.publish(topic, message)
        
        # Give Kafka time to process
        time.sleep(2)
        
        # Consume all messages
        messages = []
        for _ in range(num_messages):
            msg = client.consume(timeout=10)
            if msg:
                messages.append(msg)
        
        # Verify
        assert len(messages) == num_messages
        for i, msg in enumerate(messages):
            assert f"Message {i}" in msg.content
            
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
        client.subscribe(topic)
        
        # Publish with key
        message = Message(
            content="Message with key",
            key="test-key-123"
        )
        client.publish(topic, message)
        
        time.sleep(2)
        
        # Consume and verify
        consumed = client.consume(timeout=10)
        assert consumed is not None
        assert consumed.content == "Message with key"
        assert consumed.key == "test-key-123"
        
    finally:
        client.disconnect()
