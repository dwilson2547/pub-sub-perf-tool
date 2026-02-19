"""Integration tests for RabbitMQ client using testcontainers."""
import pytest
import time
from pub_sub_perf_tool.clients.rabbitmq_client import RabbitMQClient
from pub_sub_perf_tool.base import Message


def test_rabbitmq_publish_and_consume(rabbitmq_container):
    """Test publishing and consuming messages with real RabbitMQ."""
    # Get connection details from container
    host = rabbitmq_container.get_container_host_ip()
    port = rabbitmq_container.get_exposed_port(5672)
    
    # Create RabbitMQ client
    config = {
        'host': host,
        'port': int(port),
        'username': 'guest',
        'password': 'guest',
        'exchange': 'test-exchange',
        'exchange_type': 'topic'
    }
    
    client = RabbitMQClient(config)
    topic = "test.routing.key"
    
    try:
        # Connect
        client.connect()
        assert client.is_connected()
        
        # Create a test message
        message = Message(
            content="Hello RabbitMQ from testcontainers!",
            headers={"source": "integration-test"}
        )
        
        # Subscribe to topic
        client.subscribe(topic)
        
        # Publish message
        client.publish(topic, message)
        
        # Give RabbitMQ some time to process
        time.sleep(2)
        
        # Consume message
        consumed_message = client.consume(timeout=10)
        
        # Verify message
        assert consumed_message is not None
        assert consumed_message.content == "Hello RabbitMQ from testcontainers!"
        assert consumed_message.headers["source"] == "integration-test"
        
    finally:
        # Cleanup
        client.disconnect()
        assert not client.is_connected()


def test_rabbitmq_multiple_messages(rabbitmq_container):
    """Test publishing and consuming multiple messages."""
    host = rabbitmq_container.get_container_host_ip()
    port = rabbitmq_container.get_exposed_port(5672)
    
    config = {
        'host': host,
        'port': int(port),
        'username': 'guest',
        'password': 'guest',
        'exchange': 'test-exchange-multi',
        'exchange_type': 'topic'
    }
    
    client = RabbitMQClient(config)
    topic = "test.multi"
    
    try:
        client.connect()
        client.subscribe(topic)
        
        # Publish multiple messages
        num_messages = 5
        for i in range(num_messages):
            message = Message(
                content=f"RabbitMQ Message {i}",
                headers={"index": str(i)}
            )
            client.publish(topic, message)
        
        # Give RabbitMQ time to process
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
            assert f"RabbitMQ Message {i}" in msg.content
            
    finally:
        client.disconnect()


def test_rabbitmq_routing(rabbitmq_container):
    """Test RabbitMQ topic routing."""
    host = rabbitmq_container.get_container_host_ip()
    port = rabbitmq_container.get_exposed_port(5672)
    
    config = {
        'host': host,
        'port': int(port),
        'username': 'guest',
        'password': 'guest',
        'exchange': 'test-routing-exchange',
        'exchange_type': 'topic'
    }
    
    client = RabbitMQClient(config)
    
    try:
        client.connect()
        
        # Subscribe to a pattern
        client.subscribe("test.routing.*")
        
        # Publish to matching routing key
        message = Message(content="Routed message")
        client.publish("test.routing.key1", message)
        
        time.sleep(2)
        
        # Should receive the message
        consumed = client.consume(timeout=10)
        assert consumed is not None
        assert consumed.content == "Routed message"
        
    finally:
        client.disconnect()
