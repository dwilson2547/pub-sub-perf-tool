"""Integration tests for Pulsar client using testcontainers."""
import pytest
import time
from pub_sub_perf_tool.clients.pulsar_client import PulsarClient
from pub_sub_perf_tool.base import Message


def test_pulsar_publish_and_consume(pulsar_container):
    """Test publishing and consuming messages with real Pulsar."""
    # Get connection details from container
    host = pulsar_container.get_container_host_ip()
    port = pulsar_container.get_exposed_port(6650)
    service_url = f"pulsar://{host}:{port}"
    
    # Create Pulsar client
    config = {
        'service_url': service_url,
        'use_reader': False
    }
    
    client = PulsarClient(config)
    topic = "persistent://public/default/test-topic"
    
    try:
        # Connect
        client.connect()
        assert client.is_connected()
        
        # Create a test message
        message = Message(
            key=None,
            value=b"Hello Pulsar from testcontainers!",
            headers={"source": "integration-test"}
        )
        
        # Subscribe to topic
        client.subscribe(topic)
        
        # Publish message
        client.publish(topic, message)
        
        # Give Pulsar some time to process
        time.sleep(2)
        
        # Consume message
        consume_result = client.consume(topic, timeout_ms=10000)
        consumed_message = consume_result.message
        
        # Verify message
        assert consumed_message is not None
        assert consumed_message.value == b"Hello Pulsar from testcontainers!"
        assert consumed_message.headers["source"] == "integration-test"
        
    finally:
        # Cleanup
        client.disconnect()
        assert not client.is_connected()


def test_pulsar_reader_mode(pulsar_container):
    """Test Pulsar reader mode."""
    host = pulsar_container.get_container_host_ip()
    port = pulsar_container.get_exposed_port(6650)
    service_url = f"pulsar://{host}:{port}"
    
    # First publish a message
    config_producer = {
        'service_url': service_url,
        'use_reader': False
    }
    producer_client = PulsarClient(config_producer)
    topic = "persistent://public/default/reader-test"
    
    try:
        producer_client.connect()
        
        message = Message(key=None, value=b"Message for reader")
        producer_client.publish(topic, message)
        
        time.sleep(2)
        
        producer_client.disconnect()
    finally:
        if producer_client.is_connected():
            producer_client.disconnect()
    
    # Now read with reader mode
    config_reader = {
        'service_url': service_url,
        'use_reader': True
    }
    reader_client = PulsarClient(config_reader)
    
    try:
        reader_client.connect()
        reader_client.subscribe(topic)
        
        # Read the message
        result = reader_client.consume(topic, timeout_ms=10000)
        consumed = result.message
        
        assert consumed is not None
        assert consumed.value == b"Message for reader"
        
    finally:
        reader_client.disconnect()


def test_pulsar_multiple_messages(pulsar_container):
    """Test publishing and consuming multiple messages."""
    host = pulsar_container.get_container_host_ip()
    port = pulsar_container.get_exposed_port(6650)
    service_url = f"pulsar://{host}:{port}"
    
    config = {
        'service_url': service_url,
        'use_reader': False
    }
    
    client = PulsarClient(config)
    topic = "persistent://public/default/multi-test"
    
    try:
        client.connect()
        client.subscribe(topic)
        
        # Publish multiple messages
        num_messages = 5
        for i in range(num_messages):
            message = Message(
                key=None,
                value=f"Pulsar Message {i}".encode('utf-8'),
                headers={"index": str(i)}
            )
            client.publish(topic, message)
        
        # Give Pulsar time to process
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
            assert f"Pulsar Message {i}".encode('utf-8') == msg.value
            
    finally:
        client.disconnect()


def test_pulsar_persistent_topic(pulsar_container):
    """Test Pulsar persistent topics."""
    host = pulsar_container.get_container_host_ip()
    port = pulsar_container.get_exposed_port(6650)
    service_url = f"pulsar://{host}:{port}"
    
    config = {
        'service_url': service_url,
        'use_reader': False
    }
    
    client = PulsarClient(config)
    topic = "persistent://public/default/persistent-test"
    
    try:
        client.connect()
        client.subscribe(topic)
        
        # Publish message
        message = Message(key=None, value=b"Persistent message")
        client.publish(topic, message)
        
        time.sleep(2)
        
        # Consume and verify
        result = client.consume(topic, timeout_ms=10000)
        consumed = result.message
        assert consumed is not None
        assert consumed.value == b"Persistent message"
        
    finally:
        client.disconnect()
