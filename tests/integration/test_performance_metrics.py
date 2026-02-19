"""Integration tests that validate performance metrics and monitoring capabilities."""
import pytest
import time
import json
from pub_sub_perf_tool.clients.kafka_client import KafkaClient
from pub_sub_perf_tool.clients.rabbitmq_client import RabbitMQClient
from pub_sub_perf_tool.base import Message


def test_kafka_latency_tracking(kafka_container):
    """Test that latency metrics are captured correctly."""
    bootstrap_servers = kafka_container.get_bootstrap_server()
    
    config = {
        'bootstrap_servers': [bootstrap_servers],
        'consumer_group': 'latency-test-group'
    }
    
    client = KafkaClient(config)
    topic = "latency-test-topic"
    
    try:
        client.connect()
        client.subscribe([topic])
        
        # Publish a message and track timing
        message = Message(
            key=None,
            value=b"Latency test message",
            headers={"timestamp": str(time.time())}
        )
        
        publish_start = time.time()
        publish_result = client.publish(topic, message)
        publish_latency = (time.time() - publish_start) * 1000
        
        # Verify publish result has latency
        assert publish_result.success
        assert publish_result.latency_ms > 0
        assert publish_result.latency_ms < 5000  # Should be under 5 seconds
        
        time.sleep(1)
        
        # Consume and verify consumption latency
        consume_result = client.consume(topic, timeout_ms=10000)
        
        assert consume_result.message is not None
        assert consume_result.latency_ms > 0
        assert consume_result.latency_ms < 10000  # Should be under 10 seconds
        
        # Verify total round-trip is reasonable
        total_latency = publish_result.latency_ms + consume_result.latency_ms
        assert total_latency < 15000  # Total should be under 15 seconds
        
    finally:
        client.disconnect()


def test_kafka_throughput_multiple_messages(kafka_container):
    """Test throughput with batch message publishing."""
    bootstrap_servers = kafka_container.get_bootstrap_server()
    
    config = {
        'bootstrap_servers': [bootstrap_servers],
        'consumer_group': 'throughput-test-group'
    }
    
    client = KafkaClient(config)
    topic = "throughput-test-topic"
    num_messages = 100
    
    try:
        client.connect()
        client.subscribe([topic])
        
        # Publish multiple messages and track throughput
        start_time = time.time()
        
        for i in range(num_messages):
            message = Message(
                key=None,
                value=f"Message {i}".encode('utf-8'),
                headers={"batch_id": "test-batch-1", "index": str(i)}
            )
            result = client.publish(topic, message)
            assert result.success
        
        publish_duration = time.time() - start_time
        publish_throughput = num_messages / publish_duration
        
        # Should be able to publish at least 10 messages per second
        assert publish_throughput > 10, f"Throughput too low: {publish_throughput} msg/s"
        
        time.sleep(2)
        
        # Consume all messages
        consume_start = time.time()
        consumed_count = 0
        
        for _ in range(num_messages):
            result = client.consume(topic, timeout_ms=5000)
            if result.message:
                consumed_count += 1
        
        consume_duration = time.time() - consume_start
        consume_throughput = consumed_count / consume_duration
        
        # Verify we got all messages
        assert consumed_count == num_messages
        
        # Consumption should also be reasonably fast
        assert consume_throughput > 5, f"Consume throughput too low: {consume_throughput} msg/s"
        
    finally:
        client.disconnect()


def test_rabbitmq_message_size_validation(rabbitmq_container):
    """Test message size tracking and validation."""
    host = rabbitmq_container.get_container_host_ip()
    port = rabbitmq_container.get_exposed_port(5672)
    
    config = {
        'host': host,
        'port': int(port),
        'username': 'guest',
        'password': 'guest',
        'exchange': 'size-test-exchange',
        'exchange_type': 'topic'
    }
    
    client = RabbitMQClient(config)
    topic = "size.test"
    
    try:
        client.connect()
        client.subscribe([topic])
        
        # Test small message
        small_message = Message(
            key=None,
            value=b"Small",
            headers={"size_category": "small"}
        )
        client.publish(topic, small_message)
        
        # Test medium message (1KB)
        medium_message = Message(
            key=None,
            value=b"X" * 1024,
            headers={"size_category": "medium"}
        )
        client.publish(topic, medium_message)
        
        # Test large message (100KB)
        large_message = Message(
            key=None,
            value=b"X" * 102400,
            headers={"size_category": "large"}
        )
        client.publish(topic, large_message)
        
        time.sleep(2)
        
        # Consume and verify sizes
        for expected_size_category in ["small", "medium", "large"]:
            result = client.consume(topic, timeout_ms=10000)
            assert result.message is not None
            assert result.message.headers["size_category"] == expected_size_category
            
            # Verify message size is tracked
            message_size = len(result.message.value)
            if expected_size_category == "small":
                assert message_size < 100
            elif expected_size_category == "medium":
                assert 1000 <= message_size <= 1100
            elif expected_size_category == "large":
                assert message_size >= 100000
        
    finally:
        client.disconnect()


def test_kafka_message_ordering(kafka_container):
    """Test that message ordering is preserved within a partition."""
    bootstrap_servers = kafka_container.get_bootstrap_server()
    
    config = {
        'bootstrap_servers': [bootstrap_servers],
        'consumer_group': 'ordering-test-group'
    }
    
    client = KafkaClient(config)
    topic = "ordering-test-topic"
    num_messages = 20
    partition_key = "same-partition-key"
    
    try:
        client.connect()
        client.subscribe([topic])
        
        # Publish messages with the same key to ensure same partition
        for i in range(num_messages):
            message = Message(
                key=partition_key,
                value=f"Ordered message {i}".encode('utf-8'),
                headers={"sequence": str(i)}
            )
            result = client.publish(topic, message)
            assert result.success
        
        time.sleep(2)
        
        # Consume and verify ordering
        for expected_seq in range(num_messages):
            result = client.consume(topic, timeout_ms=10000)
            assert result.message is not None
            assert result.message.key == partition_key
            
            actual_seq = int(result.message.headers["sequence"])
            assert actual_seq == expected_seq, f"Expected sequence {expected_seq}, got {actual_seq}"
        
    finally:
        client.disconnect()


def test_kafka_concurrent_consumers(kafka_container):
    """Test behavior with multiple consumers in same consumer group."""
    bootstrap_servers = kafka_container.get_bootstrap_server()
    topic = "concurrent-test-topic"
    num_messages = 30
    
    # Create two clients in the same consumer group
    config1 = {
        'bootstrap_servers': [bootstrap_servers],
        'consumer_group': 'shared-consumer-group'
    }
    config2 = {
        'bootstrap_servers': [bootstrap_servers],
        'consumer_group': 'shared-consumer-group'
    }
    
    producer = KafkaClient(config1)
    consumer1 = KafkaClient(config1)
    consumer2 = KafkaClient(config2)
    
    try:
        producer.connect()
        consumer1.connect()
        consumer2.connect()
        
        consumer1.subscribe([topic])
        consumer2.subscribe([topic])
        
        # Publish messages
        for i in range(num_messages):
            message = Message(
                key=None,
                value=f"Concurrent test message {i}".encode('utf-8')
            )
            producer.publish(topic, message)
        
        time.sleep(3)
        
        # Both consumers should receive some messages (load balancing)
        consumer1_count = 0
        consumer2_count = 0
        
        # Try to consume from both
        for _ in range(num_messages):
            result1 = consumer1.consume(topic, timeout_ms=1000)
            if result1.message:
                consumer1_count += 1
            
            result2 = consumer2.consume(topic, timeout_ms=1000)
            if result2.message:
                consumer2_count += 1
        
        # Both consumers should have received messages
        # (Kafka distributes across consumer group members)
        total_consumed = consumer1_count + consumer2_count
        assert total_consumed == num_messages, f"Expected {num_messages}, got {total_consumed}"
        
        # Both consumers should have gotten some (though distribution may vary)
        assert consumer1_count > 0, "Consumer 1 received no messages"
        assert consumer2_count > 0, "Consumer 2 received no messages"
        
    finally:
        producer.disconnect()
        consumer1.disconnect()
        consumer2.disconnect()
