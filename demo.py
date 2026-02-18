"""
Demo script showing the pub-sub performance tool in action

This script demonstrates the tool without requiring actual messaging system connections.
"""
import json
from pub_sub_perf_tool.base import Message
from pub_sub_perf_tool.flow_engine import Validator, ValidationResult


def demo_validation():
    """Demonstrate validation capabilities"""
    print("=" * 80)
    print("PUB-SUB PERFORMANCE TOOL - VALIDATION DEMO")
    print("=" * 80)
    print()
    
    validator = Validator()
    
    # Demo 1: Exists validation
    print("1. EXISTS VALIDATION")
    print("-" * 80)
    message = Message(key="demo-1", value=b"Hello World")
    result = validator.validate_message(message, {'type': 'exists'})
    print(f"   Message: {message.value.decode()}")
    print(f"   Result: {'✓ PASS' if result.passed else '✗ FAIL'}")
    print(f"   Message: {result.message}")
    print()
    
    # Demo 2: Contains validation
    print("2. CONTAINS VALIDATION")
    print("-" * 80)
    message = Message(key="demo-2", value=b"This is a test message")
    result = validator.validate_message(message, {
        'type': 'contains',
        'params': {'text': 'test'}
    })
    print(f"   Message: {message.value.decode()}")
    print(f"   Looking for: 'test'")
    print(f"   Result: {'✓ PASS' if result.passed else '✗ FAIL'}")
    print(f"   Message: {result.message}")
    print()
    
    # Demo 3: JSON schema validation
    print("3. JSON SCHEMA VALIDATION")
    print("-" * 80)
    data = {'id': '12345', 'timestamp': '2024-01-01T00:00:00Z', 'status': 'active'}
    message = Message(key="demo-3", value=json.dumps(data).encode())
    result = validator.validate_message(message, {
        'type': 'json_schema',
        'params': {'required_fields': ['id', 'timestamp']}
    })
    print(f"   Message: {json.dumps(data, indent=6)}")
    print(f"   Required fields: id, timestamp")
    print(f"   Result: {'✓ PASS' if result.passed else '✗ FAIL'}")
    print(f"   Message: {result.message}")
    print()
    
    # Demo 4: Size validation
    print("4. SIZE VALIDATION")
    print("-" * 80)
    message = Message(key="demo-4", value=b"Short message")
    result = validator.validate_message(message, {
        'type': 'size',
        'params': {'min_bytes': 5, 'max_bytes': 100}
    })
    print(f"   Message: {message.value.decode()}")
    print(f"   Size: {len(message.value)} bytes")
    print(f"   Allowed range: 5-100 bytes")
    print(f"   Result: {'✓ PASS' if result.passed else '✗ FAIL'}")
    print(f"   Message: {result.message}")
    print()
    
    print("=" * 80)
    print("Demo complete! Check out the examples/ directory for full flow configurations.")
    print("=" * 80)


def demo_clients():
    """Demonstrate client capabilities"""
    from pub_sub_perf_tool.clients import KafkaClient, PulsarClient
    
    print("\n" + "=" * 80)
    print("CLIENT CONFIGURATION DEMO")
    print("=" * 80)
    print()
    
    # Kafka with random consumer group
    print("1. KAFKA CLIENT - Random Consumer Group")
    print("-" * 80)
    kafka_config = {'bootstrap_servers': ['localhost:9092']}
    client1 = KafkaClient(kafka_config)
    client2 = KafkaClient(kafka_config)
    print(f"   Client 1 consumer group: {client1.consumer_group}")
    print(f"   Client 2 consumer group: {client2.consumer_group}")
    print(f"   ✓ Consumer groups are unique to prevent conflicts")
    print()
    
    # Pulsar with reader mode
    print("2. PULSAR CLIENT - Reader Mode for Intermediary Hops")
    print("-" * 80)
    pulsar_config = {
        'service_url': 'pulsar://localhost:6650',
        'use_reader': True
    }
    pulsar_client = PulsarClient(pulsar_config)
    print(f"   Service URL: {pulsar_client.service_url}")
    print(f"   Use reader: {pulsar_client.use_reader}")
    print(f"   ✓ Reader mode enabled for intermediary hops")
    print()


if __name__ == '__main__':
    demo_validation()
    demo_clients()
    
    print("\n\nNEXT STEPS:")
    print("-" * 80)
    print("1. Install the tool: pip install -e .")
    print("2. Generate a config: pub-sub-perf generate-config flow.yaml")
    print("3. Run a flow: pub-sub-perf run flow.yaml --message 'Hello'")
    print("4. Check examples: ls examples/")
    print()
