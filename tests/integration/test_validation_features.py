"""Integration tests for validation features with real message brokers."""
import pytest
import time
import json
from pub_sub_perf_tool.flow_engine import MessageFlowEngine, Validator
from pub_sub_perf_tool.base import Message


def test_validation_exists(kafka_container):
    """Test 'exists' validation in a real flow."""
    bootstrap_servers = kafka_container.get_bootstrap_server()
    
    flow_config = {
        'name': 'exists-validation-flow',
        'hops': [
            {
                'name': 'publish-and-validate-exists',
                'destination': {
                    'type': 'kafka',
                    'topic': 'exists-validation-topic',
                    'config': {
                        'bootstrap_servers': [bootstrap_servers]
                    }
                },
                'validation': {
                    'type': 'exists'
                }
            }
        ]
    }
    
    engine = MessageFlowEngine(flow_config)
    message = Message(key=None, value=b'Test message for exists validation')
    
    results = engine.execute(message, num_messages=1)
    
    assert results.success
    assert len(results.hops) == 1
    assert results.hops[0].validation.passed
    assert results.hops[0].validation.message == "Message exists"


def test_validation_contains_pass(kafka_container):
    """Test 'contains' validation with matching content."""
    bootstrap_servers = kafka_container.get_bootstrap_server()
    
    flow_config = {
        'name': 'contains-validation-flow',
        'hops': [
            {
                'name': 'publish-with-contains-validation',
                'destination': {
                    'type': 'kafka',
                    'topic': 'contains-validation-topic',
                    'config': {
                        'bootstrap_servers': [bootstrap_servers]
                    }
                },
                'validation': {
                    'type': 'contains',
                    'params': {
                        'text': 'important-keyword'
                    }
                }
            }
        ]
    }
    
    engine = MessageFlowEngine(flow_config)
    message = Message(key=None, value=b'This message contains the important-keyword we are looking for')
    
    results = engine.execute(message, num_messages=1)
    
    assert results.success
    assert results.hops[0].validation.passed
    assert 'important-keyword' in results.hops[0].validation.message


def test_validation_contains_fail(kafka_container):
    """Test 'contains' validation with non-matching content."""
    bootstrap_servers = kafka_container.get_bootstrap_server()
    
    flow_config = {
        'name': 'contains-fail-validation-flow',
        'hops': [
            {
                'name': 'publish-with-contains-validation',
                'destination': {
                    'type': 'kafka',
                    'topic': 'contains-fail-validation-topic',
                    'config': {
                        'bootstrap_servers': [bootstrap_servers]
                    }
                },
                'validation': {
                    'type': 'contains',
                    'params': {
                        'text': 'missing-keyword'
                    }
                }
            }
        ]
    }
    
    engine = MessageFlowEngine(flow_config)
    message = Message(key=None, value=b'This message does not have the expected content')
    
    results = engine.execute(message, num_messages=1)
    
    # Flow should fail validation
    assert not results.success
    assert not results.hops[0].validation.passed
    assert 'does not contain' in results.hops[0].validation.message


def test_validation_json_schema(kafka_container):
    """Test JSON schema validation with structured data."""
    bootstrap_servers = kafka_container.get_bootstrap_server()
    
    flow_config = {
        'name': 'json-schema-validation-flow',
        'hops': [
            {
                'name': 'publish-json-with-validation',
                'destination': {
                    'type': 'kafka',
                    'topic': 'json-validation-topic',
                    'config': {
                        'bootstrap_servers': [bootstrap_servers]
                    }
                },
                'validation': {
                    'type': 'json_schema',
                    'params': {
                        'required_fields': ['user_id', 'timestamp', 'event_type']
                    }
                }
            }
        ]
    }
    
    # Valid JSON message with required fields
    valid_data = {
        'user_id': 'user123',
        'timestamp': '2024-01-01T00:00:00Z',
        'event_type': 'login',
        'metadata': {'browser': 'Chrome'}
    }
    
    engine = MessageFlowEngine(flow_config)
    message = Message(key=None, value=json.dumps(valid_data).encode('utf-8'))
    
    results = engine.execute(message, num_messages=1)
    
    assert results.success
    assert results.hops[0].validation.passed
    assert 'JSON schema validation passed' in results.hops[0].validation.message


def test_validation_json_schema_missing_fields(kafka_container):
    """Test JSON schema validation with missing required fields."""
    bootstrap_servers = kafka_container.get_bootstrap_server()
    
    flow_config = {
        'name': 'json-schema-fail-validation-flow',
        'hops': [
            {
                'name': 'publish-incomplete-json',
                'destination': {
                    'type': 'kafka',
                    'topic': 'json-fail-validation-topic',
                    'config': {
                        'bootstrap_servers': [bootstrap_servers]
                    }
                },
                'validation': {
                    'type': 'json_schema',
                    'params': {
                        'required_fields': ['user_id', 'timestamp', 'event_type']
                    }
                }
            }
        ]
    }
    
    # Invalid JSON message - missing 'event_type'
    invalid_data = {
        'user_id': 'user123',
        'timestamp': '2024-01-01T00:00:00Z'
    }
    
    engine = MessageFlowEngine(flow_config)
    message = Message(key=None, value=json.dumps(invalid_data).encode('utf-8'))
    
    results = engine.execute(message, num_messages=1)
    
    # Validation should fail
    assert not results.success
    assert not results.hops[0].validation.passed
    assert 'Missing required fields' in results.hops[0].validation.message
    assert 'event_type' in results.hops[0].validation.details.get('missing_fields', [])


def test_validation_size_within_range(rabbitmq_container):
    """Test size validation with message within acceptable range."""
    host = rabbitmq_container.get_container_host_ip()
    port = rabbitmq_container.get_exposed_port(5672)
    
    flow_config = {
        'name': 'size-validation-flow',
        'hops': [
            {
                'name': 'publish-with-size-validation',
                'destination': {
                    'type': 'rabbitmq',
                    'topic': 'size.validation',
                    'config': {
                        'host': host,
                        'port': int(port),
                        'username': 'guest',
                        'password': 'guest',
                        'exchange': 'size-validation-exchange',
                        'exchange_type': 'topic'
                    }
                },
                'validation': {
                    'type': 'size',
                    'params': {
                        'min_bytes': 10,
                        'max_bytes': 1000
                    }
                }
            }
        ]
    }
    
    engine = MessageFlowEngine(flow_config)
    message = Message(key=None, value=b'This message is the right size for validation')
    
    results = engine.execute(message, num_messages=1)
    
    assert results.success
    assert results.hops[0].validation.passed
    assert 'within range' in results.hops[0].validation.message


def test_validation_size_too_large(rabbitmq_container):
    """Test size validation with message exceeding max size."""
    host = rabbitmq_container.get_container_host_ip()
    port = rabbitmq_container.get_exposed_port(5672)
    
    flow_config = {
        'name': 'size-fail-validation-flow',
        'hops': [
            {
                'name': 'publish-large-message',
                'destination': {
                    'type': 'rabbitmq',
                    'topic': 'size.fail.validation',
                    'config': {
                        'host': host,
                        'port': int(port),
                        'username': 'guest',
                        'password': 'guest',
                        'exchange': 'size-fail-exchange',
                        'exchange_type': 'topic'
                    }
                },
                'validation': {
                    'type': 'size',
                    'params': {
                        'min_bytes': 10,
                        'max_bytes': 100
                    }
                }
            }
        ]
    }
    
    # Message that's too large
    engine = MessageFlowEngine(flow_config)
    large_message = Message(key=None, value=b'X' * 200)
    
    results = engine.execute(large_message, num_messages=1)
    
    # Validation should fail
    assert not results.success
    assert not results.hops[0].validation.passed
    assert 'out of range' in results.hops[0].validation.message


def test_multi_hop_validation_chain(kafka_container, pulsar_container):
    """Test validation at each hop in a multi-system flow."""
    kafka_bootstrap = kafka_container.get_bootstrap_server()
    pulsar_host = pulsar_container.get_container_host_ip()
    pulsar_port = pulsar_container.get_exposed_port(6650)
    pulsar_url = f"pulsar://{pulsar_host}:{pulsar_port}"
    
    flow_config = {
        'name': 'multi-hop-validation-flow',
        'hops': [
            {
                'name': 'kafka-hop-with-json-validation',
                'destination': {
                    'type': 'kafka',
                    'topic': 'validation-chain-kafka',
                    'config': {
                        'bootstrap_servers': [kafka_bootstrap]
                    }
                },
                'validation': {
                    'type': 'json_schema',
                    'params': {
                        'required_fields': ['id', 'data']
                    }
                }
            },
            {
                'name': 'kafka-to-pulsar-with-contains-validation',
                'source': {
                    'type': 'kafka',
                    'topic': 'validation-chain-kafka',
                    'config': {
                        'bootstrap_servers': [kafka_bootstrap]
                    }
                },
                'destination': {
                    'type': 'pulsar',
                    'topic': 'persistent://public/default/validation-chain-output',
                    'config': {
                        'service_url': pulsar_url
                    }
                },
                'validation': {
                    'type': 'contains',
                    'params': {
                        'text': 'data'
                    }
                }
            }
        ]
    }
    
    # Create valid JSON message
    data = {'id': 'msg123', 'data': 'important payload information'}
    message = Message(key=None, value=json.dumps(data).encode('utf-8'))
    
    engine = MessageFlowEngine(flow_config)
    results = engine.execute(message, num_messages=1)
    
    # All hops should pass validation
    assert results.success
    assert len(results.hops) == 2
    
    # First hop: JSON validation
    assert results.hops[0].validation.passed
    assert 'JSON schema validation passed' in results.hops[0].validation.message
    
    # Second hop: Contains validation
    assert results.hops[1].validation.passed
    assert 'data' in results.hops[1].validation.message
