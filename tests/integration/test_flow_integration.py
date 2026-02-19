"""Integration tests for message flows using testcontainers."""
import pytest
import time
import yaml
import tempfile
from pathlib import Path
from pub_sub_perf_tool.flow_engine import MessageFlowEngine
from pub_sub_perf_tool.base import Message


def test_kafka_to_kafka_flow(kafka_container):
    """Test a complete Kafka to Kafka flow with validation."""
    bootstrap_servers = kafka_container.get_bootstrap_server()
    
    # Create flow configuration
    flow_config = {
        'name': 'kafka-integration-flow',
        'message_headers': {
            'source': 'integration-test',
            'version': '1.0'
        },
        'hops': [
            {
                'name': 'initial-publish',
                'destination': {
                    'type': 'kafka',
                    'topic': 'flow-test-input',
                    'config': {
                        'bootstrap_servers': [bootstrap_servers]
                    }
                },
                'validation': {
                    'type': 'exists'
                }
            },
            {
                'name': 'kafka-to-kafka',
                'source': {
                    'type': 'kafka',
                    'topic': 'flow-test-input',
                    'config': {
                        'bootstrap_servers': [bootstrap_servers]
                    }
                },
                'destination': {
                    'type': 'kafka',
                    'topic': 'flow-test-output',
                    'config': {
                        'bootstrap_servers': [bootstrap_servers]
                    }
                },
                'validation': {
                    'type': 'contains',
                    'params': {
                        'text': 'integration'
                    }
                }
            }
        ]
    }
    
    # Create engine and run flow
    engine = MessageFlowEngine(flow_config)
    message = Message(content='Test message for integration flow')
    
    results = engine.run_flow(message, count=1)
    
    # Verify results
    assert len(results) == 1
    result = results[0]
    
    # Check that all hops executed successfully
    for hop_result in result.hop_results:
        assert hop_result.status == 'success'
        assert hop_result.validation_result.is_valid


def test_rabbitmq_flow(rabbitmq_container):
    """Test a RabbitMQ flow with validation."""
    host = rabbitmq_container.get_container_host_ip()
    port = rabbitmq_container.get_exposed_port(5672)
    
    flow_config = {
        'name': 'rabbitmq-integration-flow',
        'hops': [
            {
                'name': 'publish-to-rabbitmq',
                'destination': {
                    'type': 'rabbitmq',
                    'topic': 'flow.test.input',
                    'config': {
                        'host': host,
                        'port': int(port),
                        'username': 'guest',
                        'password': 'guest',
                        'exchange': 'flow-test-exchange',
                        'exchange_type': 'topic'
                    }
                },
                'validation': {
                    'type': 'exists'
                }
            },
            {
                'name': 'rabbitmq-routing',
                'source': {
                    'type': 'rabbitmq',
                    'topic': 'flow.test.input',
                    'config': {
                        'host': host,
                        'port': int(port),
                        'username': 'guest',
                        'password': 'guest',
                        'exchange': 'flow-test-exchange'
                    }
                },
                'destination': {
                    'type': 'rabbitmq',
                    'topic': 'flow.test.output',
                    'config': {
                        'host': host,
                        'port': int(port),
                        'username': 'guest',
                        'password': 'guest',
                        'exchange': 'flow-test-exchange',
                        'exchange_type': 'topic'
                    }
                },
                'validation': {
                    'type': 'size',
                    'params': {
                        'min_bytes': 1,
                        'max_bytes': 1000000
                    }
                }
            }
        ]
    }
    
    engine = MessageFlowEngine(flow_config)
    message = Message(content='RabbitMQ flow test message')
    
    results = engine.run_flow(message, count=1)
    
    # Verify results
    assert len(results) == 1
    for hop_result in results[0].hop_results:
        assert hop_result.status == 'success'
        assert hop_result.validation_result.is_valid


def test_pulsar_flow_with_reader(pulsar_container):
    """Test a Pulsar flow using reader mode for intermediary hop."""
    host = pulsar_container.get_container_host_ip()
    port = pulsar_container.get_exposed_port(6650)
    service_url = f"pulsar://{host}:{port}"
    
    flow_config = {
        'name': 'pulsar-integration-flow',
        'hops': [
            {
                'name': 'publish-to-pulsar',
                'destination': {
                    'type': 'pulsar',
                    'topic': 'persistent://public/default/flow-input',
                    'config': {
                        'service_url': service_url
                    }
                },
                'validation': {
                    'type': 'exists'
                }
            },
            {
                'name': 'pulsar-reader-hop',
                'source': {
                    'type': 'pulsar',
                    'topic': 'persistent://public/default/flow-input',
                    'config': {
                        'service_url': service_url,
                        'use_reader': True
                    }
                },
                'destination': {
                    'type': 'pulsar',
                    'topic': 'persistent://public/default/flow-output',
                    'config': {
                        'service_url': service_url
                    }
                },
                'validation': {
                    'type': 'exists'
                }
            }
        ]
    }
    
    engine = MessageFlowEngine(flow_config)
    message = Message(content='Pulsar flow with reader test')
    
    results = engine.run_flow(message, count=1)
    
    # Verify results
    assert len(results) == 1
    for hop_result in results[0].hop_results:
        assert hop_result.status == 'success'
        assert hop_result.validation_result.is_valid


def test_multi_system_flow(kafka_container, pulsar_container):
    """Test a flow across multiple systems: Kafka -> Pulsar."""
    kafka_bootstrap = kafka_container.get_bootstrap_server()
    pulsar_host = pulsar_container.get_container_host_ip()
    pulsar_port = pulsar_container.get_exposed_port(6650)
    pulsar_url = f"pulsar://{pulsar_host}:{pulsar_port}"
    
    flow_config = {
        'name': 'multi-system-flow',
        'hops': [
            {
                'name': 'kafka-publish',
                'destination': {
                    'type': 'kafka',
                    'topic': 'multi-system-input',
                    'config': {
                        'bootstrap_servers': [kafka_bootstrap]
                    }
                },
                'validation': {
                    'type': 'exists'
                }
            },
            {
                'name': 'kafka-to-pulsar',
                'source': {
                    'type': 'kafka',
                    'topic': 'multi-system-input',
                    'config': {
                        'bootstrap_servers': [kafka_bootstrap]
                    }
                },
                'destination': {
                    'type': 'pulsar',
                    'topic': 'persistent://public/default/multi-output',
                    'config': {
                        'service_url': pulsar_url
                    }
                },
                'validation': {
                    'type': 'contains',
                    'params': {
                        'text': 'multi-system'
                    }
                }
            }
        ]
    }
    
    engine = MessageFlowEngine(flow_config)
    message = Message(content='Test multi-system flow message')
    
    results = engine.run_flow(message, count=1)
    
    # Verify results
    assert len(results) == 1
    for hop_result in results[0].hop_results:
        assert hop_result.status == 'success'
        assert hop_result.validation_result.is_valid


def test_flow_with_yaml_config(kafka_container, tmp_path):
    """Test loading and running a flow from a YAML configuration file."""
    bootstrap_servers = kafka_container.get_bootstrap_server()
    
    # Create YAML config
    flow_config = {
        'name': 'yaml-config-flow',
        'hops': [
            {
                'name': 'test-hop',
                'destination': {
                    'type': 'kafka',
                    'topic': 'yaml-test-topic',
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
    
    # Write to temporary YAML file
    config_file = tmp_path / "flow_config.yaml"
    with open(config_file, 'w') as f:
        yaml.dump(flow_config, f)
    
    # Load and run
    with open(config_file, 'r') as f:
        loaded_config = yaml.safe_load(f)
    
    engine = MessageFlowEngine(loaded_config)
    message = Message(content='YAML config test')
    
    results = engine.run_flow(message, count=1)
    
    # Verify
    assert len(results) == 1
    assert results[0].hop_results[0].status == 'success'
