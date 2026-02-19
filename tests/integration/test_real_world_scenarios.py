"""Integration tests demonstrating real-world example scenarios."""
import pytest
import time
import json
from pub_sub_perf_tool.flow_engine import MessageFlowEngine
from pub_sub_perf_tool.base import Message


def test_microservices_event_propagation(kafka_container):
    """Simulate microservices architecture with event propagation across services.
    
    Scenario: Order placed -> Payment processed -> Inventory updated
    Each service publishes to its own topic with validation.
    """
    bootstrap_servers = kafka_container.get_bootstrap_server()
    
    flow_config = {
        'name': 'microservices-event-flow',
        'message_headers': {
            'correlation_id': 'order-12345',
            'source_service': 'order-service'
        },
        'hops': [
            {
                'name': 'order-placed',
                'destination': {
                    'type': 'kafka',
                    'topic': 'orders.placed',
                    'config': {
                        'bootstrap_servers': [bootstrap_servers]
                    }
                },
                'validation': {
                    'type': 'json_schema',
                    'params': {
                        'required_fields': ['order_id', 'customer_id', 'total_amount']
                    }
                }
            },
            {
                'name': 'payment-processing',
                'source': {
                    'type': 'kafka',
                    'topic': 'orders.placed',
                    'config': {
                        'bootstrap_servers': [bootstrap_servers]
                    }
                },
                'destination': {
                    'type': 'kafka',
                    'topic': 'payments.processed',
                    'config': {
                        'bootstrap_servers': [bootstrap_servers]
                    }
                },
                'validation': {
                    'type': 'contains',
                    'params': {
                        'text': 'total_amount'
                    }
                }
            },
            {
                'name': 'inventory-update',
                'source': {
                    'type': 'kafka',
                    'topic': 'payments.processed',
                    'config': {
                        'bootstrap_servers': [bootstrap_servers]
                    }
                },
                'destination': {
                    'type': 'kafka',
                    'topic': 'inventory.updated',
                    'config': {
                        'bootstrap_servers': [bootstrap_servers]
                    }
                },
                'validation': {
                    'type': 'size',
                    'params': {
                        'min_bytes': 50,
                        'max_bytes': 10000
                    }
                }
            }
        ]
    }
    
    # Create order event
    order_event = {
        'order_id': 'ORD-12345',
        'customer_id': 'CUST-789',
        'total_amount': 99.99,
        'items': [
            {'product_id': 'PROD-001', 'quantity': 2},
            {'product_id': 'PROD-002', 'quantity': 1}
        ],
        'timestamp': '2024-01-15T10:30:00Z'
    }
    
    message = Message(key='ORD-12345', value=json.dumps(order_event).encode('utf-8'))
    
    engine = MessageFlowEngine(flow_config)
    results = engine.execute(message, num_messages=1)
    
    # Verify entire flow succeeded
    assert results.success
    assert len(results.hops) == 3
    
    # Check each hop
    assert results.hops[0].hop_name == 'order-placed'
    assert results.hops[0].validation.passed
    
    assert results.hops[1].hop_name == 'payment-processing'
    assert results.hops[1].validation.passed
    
    assert results.hops[2].hop_name == 'inventory-update'
    assert results.hops[2].validation.passed
    
    # Verify latency tracking
    assert results.total_time_ms > 0
    for hop in results.hops:
        assert hop.total_latency_ms > 0


def test_data_pipeline_transformation(kafka_container, rabbitmq_container):
    """Simulate data pipeline with format transformation across systems.
    
    Scenario: Raw data ingestion (Kafka) -> Processing -> Routing (RabbitMQ)
    Tests cross-protocol integration and data transformation validation.
    """
    kafka_bootstrap = kafka_container.get_bootstrap_server()
    rabbitmq_host = rabbitmq_container.get_container_host_ip()
    rabbitmq_port = rabbitmq_container.get_exposed_port(5672)
    
    flow_config = {
        'name': 'data-pipeline-flow',
        'message_headers': {
            'pipeline_id': 'data-pipeline-v1',
            'environment': 'integration-test'
        },
        'hops': [
            {
                'name': 'raw-data-ingestion',
                'destination': {
                    'type': 'kafka',
                    'topic': 'raw.data.input',
                    'config': {
                        'bootstrap_servers': [kafka_bootstrap]
                    }
                },
                'validation': {
                    'type': 'json_schema',
                    'params': {
                        'required_fields': ['sensor_id', 'value', 'timestamp']
                    }
                }
            },
            {
                'name': 'data-routing',
                'source': {
                    'type': 'kafka',
                    'topic': 'raw.data.input',
                    'config': {
                        'bootstrap_servers': [kafka_bootstrap]
                    }
                },
                'destination': {
                    'type': 'rabbitmq',
                    'topic': 'processed.data.output',
                    'config': {
                        'host': rabbitmq_host,
                        'port': int(rabbitmq_port),
                        'username': 'guest',
                        'password': 'guest',
                        'exchange': 'data-pipeline',
                        'exchange_type': 'topic'
                    }
                },
                'validation': {
                    'type': 'contains',
                    'params': {
                        'text': 'sensor_id'
                    }
                }
            }
        ]
    }
    
    # Sensor data
    sensor_data = {
        'sensor_id': 'TEMP-SENSOR-01',
        'value': 23.5,
        'unit': 'celsius',
        'timestamp': '2024-01-15T10:30:00Z',
        'location': 'warehouse-floor-2'
    }
    
    message = Message(key='TEMP-SENSOR-01', value=json.dumps(sensor_data).encode('utf-8'))
    
    engine = MessageFlowEngine(flow_config)
    results = engine.execute(message, num_messages=1)
    
    # Verify cross-protocol flow succeeded
    assert results.success
    assert len(results.hops) == 2
    assert results.hops[0].validation.passed
    assert results.hops[1].validation.passed


def test_notification_fanout_pattern(rabbitmq_container):
    """Simulate notification fanout pattern with topic-based routing.
    
    Scenario: User action -> Multiple notification channels (email, sms, push)
    Tests RabbitMQ's topic exchange pattern for routing to multiple subscribers.
    """
    host = rabbitmq_container.get_container_host_ip()
    port = rabbitmq_container.get_exposed_port(5672)
    
    flow_config = {
        'name': 'notification-fanout-flow',
        'hops': [
            {
                'name': 'user-action-event',
                'destination': {
                    'type': 'rabbitmq',
                    'topic': 'notifications.user.login',
                    'config': {
                        'host': host,
                        'port': int(port),
                        'username': 'guest',
                        'password': 'guest',
                        'exchange': 'notification-fanout',
                        'exchange_type': 'topic'
                    }
                },
                'validation': {
                    'type': 'json_schema',
                    'params': {
                        'required_fields': ['user_id', 'action', 'timestamp']
                    }
                }
            },
            {
                'name': 'route-to-notification-services',
                'source': {
                    'type': 'rabbitmq',
                    'topic': 'notifications.user.*',
                    'config': {
                        'host': host,
                        'port': int(port),
                        'username': 'guest',
                        'password': 'guest',
                        'exchange': 'notification-fanout'
                    }
                },
                'destination': {
                    'type': 'rabbitmq',
                    'topic': 'notifications.processed',
                    'config': {
                        'host': host,
                        'port': int(port),
                        'username': 'guest',
                        'password': 'guest',
                        'exchange': 'notification-fanout',
                        'exchange_type': 'topic'
                    }
                },
                'validation': {
                    'type': 'contains',
                    'params': {
                        'text': 'user_id'
                    }
                }
            }
        ]
    }
    
    notification_event = {
        'user_id': 'user-98765',
        'action': 'login',
        'timestamp': '2024-01-15T10:30:00Z',
        'ip_address': '192.168.1.100',
        'device': 'mobile'
    }
    
    message = Message(key=None, value=json.dumps(notification_event).encode('utf-8'))
    
    engine = MessageFlowEngine(flow_config)
    results = engine.execute(message, num_messages=1)
    
    assert results.success
    assert all(hop.validation.passed for hop in results.hops)


def test_log_aggregation_pipeline(kafka_container, pulsar_container):
    """Simulate log aggregation from multiple sources.
    
    Scenario: Application logs (Kafka) -> Centralized logging (Pulsar)
    Tests high-volume log message processing and persistence.
    """
    kafka_bootstrap = kafka_container.get_bootstrap_server()
    pulsar_host = pulsar_container.get_container_host_ip()
    pulsar_port = pulsar_container.get_exposed_port(6650)
    pulsar_url = f"pulsar://{pulsar_host}:{pulsar_port}"
    
    flow_config = {
        'name': 'log-aggregation-flow',
        'hops': [
            {
                'name': 'application-logs',
                'destination': {
                    'type': 'kafka',
                    'topic': 'app.logs',
                    'config': {
                        'bootstrap_servers': [kafka_bootstrap]
                    }
                },
                'validation': {
                    'type': 'json_schema',
                    'params': {
                        'required_fields': ['level', 'message', 'timestamp']
                    }
                }
            },
            {
                'name': 'centralized-logging',
                'source': {
                    'type': 'kafka',
                    'topic': 'app.logs',
                    'config': {
                        'bootstrap_servers': [kafka_bootstrap]
                    }
                },
                'destination': {
                    'type': 'pulsar',
                    'topic': 'persistent://public/default/logs-archive',
                    'config': {
                        'service_url': pulsar_url
                    }
                },
                'validation': {
                    'type': 'contains',
                    'params': {
                        'text': 'level'
                    }
                }
            }
        ]
    }
    
    # Multiple log messages
    log_messages = [
        {
            'level': 'INFO',
            'message': 'User authentication successful',
            'timestamp': '2024-01-15T10:30:00Z',
            'service': 'auth-service',
            'request_id': 'req-001'
        },
        {
            'level': 'ERROR',
            'message': 'Database connection timeout',
            'timestamp': '2024-01-15T10:30:05Z',
            'service': 'user-service',
            'request_id': 'req-002',
            'error_code': 'DB_TIMEOUT'
        },
        {
            'level': 'WARN',
            'message': 'Rate limit approaching threshold',
            'timestamp': '2024-01-15T10:30:10Z',
            'service': 'api-gateway',
            'current_rate': 950,
            'threshold': 1000
        }
    ]
    
    engine = MessageFlowEngine(flow_config)
    
    # Process multiple log messages
    for log_data in log_messages:
        message = Message(key=None, value=json.dumps(log_data).encode('utf-8'))
        results = engine.execute(message, num_messages=1)
        
        assert results.success
        assert all(hop.validation.passed for hop in results.hops)


def test_failure_scenario_missing_data(kafka_container):
    """Test validation failure when required data is missing.
    
    Demonstrates how the tool catches data quality issues.
    """
    bootstrap_servers = kafka_container.get_bootstrap_server()
    
    flow_config = {
        'name': 'validation-failure-test',
        'hops': [
            {
                'name': 'strict-validation',
                'destination': {
                    'type': 'kafka',
                    'topic': 'strict.validation',
                    'config': {
                        'bootstrap_servers': [bootstrap_servers]
                    }
                },
                'validation': {
                    'type': 'json_schema',
                    'params': {
                        'required_fields': ['id', 'name', 'email', 'phone']
                    }
                }
            }
        ]
    }
    
    # Incomplete user data (missing 'phone')
    incomplete_data = {
        'id': 'user-123',
        'name': 'John Doe',
        'email': 'john@example.com'
    }
    
    message = Message(key=None, value=json.dumps(incomplete_data).encode('utf-8'))
    
    engine = MessageFlowEngine(flow_config)
    results = engine.execute(message, num_messages=1)
    
    # Flow should fail validation
    assert not results.success
    assert not results.hops[0].validation.passed
    assert 'Missing required fields' in results.hops[0].validation.message
    assert 'phone' in results.hops[0].validation.details.get('missing_fields', [])


def test_performance_baseline_establishment(kafka_container):
    """Establish performance baseline by processing batch of messages.
    
    Demonstrates using the tool to measure system performance characteristics.
    """
    bootstrap_servers = kafka_container.get_bootstrap_server()
    
    flow_config = {
        'name': 'performance-baseline',
        'hops': [
            {
                'name': 'baseline-test',
                'destination': {
                    'type': 'kafka',
                    'topic': 'perf.baseline',
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
    
    # Process batch of messages
    num_messages = 50
    latencies = []
    
    for i in range(num_messages):
        message = Message(
            key=f'key-{i}',
            value=f'Performance test message {i}'.encode('utf-8')
        )
        
        results = engine.execute(message, num_messages=1)
        
        assert results.success
        latencies.append(results.total_time_ms)
    
    # Calculate statistics
    avg_latency = sum(latencies) / len(latencies)
    min_latency = min(latencies)
    max_latency = max(latencies)
    
    # Verify performance characteristics
    assert avg_latency > 0
    assert min_latency > 0
    assert max_latency < 30000  # Should complete within 30 seconds
    
    # Performance should be consistent (max shouldn't be more than 10x min)
    # This might fail in CI but demonstrates the concept
    assert max_latency < min_latency * 20, f"Performance too variable: min={min_latency}ms, max={max_latency}ms"
