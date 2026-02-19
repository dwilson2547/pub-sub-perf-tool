"""Unit tests for flow engine"""
import pytest
from pub_sub_perf_tool.flow_engine import ClientType, create_client, MessageFlowEngine
from pub_sub_perf_tool.clients import (
    KafkaClient,
    PulsarClient,
    RabbitMQClient,
    IggyClient,
    EventHubsClient,
    GooglePubSubClient,
    StreamNativeClient
)


def test_create_kafka_client():
    """Test creating a Kafka client"""
    client = create_client(ClientType.KAFKA, {'bootstrap_servers': ['localhost:9092']})
    assert isinstance(client, KafkaClient)


def test_create_pulsar_client():
    """Test creating a Pulsar client"""
    client = create_client(ClientType.PULSAR, {'service_url': 'pulsar://localhost:6650'})
    assert isinstance(client, PulsarClient)


def test_create_rabbitmq_client():
    """Test creating a RabbitMQ client"""
    client = create_client(ClientType.RABBITMQ, {'host': 'localhost'})
    assert isinstance(client, RabbitMQClient)


def test_create_iggy_client():
    """Test creating an Iggy client"""
    client = create_client(ClientType.IGGY, {'host': 'localhost'})
    assert isinstance(client, IggyClient)


def test_create_eventhubs_client():
    """Test creating an EventHubs client"""
    client = create_client(
        ClientType.EVENTHUBS,
        {
            'connection_string': 'Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=test',
            'eventhub_name': 'test-hub'
        }
    )
    assert isinstance(client, EventHubsClient)


def test_create_googlepubsub_client():
    """Test creating a Google Pub/Sub client"""
    client = create_client(
        ClientType.GOOGLEPUBSUB,
        {'project_id': 'test-project'}
    )
    assert isinstance(client, GooglePubSubClient)


def test_create_streamnative_client():
    """Test creating a StreamNative client"""
    client = create_client(
        ClientType.STREAMNATIVE,
        {'service_url': 'pulsar+ssl://streamnative.cloud:6651'}
    )
    assert isinstance(client, StreamNativeClient)


def test_hop_reference_resolution():
    """Test that hop references are properly resolved"""
    flow_config = {
        'name': 'test-flow',
        'hops': [
            {
                'name': 'first-hop',
                'destination': {
                    'type': 'kafka',
                    'topic': 'test-topic',
                    'config': {'bootstrap_servers': ['localhost:9092']}
                }
            },
            {
                'name': 'second-hop',
                'source': 'hop: first-hop',
                'destination': {
                    'type': 'kafka',
                    'topic': 'output-topic',
                    'config': {'bootstrap_servers': ['localhost:9092']}
                }
            }
        ]
    }
    
    engine = MessageFlowEngine(flow_config)
    
    # Verify that the source reference was resolved
    assert 'source' in engine.hops_config[1]
    assert isinstance(engine.hops_config[1]['source'], dict)
    assert engine.hops_config[1]['source']['type'] == 'kafka'
    assert engine.hops_config[1]['source']['topic'] == 'test-topic'
    assert engine.hops_config[1]['source']['config']['bootstrap_servers'] == ['localhost:9092']


def test_hop_reference_with_multiple_hops():
    """Test that hop references work with multiple intermediary hops"""
    flow_config = {
        'name': 'multi-hop-flow',
        'hops': [
            {
                'name': 'kafka-publish',
                'destination': {
                    'type': 'kafka',
                    'topic': 'kafka-input',
                    'config': {'bootstrap_servers': ['localhost:9092']}
                }
            },
            {
                'name': 'kafka-to-pulsar',
                'source': 'hop: kafka-publish',
                'destination': {
                    'type': 'pulsar',
                    'topic': 'pulsar-intermediate',
                    'config': {'service_url': 'pulsar://localhost:6650'}
                }
            },
            {
                'name': 'pulsar-to-rabbitmq',
                'source': 'hop: kafka-to-pulsar',
                'destination': {
                    'type': 'rabbitmq',
                    'topic': 'rabbitmq-output',
                    'config': {'host': 'localhost', 'port': 5672}
                }
            }
        ]
    }
    
    engine = MessageFlowEngine(flow_config)
    
    # Verify second hop
    assert engine.hops_config[1]['source']['type'] == 'kafka'
    assert engine.hops_config[1]['source']['topic'] == 'kafka-input'
    
    # Verify third hop
    assert engine.hops_config[2]['source']['type'] == 'pulsar'
    assert engine.hops_config[2]['source']['topic'] == 'pulsar-intermediate'


def test_hop_reference_invalid_name():
    """Test that invalid hop reference raises error"""
    flow_config = {
        'name': 'test-flow',
        'hops': [
            {
                'name': 'first-hop',
                'destination': {
                    'type': 'kafka',
                    'topic': 'test-topic',
                    'config': {'bootstrap_servers': ['localhost:9092']}
                }
            },
            {
                'name': 'second-hop',
                'source': 'hop: non-existent-hop',
                'destination': {
                    'type': 'kafka',
                    'topic': 'output-topic',
                    'config': {'bootstrap_servers': ['localhost:9092']}
                }
            }
        ]
    }
    
    with pytest.raises(ValueError, match="Cannot resolve hop reference: 'non-existent-hop' not found"):
        MessageFlowEngine(flow_config)


def test_hop_reference_invalid_format():
    """Test that invalid reference format raises error"""
    flow_config = {
        'name': 'test-flow',
        'hops': [
            {
                'name': 'first-hop',
                'destination': {
                    'type': 'kafka',
                    'topic': 'test-topic',
                    'config': {'bootstrap_servers': ['localhost:9092']}
                }
            },
            {
                'name': 'second-hop',
                'source': 'invalid-reference-format',
                'destination': {
                    'type': 'kafka',
                    'topic': 'output-topic',
                    'config': {'bootstrap_servers': ['localhost:9092']}
                }
            }
        ]
    }
    
    with pytest.raises(ValueError, match="Invalid source reference format"):
        MessageFlowEngine(flow_config)


def test_backward_compatibility_with_full_source_config():
    """Test that full source config (old format) still works"""
    flow_config = {
        'name': 'test-flow',
        'hops': [
            {
                'name': 'first-hop',
                'destination': {
                    'type': 'kafka',
                    'topic': 'test-topic',
                    'config': {'bootstrap_servers': ['localhost:9092']}
                }
            },
            {
                'name': 'second-hop',
                'source': {
                    'type': 'kafka',
                    'topic': 'test-topic',
                    'config': {'bootstrap_servers': ['localhost:9092']}
                },
                'destination': {
                    'type': 'kafka',
                    'topic': 'output-topic',
                    'config': {'bootstrap_servers': ['localhost:9092']}
                }
            }
        ]
    }
    
    engine = MessageFlowEngine(flow_config)
    
    # Verify that the full config is preserved
    assert engine.hops_config[1]['source']['type'] == 'kafka'
    assert engine.hops_config[1]['source']['topic'] == 'test-topic'


def test_hop_reference_deep_copy():
    """Test that hop references use deep copy to avoid side effects"""
    flow_config = {
        'name': 'test-flow',
        'hops': [
            {
                'name': 'first-hop',
                'destination': {
                    'type': 'rabbitmq',
                    'topic': 'test-topic',
                    'config': {
                        'host': 'localhost',
                        'port': 5672,
                        'nested': {
                            'key': 'value',
                            'list': [1, 2, 3]
                        }
                    }
                }
            },
            {
                'name': 'second-hop',
                'source': 'hop: first-hop',
                'destination': {
                    'type': 'kafka',
                    'topic': 'output-topic',
                    'config': {'bootstrap_servers': ['localhost:9092']}
                }
            }
        ]
    }
    
    engine = MessageFlowEngine(flow_config)
    
    # Modify the resolved source to verify it doesn't affect the original destination
    engine.hops_config[1]['source']['config']['nested']['key'] = 'modified'
    engine.hops_config[1]['source']['config']['nested']['list'].append(4)
    
    # Verify original destination is unchanged
    assert engine.hops_config[0]['destination']['config']['nested']['key'] == 'value'
    assert engine.hops_config[0]['destination']['config']['nested']['list'] == [1, 2, 3]


def test_hop_reference_forward_allowed():
    """Test that forward references (to hops defined later) are allowed
    
    While messages flow forward through hops, the reference resolution happens
    at initialization time, so references can point to any named hop regardless
    of order in the configuration.
    """
    flow_config = {
        'name': 'test-flow',
        'hops': [
            {
                'name': 'first-hop',
                'source': 'hop: second-hop',  # Forward reference to a hop defined later
                'destination': {
                    'type': 'kafka',
                    'topic': 'test-topic',
                    'config': {'bootstrap_servers': ['localhost:9092']}
                }
            },
            {
                'name': 'second-hop',
                'destination': {
                    'type': 'kafka',
                    'topic': 'output-topic',
                    'config': {'bootstrap_servers': ['localhost:9092']}
                }
            }
        ]
    }
    
    # Forward references are allowed and should resolve correctly
    engine = MessageFlowEngine(flow_config)
    
    # Verify the reference was resolved
    assert engine.hops_config[0]['source']['type'] == 'kafka'
    assert engine.hops_config[0]['source']['topic'] == 'output-topic'



