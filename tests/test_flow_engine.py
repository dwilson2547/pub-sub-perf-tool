"""Unit tests for flow engine"""
import pytest
from pub_sub_perf_tool.flow_engine import ClientType, create_client
from pub_sub_perf_tool.clients import KafkaClient, PulsarClient, RabbitMQClient, IggyClient


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
