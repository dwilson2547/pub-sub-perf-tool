"""Shared fixtures for integration tests."""
import pytest
from testcontainers.kafka import KafkaContainer
from testcontainers.rabbitmq import RabbitMqContainer
from testcontainers.core.container import DockerContainer


@pytest.fixture(scope="module")
def kafka_container():
    """Start a Kafka container for testing."""
    container = KafkaContainer()
    container.start()
    yield container
    container.stop()


@pytest.fixture(scope="module")
def rabbitmq_container():
    """Start a RabbitMQ container for testing."""
    container = RabbitMqContainer("rabbitmq:3-management")
    container.start()
    yield container
    container.stop()


@pytest.fixture(scope="module")
def pulsar_container():
    """Start a Pulsar container for testing."""
    container = (
        DockerContainer("apachepulsar/pulsar:3.2.0")
        .with_command("bin/pulsar standalone")
        .with_exposed_ports(6650, 8080)
        .with_env("PULSAR_STANDALONE_USE_ZOOKEEPER", "false")
    )
    container.start()
    
    # Wait a bit for Pulsar to be ready
    import time
    time.sleep(15)
    
    yield container
    container.stop()
