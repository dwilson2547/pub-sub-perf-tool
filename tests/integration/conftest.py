"""Shared fixtures for integration tests."""
import pytest
from testcontainers.kafka import KafkaContainer
from testcontainers.rabbitmq import RabbitMqContainer
from testcontainers.core.generic import GenericContainer
from testcontainers.core.waiting_strategies import Wait


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
        GenericContainer("apachepulsar/pulsar:3.2.0")
        .with_command("bin/pulsar standalone")
        .with_exposed_ports(6650, 8080)
        .with_env("PULSAR_STANDALONE_USE_ZOOKEEPER", "false")
        .waiting_for(Wait.for_log_message(".*messaging service is ready.*", timeout=60))
    )
    container.start()
    yield container
    container.stop()
