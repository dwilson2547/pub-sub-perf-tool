# Integration Tests with Testcontainers

This directory contains integration tests that use [Testcontainers](https://testcontainers.com/) to test the pub-sub-perf-tool with real message broker instances running in Docker containers.

## Overview

The integration tests provide:
- **Real message broker testing**: Tests run against actual Kafka, RabbitMQ, and Pulsar instances
- **End-to-end validation**: Complete message flow tests across different systems
- **Example flows**: Demonstrations of how to use the tool in real scenarios

## Supported Testcontainers

The following message brokers are supported with testcontainers:

- ✅ **Kafka**: Apache Kafka message broker
- ✅ **RabbitMQ**: AMQP-based message broker with management UI
- ✅ **Pulsar**: Apache Pulsar distributed messaging system

**Note**: Cloud-only services (Azure EventHubs, Google Pub/Sub, StreamNative) cannot be tested with testcontainers and are not included.

## Prerequisites

1. **Docker**: Docker must be installed and running on your machine
   ```bash
   docker --version
   ```

2. **Python Dependencies**: Install dev dependencies including testcontainers
   ```bash
   pip install -e ".[dev]"
   ```

## Running Integration Tests

### Run All Integration Tests

```bash
pytest tests/integration/ -v
```

### Run Specific Test Files

```bash
# Kafka integration tests
pytest tests/integration/test_kafka_integration.py -v

# RabbitMQ integration tests
pytest tests/integration/test_rabbitmq_integration.py -v

# Pulsar integration tests
pytest tests/integration/test_pulsar_integration.py -v

# Flow integration tests (multi-system)
pytest tests/integration/test_flow_integration.py -v
```

### Run Specific Tests

```bash
# Run a single test
pytest tests/integration/test_kafka_integration.py::test_kafka_publish_and_consume -v

# Run tests matching a pattern
pytest tests/integration/ -k "kafka" -v
```

## Test Structure

### Individual Client Tests

- `test_kafka_integration.py`: Tests Kafka client with real Kafka container
  - Basic publish/consume
  - Multiple messages
  - Message keys
  
- `test_rabbitmq_integration.py`: Tests RabbitMQ client with real RabbitMQ container
  - Basic publish/consume
  - Multiple messages
  - Topic routing
  
- `test_pulsar_integration.py`: Tests Pulsar client with real Pulsar container
  - Basic publish/consume
  - Reader mode
  - Multiple messages
  - Persistent topics

### Flow Tests

- `test_flow_integration.py`: End-to-end flow tests
  - Single-system flows (Kafka -> Kafka)
  - Multi-system flows (Kafka -> Pulsar)
  - RabbitMQ flows with routing
  - Pulsar flows with reader mode
  - YAML configuration loading

## Example Usage

### Using Testcontainers in Your Own Tests

```python
from testcontainers.kafka import KafkaContainer
from pub_sub_perf_tool.clients.kafka_client import KafkaClient
from pub_sub_perf_tool.base import Message


def test_my_kafka_scenario():
    """Example of using Kafka testcontainer."""
    # Start container
    container = KafkaContainer()
    container.start()
    
    try:
        # Get connection details
        bootstrap_servers = container.get_bootstrap_server()
        
        # Create client
        config = {'bootstrap_servers': [bootstrap_servers]}
        client = KafkaClient(config)
        
        # Your test code here
        client.connect()
        # ... do your testing
        
    finally:
        # Cleanup
        container.stop()
```

### Using Fixtures

The integration tests use pytest fixtures for container management. These fixtures automatically start and stop containers:

```python
def test_with_fixture(kafka_container):
    """Test using the kafka_container fixture."""
    bootstrap_servers = kafka_container.get_bootstrap_server()
    # Container is already started and will be stopped after test
```

## Building Example Flows

The integration tests demonstrate real-world usage patterns. Here's an example of a multi-hop flow:

```python
flow_config = {
    'name': 'example-flow',
    'hops': [
        {
            'name': 'initial-publish',
            'destination': {
                'type': 'kafka',
                'topic': 'input-topic',
                'config': {
                    'bootstrap_servers': [kafka_server]
                }
            },
            'validation': {'type': 'exists'}
        },
        {
            'name': 'kafka-to-pulsar',
            'source': {
                'type': 'kafka',
                'topic': 'input-topic',
                'config': {
                    'bootstrap_servers': [kafka_server]
                }
            },
            'destination': {
                'type': 'pulsar',
                'topic': 'persistent://public/default/output',
                'config': {
                    'service_url': pulsar_url
                }
            },
            'validation': {'type': 'exists'}
        }
    ]
}
```

## Troubleshooting

### Docker Not Running

If you see errors about Docker not being available:
```bash
# Check Docker status
docker ps

# Start Docker (varies by OS)
sudo systemctl start docker  # Linux
open -a Docker              # macOS
```

### Container Startup Timeouts

If containers fail to start within the timeout:
- Check your Docker resources (CPU, memory)
- Increase timeout in conftest.py if needed
- Check Docker logs: `docker logs <container_id>`

### Port Conflicts

If you see port binding errors:
- Stop conflicting services
- Testcontainers automatically assigns random ports to avoid conflicts
- Use `container.get_exposed_port()` to get the actual port

### Slow Tests

Integration tests are slower than unit tests because they:
- Start Docker containers
- Wait for services to be ready
- Perform actual network operations

Use markers or test selection to run only needed tests during development:
```bash
# Run only unit tests (fast)
pytest tests/ -v --ignore=tests/integration

# Run only integration tests
pytest tests/integration/ -v
```

## Performance Considerations

- **Container Reuse**: Fixtures use `scope="module"` to reuse containers across multiple tests in a file
- **Parallel Execution**: Not recommended for integration tests due to Docker resource constraints
- **CI/CD**: Integration tests work great in CI pipelines with Docker support (GitHub Actions, GitLab CI, etc.)

## Contributing

When adding new integration tests:

1. Add fixtures to `conftest.py` if needed
2. Follow existing test patterns
3. Include docstrings explaining what the test validates
4. Clean up resources properly
5. Consider test execution time

## Resources

- [Testcontainers Python Documentation](https://testcontainers-python.readthedocs.io/)
- [Kafka Testcontainer](https://testcontainers-python.readthedocs.io/en/latest/kafka/README.html)
- [RabbitMQ Testcontainer](https://testcontainers-python.readthedocs.io/en/latest/rabbitmq/README.html)
- [Generic Container](https://testcontainers-python.readthedocs.io/en/latest/core/README.html) (used for Pulsar)
