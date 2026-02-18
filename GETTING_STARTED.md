# Getting Started with Pub-Sub Performance Tool

This guide will help you get started with the pub-sub performance and validation tool.

## Installation

```bash
# Clone the repository
git clone https://github.com/dwilson2547/pub-sub-perf-tool.git
cd pub-sub-perf-tool

# Install dependencies
pip install -r requirements.txt

# Install the tool
pip install -e .
```

## Quick Start

### 1. Run the Demo

See the tool's capabilities without any messaging system:

```bash
python demo.py
```

### 2. Generate a Configuration

Create a sample configuration file:

```bash
pub-sub-perf generate-config my-flow.yaml --type kafka --type pulsar
```

### 3. Validate Configuration

Check if your configuration is valid:

```bash
pub-sub-perf validate-config my-flow.yaml
```

### 4. Run a Flow (requires actual messaging systems)

```bash
pub-sub-perf run my-flow.yaml --message "Hello World" --count 1
```

## Configuration Examples

### Simple Kafka Flow

```yaml
name: kafka-simple
hops:
  - name: initial-publish
    destination:
      type: kafka
      topic: test-input
      config:
        bootstrap_servers:
          - localhost:9092
    validation:
      type: exists
```

### Pulsar with Reader

```yaml
name: pulsar-with-reader
hops:
  - name: publish
    destination:
      type: pulsar
      topic: persistent://public/default/input
      config:
        service_url: pulsar://localhost:6650

  - name: intermediary
    source:
      type: pulsar
      topic: persistent://public/default/input
      config:
        service_url: pulsar://localhost:6650
        use_reader: true  # Use reader for intermediary hops
    destination:
      type: pulsar
      topic: persistent://public/default/output
      config:
        service_url: pulsar://localhost:6650
```

### Multi-System Flow

```yaml
name: multi-system
hops:
  # Kafka -> Pulsar -> RabbitMQ
  - name: kafka-publish
    destination:
      type: kafka
      topic: input
      config:
        bootstrap_servers: [localhost:9092]

  - name: kafka-to-pulsar
    source:
      type: kafka
      topic: input
      config:
        bootstrap_servers: [localhost:9092]
    destination:
      type: pulsar
      topic: persistent://public/default/intermediate
      config:
        service_url: pulsar://localhost:6650
    validation:
      type: contains
      params:
        text: "test"

  - name: pulsar-to-rabbitmq
    source:
      type: pulsar
      topic: persistent://public/default/intermediate
      config:
        service_url: pulsar://localhost:6650
        use_reader: true
    destination:
      type: rabbitmq
      topic: output
      config:
        host: localhost
        port: 5672
```

## Validation Types

### 1. Exists
Simply check if message exists:
```yaml
validation:
  type: exists
```

### 2. Contains
Check if message contains text:
```yaml
validation:
  type: contains
  params:
    text: "expected content"
```

### 3. JSON Schema
Validate JSON structure:
```yaml
validation:
  type: json_schema
  params:
    required_fields:
      - id
      - timestamp
      - status
```

### 4. Size
Check message size:
```yaml
validation:
  type: size
  params:
    min_bytes: 10
    max_bytes: 1000000
```

## Key Features

### Kafka: Random Consumer Groups
The tool automatically generates random consumer group IDs to prevent conflicts:

```python
from pub_sub_perf_tool.clients import KafkaClient

# No consumer_group specified - auto-generated
client = KafkaClient({'bootstrap_servers': ['localhost:9092']})
print(client.consumer_group)  # perf-tool-<uuid>

# Custom consumer group
client = KafkaClient({
    'bootstrap_servers': ['localhost:9092'],
    'consumer_group': 'my-group'
})
```

### Pulsar: Reader Mode
Use Pulsar readers for intermediary hops:

```yaml
source:
  type: pulsar
  topic: my-topic
  config:
    service_url: pulsar://localhost:6650
    use_reader: true  # Enable reader mode
```

## Testing

Run the test suite:

```bash
pytest tests/ -v
```

## Output Formats

### Table Format (default)
```bash
pub-sub-perf run flow.yaml --message "test" --output table
```

### JSON Format
```bash
pub-sub-perf run flow.yaml --message "test" --output json
```

## Examples

Check the `examples/` directory for complete configuration examples:
- `kafka-flow.yaml` - Simple Kafka flow
- `pulsar-flow.yaml` - Pulsar with reader
- `multi-system-flow.yaml` - Multi-system flow
- `rabbitmq-flow.yaml` - RabbitMQ flow

## Troubleshooting

### Connection Issues
- Ensure messaging systems are running
- Check connection parameters (host, port, URLs)
- Verify network accessibility

### Validation Failures
- Check message format
- Verify validation parameters
- Review error messages in output

### Performance Issues
- Monitor system resources
- Check network latency
- Review messaging system configuration

## Next Steps

1. Review the [README.md](README.md) for detailed documentation
2. Check out example configurations in `examples/`
3. Run the demo: `python demo.py`
4. Create your own flow configuration
5. Run tests: `pytest tests/`

## Support

For issues and questions, please open an issue on GitHub.
