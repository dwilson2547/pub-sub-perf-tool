# Pub-Sub Performance Tool

A generic pub-sub performance testing tool with a focus on validation across multiple messaging systems.

## Features

- **Multi-System Support**: Works with Kafka, Pulsar, RabbitMQ, Iggy, Azure EventHubs, Google Pub/Sub, and StreamNative
- **Abstraction Layer**: Seamless interface across different pub-sub systems
- **Message Flow Definition**: Define complex flows with multiple intermediary hops
- **Validation Framework**: Validate messages between each hop
- **Performance Monitoring**: Track latency and throughput metrics
- **Flexible Configuration**: YAML/JSON-based configuration

## Key Capabilities

### Kafka Support
- **Random Consumer Group IDs**: Automatically generates random consumer group IDs to prevent subscription conflicts
- Configurable producer and consumer settings
- Support for message keys and headers

### Pulsar Support
- **Reader for Intermediary Hops**: Use Pulsar readers at intermediary hops as specified in configuration
- Consumer for final consumption
- Support for topics, subscriptions, and message properties

### RabbitMQ Support
- Exchange and queue management
- Routing key support
- Message persistence

### Iggy Support
- Basic message streaming support
- Stream and topic configuration

### Azure EventHubs Support
- Azure Event Hubs integration with connection string authentication
- Consumer group support (default: $Default)
- Event batch publishing and consumption
- Message properties and partition key support

### Google Pub/Sub Support
- Google Cloud Pub/Sub integration
- Service account or default credentials authentication
- Automatic subscription creation and management
- Message attributes and acknowledgment handling

### StreamNative Support
- StreamNative Cloud (managed Pulsar) integration
- OAuth2, token, and TLS authentication support
- Reader mode for intermediary hops
- Full Pulsar feature compatibility

## Installation

```bash
pip install -r requirements.txt
pip install -e .
```

## Quick Start

### 1. Generate a Sample Configuration

```bash
pub-sub-perf generate-config flow.yaml --type kafka --type pulsar
```

### 2. Run a Message Flow

```bash
pub-sub-perf run flow.yaml --message "Hello World" --count 10
```

### 3. View Results

Results are displayed in a table format showing:
- Hop-by-hop execution status
- Latency metrics (consume, publish, total)
- Validation results
- Errors (if any)

## Configuration

### Basic Structure

```yaml
name: my-flow
message_headers:
  source: my-app
  version: "1.0"

hops:
  - name: initial-publish
    destination:
      type: kafka  # or pulsar, rabbitmq, iggy, eventhubs, googlepubsub, streamnative
      topic: topic-name
      config:
        # Client-specific configuration
    validation:
      type: exists  # or contains, json_schema, size
```

### Hop Configuration

Each hop consists of:
- **name**: Descriptive name for the hop
- **source** (optional for first hop): Source pub-sub system configuration
  - Can be a full configuration object (for backward compatibility)
  - Can be a reference to a previous hop's destination using `"hop: hop-name"` syntax (recommended to reduce duplication)
- **destination**: Destination pub-sub system configuration
- **validation**: Validation rules to apply

#### Source Reference Syntax

To reduce duplication, intermediary hops can reference the destination of a previous hop by name:

```yaml
hops:
  - name: initial-publish
    destination:
      type: kafka
      topic: input-topic
      config:
        bootstrap_servers:
          - localhost:9092
  
  - name: intermediary-hop
    source: "hop: initial-publish"  # References destination of "initial-publish"
    destination:
      type: kafka
      topic: output-topic
      config:
        bootstrap_servers:
          - localhost:9092
```

This is equivalent to the full configuration but eliminates duplication:

```yaml
  - name: intermediary-hop
    source:
      type: kafka
      topic: input-topic
      config:
        bootstrap_servers:
          - localhost:9092
    destination:
      # ... destination config
```

### Validation Types

1. **exists**: Simply check if message exists
2. **contains**: Check if message contains specific text
   ```yaml
   validation:
     type: contains
     params:
       text: "expected content"
   ```
3. **json_schema**: Validate JSON structure
   ```yaml
   validation:
     type: json_schema
     params:
       required_fields:
         - id
         - timestamp
   ```
4. **size**: Check message size
   ```yaml
   validation:
     type: size
     params:
       min_bytes: 10
       max_bytes: 1000000
   ```

## Examples

### Kafka Flow with Hop References

```yaml
name: kafka-flow
hops:
  - name: publish-to-kafka
    destination:
      type: kafka
      topic: test-input
      config:
        bootstrap_servers:
          - localhost:9092
        # consumer_group auto-generated to prevent conflicts

  - name: kafka-to-kafka
    source: "hop: publish-to-kafka"  # References previous hop's destination
    destination:
      type: kafka
      topic: test-output
      config:
        bootstrap_servers:
          - localhost:9092
```

### Pulsar Flow with Reader

```yaml
name: pulsar-flow
hops:
  - name: publish-to-pulsar
    destination:
      type: pulsar
      topic: persistent://public/default/input
      config:
        service_url: pulsar://localhost:6650

  - name: pulsar-intermediary
    source: "hop: publish-to-pulsar"  # References previous hop's destination
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
      topic: kafka-input
      config:
        bootstrap_servers: [localhost:9092]

  - name: kafka-to-pulsar
    source: "hop: kafka-publish"  # References kafka-publish destination
    destination:
      type: pulsar
      topic: persistent://public/default/intermediate
      config:
        service_url: pulsar://localhost:6650

  - name: pulsar-to-rabbitmq
    source: "hop: kafka-to-pulsar"  # References kafka-to-pulsar destination
    destination:
      type: rabbitmq
      topic: output-queue
      config:
        host: localhost
        port: 5672
```

### Azure EventHubs Flow

```yaml
name: eventhubs-flow
hops:
  - name: publish-to-eventhubs
    destination:
      type: eventhubs
      topic: test-eventhub
      config:
        connection_string: Endpoint=sb://my-namespace.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=mykey
        eventhub_name: test-eventhub
        consumer_group: $Default

  - name: eventhubs-to-kafka
    source: "hop: publish-to-eventhubs"  # References previous hop's destination
    destination:
      type: kafka
      topic: output-topic
      config:
        bootstrap_servers: [localhost:9092]
```

### Google Pub/Sub Flow

```yaml
name: googlepubsub-flow
hops:
  - name: publish-to-pubsub
    destination:
      type: googlepubsub
      topic: test-topic
      config:
        project_id: my-gcp-project
        credentials_path: /path/to/service-account.json

  - name: pubsub-to-pulsar
    source: "hop: publish-to-pubsub"  # References previous hop's destination
    destination:
      type: pulsar
      topic: persistent://public/default/output
      config:
        service_url: pulsar://localhost:6650
```

### StreamNative Flow (Managed Pulsar with OAuth2)

```yaml
name: streamnative-flow
hops:
  - name: publish-to-streamnative
    destination:
      type: streamnative
      topic: persistent://public/default/input
      config:
        service_url: pulsar+ssl://my-org.streamnative.cloud:6651
        auth_params:
          type: oauth2
          issuer_url: https://auth.streamnative.cloud
          client_id: my-client-id
          client_secret: my-client-secret
          audience: urn:sn:pulsar:my-org:my-instance

  - name: streamnative-intermediary
    source: "hop: publish-to-streamnative"  # References previous hop's destination
    destination:
      type: streamnative
      topic: persistent://public/default/output
      config:
        service_url: pulsar+ssl://my-org.streamnative.cloud:6651
        auth_params:
          type: oauth2
          issuer_url: https://auth.streamnative.cloud
          client_id: my-client-id
          client_secret: my-client-secret
          audience: urn:sn:pulsar:my-org:my-instance
```

## CLI Commands

### run
Execute a message flow from a configuration file.

```bash
pub-sub-perf run CONFIG_FILE [OPTIONS]

Options:
  -m, --message TEXT        Message content to send
  -f, --message-file PATH   File containing message content
  -k, --key TEXT           Message key
  -c, --count INTEGER      Number of messages to send (default: 1)
  -o, --output [table|json] Output format (default: table)
```

### generate-config
Generate a sample configuration file.

```bash
pub-sub-perf generate-config OUTPUT_FILE [OPTIONS]

Options:
  -t, --type [kafka|pulsar|rabbitmq|iggy|eventhubs|googlepubsub|streamnative]  Client types to include (multiple allowed)
```

### validate-config
Validate a configuration file.

```bash
pub-sub-perf validate-config CONFIG_FILE
```

## Architecture

### Abstraction Layer

The tool provides a unified `PubSubClient` interface that all messaging systems implement:
- `connect()`: Establish connection
- `disconnect()`: Close connection
- `publish()`: Publish a message
- `subscribe()`: Subscribe to topics
- `consume()`: Consume a message

### Message Flow Engine

The `MessageFlowEngine` executes configured flows:
1. Reads hop configurations
2. Creates appropriate clients
3. Executes each hop sequentially
4. Validates messages between hops
5. Collects performance metrics

### Validation Framework

The `Validator` class provides extensible validation:
- Built-in validators for common cases
- Easy to extend with custom validators
- Detailed validation results with error messages

## Performance Metrics

The tool tracks:
- **Consume Latency**: Time to receive a message
- **Publish Latency**: Time to send a message
- **Total Hop Latency**: End-to-end time for a hop
- **Flow Total Time**: Complete flow execution time

## Development

### Project Structure

```
pub_sub_perf_tool/
├── __init__.py
├── base.py              # Base abstractions
├── flow_engine.py       # Flow execution engine
├── cli.py              # Command-line interface
└── clients/
    ├── __init__.py
    ├── kafka_client.py
    ├── pulsar_client.py
    ├── rabbitmq_client.py
    ├── iggy_client.py
    ├── eventhubs_client.py
    ├── googlepubsub_client.py
    └── streamnative_client.py
```

### Running Tests

```bash
pytest
```

## License

MIT
