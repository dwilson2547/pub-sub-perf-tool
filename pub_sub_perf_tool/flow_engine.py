"""Message flow engine with validation support"""
from typing import Any, Dict, List, Optional, Callable
from dataclasses import dataclass, field
import time
import json
from enum import Enum

from .base import PubSubClient, Message
from .clients import (
    KafkaClient,
    PulsarClient,
    RabbitMQClient,
    IggyClient,
    EventHubsClient,
    GooglePubSubClient,
    StreamNativeClient
)


class ClientType(Enum):
    """Supported pub-sub client types"""
    KAFKA = "kafka"
    PULSAR = "pulsar"
    RABBITMQ = "rabbitmq"
    IGGY = "iggy"
    EVENTHUBS = "eventhubs"
    GOOGLEPUBSUB = "googlepubsub"
    STREAMNATIVE = "streamnative"


@dataclass
class ValidationResult:
    """Result of a validation check"""
    passed: bool
    message: str = ""
    details: Dict[str, Any] = field(default_factory=dict)


@dataclass
class HopResult:
    """Result of processing a hop"""
    hop_index: int
    hop_name: str
    success: bool
    validation: Optional[ValidationResult] = None
    publish_latency_ms: float = 0.0
    consume_latency_ms: float = 0.0
    total_latency_ms: float = 0.0
    error: Optional[str] = None


@dataclass
class FlowResult:
    """Result of executing a complete message flow"""
    flow_name: str
    success: bool
    hops: List[HopResult] = field(default_factory=list)
    total_time_ms: float = 0.0
    messages_processed: int = 0


class Validator:
    """Message validator for hop validation"""
    
    @staticmethod
    def validate_message(message: Message, validation_config: Dict[str, Any]) -> ValidationResult:
        """Validate a message based on configuration
        
        Args:
            message: Message to validate
            validation_config: Validation configuration dict with keys:
                - type: Validation type (exists, contains, json_schema, custom)
                - params: Parameters for the validation
                
        Returns:
            ValidationResult
        """
        if not message:
            return ValidationResult(passed=False, message="No message received")
        
        validation_type = validation_config.get('type', 'exists')
        params = validation_config.get('params', {})
        
        if validation_type == 'exists':
            return ValidationResult(passed=True, message="Message exists")
        
        elif validation_type == 'contains':
            # Check if message contains expected text
            expected = params.get('text', '').encode('utf-8')
            if expected in message.value:
                return ValidationResult(passed=True, message=f"Message contains '{expected.decode()}'")
            else:
                return ValidationResult(passed=False, message=f"Message does not contain '{expected.decode()}'")
        
        elif validation_type == 'json_schema':
            # Validate JSON structure
            try:
                data = json.loads(message.value.decode('utf-8'))
                required_fields = params.get('required_fields', [])
                
                missing_fields = [f for f in required_fields if f not in data]
                if missing_fields:
                    return ValidationResult(
                        passed=False,
                        message=f"Missing required fields: {missing_fields}",
                        details={'missing_fields': missing_fields}
                    )
                
                return ValidationResult(
                    passed=True,
                    message="JSON schema validation passed",
                    details={'data': data}
                )
            except json.JSONDecodeError as e:
                return ValidationResult(passed=False, message=f"Invalid JSON: {e}")
        
        elif validation_type == 'size':
            # Check message size
            min_size = params.get('min_bytes', 0)
            max_size = params.get('max_bytes', float('inf'))
            size = len(message.value)
            
            if min_size <= size <= max_size:
                return ValidationResult(
                    passed=True,
                    message=f"Message size {size} bytes is within range",
                    details={'size_bytes': size}
                )
            else:
                return ValidationResult(
                    passed=False,
                    message=f"Message size {size} bytes out of range [{min_size}, {max_size}]",
                    details={'size_bytes': size}
                )
        
        else:
            return ValidationResult(passed=True, message=f"Unknown validation type: {validation_type}")


def create_client(client_type: ClientType, config: Dict[str, Any]) -> PubSubClient:
    """Factory function to create pub-sub clients
    
    Args:
        client_type: Type of client to create
        config: Client configuration
        
    Returns:
        PubSubClient instance
    """
    client_map = {
        ClientType.KAFKA: KafkaClient,
        ClientType.PULSAR: PulsarClient,
        ClientType.RABBITMQ: RabbitMQClient,
        ClientType.IGGY: IggyClient,
        ClientType.EVENTHUBS: EventHubsClient,
        ClientType.GOOGLEPUBSUB: GooglePubSubClient,
        ClientType.STREAMNATIVE: StreamNativeClient,
    }
    
    client_class = client_map.get(client_type)
    if not client_class:
        raise ValueError(f"Unsupported client type: {client_type}")
    
    return client_class(config)


class MessageFlowEngine:
    """Engine to execute message flows with multiple hops and validation"""
    
    def __init__(self, flow_config: Dict[str, Any]):
        """Initialize message flow engine
        
        Args:
            flow_config: Flow configuration dict with keys:
                - name: Flow name
                - hops: List of hop configurations
        """
        self.flow_config = flow_config
        self.flow_name = flow_config.get('name', 'unnamed-flow')
        self.hops_config = flow_config.get('hops', [])
        
        self.validator = Validator()
        self._resolve_hop_references()
    
    def _resolve_hop_references(self):
        """Resolve source references to previous hop destinations
        
        Allows intermediary hops to reference previous hop destinations by name
        instead of duplicating the entire configuration.
        
        Example:
            source: "hop: initial-publish"  # References destination of hop named "initial-publish"
        """
        # Build a map of hop names to their destination configs
        hop_destinations = {}
        
        for hop_config in self.hops_config:
            hop_name = hop_config.get('name')
            if hop_name and 'destination' in hop_config:
                hop_destinations[hop_name] = hop_config['destination']
        
        # Resolve source references
        for hop_config in self.hops_config:
            source = hop_config.get('source')
            
            # Check if source is a string reference (e.g., "hop: hop-name")
            if isinstance(source, str):
                if source.startswith('hop:'):
                    # Extract the referenced hop name
                    referenced_hop = source[4:].strip()
                    
                    # Look up the referenced hop's destination
                    if referenced_hop in hop_destinations:
                        hop_config['source'] = hop_destinations[referenced_hop].copy()
                    else:
                        raise ValueError(f"Cannot resolve hop reference: '{referenced_hop}' not found")
                else:
                    raise ValueError(f"Invalid source reference format: '{source}'. Expected 'hop: hop-name'")
    
    def execute(self, initial_message: Message, num_messages: int = 1) -> FlowResult:
        """Execute the message flow
        
        Args:
            initial_message: Initial message to send
            num_messages: Number of messages to process
            
        Returns:
            FlowResult with execution details
        """
        start_time = time.time()
        flow_result = FlowResult(flow_name=self.flow_name, success=True)
        
        try:
            for msg_idx in range(num_messages):
                current_message = initial_message
                
                for hop_idx, hop_config in enumerate(self.hops_config):
                    hop_result = self._execute_hop(hop_idx, hop_config, current_message)
                    flow_result.hops.append(hop_result)
                    
                    if not hop_result.success:
                        flow_result.success = False
                        break
                    
                    # Get message for next hop if there is one
                    if hop_idx < len(self.hops_config) - 1:
                        # Message will be consumed in next hop
                        pass
                
                flow_result.messages_processed += 1
            
        except Exception as e:
            flow_result.success = False
            flow_result.hops.append(HopResult(
                hop_index=-1,
                hop_name="error",
                success=False,
                error=str(e)
            ))
        
        flow_result.total_time_ms = (time.time() - start_time) * 1000
        return flow_result
    
    def _execute_hop(self, hop_index: int, hop_config: Dict[str, Any], message: Message) -> HopResult:
        """Execute a single hop
        
        Args:
            hop_index: Index of the hop
            hop_config: Hop configuration
            message: Message to process
            
        Returns:
            HopResult
        """
        hop_start = time.time()
        hop_name = hop_config.get('name', f'hop-{hop_index}')
        
        try:
            # Parse hop configuration
            source_config = hop_config.get('source', {})
            destination_config = hop_config.get('destination', {})
            validation_config = hop_config.get('validation', {})
            
            # Create clients
            source_type = ClientType(source_config.get('type'))
            source_client = create_client(source_type, source_config.get('config', {}))
            
            dest_type = ClientType(destination_config.get('type'))
            dest_client = create_client(dest_type, destination_config.get('config', {}))
            
            # Execute hop
            with source_client, dest_client:
                # If this is not the first hop, consume from source
                consume_latency = 0.0
                if hop_index > 0:
                    source_topic = source_config.get('topic')
                    source_client.subscribe([source_topic])
                    
                    consume_result = source_client.consume(source_topic, timeout_ms=5000)
                    consume_latency = consume_result.latency_ms
                    
                    if consume_result.message:
                        message = consume_result.message
                    else:
                        return HopResult(
                            hop_index=hop_index,
                            hop_name=hop_name,
                            success=False,
                            consume_latency_ms=consume_latency,
                            error="No message received from source"
                        )
                
                # Validate message if validation is configured
                validation_result = None
                if validation_config:
                    validation_result = self.validator.validate_message(message, validation_config)
                    if not validation_result.passed:
                        return HopResult(
                            hop_index=hop_index,
                            hop_name=hop_name,
                            success=False,
                            validation=validation_result,
                            consume_latency_ms=consume_latency,
                            error=f"Validation failed: {validation_result.message}"
                        )
                
                # Publish to destination
                dest_topic = destination_config.get('topic')
                publish_result = dest_client.publish(dest_topic, message)
                
                if not publish_result.success:
                    return HopResult(
                        hop_index=hop_index,
                        hop_name=hop_name,
                        success=False,
                        validation=validation_result,
                        consume_latency_ms=consume_latency,
                        publish_latency_ms=publish_result.latency_ms,
                        error=f"Publish failed: {publish_result.error}"
                    )
                
                total_latency = (time.time() - hop_start) * 1000
                
                return HopResult(
                    hop_index=hop_index,
                    hop_name=hop_name,
                    success=True,
                    validation=validation_result,
                    consume_latency_ms=consume_latency,
                    publish_latency_ms=publish_result.latency_ms,
                    total_latency_ms=total_latency
                )
                
        except Exception as e:
            total_latency = (time.time() - hop_start) * 1000
            return HopResult(
                hop_index=hop_index,
                hop_name=hop_name,
                success=False,
                total_latency_ms=total_latency,
                error=str(e)
            )
