"""Azure EventHubs client implementation"""
import time
import uuid
from typing import Any, Dict, Optional
from azure.eventhub import EventHubProducerClient, EventHubConsumerClient, EventData
from azure.eventhub.exceptions import EventHubError

from ..base import PubSubClient, Message, PublishResult, ConsumeResult


class EventHubsClient(PubSubClient):
    """Azure EventHubs pub-sub client with consumer group support"""
    
    def __init__(self, config: Dict[str, Any]):
        """Initialize EventHubs client
        
        Args:
            config: Configuration dict with keys:
                - connection_string: EventHubs connection string
                - eventhub_name: Name of the EventHub
                - consumer_group: Consumer group (default: $Default)
                - producer_config: Optional producer configuration
                - consumer_config: Optional consumer configuration
        """
        super().__init__(config)
        self.connection_string = config.get('connection_string', '')
        self.eventhub_name = config.get('eventhub_name', '')
        
        # Generate random consumer group ID if not provided, but keep default for production
        self.consumer_group = config.get('consumer_group', '$Default')
        
        self.producer_config = config.get('producer_config', {})
        self.consumer_config = config.get('consumer_config', {})
        
        self.producer: Optional[EventHubProducerClient] = None
        self.consumer: Optional[EventHubConsumerClient] = None
        self._last_event = None
    
    def connect(self) -> None:
        """Establish connection to EventHubs"""
        if self._connected:
            return
        
        try:
            # Create producer
            self.producer = EventHubProducerClient.from_connection_string(
                conn_str=self.connection_string,
                eventhub_name=self.eventhub_name,
                **self.producer_config
            )
            
            self._connected = True
            
        except Exception as e:
            raise ConnectionError(f"Failed to connect to EventHubs: {e}")
    
    def disconnect(self) -> None:
        """Close connection to EventHubs"""
        if not self._connected:
            return
        
        if self.producer:
            self.producer.close()
            self.producer = None
        
        if self.consumer:
            self.consumer.close()
            self.consumer = None
        
        self._connected = False
    
    def publish(self, topic: str, message: Message) -> PublishResult:
        """Publish a message to EventHubs
        
        Args:
            topic: EventHub name (typically ignored, uses configured eventhub_name)
            message: Message to publish
            
        Returns:
            PublishResult with success status
        """
        if not self._connected or not self.producer:
            return PublishResult(success=False, error="Not connected")
        
        start_time = time.time()
        
        try:
            # Create event data
            event_data = EventData(message.value)
            
            # Add properties (headers)
            if message.headers:
                for key, value in message.headers.items():
                    event_data.properties[key] = value
            
            # Add partition key if message has key
            if message.key:
                event_data.properties['_key'] = message.key
            
            # Send event
            event_batch = self.producer.create_batch()
            event_batch.add(event_data)
            self.producer.send_batch(event_batch)
            
            latency_ms = (time.time() - start_time) * 1000
            
            return PublishResult(
                success=True,
                message_id=str(uuid.uuid4()),
                latency_ms=latency_ms
            )
            
        except EventHubError as e:
            latency_ms = (time.time() - start_time) * 1000
            return PublishResult(
                success=False,
                error=str(e),
                latency_ms=latency_ms
            )
    
    def subscribe(self, topics: list[str]) -> None:
        """Subscribe to EventHubs topics
        
        Args:
            topics: List of EventHub names (typically single EventHub)
        """
        if not self._connected:
            raise RuntimeError("Not connected to EventHubs")
        
        # Close existing consumer if any
        if self.consumer:
            self.consumer.close()
            self.consumer = None
        
        # EventHubs typically uses single hub, use first topic or configured name
        hub_name = topics[0] if topics else self.eventhub_name
        
        # Create consumer
        self.consumer = EventHubConsumerClient.from_connection_string(
            conn_str=self.connection_string,
            consumer_group=self.consumer_group,
            eventhub_name=hub_name,
            **self.consumer_config
        )
    
    def consume(self, topic: str, timeout_ms: int = 1000) -> ConsumeResult:
        """Consume a message from EventHubs
        
        Args:
            topic: EventHub name (must be subscribed)
            timeout_ms: Timeout in milliseconds
            
        Returns:
            ConsumeResult with message and metadata
        """
        if not self._connected or not self.consumer:
            return ConsumeResult(message=None, latency_ms=0)
        
        start_time = time.time()
        
        try:
            # Receive events with timeout
            timeout_seconds = timeout_ms / 1000.0
            received_event = None
            
            # Receive from all partitions
            with self.consumer:
                received = self.consumer.receive(
                    max_wait_time=timeout_seconds,
                    max_batch_size=1
                )
                
                # Get first event from any partition
                for partition_events in received:
                    if partition_events:
                        received_event = partition_events[0]
                        break
            
            latency_ms = (time.time() - start_time) * 1000
            
            if not received_event:
                return ConsumeResult(message=None, latency_ms=latency_ms)
            
            # Extract properties (headers) and key
            properties = dict(received_event.properties) if received_event.properties else {}
            key = properties.pop('_key', None)
            
            # Get body
            body = received_event.body_as_str().encode('utf-8') if isinstance(received_event.body, str) else received_event.body
            
            message = Message(
                key=key,
                value=body,
                headers=properties if properties else None,
                timestamp=received_event.enqueued_time.timestamp() if received_event.enqueued_time else None
            )
            
            return ConsumeResult(
                message=message,
                partition=received_event.partition_key,
                offset=str(received_event.offset),
                latency_ms=latency_ms
            )
            
        except Exception as e:
            latency_ms = (time.time() - start_time) * 1000
            return ConsumeResult(message=None, latency_ms=latency_ms)
