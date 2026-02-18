"""Pulsar client implementation"""
import time
from typing import Any, Dict, Optional
import pulsar

from ..base import PubSubClient, Message, PublishResult, ConsumeResult


class PulsarClient(PubSubClient):
    """Pulsar pub-sub client with reader support for intermediary hops"""
    
    def __init__(self, config: Dict[str, Any]):
        """Initialize Pulsar client
        
        Args:
            config: Configuration dict with keys:
                - service_url: Pulsar service URL
                - use_reader: Boolean, use reader instead of consumer (for intermediary hops)
                - subscription_name: Subscription name (required if not using reader)
                - producer_config: Optional producer configuration
                - consumer_config: Optional consumer configuration
                - reader_config: Optional reader configuration
        """
        super().__init__(config)
        self.service_url = config.get('service_url', 'pulsar://localhost:6650')
        self.use_reader = config.get('use_reader', False)
        self.subscription_name = config.get('subscription_name', 'perf-tool-sub')
        
        self.producer_config = config.get('producer_config', {})
        self.consumer_config = config.get('consumer_config', {})
        self.reader_config = config.get('reader_config', {})
        
        self.client: Optional[pulsar.Client] = None
        self.producer: Optional[pulsar.Producer] = None
        self.consumer: Optional[pulsar.Consumer] = None
        self.reader: Optional[pulsar.Reader] = None
        self.current_topic: Optional[str] = None
    
    def connect(self) -> None:
        """Establish connection to Pulsar"""
        if self._connected:
            return
        
        try:
            self.client = pulsar.Client(self.service_url)
            self._connected = True
        except Exception as e:
            raise ConnectionError(f"Failed to connect to Pulsar: {e}")
    
    def disconnect(self) -> None:
        """Close connection to Pulsar"""
        if not self._connected:
            return
        
        if self.producer:
            self.producer.close()
            self.producer = None
        
        if self.consumer:
            self.consumer.close()
            self.consumer = None
        
        if self.reader:
            self.reader.close()
            self.reader = None
        
        if self.client:
            self.client.close()
            self.client = None
        
        self._connected = False
    
    def publish(self, topic: str, message: Message) -> PublishResult:
        """Publish a message to a Pulsar topic
        
        Args:
            topic: Topic name
            message: Message to publish
            
        Returns:
            PublishResult with success status
        """
        if not self._connected or not self.client:
            return PublishResult(success=False, error="Not connected")
        
        start_time = time.time()
        
        try:
            # Create producer if not exists or topic changed
            if not self.producer or self.current_topic != topic:
                if self.producer:
                    self.producer.close()
                
                producer_config = {
                    'topic': topic,
                    **self.producer_config
                }
                self.producer = self.client.create_producer(**producer_config)
                self.current_topic = topic
            
            # Prepare properties (headers)
            properties = message.headers or {}
            if message.key:
                properties['_key'] = message.key
            
            # Send message
            msg_id = self.producer.send(
                content=message.value,
                properties=properties
            )
            
            latency_ms = (time.time() - start_time) * 1000
            
            return PublishResult(
                success=True,
                message_id=str(msg_id),
                latency_ms=latency_ms
            )
            
        except Exception as e:
            latency_ms = (time.time() - start_time) * 1000
            return PublishResult(
                success=False,
                error=str(e),
                latency_ms=latency_ms
            )
    
    def subscribe(self, topics: list[str]) -> None:
        """Subscribe to Pulsar topics
        
        Args:
            topics: List of topic names
        """
        if not self._connected or not self.client:
            raise RuntimeError("Not connected to Pulsar")
        
        # Close existing consumer/reader
        if self.consumer:
            self.consumer.close()
            self.consumer = None
        
        if self.reader:
            self.reader.close()
            self.reader = None
        
        # For intermediary hops, use reader instead of consumer
        if self.use_reader:
            # Pulsar reader only supports single topic
            if len(topics) > 1:
                raise ValueError("Pulsar reader only supports single topic subscription")
            
            topic = topics[0]
            reader_config = {
                'topic': topic,
                'start_message_id': pulsar.MessageId.earliest,
                **self.reader_config
            }
            self.reader = self.client.create_reader(**reader_config)
            self.current_topic = topic
        else:
            # Use consumer for final consumption
            consumer_config = {
                'topics': topics,
                'subscription_name': self.subscription_name,
                **self.consumer_config
            }
            self.consumer = self.client.subscribe(**consumer_config)
    
    def consume(self, topic: str, timeout_ms: int = 1000) -> ConsumeResult:
        """Consume a message from Pulsar
        
        Args:
            topic: Topic name (must be subscribed)
            timeout_ms: Timeout in milliseconds
            
        Returns:
            ConsumeResult with message and metadata
        """
        if not self._connected:
            return ConsumeResult(message=None, latency_ms=0)
        
        start_time = time.time()
        
        try:
            # Use reader for intermediary hops, consumer otherwise
            if self.use_reader and self.reader:
                if not self.reader.has_message_available():
                    latency_ms = (time.time() - start_time) * 1000
                    return ConsumeResult(message=None, latency_ms=latency_ms)
                
                pulsar_msg = self.reader.read_next(timeout_millis=timeout_ms)
            elif self.consumer:
                pulsar_msg = self.consumer.receive(timeout_millis=timeout_ms)
                # Acknowledge message
                self.consumer.acknowledge(pulsar_msg)
            else:
                return ConsumeResult(message=None, latency_ms=0)
            
            latency_ms = (time.time() - start_time) * 1000
            
            # Extract key and headers
            properties = pulsar_msg.properties() or {}
            key = properties.pop('_key', None)
            
            message = Message(
                key=key,
                value=pulsar_msg.data(),
                headers=properties if properties else None,
                timestamp=pulsar_msg.publish_timestamp() / 1000.0
            )
            
            return ConsumeResult(
                message=message,
                partition=pulsar_msg.partition_key(),
                offset=str(pulsar_msg.message_id()),
                latency_ms=latency_ms
            )
            
        except Exception as e:
            latency_ms = (time.time() - start_time) * 1000
            return ConsumeResult(message=None, latency_ms=latency_ms)
