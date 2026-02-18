"""Kafka client implementation"""
import uuid
import time
from typing import Any, Dict, Optional
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError

from ..base import PubSubClient, Message, PublishResult, ConsumeResult


class KafkaClient(PubSubClient):
    """Kafka pub-sub client with random consumer group ID support"""
    
    def __init__(self, config: Dict[str, Any]):
        """Initialize Kafka client
        
        Args:
            config: Configuration dict with keys:
                - bootstrap_servers: List of broker addresses
                - consumer_group: Optional consumer group (random if not provided)
                - producer_config: Optional producer configuration
                - consumer_config: Optional consumer configuration
        """
        super().__init__(config)
        self.bootstrap_servers = config.get('bootstrap_servers', ['localhost:9092'])
        
        # Generate random consumer group ID if not provided
        self.consumer_group = config.get('consumer_group', f"perf-tool-{uuid.uuid4()}")
        
        self.producer_config = config.get('producer_config', {})
        self.consumer_config = config.get('consumer_config', {})
        
        self.producer: Optional[KafkaProducer] = None
        self.consumer: Optional[KafkaConsumer] = None
    
    def connect(self) -> None:
        """Establish connection to Kafka"""
        if self._connected:
            return
        
        try:
            # Create producer
            producer_config = {
                'bootstrap_servers': self.bootstrap_servers,
                'value_serializer': lambda v: v if isinstance(v, bytes) else v.encode('utf-8'),
                **self.producer_config
            }
            self.producer = KafkaProducer(**producer_config)
            
            # Consumer will be created when subscribe is called
            self._connected = True
            
        except Exception as e:
            raise ConnectionError(f"Failed to connect to Kafka: {e}")
    
    def disconnect(self) -> None:
        """Close connection to Kafka"""
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
        """Publish a message to a Kafka topic
        
        Args:
            topic: Topic name
            message: Message to publish
            
        Returns:
            PublishResult with success status
        """
        if not self._connected or not self.producer:
            return PublishResult(success=False, error="Not connected")
        
        start_time = time.time()
        
        try:
            # Prepare message
            key = message.key.encode('utf-8') if message.key else None
            value = message.value
            headers = [(k, v.encode('utf-8')) for k, v in (message.headers or {}).items()]
            
            # Send message
            future = self.producer.send(
                topic,
                key=key,
                value=value,
                headers=headers if headers else None
            )
            
            # Wait for result
            record_metadata = future.get(timeout=10)
            
            latency_ms = (time.time() - start_time) * 1000
            
            return PublishResult(
                success=True,
                message_id=f"{record_metadata.partition}:{record_metadata.offset}",
                latency_ms=latency_ms
            )
            
        except KafkaError as e:
            latency_ms = (time.time() - start_time) * 1000
            return PublishResult(
                success=False,
                error=str(e),
                latency_ms=latency_ms
            )
    
    def subscribe(self, topics: list[str]) -> None:
        """Subscribe to Kafka topics
        
        Args:
            topics: List of topic names
        """
        if not self._connected:
            raise RuntimeError("Not connected to Kafka")
        
        # Close existing consumer if any
        if self.consumer:
            self.consumer.close()
        
        # Create consumer with configuration
        consumer_config = {
            'bootstrap_servers': self.bootstrap_servers,
            'group_id': self.consumer_group,
            'auto_offset_reset': 'earliest',
            'enable_auto_commit': True,
            'value_deserializer': lambda v: v,  # Keep as bytes
            **self.consumer_config
        }
        
        self.consumer = KafkaConsumer(*topics, **consumer_config)
    
    def consume(self, topic: str, timeout_ms: int = 1000) -> ConsumeResult:
        """Consume a message from Kafka
        
        Args:
            topic: Topic name (must be subscribed)
            timeout_ms: Timeout in milliseconds
            
        Returns:
            ConsumeResult with message and metadata
        """
        if not self._connected or not self.consumer:
            return ConsumeResult(message=None, latency_ms=0)
        
        start_time = time.time()
        
        try:
            # Poll for messages
            records = self.consumer.poll(timeout_ms=timeout_ms, max_records=1)
            
            latency_ms = (time.time() - start_time) * 1000
            
            if not records:
                return ConsumeResult(message=None, latency_ms=latency_ms)
            
            # Get first message
            for topic_partition, messages in records.items():
                if messages:
                    kafka_msg = messages[0]
                    
                    # Convert headers
                    headers = {}
                    if kafka_msg.headers:
                        headers = {k: v.decode('utf-8') if isinstance(v, bytes) else v 
                                  for k, v in kafka_msg.headers}
                    
                    message = Message(
                        key=kafka_msg.key.decode('utf-8') if kafka_msg.key else None,
                        value=kafka_msg.value,
                        headers=headers if headers else None,
                        timestamp=kafka_msg.timestamp / 1000.0 if kafka_msg.timestamp else None
                    )
                    
                    return ConsumeResult(
                        message=message,
                        partition=kafka_msg.partition,
                        offset=kafka_msg.offset,
                        latency_ms=latency_ms
                    )
            
            return ConsumeResult(message=None, latency_ms=latency_ms)
            
        except Exception as e:
            latency_ms = (time.time() - start_time) * 1000
            return ConsumeResult(message=None, latency_ms=latency_ms)
