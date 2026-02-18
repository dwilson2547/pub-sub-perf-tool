"""RabbitMQ client implementation"""
import time
import json
from typing import Any, Dict, Optional
import pika
from pika.exceptions import AMQPError

from ..base import PubSubClient, Message, PublishResult, ConsumeResult


class RabbitMQClient(PubSubClient):
    """RabbitMQ pub-sub client"""
    
    def __init__(self, config: Dict[str, Any]):
        """Initialize RabbitMQ client
        
        Args:
            config: Configuration dict with keys:
                - host: RabbitMQ host (default: localhost)
                - port: RabbitMQ port (default: 5672)
                - username: Username (default: guest)
                - password: Password (default: guest)
                - virtual_host: Virtual host (default: /)
                - exchange: Exchange name (default: '')
                - exchange_type: Exchange type (default: topic)
                - queue_prefix: Prefix for queue names
        """
        super().__init__(config)
        self.host = config.get('host', 'localhost')
        self.port = config.get('port', 5672)
        self.username = config.get('username', 'guest')
        self.password = config.get('password', 'guest')
        self.virtual_host = config.get('virtual_host', '/')
        self.exchange = config.get('exchange', '')
        self.exchange_type = config.get('exchange_type', 'topic')
        self.queue_prefix = config.get('queue_prefix', 'perf-tool')
        
        self.connection: Optional[pika.BlockingConnection] = None
        self.channel: Optional[pika.channel.Channel] = None
        self.subscribed_queues: list[str] = []
    
    def connect(self) -> None:
        """Establish connection to RabbitMQ"""
        if self._connected:
            return
        
        try:
            credentials = pika.PlainCredentials(self.username, self.password)
            parameters = pika.ConnectionParameters(
                host=self.host,
                port=self.port,
                virtual_host=self.virtual_host,
                credentials=credentials
            )
            
            self.connection = pika.BlockingConnection(parameters)
            self.channel = self.connection.channel()
            
            # Declare exchange if specified
            if self.exchange:
                self.channel.exchange_declare(
                    exchange=self.exchange,
                    exchange_type=self.exchange_type,
                    durable=True
                )
            
            self._connected = True
            
        except Exception as e:
            raise ConnectionError(f"Failed to connect to RabbitMQ: {e}")
    
    def disconnect(self) -> None:
        """Close connection to RabbitMQ"""
        if not self._connected:
            return
        
        if self.connection and not self.connection.is_closed:
            self.connection.close()
        
        self.connection = None
        self.channel = None
        self._connected = False
    
    def publish(self, topic: str, message: Message) -> PublishResult:
        """Publish a message to RabbitMQ
        
        Args:
            topic: Routing key/topic name
            message: Message to publish
            
        Returns:
            PublishResult with success status
        """
        if not self._connected or not self.channel:
            return PublishResult(success=False, error="Not connected")
        
        start_time = time.time()
        
        try:
            # Prepare headers
            headers = message.headers or {}
            if message.key:
                headers['_key'] = message.key
            
            # Create properties
            properties = pika.BasicProperties(
                delivery_mode=2,  # Persistent
                headers=headers,
                timestamp=int(message.timestamp)
            )
            
            # Publish message
            self.channel.basic_publish(
                exchange=self.exchange,
                routing_key=topic,
                body=message.value,
                properties=properties
            )
            
            latency_ms = (time.time() - start_time) * 1000
            
            return PublishResult(
                success=True,
                latency_ms=latency_ms
            )
            
        except AMQPError as e:
            latency_ms = (time.time() - start_time) * 1000
            return PublishResult(
                success=False,
                error=str(e),
                latency_ms=latency_ms
            )
    
    def subscribe(self, topics: list[str]) -> None:
        """Subscribe to RabbitMQ topics
        
        Args:
            topics: List of routing keys/topic names
        """
        if not self._connected or not self.channel:
            raise RuntimeError("Not connected to RabbitMQ")
        
        # Clear existing subscriptions
        self.subscribed_queues = []
        
        for topic in topics:
            # Declare queue for this topic
            queue_name = f"{self.queue_prefix}-{topic}"
            self.channel.queue_declare(queue=queue_name, durable=True)
            
            # Bind queue to exchange
            if self.exchange:
                self.channel.queue_bind(
                    exchange=self.exchange,
                    queue=queue_name,
                    routing_key=topic
                )
            
            self.subscribed_queues.append(queue_name)
    
    def consume(self, topic: str, timeout_ms: int = 1000) -> ConsumeResult:
        """Consume a message from RabbitMQ
        
        Args:
            topic: Topic name (must be subscribed)
            timeout_ms: Timeout in milliseconds
            
        Returns:
            ConsumeResult with message and metadata
        """
        if not self._connected or not self.channel:
            return ConsumeResult(message=None, latency_ms=0)
        
        start_time = time.time()
        queue_name = f"{self.queue_prefix}-{topic}"
        
        if queue_name not in self.subscribed_queues:
            return ConsumeResult(message=None, latency_ms=0)
        
        try:
            # Get message with timeout
            method_frame, header_frame, body = self.channel.basic_get(
                queue=queue_name,
                auto_ack=True
            )
            
            latency_ms = (time.time() - start_time) * 1000
            
            if method_frame is None:
                return ConsumeResult(message=None, latency_ms=latency_ms)
            
            # Extract headers and key
            headers = header_frame.headers or {}
            key = headers.pop('_key', None)
            
            # Get timestamp
            timestamp = None
            if header_frame.timestamp:
                timestamp = float(header_frame.timestamp)
            
            message = Message(
                key=key,
                value=body,
                headers=headers if headers else None,
                timestamp=timestamp
            )
            
            return ConsumeResult(
                message=message,
                partition=None,
                offset=method_frame.delivery_tag,
                latency_ms=latency_ms
            )
            
        except Exception as e:
            latency_ms = (time.time() - start_time) * 1000
            return ConsumeResult(message=None, latency_ms=latency_ms)
