"""Google Pub/Sub client implementation"""
import time
from typing import Any, Dict, Optional
from google.cloud import pubsub_v1
from google.api_core import exceptions as google_exceptions

from ..base import PubSubClient, Message, PublishResult, ConsumeResult


class GooglePubSubClient(PubSubClient):
    """Google Pub/Sub client with subscription support"""
    
    def __init__(self, config: Dict[str, Any]):
        """Initialize Google Pub/Sub client
        
        Args:
            config: Configuration dict with keys:
                - project_id: Google Cloud project ID
                - credentials_path: Optional path to service account JSON
                - publisher_config: Optional publisher configuration
                - subscriber_config: Optional subscriber configuration
        """
        super().__init__(config)
        self.project_id = config.get('project_id', '')
        self.credentials_path = config.get('credentials_path')
        
        self.publisher_config = config.get('publisher_config', {})
        self.subscriber_config = config.get('subscriber_config', {})
        
        self.publisher: Optional[pubsub_v1.PublisherClient] = None
        self.subscriber: Optional[pubsub_v1.SubscriberClient] = None
        self.subscriptions: Dict[str, str] = {}  # topic -> subscription_path
        self._received_messages = []
    
    def connect(self) -> None:
        """Establish connection to Google Pub/Sub"""
        if self._connected:
            return
        
        try:
            # Create publisher
            if self.credentials_path:
                self.publisher = pubsub_v1.PublisherClient.from_service_account_json(
                    self.credentials_path,
                    **self.publisher_config
                )
            else:
                self.publisher = pubsub_v1.PublisherClient(**self.publisher_config)
            
            # Create subscriber
            if self.credentials_path:
                self.subscriber = pubsub_v1.SubscriberClient.from_service_account_json(
                    self.credentials_path,
                    **self.subscriber_config
                )
            else:
                self.subscriber = pubsub_v1.SubscriberClient(**self.subscriber_config)
            
            self._connected = True
            
        except Exception as e:
            raise ConnectionError(f"Failed to connect to Google Pub/Sub: {e}")
    
    def disconnect(self) -> None:
        """Close connection to Google Pub/Sub"""
        if not self._connected:
            return
        
        # Close publisher
        if self.publisher:
            # Publisher client doesn't need explicit close in newer versions
            self.publisher = None
        
        # Close subscriber
        if self.subscriber:
            self.subscriber = None
        
        self._connected = False
    
    def publish(self, topic: str, message: Message) -> PublishResult:
        """Publish a message to Google Pub/Sub
        
        Args:
            topic: Topic name (will be formatted as projects/{project}/topics/{topic})
            message: Message to publish
            
        Returns:
            PublishResult with success status
        """
        if not self._connected or not self.publisher:
            return PublishResult(success=False, error="Not connected")
        
        start_time = time.time()
        
        try:
            # Format topic path
            if not topic.startswith('projects/'):
                topic_path = self.publisher.topic_path(self.project_id, topic)
            else:
                topic_path = topic
            
            # Prepare attributes (headers)
            attributes = {}
            if message.headers:
                attributes.update({k: str(v) for k, v in message.headers.items()})
            
            if message.key:
                attributes['_key'] = message.key
            
            # Publish message
            future = self.publisher.publish(
                topic_path,
                message.value,
                **attributes
            )
            
            # Wait for result
            message_id = future.result(timeout=10)
            
            latency_ms = (time.time() - start_time) * 1000
            
            return PublishResult(
                success=True,
                message_id=message_id,
                latency_ms=latency_ms
            )
            
        except google_exceptions.GoogleAPIError as e:
            latency_ms = (time.time() - start_time) * 1000
            return PublishResult(
                success=False,
                error=str(e),
                latency_ms=latency_ms
            )
    
    def subscribe(self, topics: list[str]) -> None:
        """Subscribe to Google Pub/Sub topics
        
        Args:
            topics: List of topic names
        """
        if not self._connected or not self.subscriber:
            raise RuntimeError("Not connected to Google Pub/Sub")
        
        # Create or get subscriptions for each topic
        for topic in topics:
            # Format topic and subscription paths
            if not topic.startswith('projects/'):
                topic_path = self.publisher.topic_path(self.project_id, topic)
            else:
                topic_path = topic
            
            # Use a deterministic subscription name based on topic
            subscription_name = f"{topic.split('/')[-1]}-perf-tool-sub"
            subscription_path = self.subscriber.subscription_path(self.project_id, subscription_name)
            
            # Try to create subscription if it doesn't exist
            try:
                self.subscriber.create_subscription(
                    request={"name": subscription_path, "topic": topic_path}
                )
            except google_exceptions.AlreadyExists:
                # Subscription already exists, that's fine
                pass
            except Exception as e:
                # If we can't create, assume it exists or will fail on pull
                pass
            
            self.subscriptions[topic] = subscription_path
    
    def consume(self, topic: str, timeout_ms: int = 1000) -> ConsumeResult:
        """Consume a message from Google Pub/Sub
        
        Args:
            topic: Topic name (must be subscribed)
            timeout_ms: Timeout in milliseconds
            
        Returns:
            ConsumeResult with message and metadata
        """
        if not self._connected or not self.subscriber:
            return ConsumeResult(message=None, latency_ms=0)
        
        start_time = time.time()
        
        try:
            # Get subscription for topic
            subscription_path = self.subscriptions.get(topic)
            if not subscription_path:
                return ConsumeResult(message=None, latency_ms=0)
            
            # Pull a single message
            timeout_seconds = timeout_ms / 1000.0
            response = self.subscriber.pull(
                request={
                    "subscription": subscription_path,
                    "max_messages": 1
                },
                timeout=timeout_seconds
            )
            
            latency_ms = (time.time() - start_time) * 1000
            
            if not response.received_messages:
                return ConsumeResult(message=None, latency_ms=latency_ms)
            
            received = response.received_messages[0]
            pubsub_message = received.message
            
            # Acknowledge the message
            self.subscriber.acknowledge(
                request={
                    "subscription": subscription_path,
                    "ack_ids": [received.ack_id]
                }
            )
            
            # Extract attributes (headers) and key
            attributes = dict(pubsub_message.attributes) if pubsub_message.attributes else {}
            key = attributes.pop('_key', None)
            
            message = Message(
                key=key,
                value=pubsub_message.data,
                headers=attributes if attributes else None,
                timestamp=pubsub_message.publish_time.timestamp()
            )
            
            return ConsumeResult(
                message=message,
                partition=None,
                offset=pubsub_message.message_id,
                latency_ms=latency_ms
            )
            
        except Exception as e:
            latency_ms = (time.time() - start_time) * 1000
            return ConsumeResult(message=None, latency_ms=latency_ms)
