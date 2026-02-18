"""Base abstraction layer for pub-sub systems"""
from abc import ABC, abstractmethod
from typing import Any, Dict, Optional, Callable
from dataclasses import dataclass
import time


@dataclass
class Message:
    """Generic message structure"""
    key: Optional[str]
    value: bytes
    headers: Optional[Dict[str, str]] = None
    timestamp: float = None
    
    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = time.time()


@dataclass
class PublishResult:
    """Result of a publish operation"""
    success: bool
    message_id: Optional[str] = None
    error: Optional[str] = None
    latency_ms: float = 0.0


@dataclass
class ConsumeResult:
    """Result of a consume operation"""
    message: Optional[Message]
    partition: Optional[int] = None
    offset: Optional[Any] = None
    latency_ms: float = 0.0


class PubSubClient(ABC):
    """Abstract base class for pub-sub clients"""
    
    def __init__(self, config: Dict[str, Any]):
        """Initialize client with configuration
        
        Args:
            config: Client-specific configuration
        """
        self.config = config
        self._connected = False
    
    @abstractmethod
    def connect(self) -> None:
        """Establish connection to the pub-sub system"""
        pass
    
    @abstractmethod
    def disconnect(self) -> None:
        """Close connection to the pub-sub system"""
        pass
    
    @abstractmethod
    def publish(self, topic: str, message: Message) -> PublishResult:
        """Publish a message to a topic
        
        Args:
            topic: Topic/queue name
            message: Message to publish
            
        Returns:
            PublishResult with success status and metadata
        """
        pass
    
    @abstractmethod
    def consume(self, topic: str, timeout_ms: int = 1000) -> ConsumeResult:
        """Consume a message from a topic
        
        Args:
            topic: Topic/queue name
            timeout_ms: Timeout in milliseconds
            
        Returns:
            ConsumeResult with message and metadata
        """
        pass
    
    @abstractmethod
    def subscribe(self, topics: list[str]) -> None:
        """Subscribe to topics for consumption
        
        Args:
            topics: List of topic names
        """
        pass
    
    def is_connected(self) -> bool:
        """Check if client is connected"""
        return self._connected
    
    def __enter__(self):
        """Context manager entry"""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.disconnect()
