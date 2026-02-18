"""StreamNative client implementation (Managed Pulsar)"""
import time
from typing import Any, Dict, Optional
import pulsar

from ..base import PubSubClient, Message, PublishResult, ConsumeResult
from .pulsar_client import PulsarClient


class StreamNativeClient(PulsarClient):
    """StreamNative pub-sub client (managed Pulsar with authentication)
    
    StreamNative is a managed Pulsar service that typically requires OAuth2 authentication.
    This client extends PulsarClient with StreamNative-specific configuration.
    """
    
    def __init__(self, config: Dict[str, Any]):
        """Initialize StreamNative client
        
        Args:
            config: Configuration dict with keys:
                - service_url: StreamNative service URL (e.g., pulsar+ssl://...)
                - use_reader: Boolean, use reader instead of consumer (for intermediary hops)
                - subscription_name: Subscription name (required if not using reader)
                - auth_params: Authentication parameters dict with:
                    - type: Authentication type (token, oauth2, tls)
                    - token: Token string (for token auth)
                    - issuer_url: OAuth2 issuer URL (for oauth2)
                    - client_id: OAuth2 client ID (for oauth2)
                    - client_secret: OAuth2 client secret (for oauth2)
                    - audience: OAuth2 audience (for oauth2)
                    - cert_file: Certificate file path (for TLS)
                    - key_file: Key file path (for TLS)
                - producer_config: Optional producer configuration
                - consumer_config: Optional consumer configuration
                - reader_config: Optional reader configuration
        """
        # Extract auth params before calling super
        self.auth_params = config.get('auth_params', {})
        
        # Initialize parent class
        super().__init__(config)
        
        # StreamNative typically uses SSL
        if 'service_url' not in config:
            self.service_url = config.get('service_url', 'pulsar+ssl://streamnative.cloud:6651')
    
    def connect(self) -> None:
        """Establish connection to StreamNative"""
        if self._connected:
            return
        
        try:
            # Prepare authentication
            authentication = self._create_authentication()
            
            # Create client with authentication
            if authentication:
                self.client = pulsar.Client(
                    self.service_url,
                    authentication=authentication
                )
            else:
                self.client = pulsar.Client(self.service_url)
            
            self._connected = True
            
        except Exception as e:
            raise ConnectionError(f"Failed to connect to StreamNative: {e}")
    
    def _create_authentication(self) -> Optional[pulsar.Authentication]:
        """Create authentication object based on config
        
        Returns:
            pulsar.Authentication object or None
        """
        if not self.auth_params:
            return None
        
        auth_type = self.auth_params.get('type', '').lower()
        
        if auth_type == 'token':
            # Token authentication
            token = self.auth_params.get('token', '')
            return pulsar.AuthenticationToken(token)
        
        elif auth_type == 'oauth2':
            # OAuth2 authentication (common for StreamNative Cloud)
            oauth_params = {
                'type': 'client_credentials',
                'issuer_url': self.auth_params.get('issuer_url', ''),
                'client_id': self.auth_params.get('client_id', ''),
                'client_secret': self.auth_params.get('client_secret', ''),
                'audience': self.auth_params.get('audience', ''),
            }
            return pulsar.AuthenticationOauth2(oauth_params)
        
        elif auth_type == 'tls':
            # TLS authentication
            cert_file = self.auth_params.get('cert_file', '')
            key_file = self.auth_params.get('key_file', '')
            return pulsar.AuthenticationTLS(cert_file, key_file)
        
        return None
