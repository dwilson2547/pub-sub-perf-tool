"""Helper services that simulate message transformation in a real system.

These services demonstrate how the monitoring framework tracks messages as they
are transformed through different processing stages.
"""
import json
import time
from typing import Dict, Any
from pub_sub_perf_tool.base import Message


class MessageEnricher:
    """Simulates a service that enriches messages with additional data."""
    
    @staticmethod
    def enrich(message: Message, enrichment_data: Dict[str, Any]) -> Message:
        """Add enrichment data to message.
        
        Args:
            message: Original message
            enrichment_data: Data to add to the message
            
        Returns:
            Enriched message
        """
        try:
            # Parse existing message
            data = json.loads(message.value.decode('utf-8'))
            
            # Add enrichment
            data['enrichment'] = enrichment_data
            data['enriched_at'] = time.time()
            
            # Create new message with enriched data
            return Message(
                key=message.key,
                value=json.dumps(data).encode('utf-8'),
                headers={**message.headers, 'enriched': 'true'} if message.headers else {'enriched': 'true'}
            )
        except Exception as e:
            raise ValueError(f"Failed to enrich message: {e}")


class MessageTransformer:
    """Simulates a service that transforms message format."""
    
    @staticmethod
    def transform_to_uppercase(message: Message) -> Message:
        """Transform message content to uppercase.
        
        Simulates format transformation like JSON -> XML or data normalization.
        """
        try:
            data = json.loads(message.value.decode('utf-8'))
            
            # Transform string values to uppercase
            transformed = {}
            for key, value in data.items():
                if isinstance(value, str):
                    transformed[key] = value.upper()
                else:
                    transformed[key] = value
            
            transformed['transformed_by'] = 'uppercase_transformer'
            transformed['transformed_at'] = time.time()
            
            return Message(
                key=message.key,
                value=json.dumps(transformed).encode('utf-8'),
                headers={**message.headers, 'transformed': 'true'} if message.headers else {'transformed': 'true'}
            )
        except Exception as e:
            raise ValueError(f"Failed to transform message: {e}")


class MessageAggregator:
    """Simulates a service that aggregates information from messages."""
    
    def __init__(self):
        self.message_count = 0
        self.total_size = 0
    
    def aggregate(self, message: Message) -> Message:
        """Add aggregation metadata to message."""
        self.message_count += 1
        self.total_size += len(message.value)
        
        try:
            data = json.loads(message.value.decode('utf-8'))
            
            data['aggregation_metadata'] = {
                'message_number': self.message_count,
                'cumulative_size_bytes': self.total_size,
                'aggregated_at': time.time()
            }
            
            return Message(
                key=message.key,
                value=json.dumps(data).encode('utf-8'),
                headers={**message.headers, 'aggregated': 'true'} if message.headers else {'aggregated': 'true'}
            )
        except Exception as e:
            raise ValueError(f"Failed to aggregate message: {e}")


class MessageFilter:
    """Simulates a service that filters/modifies message content."""
    
    @staticmethod
    def redact_sensitive_data(message: Message) -> Message:
        """Redact sensitive fields from message.
        
        Simulates data masking/filtering in compliance pipelines.
        """
        try:
            data = json.loads(message.value.decode('utf-8'))
            
            # Redact sensitive fields
            sensitive_fields = ['ssn', 'credit_card', 'password', 'api_key']
            for field in sensitive_fields:
                if field in data:
                    data[field] = '***REDACTED***'
            
            data['filtered_by'] = 'sensitive_data_filter'
            data['filtered_at'] = time.time()
            
            return Message(
                key=message.key,
                value=json.dumps(data).encode('utf-8'),
                headers={**message.headers, 'filtered': 'true'} if message.headers else {'filtered': 'true'}
            )
        except Exception as e:
            raise ValueError(f"Failed to filter message: {e}")


class MessageRouter:
    """Simulates a service that routes messages based on content."""
    
    @staticmethod
    def add_routing_info(message: Message, route_decision: str) -> Message:
        """Add routing decision to message."""
        try:
            data = json.loads(message.value.decode('utf-8'))
            
            data['routing'] = {
                'destination': route_decision,
                'routed_at': time.time(),
                'router_version': '1.0'
            }
            
            return Message(
                key=message.key,
                value=json.dumps(data).encode('utf-8'),
                headers={**message.headers, 'routed': 'true', 'route': route_decision} if message.headers else {'routed': 'true', 'route': route_decision}
            )
        except Exception as e:
            raise ValueError(f"Failed to route message: {e}")


def apply_service_transformation(message: Message, service_type: str, **kwargs) -> Message:
    """Apply a service transformation to a message.
    
    This simulates what would happen when a message passes through
    a microservice in a real system.
    
    Args:
        message: Original message
        service_type: Type of service (enricher, transformer, aggregator, filter, router)
        **kwargs: Additional arguments for the service
        
    Returns:
        Transformed message
    """
    if service_type == 'enricher':
        enrichment = kwargs.get('enrichment_data', {'source': 'test'})
        return MessageEnricher.enrich(message, enrichment)
    
    elif service_type == 'transformer':
        return MessageTransformer.transform_to_uppercase(message)
    
    elif service_type == 'filter':
        return MessageFilter.redact_sensitive_data(message)
    
    elif service_type == 'router':
        route = kwargs.get('route_decision', 'default')
        return MessageRouter.add_routing_info(message, route)
    
    else:
        raise ValueError(f"Unknown service type: {service_type}")
