"""Integration tests demonstrating message transformation tracking and monitoring.

These tests show the core value of the tool: monitoring messages as they flow
through a system, tracking transformations and latency at each hop.
"""
import pytest
import json
import time
from pub_sub_perf_tool.clients.kafka_client import KafkaClient
from pub_sub_perf_tool.base import Message
from .message_transformers import (
    MessageEnricher,
    MessageTransformer,
    MessageFilter,
    MessageRouter,
    MessageAggregator
)


def test_message_enrichment_tracking(kafka_container):
    """Track a message being enriched with additional data through the pipeline.
    
    Demonstrates: Monitoring how a message gains additional fields as it flows
    through enrichment services.
    """
    bootstrap_servers = kafka_container.get_bootstrap_server()
    
    config = {
        'bootstrap_servers': [bootstrap_servers],
        'consumer_group': 'enrichment-tracker'
    }
    
    # Simulate a 3-hop enrichment pipeline
    topics = ['raw-events', 'user-enriched', 'final-enriched']
    
    client = KafkaClient(config)
    
    try:
        client.connect()
        
        # Original message (simulating raw event from app)
        original_data = {
            'event_id': 'evt-001',
            'user_id': 'user-123',
            'action': 'login'
        }
        original_message = Message(
            key='user-123',
            value=json.dumps(original_data).encode('utf-8')
        )
        
        # Hop 1: Publish original message
        client.subscribe([topics[0]])
        hop1_start = time.time()
        result1 = client.publish(topics[0], original_message)
        hop1_latency = result1.latency_ms
        
        time.sleep(1)
        
        # Consume and enrich (simulating enrichment service)
        consume1 = client.consume(topics[0], timeout_ms=10000)
        message_after_hop1 = consume1.message
        
        assert message_after_hop1 is not None, "Failed to consume message from hop 1"
        
        # Enrich with user data
        enriched_message = MessageEnricher.enrich(
            message_after_hop1,
            {'user_name': 'John Doe', 'user_tier': 'premium'}
        )
        
        # Hop 2: Publish enriched message
        client.subscribe([topics[1]])
        hop2_start = time.time()
        result2 = client.publish(topics[1], enriched_message)
        hop2_latency = result2.latency_ms
        
        time.sleep(1)
        
        # Consume and further enrich
        consume2 = client.consume(topics[1], timeout_ms=10000)
        message_after_hop2 = consume2.message
        
        assert message_after_hop2 is not None, "Failed to consume message from hop 2"
        
        # Add geo data
        final_enriched = MessageEnricher.enrich(
            message_after_hop2,
            {'geo_location': 'US-CA', 'ip_address': '192.168.1.1'}
        )
        
        # Hop 3: Publish final enriched message
        client.subscribe([topics[2]])
        hop3_start = time.time()
        result3 = client.publish(topics[2], final_enriched)
        hop3_latency = result3.latency_ms
        
        time.sleep(1)
        
        # Final consume
        consume3 = client.consume(topics[2], timeout_ms=10000)
        final_message = consume3.message
        
        # Validate transformation tracking
        assert final_message is not None
        
        final_data = json.loads(final_message.value.decode('utf-8'))
        
        # Verify original fields preserved
        assert final_data['event_id'] == 'evt-001'
        assert final_data['user_id'] == 'user-123'
        assert final_data['action'] == 'login'
        
        # Verify enrichments added
        assert 'enrichment' in final_data
        assert final_data.get('enriched_at') is not None
        
        # Verify headers show transformation
        assert final_message.headers.get('enriched') == 'true'
        
        # Verify latency tracking at each hop
        assert hop1_latency > 0
        assert hop2_latency > 0
        assert hop3_latency > 0
        
        total_latency = hop1_latency + hop2_latency + hop3_latency
        
        print(f"\n=== Message Enrichment Pipeline Monitoring ===")
        print(f"Hop 1 (Raw Event):        {hop1_latency:.2f}ms")
        print(f"Hop 2 (User Enriched):    {hop2_latency:.2f}ms")
        print(f"Hop 3 (Fully Enriched):   {hop3_latency:.2f}ms")
        print(f"Total Pipeline Latency:   {total_latency:.2f}ms")
        print(f"Original message size:    {len(json.dumps(original_data))} bytes")
        print(f"Final message size:       {len(final_message.value)} bytes")
        print(f"Size increase:            {len(final_message.value) - len(json.dumps(original_data))} bytes")
        
        # Verify message grew through enrichment
        assert len(final_message.value) > len(json.dumps(original_data))
        
    finally:
        client.disconnect()


def test_message_transformation_pipeline(kafka_container):
    """Track message format transformation through processing stages.
    
    Demonstrates: Monitoring how message format/content changes at each stage.
    """
    bootstrap_servers = kafka_container.get_bootstrap_server()
    
    config = {
        'bootstrap_servers': [bootstrap_servers],
        'consumer_group': 'transformation-tracker'
    }
    
    topics = ['input-raw', 'transformed-data', 'filtered-data']
    
    client = KafkaClient(config)
    
    try:
        client.connect()
        
        # Original message with mixed case and sensitive data
        original_data = {
            'user_id': 'user-456',
            'name': 'jane doe',
            'email': 'jane@example.com',
            'ssn': '123-45-6789',
            'status': 'active'
        }
        
        original_message = Message(
            key='user-456',
            value=json.dumps(original_data).encode('utf-8'),
            headers={'source': 'user-service'}
        )
        
        # Hop 1: Original data
        latencies = []
        message_snapshots = []
        
        client.subscribe([topics[0]])
        result1 = client.publish(topics[0], original_message)
        latencies.append(('publish_raw', result1.latency_ms))
        message_snapshots.append(('raw', original_data.copy()))
        
        time.sleep(1)
        
        # Consume and transform (simulating transformation service)
        consume1 = client.consume(topics[0], timeout_ms=10000)
        latencies.append(('consume_raw', consume1.latency_ms))
        
        assert consume1.message is not None, "Failed to consume from hop 1"
        
        transformed = MessageTransformer.transform_to_uppercase(consume1.message)
        
        # Hop 2: Transformed data
        client.subscribe([topics[1]])
        result2 = client.publish(topics[1], transformed)
        latencies.append(('publish_transformed', result2.latency_ms))
        
        time.sleep(1)
        
        consume2 = client.consume(topics[1], timeout_ms=10000)
        latencies.append(('consume_transformed', consume2.latency_ms))
        
        assert consume2.message is not None, "Failed to consume from hop 2"
        
        transformed_data = json.loads(consume2.message.value.decode('utf-8'))
        message_snapshots.append(('transformed', transformed_data.copy()))
        
        # Hop 3: Filter sensitive data
        filtered = MessageFilter.redact_sensitive_data(consume2.message)
        
        client.subscribe([topics[2]])
        result3 = client.publish(topics[2], filtered)
        latencies.append(('publish_filtered', result3.latency_ms))
        
        time.sleep(1)
        
        consume3 = client.consume(topics[2], timeout_ms=10000)
        latencies.append(('consume_filtered', consume3.latency_ms))
        
        assert consume3.message is not None, "Failed to consume from hop 3"
        
        final_data = json.loads(consume3.message.value.decode('utf-8'))
        message_snapshots.append(('filtered', final_data.copy()))
        
        # Print monitoring report
        print(f"\n=== Message Transformation Pipeline Report ===")
        print("\nLatency at each stage:")
        for stage, latency in latencies:
            print(f"  {stage:25s}: {latency:8.2f}ms")
        
        print("\nMessage Evolution:")
        print("\n1. Raw Message:")
        print(f"   {json.dumps(message_snapshots[0][1], indent=2)}")
        
        print("\n2. After Transformation (Uppercase):")
        print(f"   {json.dumps(message_snapshots[1][1], indent=2)}")
        
        print("\n3. After Filtering (Sensitive Data Redacted):")
        print(f"   {json.dumps(message_snapshots[2][1], indent=2)}")
        
        # Validate transformations were tracked
        assert final_data['name'] == 'JANE DOE'  # Transformed to uppercase
        assert final_data['email'] == 'JANE@EXAMPLE.COM'  # Transformed
        assert final_data['ssn'] == '***REDACTED***'  # Filtered
        assert 'transformed_by' in final_data
        assert 'filtered_by' in final_data
        
        # Validate headers tracked transformations
        assert consume3.message.headers.get('transformed') == 'true'
        assert consume3.message.headers.get('filtered') == 'true'
        
    finally:
        client.disconnect()


def test_multi_message_aggregation_tracking(kafka_container):
    """Track batch processing with aggregation metadata.
    
    Demonstrates: Monitoring cumulative processing stats across multiple messages.
    """
    bootstrap_servers = kafka_container.get_bootstrap_server()
    
    config = {
        'bootstrap_servers': [bootstrap_servers],
        'consumer_group': 'aggregation-tracker'
    }
    
    input_topic = 'batch-input'
    output_topic = 'batch-aggregated'
    
    client = KafkaClient(config)
    aggregator = MessageAggregator()
    
    try:
        client.connect()
        client.subscribe([input_topic])
        
        # Process batch of messages
        batch_size = 10
        batch_results = []
        
        for i in range(batch_size):
            # Create test message
            data = {
                'batch_id': 'batch-001',
                'message_num': i,
                'payload': f'Message {i} data'
            }
            
            message = Message(
                key=f'msg-{i}',
                value=json.dumps(data).encode('utf-8')
            )
            
            # Publish
            pub_result = client.publish(input_topic, message)
            
            time.sleep(0.5)
            
            # Consume
            cons_result = client.consume(input_topic, timeout_ms=5000)
            
            if cons_result.message:
                # Apply aggregation (simulating aggregator service)
                aggregated = aggregator.aggregate(cons_result.message)
                
                # Publish aggregated
                agg_result = client.publish(output_topic, aggregated)
                
                # Track results
                agg_data = json.loads(aggregated.value.decode('utf-8'))
                batch_results.append({
                    'message_num': i,
                    'publish_latency': pub_result.latency_ms,
                    'consume_latency': cons_result.latency_ms,
                    'cumulative_size': agg_data['aggregation_metadata']['cumulative_size_bytes'],
                    'message_count': agg_data['aggregation_metadata']['message_number']
                })
        
        # Print aggregation monitoring report
        print(f"\n=== Batch Processing Monitoring Report ===")
        print(f"Total messages processed: {len(batch_results)}")
        print("\nPer-message metrics:")
        print(f"{'Msg #':<6} {'Pub(ms)':<10} {'Cons(ms)':<10} {'Cumulative Size':<20} {'Msg Count':<10}")
        print("-" * 60)
        
        for result in batch_results:
            print(f"{result['message_num']:<6} "
                  f"{result['publish_latency']:<10.2f} "
                  f"{result['consume_latency']:<10.2f} "
                  f"{result['cumulative_size']:<20} "
                  f"{result['message_count']:<10}")
        
        # Calculate aggregate statistics
        avg_pub_latency = sum(r['publish_latency'] for r in batch_results) / len(batch_results)
        avg_cons_latency = sum(r['consume_latency'] for r in batch_results) / len(batch_results)
        total_size = batch_results[-1]['cumulative_size']
        
        print(f"\nAggregate Statistics:")
        print(f"  Average publish latency:  {avg_pub_latency:.2f}ms")
        print(f"  Average consume latency:  {avg_cons_latency:.2f}ms")
        print(f"  Total data processed:     {total_size} bytes")
        
        # Validate aggregation tracking
        assert len(batch_results) == batch_size
        assert batch_results[-1]['message_count'] == batch_size
        assert batch_results[-1]['cumulative_size'] > 0
        
        # Verify cumulative size increases
        for i in range(1, len(batch_results)):
            assert batch_results[i]['cumulative_size'] > batch_results[i-1]['cumulative_size']
        
    finally:
        client.disconnect()


def test_message_routing_decision_tracking(kafka_container):
    """Track routing decisions and message flow branching.
    
    Demonstrates: Monitoring how messages are routed to different destinations
    based on content.
    """
    bootstrap_servers = kafka_container.get_bootstrap_server()
    
    config = {
        'bootstrap_servers': [bootstrap_servers],
        'consumer_group': 'routing-tracker'
    }
    
    input_topic = 'routing-input'
    priority_topic = 'priority-queue'
    standard_topic = 'standard-queue'
    
    client = KafkaClient(config)
    
    try:
        client.connect()
        client.subscribe([input_topic])
        
        test_messages = [
            {'order_id': 'ORD-001', 'priority': 'high', 'amount': 1000},
            {'order_id': 'ORD-002', 'priority': 'standard', 'amount': 50},
            {'order_id': 'ORD-003', 'priority': 'high', 'amount': 500},
            {'order_id': 'ORD-004', 'priority': 'standard', 'amount': 25},
        ]
        
        routing_results = []
        
        for msg_data in test_messages:
            # Publish to input
            message = Message(
                key=msg_data['order_id'],
                value=json.dumps(msg_data).encode('utf-8')
            )
            
            pub_result = client.publish(input_topic, message)
            
            time.sleep(0.5)
            
            # Consume and route (simulating router service)
            cons_result = client.consume(input_topic, timeout_ms=5000)
            
            if cons_result.message:
                consumed_data = json.loads(cons_result.message.value.decode('utf-8'))
                
                # Determine route based on priority
                if consumed_data['priority'] == 'high':
                    route = 'priority'
                    dest_topic = priority_topic
                else:
                    route = 'standard'
                    dest_topic = standard_topic
                
                # Add routing info
                routed_message = MessageRouter.add_routing_info(
                    cons_result.message,
                    route
                )
                
                # Publish to appropriate queue
                route_result = client.publish(dest_topic, routed_message)
                
                routing_results.append({
                    'order_id': msg_data['order_id'],
                    'priority': msg_data['priority'],
                    'routed_to': route,
                    'input_latency': pub_result.latency_ms,
                    'routing_latency': route_result.latency_ms,
                    'total_latency': pub_result.latency_ms + cons_result.latency_ms + route_result.latency_ms
                })
        
        # Print routing monitoring report
        print(f"\n=== Message Routing Monitoring Report ===")
        print(f"{'Order ID':<12} {'Priority':<12} {'Routed To':<15} {'Total Latency':<15}")
        print("-" * 60)
        
        for result in routing_results:
            print(f"{result['order_id']:<12} "
                  f"{result['priority']:<12} "
                  f"{result['routed_to']:<15} "
                  f"{result['total_latency']:<15.2f}ms")
        
        # Calculate routing statistics
        high_priority_count = sum(1 for r in routing_results if r['routed_to'] == 'priority')
        standard_count = sum(1 for r in routing_results if r['routed_to'] == 'standard')
        
        print(f"\nRouting Statistics:")
        print(f"  High priority messages:     {high_priority_count}")
        print(f"  Standard priority messages: {standard_count}")
        
        # Validate routing
        assert len(routing_results) == len(test_messages)
        assert high_priority_count == 2
        assert standard_count == 2
        
        # Verify routing decisions match original priority
        for result in routing_results:
            if result['priority'] == 'high':
                assert result['routed_to'] == 'priority'
            else:
                assert result['routed_to'] == 'standard'
        
    finally:
        client.disconnect()
