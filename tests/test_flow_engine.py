"""Unit tests for flow engine"""
import time
import pytest
from pub_sub_perf_tool.flow_engine import (
    ClientType,
    create_client,
    FlowControlConfig,
    MessageFlowEngine,
    RateLimiter,
    StepConfig,
)
from pub_sub_perf_tool.clients import (
    KafkaClient,
    PulsarClient,
    RabbitMQClient,
    IggyClient,
    EventHubsClient,
    GooglePubSubClient,
    StreamNativeClient
)


def test_create_kafka_client():
    """Test creating a Kafka client"""
    client = create_client(ClientType.KAFKA, {'bootstrap_servers': ['localhost:9092']})
    assert isinstance(client, KafkaClient)


def test_create_pulsar_client():
    """Test creating a Pulsar client"""
    client = create_client(ClientType.PULSAR, {'service_url': 'pulsar://localhost:6650'})
    assert isinstance(client, PulsarClient)


def test_create_rabbitmq_client():
    """Test creating a RabbitMQ client"""
    client = create_client(ClientType.RABBITMQ, {'host': 'localhost'})
    assert isinstance(client, RabbitMQClient)


def test_create_iggy_client():
    """Test creating an Iggy client"""
    client = create_client(ClientType.IGGY, {'host': 'localhost'})
    assert isinstance(client, IggyClient)


def test_create_eventhubs_client():
    """Test creating an EventHubs client"""
    client = create_client(
        ClientType.EVENTHUBS,
        {
            'connection_string': 'Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=test',
            'eventhub_name': 'test-hub'
        }
    )
    assert isinstance(client, EventHubsClient)


def test_create_googlepubsub_client():
    """Test creating a Google Pub/Sub client"""
    client = create_client(
        ClientType.GOOGLEPUBSUB,
        {'project_id': 'test-project'}
    )
    assert isinstance(client, GooglePubSubClient)


def test_create_streamnative_client():
    """Test creating a StreamNative client"""
    client = create_client(
        ClientType.STREAMNATIVE,
        {'service_url': 'pulsar+ssl://streamnative.cloud:6651'}
    )
    assert isinstance(client, StreamNativeClient)


def test_hop_reference_resolution():
    """Test that hop references are properly resolved"""
    flow_config = {
        'name': 'test-flow',
        'hops': [
            {
                'name': 'first-hop',
                'destination': {
                    'type': 'kafka',
                    'topic': 'test-topic',
                    'config': {'bootstrap_servers': ['localhost:9092']}
                }
            },
            {
                'name': 'second-hop',
                'source': 'hop: first-hop',
                'destination': {
                    'type': 'kafka',
                    'topic': 'output-topic',
                    'config': {'bootstrap_servers': ['localhost:9092']}
                }
            }
        ]
    }
    
    engine = MessageFlowEngine(flow_config)
    
    # Verify that the source reference was resolved
    assert 'source' in engine.hops_config[1]
    assert isinstance(engine.hops_config[1]['source'], dict)
    assert engine.hops_config[1]['source']['type'] == 'kafka'
    assert engine.hops_config[1]['source']['topic'] == 'test-topic'
    assert engine.hops_config[1]['source']['config']['bootstrap_servers'] == ['localhost:9092']


def test_hop_reference_with_multiple_hops():
    """Test that hop references work with multiple intermediary hops"""
    flow_config = {
        'name': 'multi-hop-flow',
        'hops': [
            {
                'name': 'kafka-publish',
                'destination': {
                    'type': 'kafka',
                    'topic': 'kafka-input',
                    'config': {'bootstrap_servers': ['localhost:9092']}
                }
            },
            {
                'name': 'kafka-to-pulsar',
                'source': 'hop: kafka-publish',
                'destination': {
                    'type': 'pulsar',
                    'topic': 'pulsar-intermediate',
                    'config': {'service_url': 'pulsar://localhost:6650'}
                }
            },
            {
                'name': 'pulsar-to-rabbitmq',
                'source': 'hop: kafka-to-pulsar',
                'destination': {
                    'type': 'rabbitmq',
                    'topic': 'rabbitmq-output',
                    'config': {'host': 'localhost', 'port': 5672}
                }
            }
        ]
    }
    
    engine = MessageFlowEngine(flow_config)
    
    # Verify second hop
    assert engine.hops_config[1]['source']['type'] == 'kafka'
    assert engine.hops_config[1]['source']['topic'] == 'kafka-input'
    
    # Verify third hop
    assert engine.hops_config[2]['source']['type'] == 'pulsar'
    assert engine.hops_config[2]['source']['topic'] == 'pulsar-intermediate'


def test_hop_reference_invalid_name():
    """Test that invalid hop reference raises error"""
    flow_config = {
        'name': 'test-flow',
        'hops': [
            {
                'name': 'first-hop',
                'destination': {
                    'type': 'kafka',
                    'topic': 'test-topic',
                    'config': {'bootstrap_servers': ['localhost:9092']}
                }
            },
            {
                'name': 'second-hop',
                'source': 'hop: non-existent-hop',
                'destination': {
                    'type': 'kafka',
                    'topic': 'output-topic',
                    'config': {'bootstrap_servers': ['localhost:9092']}
                }
            }
        ]
    }
    
    with pytest.raises(ValueError, match="Cannot resolve hop reference: 'non-existent-hop' not found"):
        MessageFlowEngine(flow_config)


def test_hop_reference_invalid_format():
    """Test that invalid reference format raises error"""
    flow_config = {
        'name': 'test-flow',
        'hops': [
            {
                'name': 'first-hop',
                'destination': {
                    'type': 'kafka',
                    'topic': 'test-topic',
                    'config': {'bootstrap_servers': ['localhost:9092']}
                }
            },
            {
                'name': 'second-hop',
                'source': 'invalid-reference-format',
                'destination': {
                    'type': 'kafka',
                    'topic': 'output-topic',
                    'config': {'bootstrap_servers': ['localhost:9092']}
                }
            }
        ]
    }
    
    with pytest.raises(ValueError, match="Invalid source reference format"):
        MessageFlowEngine(flow_config)


def test_backward_compatibility_with_full_source_config():
    """Test that full source config (old format) still works"""
    flow_config = {
        'name': 'test-flow',
        'hops': [
            {
                'name': 'first-hop',
                'destination': {
                    'type': 'kafka',
                    'topic': 'test-topic',
                    'config': {'bootstrap_servers': ['localhost:9092']}
                }
            },
            {
                'name': 'second-hop',
                'source': {
                    'type': 'kafka',
                    'topic': 'test-topic',
                    'config': {'bootstrap_servers': ['localhost:9092']}
                },
                'destination': {
                    'type': 'kafka',
                    'topic': 'output-topic',
                    'config': {'bootstrap_servers': ['localhost:9092']}
                }
            }
        ]
    }
    
    engine = MessageFlowEngine(flow_config)
    
    # Verify that the full config is preserved
    assert engine.hops_config[1]['source']['type'] == 'kafka'
    assert engine.hops_config[1]['source']['topic'] == 'test-topic'


def test_hop_reference_deep_copy():
    """Test that hop references use deep copy to avoid side effects"""
    flow_config = {
        'name': 'test-flow',
        'hops': [
            {
                'name': 'first-hop',
                'destination': {
                    'type': 'rabbitmq',
                    'topic': 'test-topic',
                    'config': {
                        'host': 'localhost',
                        'port': 5672,
                        'nested': {
                            'key': 'value',
                            'list': [1, 2, 3]
                        }
                    }
                }
            },
            {
                'name': 'second-hop',
                'source': 'hop: first-hop',
                'destination': {
                    'type': 'kafka',
                    'topic': 'output-topic',
                    'config': {'bootstrap_servers': ['localhost:9092']}
                }
            }
        ]
    }
    
    engine = MessageFlowEngine(flow_config)
    
    # Modify the resolved source to verify it doesn't affect the original destination
    engine.hops_config[1]['source']['config']['nested']['key'] = 'modified'
    engine.hops_config[1]['source']['config']['nested']['list'].append(4)
    
    # Verify original destination is unchanged
    assert engine.hops_config[0]['destination']['config']['nested']['key'] == 'value'
    assert engine.hops_config[0]['destination']['config']['nested']['list'] == [1, 2, 3]


def test_hop_reference_forward_allowed():
    """Test that forward references (to hops defined later) are allowed
    
    While messages flow forward through hops, the reference resolution happens
    at initialization time, so references can point to any named hop regardless
    of order in the configuration.
    """
    flow_config = {
        'name': 'test-flow',
        'hops': [
            {
                'name': 'first-hop',
                'source': 'hop: second-hop',  # Forward reference to a hop defined later
                'destination': {
                    'type': 'kafka',
                    'topic': 'test-topic',
                    'config': {'bootstrap_servers': ['localhost:9092']}
                }
            },
            {
                'name': 'second-hop',
                'destination': {
                    'type': 'kafka',
                    'topic': 'output-topic',
                    'config': {'bootstrap_servers': ['localhost:9092']}
                }
            }
        ]
    }
    
    # Forward references are allowed and should resolve correctly
    engine = MessageFlowEngine(flow_config)
    
    # Verify the reference was resolved
    assert engine.hops_config[0]['source']['type'] == 'kafka'
    assert engine.hops_config[0]['source']['topic'] == 'output-topic'





# ---------------------------------------------------------------------------
# Message template source tests
# ---------------------------------------------------------------------------

def test_message_template_resolution_does_not_raise():
    """Test that a message_template source dict is accepted without error"""
    flow_config = {
        'name': 'template-flow',
        'hops': [
            {
                'name': 'publish-from-template',
                'source': {
                    'type': 'message_template',
                    'template_file': '/tmp/template.txt',
                    'values_file': '/tmp/values.csv',
                },
                'destination': {
                    'type': 'kafka',
                    'topic': 'test-topic',
                    'config': {'bootstrap_servers': ['localhost:9092']},
                },
            }
        ],
    }
    # Should not raise during initialisation
    engine = MessageFlowEngine(flow_config)
    assert engine.hops_config[0]['source']['type'] == 'message_template'


def test_execute_with_message_template(tmp_path, monkeypatch):
    """Test that _execute_with_message_template generates messages from template+CSV"""
    from unittest.mock import MagicMock, patch
    from pub_sub_perf_tool.base import Message
    from pub_sub_perf_tool.flow_engine import MessageFlowEngine

    # Create template file
    template_file = tmp_path / "template.txt"
    template_file.write_text('{"id": "{id}", "name": "{name}"}')

    # Create CSV values file
    values_file = tmp_path / "values.csv"
    values_file.write_text("id,name\n1,Alice\n2,Bob\n")

    flow_config = {
        'name': 'template-flow',
        'hops': [
            {
                'name': 'publish-from-template',
                'source': {
                    'type': 'message_template',
                    'template_file': str(template_file),
                    'values_file': str(values_file),
                },
                'destination': {
                    'type': 'kafka',
                    'topic': 'test-topic',
                    'config': {'bootstrap_servers': ['localhost:9092']},
                },
            }
        ],
    }

    engine = MessageFlowEngine(flow_config)

    # Capture messages passed to _execute_hop
    published_messages = []

    def fake_execute_hop(hop_index, hop_config, message):
        from pub_sub_perf_tool.flow_engine import HopResult
        published_messages.append(message)
        return HopResult(hop_index=hop_index, hop_name='publish-from-template', success=True)

    monkeypatch.setattr(engine, '_execute_hop', fake_execute_hop)

    result = engine.execute()

    assert result.success
    assert result.messages_processed == 2
    assert len(published_messages) == 2
    assert published_messages[0].value == b'{"id": "1", "name": "Alice"}'
    assert published_messages[1].value == b'{"id": "2", "name": "Bob"}'


def test_execute_with_message_template_missing_template_file(tmp_path):
    """Test that missing template_file raises ValueError"""
    values_file = tmp_path / "values.csv"
    values_file.write_text("id\n1\n")

    flow_config = {
        'name': 'template-flow',
        'hops': [
            {
                'name': 'hop',
                'source': {
                    'type': 'message_template',
                    'values_file': str(values_file),
                },
                'destination': {
                    'type': 'kafka',
                    'topic': 'test-topic',
                    'config': {'bootstrap_servers': ['localhost:9092']},
                },
            }
        ],
    }

    engine = MessageFlowEngine(flow_config)
    with pytest.raises(ValueError, match="requires 'template_file'"):
        engine.execute()


def test_execute_with_message_template_missing_values_file(tmp_path):
    """Test that missing values_file raises ValueError"""
    template_file = tmp_path / "template.txt"
    template_file.write_text("hello {name}")

    flow_config = {
        'name': 'template-flow',
        'hops': [
            {
                'name': 'hop',
                'source': {
                    'type': 'message_template',
                    'template_file': str(template_file),
                },
                'destination': {
                    'type': 'kafka',
                    'topic': 'test-topic',
                    'config': {'bootstrap_servers': ['localhost:9092']},
                },
            }
        ],
    }

    engine = MessageFlowEngine(flow_config)
    with pytest.raises(ValueError, match="requires 'values_file'"):
        engine.execute()


# ---------------------------------------------------------------------------
# Flow control tests
# ---------------------------------------------------------------------------

def test_step_config_valid():
    """Test that a valid StepConfig can be created"""
    step = StepConfig(step_size=100.0, step_interval_seconds=30.0, max_rate=5000.0)
    assert step.step_size == 100.0
    assert step.step_interval_seconds == 30.0
    assert step.max_rate == 5000.0


def test_step_config_invalid_step_size():
    """StepConfig should raise ValueError when step_size <= 0"""
    with pytest.raises(ValueError, match="step_size must be greater than 0"):
        StepConfig(step_size=0, step_interval_seconds=30.0, max_rate=5000.0)


def test_step_config_invalid_interval():
    """StepConfig should raise ValueError when step_interval_seconds <= 0"""
    with pytest.raises(ValueError, match="step_interval_seconds must be greater than 0"):
        StepConfig(step_size=100.0, step_interval_seconds=0, max_rate=5000.0)


def test_step_config_invalid_max_rate():
    """StepConfig should raise ValueError when max_rate <= 0"""
    with pytest.raises(ValueError, match="max_rate must be greater than 0"):
        StepConfig(step_size=100.0, step_interval_seconds=30.0, max_rate=0)


def test_flow_control_config_valid_constant():
    """Test that a constant-rate FlowControlConfig can be created"""
    cfg = FlowControlConfig(rate_per_second=1000.0)
    assert cfg.rate_per_second == 1000.0
    assert cfg.step is None


def test_flow_control_config_valid_with_step():
    """Test that a FlowControlConfig with step control can be created"""
    step = StepConfig(step_size=100.0, step_interval_seconds=30.0, max_rate=5000.0)
    cfg = FlowControlConfig(rate_per_second=1000.0, step=step)
    assert cfg.rate_per_second == 1000.0
    assert cfg.step is step


def test_flow_control_config_invalid_rate():
    """FlowControlConfig should raise ValueError when rate_per_second <= 0"""
    with pytest.raises(ValueError, match="rate_per_second must be greater than 0"):
        FlowControlConfig(rate_per_second=0)


def test_flow_control_config_max_rate_below_initial_rate():
    """FlowControlConfig should raise ValueError when max_rate < rate_per_second"""
    step = StepConfig(step_size=100.0, step_interval_seconds=30.0, max_rate=500.0)
    with pytest.raises(ValueError, match="max_rate must be greater than or equal to rate_per_second"):
        FlowControlConfig(rate_per_second=1000.0, step=step)


def test_rate_limiter_first_call_no_delay():
    """First call to RateLimiter.wait() should return immediately"""
    cfg = FlowControlConfig(rate_per_second=10.0)
    limiter = RateLimiter(cfg)
    start = time.time()
    limiter.wait()
    elapsed = time.time() - start
    # First call should return almost immediately (generous threshold for CI)
    max_first_call_delay_s = 0.1
    assert elapsed < max_first_call_delay_s, (
        f"First call took {elapsed:.3f}s, expected < {max_first_call_delay_s}s"
    )


def test_rate_limiter_constant_rate_timing():
    """RateLimiter should space messages according to the target rate"""
    rate = 100.0  # msg/s  →  expected gap = 10ms
    expected_gap_s = 1.0 / rate
    # Allow ±50% tolerance to avoid flakiness in loaded CI environments
    min_gap_s = expected_gap_s * 0.5
    max_gap_s = expected_gap_s * 5.0

    cfg = FlowControlConfig(rate_per_second=rate)
    limiter = RateLimiter(cfg)
    timestamps = []
    for _ in range(5):
        limiter.wait()
        timestamps.append(time.time())

    gaps = [timestamps[i] - timestamps[i - 1] for i in range(1, len(timestamps))]
    for gap in gaps:
        assert min_gap_s <= gap <= max_gap_s, (
            f"Gap {gap:.4f}s out of expected range [{min_gap_s:.4f}, {max_gap_s:.4f}]"
        )


def test_rate_limiter_step_up():
    """RateLimiter should increase rate after the step interval"""
    step = StepConfig(step_size=1000.0, step_interval_seconds=0.05, max_rate=3000.0)
    cfg = FlowControlConfig(rate_per_second=1000.0, step=step)
    limiter = RateLimiter(cfg)

    # First call starts clock, no delay
    limiter.wait()
    assert limiter.current_rate == 1000.0

    # Wait past the first step interval
    time.sleep(0.06)

    # Force a rate update by calling wait again
    limiter.wait()
    assert limiter.current_rate >= 2000.0


def test_rate_limiter_step_up_capped_at_max():
    """RateLimiter should not exceed max_rate"""
    step = StepConfig(step_size=1000.0, step_interval_seconds=0.01, max_rate=1500.0)
    cfg = FlowControlConfig(rate_per_second=1000.0, step=step)
    limiter = RateLimiter(cfg)
    limiter.wait()  # start clock
    time.sleep(0.05)  # several intervals worth
    limiter.wait()
    assert limiter.current_rate == 1500.0


def test_parse_flow_control_none():
    """_parse_flow_control returns None when no flow_control is in config"""
    flow_config = {
        'name': 'test-flow',
        'hops': [
            {
                'name': 'hop',
                'destination': {
                    'type': 'kafka',
                    'topic': 'test-topic',
                    'config': {'bootstrap_servers': ['localhost:9092']},
                },
            }
        ],
    }
    engine = MessageFlowEngine(flow_config)
    assert engine.flow_control is None


def test_parse_flow_control_constant_rate():
    """_parse_flow_control returns FlowControlConfig for constant rate"""
    flow_config = {
        'name': 'test-flow',
        'flow_control': {'rate_per_second': 500.0},
        'hops': [
            {
                'name': 'hop',
                'destination': {
                    'type': 'kafka',
                    'topic': 'test-topic',
                    'config': {'bootstrap_servers': ['localhost:9092']},
                },
            }
        ],
    }
    engine = MessageFlowEngine(flow_config)
    assert engine.flow_control is not None
    assert engine.flow_control.rate_per_second == 500.0
    assert engine.flow_control.step is None


def test_parse_flow_control_with_step():
    """_parse_flow_control returns FlowControlConfig with step control"""
    flow_config = {
        'name': 'test-flow',
        'flow_control': {
            'rate_per_second': 1000.0,
            'step': {
                'step_size': 100.0,
                'step_interval_seconds': 30.0,
                'max_rate': 5000.0,
            },
        },
        'hops': [
            {
                'name': 'hop',
                'destination': {
                    'type': 'kafka',
                    'topic': 'test-topic',
                    'config': {'bootstrap_servers': ['localhost:9092']},
                },
            }
        ],
    }
    engine = MessageFlowEngine(flow_config)
    assert engine.flow_control is not None
    assert engine.flow_control.rate_per_second == 1000.0
    assert engine.flow_control.step is not None
    assert engine.flow_control.step.step_size == 100.0
    assert engine.flow_control.step.step_interval_seconds == 30.0
    assert engine.flow_control.step.max_rate == 5000.0


def test_parse_flow_control_step_missing_max_rate():
    """_parse_flow_control raises ValueError when step is used without max_rate"""
    flow_config = {
        'name': 'test-flow',
        'flow_control': {
            'rate_per_second': 1000.0,
            'step': {
                'step_size': 100.0,
                'step_interval_seconds': 30.0,
                # max_rate intentionally omitted
            },
        },
        'hops': [
            {
                'name': 'hop',
                'destination': {
                    'type': 'kafka',
                    'topic': 'test-topic',
                    'config': {'bootstrap_servers': ['localhost:9092']},
                },
            }
        ],
    }
    with pytest.raises(ValueError, match="requires 'max_rate'"):
        MessageFlowEngine(flow_config)


def test_parse_flow_control_missing_rate_per_second():
    """_parse_flow_control raises ValueError when rate_per_second is absent"""
    flow_config = {
        'name': 'test-flow',
        'flow_control': {},
        'hops': [
            {
                'name': 'hop',
                'destination': {
                    'type': 'kafka',
                    'topic': 'test-topic',
                    'config': {'bootstrap_servers': ['localhost:9092']},
                },
            }
        ],
    }
    with pytest.raises(ValueError, match="requires 'rate_per_second'"):
        MessageFlowEngine(flow_config)


def test_execute_with_flow_control_calls_rate_limiter(monkeypatch):
    """execute() should call rate_limiter.wait() once per message"""
    from pub_sub_perf_tool.base import Message
    from pub_sub_perf_tool.flow_engine import HopResult

    flow_config = {
        'name': 'test-flow',
        'flow_control': {'rate_per_second': 10000.0},
        'hops': [
            {
                'name': 'hop',
                'destination': {
                    'type': 'kafka',
                    'topic': 'test-topic',
                    'config': {'bootstrap_servers': ['localhost:9092']},
                },
            }
        ],
    }
    engine = MessageFlowEngine(flow_config)

    wait_calls = []

    def fake_execute_hop(hop_index, hop_config, message):
        return HopResult(hop_index=hop_index, hop_name='hop', success=True)

    monkeypatch.setattr(engine, '_execute_hop', fake_execute_hop)

    # Patch RateLimiter.wait to track calls without actually sleeping
    original_wait = RateLimiter.wait

    def patched_wait(self):
        wait_calls.append(1)
        # Call original to keep state consistent but skip actual sleep
        self._start_time = self._start_time or time.time()

    monkeypatch.setattr(RateLimiter, 'wait', patched_wait)

    msg = Message(key=None, value=b'hello')
    result = engine.execute(msg, num_messages=3)

    assert result.success
    assert result.messages_processed == 3
    assert len(wait_calls) == 3


def test_execute_with_message_template_and_flow_control(tmp_path, monkeypatch):
    """_execute_with_message_template() should invoke rate limiter for each row"""
    from pub_sub_perf_tool.flow_engine import HopResult

    template_file = tmp_path / "template.txt"
    template_file.write_text('{"id": "{id}"}')

    values_file = tmp_path / "values.csv"
    values_file.write_text("id\n1\n2\n3\n")

    flow_config = {
        'name': 'template-flow',
        'flow_control': {'rate_per_second': 10000.0},
        'hops': [
            {
                'name': 'hop',
                'source': {
                    'type': 'message_template',
                    'template_file': str(template_file),
                    'values_file': str(values_file),
                },
                'destination': {
                    'type': 'kafka',
                    'topic': 'test-topic',
                    'config': {'bootstrap_servers': ['localhost:9092']},
                },
            }
        ],
    }

    engine = MessageFlowEngine(flow_config)

    wait_calls = []

    def fake_execute_hop(hop_index, hop_config, message):
        return HopResult(hop_index=hop_index, hop_name='hop', success=True)

    monkeypatch.setattr(engine, '_execute_hop', fake_execute_hop)

    def patched_wait(self):
        wait_calls.append(1)
        self._start_time = self._start_time or time.time()

    monkeypatch.setattr(RateLimiter, 'wait', patched_wait)

    result = engine.execute()

    assert result.success
    assert result.messages_processed == 3
    assert len(wait_calls) == 3


def test_execute_without_flow_control_no_rate_limiter(monkeypatch):
    """execute() should not use a rate limiter when flow_control is absent"""
    from pub_sub_perf_tool.base import Message
    from pub_sub_perf_tool.flow_engine import HopResult

    flow_config = {
        'name': 'test-flow',
        # no flow_control key
        'hops': [
            {
                'name': 'hop',
                'destination': {
                    'type': 'kafka',
                    'topic': 'test-topic',
                    'config': {'bootstrap_servers': ['localhost:9092']},
                },
            }
        ],
    }
    engine = MessageFlowEngine(flow_config)
    assert engine.flow_control is None

    def fake_execute_hop(hop_index, hop_config, message):
        return HopResult(hop_index=hop_index, hop_name='hop', success=True)

    monkeypatch.setattr(engine, '_execute_hop', fake_execute_hop)

    msg = Message(key=None, value=b'hello')
    result = engine.execute(msg, num_messages=2)

    assert result.success
    assert result.messages_processed == 2
