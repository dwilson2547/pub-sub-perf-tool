"""Unit tests for the timeline module"""
import base64
import json
import os
import tempfile
import time
from unittest.mock import MagicMock, patch

import pytest

from pub_sub_perf_tool.base import Message
from pub_sub_perf_tool.timeline.timeline import Timeline, TimelineEntry
from pub_sub_perf_tool.timeline.capture import TimelineCapture
from pub_sub_perf_tool.flow_engine import MessageFlowEngine


# ---------------------------------------------------------------------------
# TimelineEntry
# ---------------------------------------------------------------------------

class TestTimelineEntry:
    def test_to_dict_encodes_value_as_base64(self):
        entry = TimelineEntry(offset_ms=0.0, value=b'hello', key='k', headers={'h': 'v'})
        d = entry.to_dict()
        assert d['offset_ms'] == 0.0
        assert d['key'] == 'k'
        assert d['headers'] == {'h': 'v'}
        assert base64.b64decode(d['value']) == b'hello'

    def test_from_dict_round_trip(self):
        original = TimelineEntry(offset_ms=123.4, value=b'\x00\x01\x02', key=None, headers=None)
        restored = TimelineEntry.from_dict(original.to_dict())
        assert restored.offset_ms == original.offset_ms
        assert restored.value == original.value
        assert restored.key == original.key
        assert restored.headers == original.headers

    def test_from_dict_with_all_fields(self):
        d = {
            'offset_ms': 500.0,
            'key': 'my-key',
            'value': base64.b64encode(b'payload').decode(),
            'headers': {'x-source': 'test'},
        }
        entry = TimelineEntry.from_dict(d)
        assert entry.offset_ms == 500.0
        assert entry.key == 'my-key'
        assert entry.value == b'payload'
        assert entry.headers == {'x-source': 'test'}

    def test_from_dict_optional_fields_default_to_none(self):
        d = {
            'offset_ms': 0.0,
            'value': base64.b64encode(b'x').decode(),
        }
        entry = TimelineEntry.from_dict(d)
        assert entry.key is None
        assert entry.headers is None


# ---------------------------------------------------------------------------
# Timeline save / load
# ---------------------------------------------------------------------------

class TestTimelineSaveLoad:
    def test_save_and_load_round_trip(self, tmp_path):
        tl = Timeline(
            source_type='kafka',
            source_topic='test-topic',
            captured_at='2024-01-01T00:00:00+00:00',
            entries=[
                TimelineEntry(offset_ms=0.0, value=b'msg1', key='k1'),
                TimelineEntry(offset_ms=1000.0, value=b'msg2', key=None, headers={'h': '1'}),
            ],
        )
        path = str(tmp_path / 'timeline.json')
        tl.save(path)

        loaded = Timeline.load(path)
        assert loaded.version == tl.version
        assert loaded.source_type == 'kafka'
        assert loaded.source_topic == 'test-topic'
        assert loaded.captured_at == tl.captured_at
        assert len(loaded.entries) == 2
        assert loaded.entries[0].value == b'msg1'
        assert loaded.entries[0].key == 'k1'
        assert loaded.entries[1].offset_ms == 1000.0
        assert loaded.entries[1].headers == {'h': '1'}

    def test_save_writes_valid_json(self, tmp_path):
        tl = Timeline(
            source_type='pulsar',
            source_topic='persistent://public/default/t',
            captured_at='2024-06-01T12:00:00+00:00',
            entries=[TimelineEntry(offset_ms=0.0, value=b'data')],
        )
        path = str(tmp_path / 'tl.json')
        tl.save(path)
        with open(path) as f:
            data = json.load(f)
        assert data['source_type'] == 'pulsar'
        assert data['entry_count'] == 1
        assert 'entries' in data

    def test_save_empty_timeline(self, tmp_path):
        tl = Timeline(
            source_type='rabbitmq',
            source_topic='my-queue',
            captured_at='2024-01-01T00:00:00+00:00',
        )
        path = str(tmp_path / 'empty.json')
        tl.save(path)
        loaded = Timeline.load(path)
        assert loaded.entries == []

    def test_load_sets_default_version(self, tmp_path):
        data = {
            'source_type': 'kafka',
            'source_topic': 'topic',
            'captured_at': '2024-01-01T00:00:00+00:00',
            'entries': [],
        }
        path = str(tmp_path / 'no_version.json')
        with open(path, 'w') as f:
            json.dump(data, f)
        loaded = Timeline.load(path)
        assert loaded.version == '1.0'


# ---------------------------------------------------------------------------
# TimelineCapture – consumer-group / reader-mode config
# ---------------------------------------------------------------------------

class TestTimelineCaptureSafeDefaults:
    def test_kafka_gets_random_consumer_group(self):
        cap1 = TimelineCapture('kafka', 'topic', {})
        cap2 = TimelineCapture('kafka', 'topic', {})
        assert cap1.config['consumer_group'].startswith('timeline-capture-')
        assert cap2.config['consumer_group'].startswith('timeline-capture-')
        assert cap1.config['consumer_group'] != cap2.config['consumer_group']

    def test_kafka_respects_explicit_consumer_group(self):
        cap = TimelineCapture('kafka', 'topic', {'consumer_group': 'my-group'})
        assert cap.config['consumer_group'] == 'my-group'

    def test_pulsar_uses_reader_mode(self):
        cap = TimelineCapture('pulsar', 'topic', {})
        assert cap.config['use_reader'] is True

    def test_streamnative_uses_reader_mode(self):
        cap = TimelineCapture('streamnative', 'topic', {})
        assert cap.config['use_reader'] is True

    def test_capture_does_not_mutate_caller_config(self):
        original = {'bootstrap_servers': ['localhost:9092']}
        TimelineCapture('kafka', 'topic', original)
        assert 'consumer_group' not in original

    def test_rabbitmq_config_unchanged(self):
        cap = TimelineCapture('rabbitmq', 'queue', {'host': 'localhost'})
        assert 'consumer_group' not in cap.config
        assert 'use_reader' not in cap.config


# ---------------------------------------------------------------------------
# TimelineCapture.capture – with mocked client
# ---------------------------------------------------------------------------

class TestTimelineCaptureMessages:
    def _make_message(self, value: bytes, key=None, headers=None):
        from pub_sub_perf_tool.base import ConsumeResult
        msg = Message(key=key, value=value, headers=headers)
        return ConsumeResult(message=msg, latency_ms=1.0)

    def _make_empty_result(self):
        from pub_sub_perf_tool.base import ConsumeResult
        return ConsumeResult(message=None, latency_ms=1.0)

    @patch('pub_sub_perf_tool.timeline.capture.create_client')
    def test_capture_records_messages(self, mock_create_client):
        mock_client = MagicMock()
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client.consume.side_effect = [
            self._make_message(b'msg1', key='k1'),
            self._make_message(b'msg2'),
            self._make_empty_result(),  # timeout
        ]
        mock_create_client.return_value = mock_client

        cap = TimelineCapture('kafka', 'my-topic', {})
        tl = cap.capture(max_messages=10, timeout_ms=100)

        assert len(tl.entries) == 2
        assert tl.entries[0].value == b'msg1'
        assert tl.entries[0].key == 'k1'
        assert tl.entries[1].value == b'msg2'
        assert tl.source_type == 'kafka'
        assert tl.source_topic == 'my-topic'

    @patch('pub_sub_perf_tool.timeline.capture.create_client')
    def test_capture_offsets_are_increasing(self, mock_create_client):
        mock_client = MagicMock()
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client.consume.side_effect = [
            self._make_message(b'a'),
            self._make_message(b'b'),
            self._make_message(b'c'),
            self._make_empty_result(),
        ]
        mock_create_client.return_value = mock_client

        cap = TimelineCapture('kafka', 'topic', {})
        tl = cap.capture(max_messages=100, timeout_ms=100)

        offsets = [e.offset_ms for e in tl.entries]
        assert offsets == sorted(offsets)

    @patch('pub_sub_perf_tool.timeline.capture.create_client')
    def test_capture_progress_callback_called(self, mock_create_client):
        mock_client = MagicMock()
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client.consume.side_effect = [
            self._make_message(b'x'),
            self._make_empty_result(),
        ]
        mock_create_client.return_value = mock_client

        calls = []
        cap = TimelineCapture('kafka', 'topic', {})
        cap.capture(max_messages=10, timeout_ms=100, on_message=lambda c, t: calls.append((c, t)))

        assert calls == [(1, 10)]

    @patch('pub_sub_perf_tool.timeline.capture.create_client')
    def test_capture_stops_at_max_messages(self, mock_create_client):
        mock_client = MagicMock()
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        # Always return a message – should stop at max_messages
        mock_client.consume.return_value = self._make_message(b'msg')
        mock_create_client.return_value = mock_client

        cap = TimelineCapture('kafka', 'topic', {})
        tl = cap.capture(max_messages=3, timeout_ms=100)
        assert len(tl.entries) == 3


# ---------------------------------------------------------------------------
# Flow engine – timeline source support
# ---------------------------------------------------------------------------

class TestFlowEngineTimelineSource:
    def test_timeline_source_string_is_accepted(self):
        """Hop source 'timeline: path' must not raise during init."""
        flow_config = {
            'name': 'tl-flow',
            'hops': [
                {
                    'name': 'replay',
                    'source': 'timeline: /some/file.json',
                    'destination': {
                        'type': 'kafka',
                        'topic': 'out-topic',
                        'config': {'bootstrap_servers': ['localhost:9092']},
                    },
                }
            ],
        }
        engine = MessageFlowEngine(flow_config)
        assert engine.hops_config[0]['source'] == 'timeline: /some/file.json'

    def test_timeline_source_triggers_replay(self, tmp_path):
        """execute() with a timeline source publishes all entries."""
        # Build a small timeline file
        tl = Timeline(
            source_type='kafka',
            source_topic='src-topic',
            captured_at='2024-01-01T00:00:00+00:00',
            entries=[
                TimelineEntry(offset_ms=0.0, value=b'hello'),
                TimelineEntry(offset_ms=10.0, value=b'world'),
            ],
        )
        tl_path = str(tmp_path / 'tl.json')
        tl.save(tl_path)

        flow_config = {
            'name': 'replay-flow',
            'hops': [
                {
                    'name': 'replay',
                    'source': f'timeline: {tl_path}',
                    'destination': {
                        'type': 'kafka',
                        'topic': 'out-topic',
                        'config': {'bootstrap_servers': ['localhost:9092']},
                    },
                }
            ],
        }

        engine = MessageFlowEngine(flow_config)

        # Mock the Kafka client so no real broker is required
        mock_client = MagicMock()
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        from pub_sub_perf_tool.base import PublishResult
        mock_client.publish.return_value = PublishResult(success=True, latency_ms=1.0)

        with patch('pub_sub_perf_tool.flow_engine.create_client', return_value=mock_client):
            result = engine.execute()

        assert result.success
        assert result.messages_processed == 2
        assert mock_client.publish.call_count == 2

    def test_execute_without_message_raises_without_timeline(self):
        flow_config = {
            'name': 'no-msg-flow',
            'hops': [
                {
                    'name': 'publish',
                    'destination': {
                        'type': 'kafka',
                        'topic': 'out-topic',
                        'config': {'bootstrap_servers': ['localhost:9092']},
                    },
                }
            ],
        }
        engine = MessageFlowEngine(flow_config)
        with pytest.raises(ValueError, match="initial_message is required"):
            engine.execute()

    def test_timeline_replay_preserves_message_content(self, tmp_path):
        """Values and keys from the timeline reach the publisher unchanged."""
        tl = Timeline(
            source_type='kafka',
            source_topic='src',
            captured_at='2024-01-01T00:00:00+00:00',
            entries=[
                TimelineEntry(offset_ms=0.0, value=b'payload-A', key='key-A', headers={'h': '1'}),
            ],
        )
        tl_path = str(tmp_path / 'tl.json')
        tl.save(tl_path)

        flow_config = {
            'name': 'content-check',
            'hops': [
                {
                    'name': 'hop',
                    'source': f'timeline: {tl_path}',
                    'destination': {
                        'type': 'kafka',
                        'topic': 'out',
                        'config': {'bootstrap_servers': ['localhost:9092']},
                    },
                }
            ],
        }

        engine = MessageFlowEngine(flow_config)
        mock_client = MagicMock()
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        from pub_sub_perf_tool.base import PublishResult
        mock_client.publish.return_value = PublishResult(success=True, latency_ms=1.0)

        with patch('pub_sub_perf_tool.flow_engine.create_client', return_value=mock_client):
            result = engine.execute()

        assert result.success
        published_msg = mock_client.publish.call_args[0][1]
        assert published_msg.value == b'payload-A'
        assert published_msg.key == 'key-A'
        assert published_msg.headers == {'h': '1'}
