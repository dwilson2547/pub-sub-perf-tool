"""Unit tests for the timeline module"""
import base64
import json
import os
import tempfile
import time
from unittest.mock import MagicMock, patch

import pytest

from pub_sub_perf_tool.base import Message
from pub_sub_perf_tool.timeline.timeline import Timeline, TimelineEntry, TimelineWriter
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


# ---------------------------------------------------------------------------
# Timeline compressed save / load
# ---------------------------------------------------------------------------

class TestTimelineCompressed:
    def test_save_and_load_compressed_round_trip(self, tmp_path):
        tl = Timeline(
            source_type='kafka',
            source_topic='test-topic',
            captured_at='2024-01-01T00:00:00+00:00',
            entries=[
                TimelineEntry(offset_ms=0.0, value=b'msg1', key='k1'),
                TimelineEntry(offset_ms=500.0, value=b'\x00\xff\xfe', key=None,
                              headers={'x': 'y'}),
            ],
        )
        path = str(tmp_path / 'tl.timeline')
        tl.save_compressed(path)

        loaded = Timeline.load_compressed(path)
        assert loaded.version == '2.0'
        assert loaded.source_type == 'kafka'
        assert loaded.source_topic == 'test-topic'
        assert loaded.captured_at == tl.captured_at
        assert len(loaded.entries) == 2
        assert loaded.entries[0].value == b'msg1'
        assert loaded.entries[0].key == 'k1'
        assert loaded.entries[1].value == b'\x00\xff\xfe'
        assert loaded.entries[1].headers == {'x': 'y'}

    def test_compressed_file_is_smaller_than_json(self, tmp_path):
        """Compressed binary format should be smaller than JSON for binary payloads."""
        entries = [
            TimelineEntry(offset_ms=float(i), value=bytes(range(256)) * 4, key=str(i))
            for i in range(100)
        ]
        tl = Timeline(
            source_type='kafka',
            source_topic='bench',
            captured_at='2024-01-01T00:00:00+00:00',
            entries=entries,
        )
        json_path = str(tmp_path / 'tl.json')
        compressed_path = str(tmp_path / 'tl.timeline')
        tl.save(json_path)
        tl.save_compressed(compressed_path)

        assert os.path.getsize(compressed_path) < os.path.getsize(json_path)

    def test_save_compressed_empty_timeline(self, tmp_path):
        tl = Timeline(
            source_type='rabbitmq',
            source_topic='q',
            captured_at='2024-01-01T00:00:00+00:00',
        )
        path = str(tmp_path / 'empty.timeline')
        tl.save_compressed(path)
        loaded = Timeline.load_compressed(path)
        assert loaded.entries == []


# ---------------------------------------------------------------------------
# TimelineWriter
# ---------------------------------------------------------------------------

class TestTimelineWriter:
    def test_single_chunk_writes_one_file(self, tmp_path):
        writer = TimelineWriter(
            str(tmp_path / 'capture.timeline'), chunk_size=0, worker_id=0
        )
        writer.open('kafka', 'topic', '2024-01-01T00:00:00+00:00')
        for i in range(5):
            writer.write_entry(TimelineEntry(offset_ms=float(i), value=f'v{i}'.encode()))
        paths = writer.close()
        assert len(paths) == 1
        assert paths[0].endswith('_w0_p0.timeline')

    def test_chunked_write_splits_into_parts(self, tmp_path):
        writer = TimelineWriter(
            str(tmp_path / 'capture'), chunk_size=3, worker_id=1
        )
        writer.open('kafka', 'topic', '2024-01-01T00:00:00+00:00')
        for i in range(7):
            writer.write_entry(TimelineEntry(offset_ms=float(i), value=b'x'))
        paths = writer.close()
        # 7 entries / chunk_size=3 → parts 0,1,2 (3+3+1)
        assert len(paths) == 3
        assert all(p.endswith('.timeline') for p in paths)
        assert '_w1_p0.timeline' in paths[0]
        assert '_w1_p1.timeline' in paths[1]
        assert '_w1_p2.timeline' in paths[2]

    def test_writer_files_are_readable_by_load_compressed(self, tmp_path):
        writer = TimelineWriter(str(tmp_path / 'cap'), chunk_size=0, worker_id=0)
        writer.open('pulsar', 'persistent://public/default/t', '2024-06-01T00:00:00+00:00')
        writer.write_entry(TimelineEntry(offset_ms=0.0, value=b'hello', key='k'))
        writer.write_entry(TimelineEntry(offset_ms=10.0, value=b'world'))
        paths = writer.close()

        loaded = Timeline.load_compressed(paths[0])
        assert loaded.source_type == 'pulsar'
        assert len(loaded.entries) == 2
        assert loaded.entries[0].value == b'hello'
        assert loaded.entries[0].key == 'k'
        assert loaded.entries[1].value == b'world'

    def test_writer_worker_id_in_filename(self, tmp_path):
        writer = TimelineWriter(str(tmp_path / 'cap'), chunk_size=0, worker_id=7)
        writer.open('kafka', 't', '2024-01-01T00:00:00+00:00')
        writer.write_entry(TimelineEntry(offset_ms=0.0, value=b'x'))
        paths = writer.close()
        assert '_w7_p0' in paths[0]

    def test_close_on_empty_writer_returns_empty_list(self, tmp_path):
        writer = TimelineWriter(str(tmp_path / 'cap'), chunk_size=0, worker_id=0)
        writer.open('kafka', 't', '2024-01-01T00:00:00+00:00')
        paths = writer.close()
        assert paths == []


# ---------------------------------------------------------------------------
# TimelineCapture – multi-worker config
# ---------------------------------------------------------------------------

class TestTimelineCaptureMultiWorker:
    def test_kafka_multi_worker_shares_consumer_group(self):
        cap = TimelineCapture('kafka', 'topic', {}, num_workers=4)
        assert cap.config['consumer_group'].startswith('timeline-capture-')
        # All workers use the same group (set once in __init__)

    def test_pulsar_single_worker_uses_reader_mode(self):
        cap = TimelineCapture('pulsar', 'topic', {}, num_workers=1)
        assert cap.config.get('use_reader') is True

    def test_pulsar_multi_worker_uses_shared_subscription(self):
        cap = TimelineCapture('pulsar', 'topic', {}, num_workers=4)
        assert 'use_reader' not in cap.config
        assert cap.config.get('subscription_name', '').startswith('timeline-capture-')
        assert cap.config['consumer_config']['subscription_type'] == 'Shared'

    def test_streamnative_multi_worker_uses_shared_subscription(self):
        cap = TimelineCapture('streamnative', 'topic', {}, num_workers=2)
        assert 'use_reader' not in cap.config
        assert cap.config.get('subscription_name', '').startswith('timeline-capture-')

    def test_num_workers_defaults_to_one(self):
        cap = TimelineCapture('kafka', 'topic', {})
        assert cap.num_workers == 1

    def test_capture_to_files_writes_compressed_parts(self, tmp_path):
        """capture_to_files should create worker-specific compressed files."""
        from pub_sub_perf_tool.base import ConsumeResult

        def _make_result(value):
            msg = Message(key=None, value=value, headers=None)
            return ConsumeResult(message=msg, latency_ms=0.5)

        def _empty():
            return ConsumeResult(message=None, latency_ms=0.5)

        with patch('pub_sub_perf_tool.timeline.capture.create_client') as mock_cc:
            def make_mock_client():
                mc = MagicMock()
                mc.__enter__ = MagicMock(return_value=mc)
                mc.__exit__ = MagicMock(return_value=False)
                mc.consume.side_effect = [
                    _make_result(b'a'), _make_result(b'b'), _empty()
                ]
                return mc
            mock_cc.side_effect = lambda *a, **kw: make_mock_client()

            cap = TimelineCapture('kafka', 'my-topic', {}, num_workers=2)
            paths = cap.capture_to_files(
                output_base=str(tmp_path / 'cap'),
                max_messages=4,
                timeout_ms=100,
                chunk_size=0,
            )

        assert len(paths) == 2
        assert all(p.endswith('.timeline') for p in paths)
        # Each file should be a valid compressed timeline
        for p in paths:
            tl = Timeline.load_compressed(p)
            assert tl.source_type == 'kafka'
            assert tl.source_topic == 'my-topic'

    def test_capture_to_files_progress_callback(self, tmp_path):
        """on_progress should be called with running total."""
        from pub_sub_perf_tool.base import ConsumeResult

        def _make_result(value):
            msg = Message(key=None, value=value, headers=None)
            return ConsumeResult(message=msg, latency_ms=0.5)

        def _empty():
            return ConsumeResult(message=None, latency_ms=0.5)

        with patch('pub_sub_perf_tool.timeline.capture.create_client') as mock_cc:
            mc = MagicMock()
            mc.__enter__ = MagicMock(return_value=mc)
            mc.__exit__ = MagicMock(return_value=False)
            mc.consume.side_effect = [_make_result(b'x'), _empty()]
            mock_cc.return_value = mc

            counts = []
            cap = TimelineCapture('kafka', 't', {}, num_workers=1)
            cap.capture_to_files(
                output_base=str(tmp_path / 'cap'),
                max_messages=10,
                timeout_ms=100,
                chunk_size=0,
                on_progress=lambda c: counts.append(c),
            )

        assert counts == [1]


# ---------------------------------------------------------------------------
# Flow engine – compressed .timeline format and base-path glob
# ---------------------------------------------------------------------------

class TestFlowEngineCompressedTimeline:
    """Verify that the flow engine replays compressed timeline files."""

    def _mock_client(self):
        from pub_sub_perf_tool.base import PublishResult
        mc = MagicMock()
        mc.__enter__ = MagicMock(return_value=mc)
        mc.__exit__ = MagicMock(return_value=False)
        mc.publish.return_value = PublishResult(success=True, latency_ms=1.0)
        return mc

    def _flow_config(self, source: str) -> dict:
        return {
            'name': 'test-flow',
            'hops': [
                {
                    'name': 'replay',
                    'source': source,
                    'destination': {
                        'type': 'kafka',
                        'topic': 'out-topic',
                        'config': {'bootstrap_servers': ['localhost:9092']},
                    },
                }
            ],
        }

    def test_replay_compressed_single_file(self, tmp_path):
        """engine.execute() should replay a single .timeline compressed file."""
        writer = TimelineWriter(str(tmp_path / 'cap'), chunk_size=0, worker_id=0)
        writer.open('kafka', 'src', '2024-01-01T00:00:00+00:00')
        writer.write_entry(TimelineEntry(offset_ms=0.0, value=b'a'))
        writer.write_entry(TimelineEntry(offset_ms=5.0, value=b'b'))
        paths = writer.close()

        engine = MessageFlowEngine(self._flow_config(f'timeline: {paths[0]}'))
        with patch('pub_sub_perf_tool.flow_engine.create_client', return_value=self._mock_client()):
            result = engine.execute()

        assert result.success
        assert result.messages_processed == 2

    def test_replay_base_path_discovers_part_files(self, tmp_path):
        """A base path without extension should glob for _w*_p*.timeline files."""
        base = str(tmp_path / 'cap')
        writer = TimelineWriter(base, chunk_size=0, worker_id=0)
        writer.open('kafka', 'src', '2024-01-01T00:00:00+00:00')
        for i in range(3):
            writer.write_entry(TimelineEntry(offset_ms=float(i * 10), value=f'v{i}'.encode()))
        writer.close()

        engine = MessageFlowEngine(self._flow_config(f'timeline: {base}'))
        with patch('pub_sub_perf_tool.flow_engine.create_client', return_value=self._mock_client()):
            result = engine.execute()

        assert result.success
        assert result.messages_processed == 3

    def test_replay_base_path_multiple_workers(self, tmp_path):
        """All workers' entries should be replayed when num_pods == 1."""
        base = str(tmp_path / 'cap')
        for wid in range(3):
            w = TimelineWriter(base, chunk_size=0, worker_id=wid)
            w.open('kafka', 'src', '2024-01-01T00:00:00+00:00')
            w.write_entry(TimelineEntry(offset_ms=0.0, value=f'w{wid}'.encode()))
            w.write_entry(TimelineEntry(offset_ms=10.0, value=f'w{wid}-2'.encode()))
            w.close()

        engine = MessageFlowEngine(self._flow_config(f'timeline: {base}'))
        with patch('pub_sub_perf_tool.flow_engine.create_client', return_value=self._mock_client()):
            result = engine.execute()

        assert result.success
        assert result.messages_processed == 6  # 3 workers × 2 entries each

    def test_base_path_no_files_raises(self, tmp_path):
        """ValueError should be raised when no part files are found."""
        from pub_sub_perf_tool.flow_engine import MessageFlowEngine
        engine = MessageFlowEngine(self._flow_config(f'timeline: {tmp_path / "missing"}'))
        with patch('pub_sub_perf_tool.flow_engine.create_client', return_value=self._mock_client()):
            result = engine.execute()
        assert not result.success


# ---------------------------------------------------------------------------
# Flow engine – multi-pod timeline distribution
# ---------------------------------------------------------------------------

class TestFlowEngineMultiPod:
    """Verify that pod_id / num_pods correctly partitions worker files."""

    def _mock_client(self):
        from pub_sub_perf_tool.base import PublishResult
        mc = MagicMock()
        mc.__enter__ = MagicMock(return_value=mc)
        mc.__exit__ = MagicMock(return_value=False)
        mc.publish.return_value = PublishResult(success=True, latency_ms=1.0)
        return mc

    def _make_capture(self, tmp_path, num_workers=4, entries_per_worker=2):
        base = str(tmp_path / 'cap')
        for wid in range(num_workers):
            w = TimelineWriter(base, chunk_size=0, worker_id=wid)
            w.open('kafka', 'src', '2024-01-01T00:00:00+00:00')
            for i in range(entries_per_worker):
                w.write_entry(TimelineEntry(offset_ms=float(i * 10), value=f'w{wid}m{i}'.encode()))
            w.close()
        return base

    def test_two_pods_process_disjoint_subsets(self, tmp_path):
        """Two pods with num_pods=2 together process all entries exactly once."""
        base = self._make_capture(tmp_path, num_workers=4, entries_per_worker=2)

        counts = []
        for pod_id in range(2):
            cfg = {
                'name': 'flow',
                'hops': [{
                    'name': 'replay',
                    'source': f'timeline: {base}',
                    'pod_id': pod_id,
                    'num_pods': 2,
                    'destination': {
                        'type': 'kafka',
                        'topic': 'out',
                        'config': {'bootstrap_servers': ['localhost:9092']},
                    },
                }],
            }
            engine = MessageFlowEngine(cfg)
            with patch('pub_sub_perf_tool.flow_engine.create_client', return_value=self._mock_client()):
                result = engine.execute()
            assert result.success
            counts.append(result.messages_processed)

        # Each pod gets 2 workers × 2 entries = 4; total = 8
        assert sum(counts) == 8
        # Both pods get equal share
        assert counts[0] == counts[1]

    def test_pod_id_from_flow_config(self, tmp_path):
        """pod_id / num_pods set at the flow level (not hop level) are respected."""
        base = self._make_capture(tmp_path, num_workers=4, entries_per_worker=1)

        cfg = {
            'name': 'flow',
            'pod_id': 1,
            'num_pods': 4,
            'hops': [{
                'name': 'replay',
                'source': f'timeline: {base}',
                'destination': {
                    'type': 'kafka',
                    'topic': 'out',
                    'config': {'bootstrap_servers': ['localhost:9092']},
                },
            }],
        }
        engine = MessageFlowEngine(cfg)
        with patch('pub_sub_perf_tool.flow_engine.create_client', return_value=self._mock_client()):
            result = engine.execute()

        # pod_id=1 with num_pods=4 and 4 workers → gets worker index 1 only → 1 entry
        assert result.success
        assert result.messages_processed == 1

    def test_hop_pod_config_overrides_flow_pod_config(self, tmp_path):
        """pod_id / num_pods on the hop take precedence over the flow-level values."""
        base = self._make_capture(tmp_path, num_workers=4, entries_per_worker=1)

        cfg = {
            'name': 'flow',
            'pod_id': 99,   # should be ignored
            'num_pods': 99,  # should be ignored
            'hops': [{
                'name': 'replay',
                'source': f'timeline: {base}',
                'pod_id': 0,
                'num_pods': 2,
                'destination': {
                    'type': 'kafka',
                    'topic': 'out',
                    'config': {'bootstrap_servers': ['localhost:9092']},
                },
            }],
        }
        engine = MessageFlowEngine(cfg)
        with patch('pub_sub_perf_tool.flow_engine.create_client', return_value=self._mock_client()):
            result = engine.execute()

        # pod_id=0, num_pods=2, 4 workers → workers 0,2 → 2 entries
        assert result.success
        assert result.messages_processed == 2

    def test_single_pod_default_processes_all(self, tmp_path):
        """Default (pod_id=0, num_pods=1) processes all workers."""
        base = self._make_capture(tmp_path, num_workers=3, entries_per_worker=2)

        cfg = {
            'name': 'flow',
            'hops': [{
                'name': 'replay',
                'source': f'timeline: {base}',
                'destination': {
                    'type': 'kafka',
                    'topic': 'out',
                    'config': {'bootstrap_servers': ['localhost:9092']},
                },
            }],
        }
        engine = MessageFlowEngine(cfg)
        with patch('pub_sub_perf_tool.flow_engine.create_client', return_value=self._mock_client()):
            result = engine.execute()

        assert result.success
        assert result.messages_processed == 6  # 3 workers × 2 entries
