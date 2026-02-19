"""Timeline data model and file I/O"""
import json
import base64
import os
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import msgpack
import zstandard as zstd


@dataclass
class TimelineEntry:
    """A single captured message with a time offset from capture start"""
    offset_ms: float
    value: bytes
    key: Optional[str] = None
    headers: Optional[Dict[str, str]] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            'offset_ms': self.offset_ms,
            'key': self.key,
            'value': base64.b64encode(self.value).decode('utf-8'),
            'headers': self.headers,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'TimelineEntry':
        return cls(
            offset_ms=data['offset_ms'],
            key=data.get('key'),
            value=base64.b64decode(data['value']),
            headers=data.get('headers'),
        )


@dataclass
class Timeline:
    """A time-series collection of messages captured from a pub-sub topic"""
    source_type: str
    source_topic: str
    captured_at: str
    entries: List[TimelineEntry] = field(default_factory=list)
    version: str = '1.0'

    def save(self, path: str) -> None:
        """Save timeline to a JSON file"""
        data = {
            'version': self.version,
            'source_type': self.source_type,
            'source_topic': self.source_topic,
            'captured_at': self.captured_at,
            'entry_count': len(self.entries),
            'entries': [e.to_dict() for e in self.entries],
        }
        with open(path, 'w') as f:
            json.dump(data, f, indent=2)

    @classmethod
    def load(cls, path: str) -> 'Timeline':
        """Load timeline from a JSON file"""
        with open(path, 'r') as f:
            data = json.load(f)
        entries = [TimelineEntry.from_dict(e) for e in data.get('entries', [])]
        return cls(
            version=data.get('version', '1.0'),
            source_type=data['source_type'],
            source_topic=data['source_topic'],
            captured_at=data['captured_at'],
            entries=entries,
        )

    def save_compressed(self, path: str, compression_level: int = 3) -> None:
        """Save timeline to a compressed binary file (msgpack + zstd).

        The resulting file is significantly smaller and faster to write/read
        than the JSON format, making it suitable for high-throughput capture.
        """
        data = {
            'version': '2.0',
            'source_type': self.source_type,
            'source_topic': self.source_topic,
            'captured_at': self.captured_at,
            'entry_count': len(self.entries),
            'entries': [
                {
                    'offset_ms': e.offset_ms,
                    'key': e.key,
                    'value': e.value,
                    'headers': e.headers,
                }
                for e in self.entries
            ],
        }
        raw = msgpack.packb(data, use_bin_type=True)
        cctx = zstd.ZstdCompressor(level=compression_level)
        compressed = cctx.compress(raw)
        with open(path, 'wb') as f:
            f.write(compressed)

    @classmethod
    def load_compressed(cls, path: str) -> 'Timeline':
        """Load timeline from a compressed binary file (msgpack + zstd)."""
        with open(path, 'rb') as f:
            compressed = f.read()
        dctx = zstd.ZstdDecompressor()
        raw = dctx.decompress(compressed)
        data = msgpack.unpackb(raw, raw=False)
        entries = [
            TimelineEntry(
                offset_ms=e['offset_ms'],
                key=e.get('key'),
                value=e['value'] if isinstance(e['value'], bytes) else (e['value'].encode() if e['value'] is not None else b''),
                headers=e.get('headers'),
            )
            for e in data.get('entries', [])
        ]
        return cls(
            version=data.get('version', '2.0'),
            source_type=data['source_type'],
            source_topic=data['source_topic'],
            captured_at=data['captured_at'],
            entries=entries,
        )


class TimelineWriter:
    """Streaming compressed timeline writer with automatic file splitting.

    Writes msgpack + zstd compressed part files.  When *chunk_size* is
    non-zero each part file contains at most *chunk_size* entries; when it is
    zero all entries are flushed into a single file on :meth:`close`.

    Part files are named ``<base>_w<worker_id>_p<part_index>.timeline``.

    Usage::

        writer = TimelineWriter("capture", chunk_size=100_000, worker_id=0)
        writer.open("kafka", "my-topic", "2024-01-01T00:00:00+00:00")
        for entry in entries:
            writer.write_entry(entry)
        paths = writer.close()
    """

    def __init__(
        self,
        base_path: str,
        chunk_size: int = 100_000,
        worker_id: int = 0,
        compression_level: int = 3,
    ) -> None:
        # Strip .timeline extension from base_path so suffixes are appended cleanly
        if base_path.endswith('.timeline'):
            base_path = base_path[: -len('.timeline')]
        self._base = base_path
        self._chunk_size = chunk_size
        self._worker_id = worker_id
        self._compression_level = compression_level
        self._meta: Dict[str, str] = {}
        self._buffer: List[Dict[str, Any]] = []
        self._part = 0
        self._paths: List[str] = []

    def open(self, source_type: str, source_topic: str, captured_at: str) -> None:
        """Set capture metadata before writing entries."""
        self._meta = {
            'source_type': source_type,
            'source_topic': source_topic,
            'captured_at': captured_at,
        }

    def write_entry(self, entry: TimelineEntry) -> None:
        """Buffer a single entry, flushing to disk when the chunk limit is reached."""
        self._buffer.append({
            'offset_ms': entry.offset_ms,
            'key': entry.key,
            'value': entry.value,
            'headers': entry.headers,
        })
        if self._chunk_size > 0 and len(self._buffer) >= self._chunk_size:
            self._flush()

    def close(self) -> List[str]:
        """Flush any remaining buffered entries and return the list of written paths."""
        if self._buffer:
            self._flush()
        return list(self._paths)

    def _part_path(self) -> str:
        return f"{self._base}_w{self._worker_id}_p{self._part}.timeline"

    def _flush(self) -> None:
        path = self._part_path()
        os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
        data = {
            'version': '2.0',
            **self._meta,
            'worker_id': self._worker_id,
            'part': self._part,
            'entry_count': len(self._buffer),
            'entries': self._buffer,
        }
        raw = msgpack.packb(data, use_bin_type=True)
        cctx = zstd.ZstdCompressor(level=self._compression_level)
        compressed = cctx.compress(raw)
        with open(path, 'wb') as f:
            f.write(compressed)
        self._paths.append(path)
        self._buffer = []
        self._part += 1
