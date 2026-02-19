"""Timeline data model and file I/O"""
import json
import base64
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


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
