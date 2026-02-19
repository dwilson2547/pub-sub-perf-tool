"""Timeline capture – reads messages from a pub-sub topic without altering the feed"""
import time
import uuid
from datetime import datetime, timezone
from typing import Callable, Optional

from ..flow_engine import ClientType, create_client
from .timeline import Timeline, TimelineEntry


class TimelineCapture:
    """Captures messages from a pub-sub topic into a Timeline.

    Design constraints (must not alter the original feed):
    - Kafka: always uses a fresh random consumer group
    - Pulsar / StreamNative: always enables reader mode
    """

    def __init__(self, source_type: str, source_topic: str, config: dict):
        """Initialize capture.

        Args:
            source_type: Pub-sub system type (e.g. 'kafka', 'pulsar').
            source_topic: Topic to capture from.
            config: Client configuration dict (will be copied and adjusted).
        """
        self.source_type = source_type
        self.source_topic = source_topic
        # Work on a copy so the caller's dict is not mutated
        self.config = dict(config)

        # Non-destructive consumer settings
        if source_type == 'kafka':
            # Random group so this capture never commits offsets that affect other consumers
            self.config.setdefault('consumer_group', f'timeline-capture-{uuid.uuid4()}')
        elif source_type in ('pulsar', 'streamnative'):
            # Reader mode – does not create or advance a subscription
            self.config['use_reader'] = True

    def capture(
        self,
        max_messages: int = 100,
        timeout_ms: int = 5000,
        on_message: Optional[Callable[[int, int], None]] = None,
    ) -> Timeline:
        """Capture messages from the pub-sub topic.

        Args:
            max_messages: Stop after this many messages.
            timeout_ms: Per-poll timeout; capture stops on the first timeout.
            on_message: Optional progress callback ``(captured_count, max_messages)``.

        Returns:
            A :class:`Timeline` containing all captured entries.
        """
        client_type = ClientType(self.source_type)
        client = create_client(client_type, self.config)

        captured_at = datetime.now(timezone.utc).isoformat()
        timeline = Timeline(
            source_type=self.source_type,
            source_topic=self.source_topic,
            captured_at=captured_at,
        )

        with client:
            client.subscribe([self.source_topic])
            capture_start = time.time()

            for _ in range(max_messages):
                result = client.consume(self.source_topic, timeout_ms=timeout_ms)

                if result.message is None:
                    break  # Timeout – no more messages available

                offset_ms = (time.time() - capture_start) * 1000
                entry = TimelineEntry(
                    offset_ms=offset_ms,
                    value=result.message.value,
                    key=result.message.key,
                    headers=result.message.headers,
                )
                timeline.entries.append(entry)

                if on_message:
                    on_message(len(timeline.entries), max_messages)

        return timeline
