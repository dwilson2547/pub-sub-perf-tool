"""Timeline capture – reads messages from a pub-sub topic without altering the feed"""
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Callable, List, Optional

from ..flow_engine import ClientType, create_client
from .timeline import Timeline, TimelineEntry, TimelineWriter


class TimelineCapture:
    """Captures messages from a pub-sub topic into a Timeline.

    Design constraints (must not alter the original feed):
    - Kafka: always uses a fresh random consumer group; when *num_workers* > 1
      all workers share that group so Kafka distributes partitions among them.
    - Pulsar / StreamNative with a single worker: uses reader mode (no durable
      subscription created).
    - Pulsar / StreamNative with multiple workers: uses a Shared subscription
      with a random name so messages are distributed without duplication.
    """

    def __init__(self, source_type: str, source_topic: str, config: dict,
                 num_workers: int = 1):
        """Initialize capture.

        Args:
            source_type: Pub-sub system type (e.g. 'kafka', 'pulsar').
            source_topic: Topic to capture from.
            config: Client configuration dict (will be copied and adjusted).
            num_workers: Number of concurrent consumer threads used by
                :meth:`capture_to_files`.  Has no effect on the single-
                threaded :meth:`capture` method.
        """
        self.source_type = source_type
        self.source_topic = source_topic
        self.num_workers = max(1, num_workers)
        # Work on a copy so the caller's dict is not mutated
        self.config = dict(config)
        # Stable session ID reused as consumer group / subscription name
        self._session_id = uuid.uuid4()

        # Non-destructive consumer settings
        if source_type == 'kafka':
            # Random group so this capture never commits offsets that affect other consumers
            self.config.setdefault('consumer_group', f'timeline-capture-{self._session_id}')
        elif source_type in ('pulsar', 'streamnative'):
            if self.num_workers > 1:
                # Shared subscription distributes messages among workers without duplication.
                # Random name avoids interfering with real consumer subscriptions.
                self.config['subscription_name'] = f'timeline-capture-{self._session_id}'
                self.config.setdefault('consumer_config', {})
                self.config['consumer_config']['subscription_type'] = 'Shared'
            else:
                # Reader mode – does not create or advance a subscription
                self.config['use_reader'] = True

    def capture(
        self,
        max_messages: int = 100,
        timeout_ms: int = 5000,
        on_message: Optional[Callable[[int, int], None]] = None,
    ) -> Timeline:
        """Capture messages from the pub-sub topic (single-threaded).

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

    def capture_to_files(
        self,
        output_base: str,
        max_messages: int = 0,
        timeout_ms: int = 5000,
        chunk_size: int = 100_000,
        on_progress: Optional[Callable[[int], None]] = None,
    ) -> List[str]:
        """Capture messages using multiple worker threads, each writing its own
        compressed part files (msgpack + zstd).

        For Kafka all workers share the same consumer group so partitions are
        distributed automatically.  For Pulsar / StreamNative a Shared
        subscription is used with a random name.

        Args:
            output_base: Base path for output files, e.g. ``"capture.timeline"``
                or ``"/data/my-capture"``.  Part files are named
                ``<base>_w<worker_id>_p<part>.timeline``.
            max_messages: Total messages to capture across all workers
                (0 = run every worker until its poll times out).
            timeout_ms: Per-poll timeout in milliseconds.
            chunk_size: Maximum entries per part file (0 = no splitting).
            on_progress: Optional callback invoked with the running total of
                captured messages after every message.

        Returns:
            Sorted list of file paths written.
        """
        per_worker_limit = (max_messages // self.num_workers) if max_messages > 0 else 0
        captured_total = 0
        lock = threading.Lock()
        all_paths: List[str] = []

        def worker_func(worker_id: int) -> List[str]:
            nonlocal captured_total

            client_type = ClientType(self.source_type)
            client = create_client(client_type, self.config)

            captured_at = datetime.now(timezone.utc).isoformat()
            writer = TimelineWriter(
                output_base,
                chunk_size=chunk_size,
                worker_id=worker_id,
            )
            writer.open(self.source_type, self.source_topic, captured_at)

            count = 0
            limit = per_worker_limit if per_worker_limit > 0 else float('inf')

            with client:
                client.subscribe([self.source_topic])
                capture_start = time.time()

                while count < limit:
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
                    writer.write_entry(entry)
                    count += 1

                    if on_progress:
                        with lock:
                            captured_total += 1
                            on_progress(captured_total)

            return writer.close()

        with ThreadPoolExecutor(max_workers=self.num_workers) as executor:
            futures = [executor.submit(worker_func, i) for i in range(self.num_workers)]
            for future in as_completed(futures):
                all_paths.extend(future.result())

        return sorted(all_paths)
