"""Message flow engine with validation support"""
from typing import Any, Dict, Generator, List, Optional, Callable, Tuple
from dataclasses import dataclass, field
import glob
import re
import time
import json
import copy
from enum import Enum

from .base import PubSubClient, Message
from .clients import (
    KafkaClient,
    PulsarClient,
    RabbitMQClient,
    IggyClient,
    EventHubsClient,
    GooglePubSubClient,
    StreamNativeClient
)


class ClientType(Enum):
    """Supported pub-sub client types"""
    KAFKA = "kafka"
    PULSAR = "pulsar"
    RABBITMQ = "rabbitmq"
    IGGY = "iggy"
    EVENTHUBS = "eventhubs"
    GOOGLEPUBSUB = "googlepubsub"
    STREAMNATIVE = "streamnative"


@dataclass
class ValidationResult:
    """Result of a validation check"""
    passed: bool
    message: str = ""
    details: Dict[str, Any] = field(default_factory=dict)


@dataclass
class HopResult:
    """Result of processing a hop"""
    hop_index: int
    hop_name: str
    success: bool
    validation: Optional[ValidationResult] = None
    publish_latency_ms: float = 0.0
    consume_latency_ms: float = 0.0
    total_latency_ms: float = 0.0
    error: Optional[str] = None


@dataclass
class FlowResult:
    """Result of executing a complete message flow"""
    flow_name: str
    success: bool
    hops: List[HopResult] = field(default_factory=list)
    total_time_ms: float = 0.0
    messages_processed: int = 0


class Validator:
    """Message validator for hop validation"""

    @staticmethod
    def validate_message(message: Message, validation_config: Dict[str, Any]) -> ValidationResult:
        """Validate a message based on configuration

        Args:
            message: Message to validate
            validation_config: Validation configuration dict with keys:
                - type: Validation type (exists, contains, json_schema, custom)
                - params: Parameters for the validation

        Returns:
            ValidationResult
        """
        if not message:
            return ValidationResult(passed=False, message="No message received")

        validation_type = validation_config.get('type', 'exists')
        params = validation_config.get('params', {})

        if validation_type == 'exists':
            return ValidationResult(passed=True, message="Message exists")

        elif validation_type == 'contains':
            # Check if message contains expected text
            expected = params.get('text', '').encode('utf-8')
            if expected in message.value:
                return ValidationResult(passed=True, message=f"Message contains '{expected.decode()}'")
            else:
                return ValidationResult(passed=False, message=f"Message does not contain '{expected.decode()}'")

        elif validation_type == 'json_schema':
            # Validate JSON structure
            try:
                data = json.loads(message.value.decode('utf-8'))
                required_fields = params.get('required_fields', [])

                missing_fields = [f for f in required_fields if f not in data]
                if missing_fields:
                    return ValidationResult(
                        passed=False,
                        message=f"Missing required fields: {missing_fields}",
                        details={'missing_fields': missing_fields}
                    )

                return ValidationResult(
                    passed=True,
                    message="JSON schema validation passed",
                    details={'data': data}
                )
            except json.JSONDecodeError as e:
                return ValidationResult(passed=False, message=f"Invalid JSON: {e}")

        elif validation_type == 'size':
            # Check message size
            min_size = params.get('min_bytes', 0)
            max_size = params.get('max_bytes', float('inf'))
            size = len(message.value)

            if min_size <= size <= max_size:
                return ValidationResult(
                    passed=True,
                    message=f"Message size {size} bytes is within range",
                    details={'size_bytes': size}
                )
            else:
                return ValidationResult(
                    passed=False,
                    message=f"Message size {size} bytes out of range [{min_size}, {max_size}]",
                    details={'size_bytes': size}
                )

        elif validation_type == 'avro_schema':
            # Decode and validate an Avro-encoded message
            from .serialization.avro import decode as avro_decode
            schema = params.get('schema')
            if not schema:
                return ValidationResult(passed=False, message="avro_schema validation requires 'schema' in params")
            try:
                record = avro_decode(message.value, schema)
                required_fields = params.get('required_fields', [])
                missing_fields = [f for f in required_fields if f not in record]
                if missing_fields:
                    return ValidationResult(
                        passed=False,
                        message=f"Avro record missing required fields: {missing_fields}",
                        details={'missing_fields': missing_fields}
                    )
                return ValidationResult(
                    passed=True,
                    message="Avro schema validation passed",
                    details={'record': record}
                )
            except Exception as e:
                return ValidationResult(passed=False, message=f"Avro decode error: {e}")

        else:
            return ValidationResult(passed=True, message=f"Unknown validation type: {validation_type}")


def create_client(client_type: ClientType, config: Dict[str, Any]) -> PubSubClient:
    """Factory function to create pub-sub clients

    Args:
        client_type: Type of client to create
        config: Client configuration

    Returns:
        PubSubClient instance
    """
    client_map = {
        ClientType.KAFKA: KafkaClient,
        ClientType.PULSAR: PulsarClient,
        ClientType.RABBITMQ: RabbitMQClient,
        ClientType.IGGY: IggyClient,
        ClientType.EVENTHUBS: EventHubsClient,
        ClientType.GOOGLEPUBSUB: GooglePubSubClient,
        ClientType.STREAMNATIVE: StreamNativeClient,
    }

    client_class = client_map.get(client_type)
    if not client_class:
        raise ValueError(f"Unsupported client type: {client_type}")

    return client_class(config)


class MessageFlowEngine:
    """Engine to execute message flows with multiple hops and validation"""

    def __init__(self, flow_config: Dict[str, Any]):
        """Initialize message flow engine

        Args:
            flow_config: Flow configuration dict with keys:
                - name: Flow name
                - hops: List of hop configurations
        """
        self.flow_config = flow_config
        self.flow_name = flow_config.get('name', 'unnamed-flow')
        self.hops_config = flow_config.get('hops', [])

        self.validator = Validator()
        self._resolve_hop_references()

    def _resolve_hop_references(self):
        """Resolve source references to previous hop destinations

        Allows intermediary hops to reference previous hop destinations by name
        instead of duplicating the entire configuration.

        Example:
            source: "hop: initial-publish"  # References destination of hop named "initial-publish"
        """
        # Build a map of hop names to their destination configs
        hop_destinations = {}

        for hop_config in self.hops_config:
            hop_name = hop_config.get('name')
            if hop_name and 'destination' in hop_config:
                hop_destinations[hop_name] = hop_config['destination']

        # Resolve source references
        for hop_config in self.hops_config:
            source = hop_config.get('source')

            # Check if source is a string reference (e.g., "hop: hop-name")
            if isinstance(source, str):
                if source.startswith('hop:'):
                    # Extract the referenced hop name
                    referenced_hop = source[4:].strip()

                    # Look up the referenced hop's destination
                    if referenced_hop in hop_destinations:
                        hop_config['source'] = copy.deepcopy(hop_destinations[referenced_hop])
                    else:
                        raise ValueError(f"Cannot resolve hop reference: '{referenced_hop}' not found")
                elif source.startswith('timeline:'):
                    # Timeline file reference – kept as-is; handled at execution time
                    pass
                else:
                    raise ValueError(f"Invalid source reference format: '{source}'. Expected 'hop: hop-name' or 'timeline: /path/to/file.json'")

    def execute(self, initial_message: Optional[Message] = None, num_messages: int = 1) -> FlowResult:
        """Execute the message flow

        Args:
            initial_message: Initial message to send (ignored when using a timeline source)
            num_messages: Number of messages to process (ignored when using a timeline source)

        Returns:
            FlowResult with execution details
        """
        # Check if the first hop uses a timeline source
        first_hop_source = self.hops_config[0].get('source') if self.hops_config else None
        if isinstance(first_hop_source, str) and first_hop_source.startswith('timeline:'):
            timeline_path = first_hop_source[len('timeline:'):].strip()
            first_hop = self.hops_config[0]
            pod_id = int(first_hop.get('pod_id', self.flow_config.get('pod_id', 0)))
            num_pods = int(first_hop.get('num_pods', self.flow_config.get('num_pods', 1)))
            return self._execute_with_timeline_path(timeline_path, pod_id, num_pods)

        if initial_message is None:
            raise ValueError("initial_message is required when not using a timeline source")

        start_time = time.time()
        flow_result = FlowResult(flow_name=self.flow_name, success=True)

        try:
            for msg_idx in range(num_messages):
                current_message = initial_message

                for hop_idx, hop_config in enumerate(self.hops_config):
                    hop_result = self._execute_hop(hop_idx, hop_config, current_message)
                    flow_result.hops.append(hop_result)

                    if not hop_result.success:
                        flow_result.success = False
                        break

                    # Get message for next hop if there is one
                    if hop_idx < len(self.hops_config) - 1:
                        # Message will be consumed in next hop
                        pass

                flow_result.messages_processed += 1

        except Exception as e:
            flow_result.success = False
            flow_result.hops.append(HopResult(
                hop_index=-1,
                hop_name="error",
                success=False,
                error=str(e)
            ))

        flow_result.total_time_ms = (time.time() - start_time) * 1000
        return flow_result

    def _execute_with_timeline(self, timeline) -> FlowResult:
        """Execute the message flow by replaying messages from a timeline.

        Messages are published at the same relative rate at which they were
        originally captured (inter-message gaps are preserved).

        Args:
            timeline: Timeline whose entries will be replayed.

        Returns:
            FlowResult with execution details.
        """
        start_time = time.time()
        flow_result = FlowResult(flow_name=self.flow_name, success=True)

        prev_offset_ms = 0.0

        try:
            for entry in timeline.entries:
                # Sleep to maintain the original inter-message timing
                gap_ms = entry.offset_ms - prev_offset_ms
                if gap_ms > 0:
                    time.sleep(gap_ms / 1000.0)
                prev_offset_ms = entry.offset_ms

                current_message = Message(
                    key=entry.key,
                    value=entry.value,
                    headers=entry.headers,
                )

                for hop_idx, hop_config in enumerate(self.hops_config):
                    if hop_idx == 0:
                        # Strip the timeline source so _execute_hop treats this
                        # as a first hop (publish-only, no source to consume from)
                        hop_cfg = {k: v for k, v in hop_config.items() if k != 'source'}
                    else:
                        hop_cfg = hop_config

                    hop_result = self._execute_hop(hop_idx, hop_cfg, current_message)
                    flow_result.hops.append(hop_result)

                    if not hop_result.success:
                        flow_result.success = False
                        break

                flow_result.messages_processed += 1

        except Exception as e:
            flow_result.success = False
            flow_result.hops.append(HopResult(
                hop_index=-1,
                hop_name="error",
                success=False,
                error=str(e)
            ))

        flow_result.total_time_ms = (time.time() - start_time) * 1000
        return flow_result

    def _execute_with_timeline_path(
        self,
        timeline_path: str,
        pod_id: int = 0,
        num_pods: int = 1,
    ) -> FlowResult:
        """Execute timeline replay from a file path.

        Supports all timeline formats (JSON, compressed ``.timeline``, and
        multi-part captures produced by :class:`TimelineWriter`).  When
        *num_pods* > 1 each pod processes a disjoint subset of worker files
        so pods can run concurrently without duplicating work.

        Inter-message timing is preserved within each worker's data.  When
        entries from a new worker begin, the timing clock resets to avoid
        accumulating drift across independently-captured workers.

        Args:
            timeline_path: Path to a ``.json`` file, a single ``.timeline``
                file, or a base path whose part files will be globbed.
            pod_id: Zero-based index of this pod (default 0).
            num_pods: Total number of pods sharing the timeline (default 1).

        Returns:
            FlowResult with execution details.
        """
        start_time = time.time()
        flow_result = FlowResult(flow_name=self.flow_name, success=True)

        playback_start: Optional[float] = None

        try:
            for entry, reset_timing in self._iter_timeline_entries(
                timeline_path, pod_id, num_pods
            ):
                if playback_start is None or reset_timing:
                    # Anchor the clock so entry.offset_ms maps to "now"
                    playback_start = time.time() - entry.offset_ms / 1000.0

                target_time = playback_start + entry.offset_ms / 1000.0
                sleep_s = target_time - time.time()
                if sleep_s > 0:
                    time.sleep(sleep_s)

                current_message = Message(
                    key=entry.key,
                    value=entry.value,
                    headers=entry.headers,
                )

                for hop_idx, hop_config in enumerate(self.hops_config):
                    if hop_idx == 0:
                        hop_cfg = {k: v for k, v in hop_config.items() if k != 'source'}
                    else:
                        hop_cfg = hop_config

                    hop_result = self._execute_hop(hop_idx, hop_cfg, current_message)
                    flow_result.hops.append(hop_result)

                    if not hop_result.success:
                        flow_result.success = False
                        break

                flow_result.messages_processed += 1

        except Exception as e:
            flow_result.success = False
            flow_result.hops.append(HopResult(
                hop_index=-1,
                hop_name="error",
                success=False,
                error=str(e),
            ))

        flow_result.total_time_ms = (time.time() - start_time) * 1000
        return flow_result

    @staticmethod
    def _iter_timeline_entries(
        source_path: str,
        pod_id: int = 0,
        num_pods: int = 1,
    ) -> Generator[Tuple[Any, bool], None, None]:
        """Yield ``(TimelineEntry, reset_timing)`` pairs from a timeline source.

        Handles three source formats:

        * **JSON** (``*.json``) – legacy single-file format loaded entirely
          into memory.
        * **Compressed single file** (``*.timeline``) – msgpack + zstd part
          file loaded entirely into memory.
        * **Base path** – all ``<base>_w*_p*.timeline`` files are discovered,
          worker IDs are distributed round-robin across *num_pods* pods, and
          this pod streams the part files for its assigned workers one at a
          time (low memory footprint for large captures).

        ``reset_timing`` is ``True`` at the first entry from each new worker
        (after the very first worker), signalling the caller to reset the
        playback clock so per-worker offset values are interpreted correctly.
        """
        from .timeline.timeline import Timeline  # local import – avoids circular dependency

        # -- Legacy JSON -------------------------------------------------------
        if source_path.endswith('.json'):
            tl = Timeline.load(source_path)
            for entry in tl.entries:
                yield entry, False
            return

        # -- Single compressed file --------------------------------------------
        if source_path.endswith('.timeline'):
            tl = Timeline.load_compressed(source_path)
            for entry in tl.entries:
                yield entry, False
            return

        # -- Base path: discover all worker/part files -------------------------
        pattern = f"{source_path}_w*_p*.timeline"
        all_files = sorted(glob.glob(pattern))

        if not all_files:
            raise ValueError(
                f"No timeline part files found matching pattern: {pattern}"
            )

        # Group files by worker ID
        worker_files: Dict[int, List[Tuple[int, str]]] = {}
        for fpath in all_files:
            m = re.search(r'_w(\d+)_p(\d+)\.timeline$', fpath)
            if m:
                wid, part = int(m.group(1)), int(m.group(2))
                worker_files.setdefault(wid, []).append((part, fpath))

        # Assign workers to this pod via round-robin on sorted worker index
        sorted_worker_ids = sorted(worker_files)
        my_worker_ids = [
            wid
            for i, wid in enumerate(sorted_worker_ids)
            if i % num_pods == pod_id
        ]

        for worker_idx, wid in enumerate(my_worker_ids):
            parts = sorted(worker_files[wid])  # sort by part number
            for part_idx, (_part_num, fpath) in enumerate(parts):
                tl = Timeline.load_compressed(fpath)
                for entry_idx, entry in enumerate(tl.entries):
                    # Signal timing reset at the first entry of each new worker
                    reset = part_idx == 0 and entry_idx == 0 and worker_idx > 0
                    yield entry, reset

    def _execute_hop(self, hop_index: int, hop_config: Dict[str, Any], message: Message) -> HopResult:
        """Execute a single hop

        Args:
            hop_index: Index of the hop
            hop_config: Hop configuration
            message: Message to process

        Returns:
            HopResult
        """
        hop_start = time.time()
        hop_name = hop_config.get('name', f'hop-{hop_index}')

        try:
            # Parse hop configuration
            source_config = hop_config.get('source', {})
            destination_config = hop_config.get('destination', {})
            validation_config = hop_config.get('validation', {})

            # Create destination client (always needed)
            dest_type = ClientType(destination_config.get('type'))
            dest_client = create_client(dest_type, destination_config.get('config', {}))

            # Create source client only if source is specified
            source_client = None
            if source_config and source_config.get('type'):
                source_type = ClientType(source_config.get('type'))
                source_client = create_client(source_type, source_config.get('config', {}))

            # Execute hop
            if source_client:
                with source_client, dest_client:
                    # Consume from source if this is not the first hop
                    consume_latency = 0.0
                    if hop_index > 0:
                        source_topic = source_config.get('topic')
                        source_client.subscribe([source_topic])

                        consume_result = source_client.consume(source_topic, timeout_ms=5000)
                        consume_latency = consume_result.latency_ms

                        if consume_result.message:
                            message = consume_result.message
                        else:
                            return HopResult(
                                hop_index=hop_index,
                                hop_name=hop_name,
                                success=False,
                                consume_latency_ms=consume_latency,
                                error="No message received from source"
                            )

                    # Validate message if validation is configured
                    validation_result = None
                    if validation_config:
                        validation_result = self.validator.validate_message(message, validation_config)
                        if not validation_result.passed:
                            return HopResult(
                                hop_index=hop_index,
                                hop_name=hop_name,
                                success=False,
                                validation=validation_result,
                                consume_latency_ms=consume_latency,
                                error=f"Validation failed: {validation_result.message}"
                            )

                    # Publish to destination
                    dest_topic = destination_config.get('topic')
                    publish_result = dest_client.publish(dest_topic, message)

                    if not publish_result.success:
                        return HopResult(
                            hop_index=hop_index,
                            hop_name=hop_name,
                            success=False,
                            validation=validation_result,
                            consume_latency_ms=consume_latency,
                            publish_latency_ms=publish_result.latency_ms,
                            error=f"Publish failed: {publish_result.error}"
                        )

                    total_latency = (time.time() - hop_start) * 1000

                    return HopResult(
                        hop_index=hop_index,
                        hop_name=hop_name,
                        success=True,
                        validation=validation_result,
                        consume_latency_ms=consume_latency,
                        publish_latency_ms=publish_result.latency_ms,
                        total_latency_ms=total_latency
                    )
            else:
                # First hop - no source, just publish to destination
                with dest_client:
                    # Validate message if validation is configured
                    validation_result = None
                    if validation_config:
                        validation_result = self.validator.validate_message(message, validation_config)
                        if not validation_result.passed:
                            return HopResult(
                                hop_index=hop_index,
                                hop_name=hop_name,
                                success=False,
                                validation=validation_result,
                                error=f"Validation failed: {validation_result.message}"
                            )

                    # Publish to destination
                    dest_topic = destination_config.get('topic')
                    publish_result = dest_client.publish(dest_topic, message)

                    if not publish_result.success:
                        return HopResult(
                            hop_index=hop_index,
                            hop_name=hop_name,
                            success=False,
                            validation=validation_result,
                            publish_latency_ms=publish_result.latency_ms,
                            error=f"Publish failed: {publish_result.error}"
                        )

                    total_latency = (time.time() - hop_start) * 1000

                    return HopResult(
                        hop_index=hop_index,
                        hop_name=hop_name,
                        success=True,
                        validation=validation_result,
                        publish_latency_ms=publish_result.latency_ms,
                        total_latency_ms=total_latency
                    )

        except Exception as e:
            total_latency = (time.time() - hop_start) * 1000
            return HopResult(
                hop_index=hop_index,
                hop_name=hop_name,
                success=False,
                total_latency_ms=total_latency,
                error=str(e)
            )
