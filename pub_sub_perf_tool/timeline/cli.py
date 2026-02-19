"""Standalone CLI for timeline capture"""
import json
import sys
from pathlib import Path

import click
import yaml

from .capture import TimelineCapture


@click.group()
@click.version_option(version='0.1.0')
def timeline_main():
    """Pub-Sub Timeline Tool

    Capture messages from a pub-sub topic into a replayable timeline file.
    """
    pass


@timeline_main.command()
@click.argument('output_file', type=click.Path())
@click.option(
    '--source-type', '-s', required=True,
    type=click.Choice(['kafka', 'pulsar', 'rabbitmq', 'eventhubs', 'googlepubsub', 'streamnative']),
    help='Pub-sub system type',
)
@click.option('--topic', '-t', required=True, help='Topic to capture from')
@click.option(
    '--config-file', '-c', type=click.Path(exists=True),
    help='Client config file (YAML or JSON)',
)
@click.option('--max-messages', '-n', default=100, type=int, show_default=True,
              help='Maximum number of messages to capture')
@click.option('--timeout-ms', default=5000, type=int, show_default=True,
              help='Per-poll timeout in milliseconds; capture stops on first timeout')
@click.option('--workers', '-w', default=1, type=int, show_default=True,
              help='Number of concurrent consumer threads (enables compressed output)')
@click.option('--chunk-size', default=100_000, type=int, show_default=True,
              help='Max entries per output file part; 0 = no splitting (workers mode only)')
def capture(output_file, source_type, topic, config_file, max_messages, timeout_ms,
            workers, chunk_size):
    """Capture messages from a pub-sub topic into OUTPUT_FILE.

    The timeline file can later be used as the source for a pub-sub-perf
    flow to replay the captured messages at the original rate.

    When --workers is greater than 1 (or --chunk-size is set), the capture
    tool runs multiple concurrent consumer threads and writes compressed
    msgpack+zstd part files named <OUTPUT_FILE>_w<N>_p<N>.timeline.  Each
    worker writes its own files independently for maximum throughput.

    For Kafka, multiple workers share the same consumer group so partitions
    are distributed automatically.  For Pulsar/StreamNative, a Shared
    subscription with a random name is used.

    Example (single-threaded JSON):

    \b
        pub-sub-timeline capture capture.json \\
            --source-type kafka \\
            --topic my-topic \\
            --config-file kafka-config.yaml \\
            --max-messages 500

    Example (multi-threaded compressed):

    \b
        pub-sub-timeline capture capture \\
            --source-type kafka \\
            --topic my-topic \\
            --config-file kafka-config.yaml \\
            --workers 4 \\
            --chunk-size 100000
    """
    config = {}
    if config_file:
        config_path = Path(config_file)
        with open(config_path, 'r') as f:
            if config_path.suffix in ('.yaml', '.yml'):
                config = yaml.safe_load(f) or {}
            else:
                config = json.load(f)

    use_compressed_format = workers > 1 or chunk_size > 0

    capturer = TimelineCapture(source_type, topic, config, num_workers=workers)

    click.echo(f"Connecting to {source_type}, topic '{topic}'...")

    if use_compressed_format:
        click.echo(
            f"Starting {workers} worker(s), chunk size {chunk_size or 'unlimited'}..."
        )
        total_captured = 0

        def progress(count: int) -> None:
            nonlocal total_captured
            total_captured = count
            click.echo(f"\rCapturing: {count} messages", nl=False)

        try:
            paths = capturer.capture_to_files(
                output_base=output_file,
                max_messages=max_messages,
                timeout_ms=timeout_ms,
                chunk_size=chunk_size,
                on_progress=progress,
            )
        except Exception as exc:
            click.echo(f"\nError during capture: {exc}", err=True)
            sys.exit(1)

        click.echo(f"\nCaptured {total_captured} messages across {len(paths)} file(s):")
        for p in paths:
            click.echo(f"  {p}")
    else:
        def progress(count: int, total: int) -> None:
            click.echo(f"\rCapturing: {count}/{total} messages", nl=False)

        try:
            timeline = capturer.capture(
                max_messages=max_messages,
                timeout_ms=timeout_ms,
                on_message=progress,
            )
        except Exception as exc:
            click.echo(f"\nError during capture: {exc}", err=True)
            sys.exit(1)

        click.echo(f"\nCaptured {len(timeline.entries)} messages")

        if not timeline.entries:
            click.echo("Warning: no messages captured – timeline file will be empty", err=True)

        timeline.save(output_file)
        click.echo(f"Timeline saved to {output_file}")


if __name__ == '__main__':
    timeline_main()
