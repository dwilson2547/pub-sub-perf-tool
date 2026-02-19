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
def capture(output_file, source_type, topic, config_file, max_messages, timeout_ms):
    """Capture messages from a pub-sub topic into OUTPUT_FILE.

    The timeline file can later be used as the source for a pub-sub-perf
    flow to replay the captured messages at the original rate.

    Example:

    \b
        pub-sub-timeline capture capture.json \\
            --source-type kafka \\
            --topic my-topic \\
            --config-file kafka-config.yaml \\
            --max-messages 500
    """
    config = {}
    if config_file:
        config_path = Path(config_file)
        with open(config_path, 'r') as f:
            if config_path.suffix in ('.yaml', '.yml'):
                config = yaml.safe_load(f) or {}
            else:
                config = json.load(f)

    capturer = TimelineCapture(source_type, topic, config)

    def progress(count: int, total: int) -> None:
        click.echo(f"\rCapturing: {count}/{total} messages", nl=False)

    click.echo(f"Connecting to {source_type}, topic '{topic}'...")

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
