"""Command-line interface for pub-sub performance tool"""
import click
import yaml
import json
import sys
from pathlib import Path
from tabulate import tabulate

from .flow_engine import MessageFlowEngine, Message, FlowResult
from .base import Message as BaseMessage


@click.group()
@click.version_option(version='0.1.0')
def main():
    """Pub-Sub Performance and Validation Tool
    
    A tool for testing message flows across different pub-sub systems
    with validation at each hop.
    """
    pass


@main.command()
@click.argument('config_file', type=click.Path(exists=True))
@click.option('--message', '-m', help='Message content to send')
@click.option('--message-file', '-f', type=click.Path(exists=True), help='File containing message content')
@click.option('--key', '-k', help='Message key')
@click.option('--count', '-c', default=1, help='Number of messages to send', type=int)
@click.option('--output', '-o', type=click.Choice(['table', 'json']), default='table', help='Output format')
def run(config_file, message, message_file, key, count, output):
    """Run a message flow from configuration file
    
    Example:
        pub-sub-perf run flow.yaml --message "Hello World" --count 10
    """
    # Load configuration
    config_path = Path(config_file)
    with open(config_path, 'r') as f:
        if config_path.suffix in ['.yaml', '.yml']:
            config = yaml.safe_load(f)
        else:
            config = json.load(f)
    
    # Prepare message
    if message_file:
        with open(message_file, 'rb') as f:
            message_content = f.read()
    elif message:
        message_content = message.encode('utf-8')
    else:
        click.echo("Error: Either --message or --message-file must be provided", err=True)
        sys.exit(1)
    
    msg = BaseMessage(
        key=key,
        value=message_content,
        headers=config.get('message_headers', {})
    )
    
    # Execute flow
    click.echo(f"Executing flow: {config.get('name', 'unnamed')}")
    click.echo(f"Processing {count} message(s)...")
    
    engine = MessageFlowEngine(config)
    result = engine.execute(msg, num_messages=count)
    
    # Display results
    if output == 'json':
        _display_json_results(result)
    else:
        _display_table_results(result)
    
    # Exit with error code if flow failed
    if not result.success:
        sys.exit(1)


@main.command()
@click.argument('output_file', type=click.Path())
@click.option('--type', '-t', type=click.Choice(['kafka', 'pulsar', 'rabbitmq', 'iggy']), 
              multiple=True, default=['kafka'], help='Client types to include')
def generate_config(output_file, type):
    """Generate a sample configuration file
    
    Example:
        pub-sub-perf generate-config flow.yaml --type kafka --type pulsar
    """
    config = _create_sample_config(list(type))
    
    output_path = Path(output_file)
    with open(output_path, 'w') as f:
        if output_path.suffix in ['.yaml', '.yml']:
            yaml.dump(config, f, default_flow_style=False, sort_keys=False)
        else:
            json.dump(config, f, indent=2)
    
    click.echo(f"Sample configuration written to {output_file}")


@main.command()
@click.argument('config_file', type=click.Path(exists=True))
def validate_config(config_file):
    """Validate a configuration file
    
    Example:
        pub-sub-perf validate-config flow.yaml
    """
    try:
        config_path = Path(config_file)
        with open(config_path, 'r') as f:
            if config_path.suffix in ['.yaml', '.yml']:
                config = yaml.safe_load(f)
            else:
                config = json.load(f)
        
        # Basic validation
        errors = []
        
        if 'hops' not in config:
            errors.append("Missing 'hops' in configuration")
        else:
            for idx, hop in enumerate(config['hops']):
                if 'destination' not in hop:
                    errors.append(f"Hop {idx}: Missing 'destination'")
                if idx > 0 and 'source' not in hop:
                    errors.append(f"Hop {idx}: Missing 'source' (required for non-first hops)")
        
        if errors:
            click.echo("Configuration validation failed:", err=True)
            for error in errors:
                click.echo(f"  - {error}", err=True)
            sys.exit(1)
        else:
            click.echo("✓ Configuration is valid")
            
    except Exception as e:
        click.echo(f"Error validating configuration: {e}", err=True)
        sys.exit(1)


def _display_table_results(result: FlowResult):
    """Display results in table format"""
    click.echo("\n" + "="*80)
    click.echo(f"Flow: {result.flow_name}")
    click.echo(f"Status: {'✓ SUCCESS' if result.success else '✗ FAILED'}")
    click.echo(f"Messages Processed: {result.messages_processed}")
    click.echo(f"Total Time: {result.total_time_ms:.2f} ms")
    click.echo("="*80)
    
    if result.hops:
        # Prepare table data
        table_data = []
        for hop in result.hops:
            status = "✓" if hop.success else "✗"
            validation = ""
            if hop.validation:
                validation = "✓" if hop.validation.passed else f"✗ {hop.validation.message}"
            
            table_data.append([
                hop.hop_index,
                hop.hop_name,
                status,
                f"{hop.consume_latency_ms:.2f}",
                f"{hop.publish_latency_ms:.2f}",
                f"{hop.total_latency_ms:.2f}",
                validation,
                hop.error or ""
            ])
        
        headers = ["Hop", "Name", "Status", "Consume(ms)", "Publish(ms)", "Total(ms)", "Validation", "Error"]
        click.echo("\n" + tabulate(table_data, headers=headers, tablefmt="grid"))
    
    click.echo()


def _display_json_results(result: FlowResult):
    """Display results in JSON format"""
    output = {
        'flow_name': result.flow_name,
        'success': result.success,
        'messages_processed': result.messages_processed,
        'total_time_ms': result.total_time_ms,
        'hops': []
    }
    
    for hop in result.hops:
        hop_data = {
            'hop_index': hop.hop_index,
            'hop_name': hop.hop_name,
            'success': hop.success,
            'consume_latency_ms': hop.consume_latency_ms,
            'publish_latency_ms': hop.publish_latency_ms,
            'total_latency_ms': hop.total_latency_ms,
            'error': hop.error
        }
        
        if hop.validation:
            hop_data['validation'] = {
                'passed': hop.validation.passed,
                'message': hop.validation.message,
                'details': hop.validation.details
            }
        
        output['hops'].append(hop_data)
    
    click.echo(json.dumps(output, indent=2))


def _create_sample_config(client_types):
    """Create a sample configuration"""
    config = {
        'name': 'sample-flow',
        'message_headers': {
            'source': 'perf-tool',
            'version': '1.0'
        },
        'hops': []
    }
    
    # Add first hop based on first client type
    first_type = client_types[0] if client_types else 'kafka'
    
    if first_type == 'kafka':
        config['hops'].append({
            'name': 'initial-publish',
            'destination': {
                'type': 'kafka',
                'topic': 'test-topic-1',
                'config': {
                    'bootstrap_servers': ['localhost:9092']
                }
            },
            'validation': {
                'type': 'exists'
            }
        })
    elif first_type == 'pulsar':
        config['hops'].append({
            'name': 'initial-publish',
            'destination': {
                'type': 'pulsar',
                'topic': 'test-topic-1',
                'config': {
                    'service_url': 'pulsar://localhost:6650',
                    'use_reader': False
                }
            },
            'validation': {
                'type': 'exists'
            }
        })
    elif first_type == 'rabbitmq':
        config['hops'].append({
            'name': 'initial-publish',
            'destination': {
                'type': 'rabbitmq',
                'topic': 'test-topic-1',
                'config': {
                    'host': 'localhost',
                    'port': 5672
                }
            },
            'validation': {
                'type': 'exists'
            }
        })
    elif first_type == 'iggy':
        config['hops'].append({
            'name': 'initial-publish',
            'destination': {
                'type': 'iggy',
                'topic': 'test-topic-1',
                'config': {
                    'host': 'localhost',
                    'port': 8090
                }
            },
            'validation': {
                'type': 'exists'
            }
        })
    
    # Add intermediary hop if multiple types
    if len(client_types) > 1:
        second_type = client_types[1]
        
        hop_config = {
            'name': 'intermediary-hop',
            'source': {
                'type': first_type,
                'topic': 'test-topic-1',
                'config': {}
            },
            'destination': {
                'type': second_type,
                'topic': 'test-topic-2',
                'config': {}
            },
            'validation': {
                'type': 'size',
                'params': {
                    'min_bytes': 1,
                    'max_bytes': 1000000
                }
            }
        }
        
        # Add specific configs
        if first_type == 'kafka':
            hop_config['source']['config'] = {'bootstrap_servers': ['localhost:9092']}
        elif first_type == 'pulsar':
            hop_config['source']['config'] = {
                'service_url': 'pulsar://localhost:6650',
                'use_reader': True  # Use reader for intermediary hops
            }
        
        if second_type == 'kafka':
            hop_config['destination']['config'] = {'bootstrap_servers': ['localhost:9092']}
        elif second_type == 'pulsar':
            hop_config['destination']['config'] = {'service_url': 'pulsar://localhost:6650'}
        elif second_type == 'rabbitmq':
            hop_config['destination']['config'] = {'host': 'localhost', 'port': 5672}
        elif second_type == 'iggy':
            hop_config['destination']['config'] = {'host': 'localhost', 'port': 8090}
        
        config['hops'].append(hop_config)
    
    return config


if __name__ == '__main__':
    main()
