# src/data_professor/cli.py

import json
import click
from data_professor.core import DataProfessor

@click.group()
def cli():
    """Entry point for data-professor commands."""
    pass

@cli.command()
@click.option('--config', '-c', default='config/default.yaml',
              help='Path to YAML config file.')
@click.option('--source', '-s', default=None,
              help='Data source URI (overrides config).')
@click.option('--format', '-f', 'fmt', default=None,
              help='Data format (parquet, delta, csv, etc.).')
@click.option('--options', '-o', default=None,
              help='Additional read options as JSON string.')
def chat(config, source, fmt, options):
    """
    Launch an interactive chatbot to ask questions about the dataset.
    """
    # Parse extra options if provided
    opts = None
    if options:
        try:
            opts = json.loads(options)
        except Exception as e:
            click.echo(f"[!] Failed to parse --options JSON: {e}")
            return

    # Initialize and load data
    prof = DataProfessor(config_path=config)
    prof.load_source(source=source, format=fmt, options=opts)

    click.echo("\nInteractive dataset chat — type 'exit', 'quit', or 'bye' to end.\n")

    while True:
        q = click.prompt('You')
        text = q.strip().lower()

        # Handle greetings
        if text in ('hi', 'hello', 'hey'):
            click.echo("Bot: Hello! How can I help you with your dataset?\n")
            continue

        # Handle farewells
        if text in ('exit', 'quit', 'bye', 'goodbye'):
            click.echo('Bot: Goodbye!')
            break

        # Otherwise, treat as a question
        try:
            ans = prof.ask(q)
        except Exception as e:
            click.echo(f"Bot: Oops, something went wrong: {e}\n")
            continue

        click.echo(f'Bot: {ans}\n')

if __name__ == '__main__':
    cli()