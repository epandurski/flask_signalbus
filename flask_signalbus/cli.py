import click
from flask.cli import with_appcontext
from flask import current_app


@click.group()
def signalbus():
    """Perform SignalBus operations."""


@signalbus.command()
@with_appcontext
def flush():
    """Flushes pending signals."""

    signalbus = current_app.extensions['signalbus']
    signal_count = signalbus.process_signals()
    if signal_count == 1:
        click.echo('{} signal has been processed.'.format(signal_count))
    elif signal_count > 1:
        click.echo('{} signals have been processed.'.format(signal_count))
