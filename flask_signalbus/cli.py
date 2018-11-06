import click
from flask.cli import with_appcontext
from flask import current_app


@click.group()
def signalbus():
    """Perform SignalBus operations."""


@signalbus.command()
@with_appcontext
def flush():
    """Flush all pending signals."""

    signal_count = 0
    signalbus_set = current_app.extensions['signalbus']
    for signalbus in signalbus_set:
        signal_count += signalbus.flush()
    if signal_count == 1:
        click.echo('{} signal has been successfully processed.'.format(signal_count))
    elif signal_count > 1:
        click.echo('{} signals have been successfully processed.'.format(signal_count))
