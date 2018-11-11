import click
from flask.cli import with_appcontext
from flask import current_app


@click.group()
def signalbus():
    """Perform SignalBus operations."""


@signalbus.command()
@with_appcontext
@click.option('-e', '--exclude', multiple=True, help="Do not flush signals with the specified name.")
@click.argument('signal_names', nargs=-1)
def flush(signal_names, exclude):
    """Send pending signals over the message bus.

    If a list of SIGNAL_NAMES is specified, flushes only those
    signals. If no SIGNAL_NAMES are specified, flushes all signals.

    """

    signalbus = current_app.extensions['signalbus']
    signal_names = set(signal_names)
    exclude = set(exclude)
    models_to_flush = signalbus.get_signal_models()
    if signal_names and exclude:
        click.echo('Warning: Specified both SIGNAL_NAMES and exclude option.')
    if signal_names:
        wrong_signal_names = signal_names - {m.__name__ for m in models_to_flush}
        models_to_flush = [m for m in models_to_flush if type(m).__name__ in signal_names]
    else:
        wrong_signal_names = exclude - {m.__name__ for m in models_to_flush}
    for name in wrong_signal_names:
        click.echo('Warning: A signal with name "{}" does not exist.'.format(name))
    models_to_flush = [m for m in models_to_flush if type(m).__name__ not in exclude]
    signal_count = 0
    for model in models_to_flush:
        signal_count += signalbus.flush()
    if signal_count == 1:
        click.echo('{} signal has been successfully processed.'.format(signal_count))
    elif signal_count > 1:
        click.echo('{} signals have been successfully processed.'.format(signal_count))


@signalbus.command()
@with_appcontext
def signals():
    """Show all signal types."""

    signalbus = current_app.extensions['signalbus']
    for signal_model in signalbus.get_signal_models():
        click.echo(signal_model.__name__)


@signalbus.command()
@with_appcontext
def pending():
    """Show the number of pending signals by signal type."""

    signalbus = current_app.extensions['signalbus']
    pending = []
    total_pending = 0
    for signal_model in signalbus.get_signal_models():
        count = signal_model.query.count()
        if count > 0:
            pending.append((count, signal_model.__name__))
        total_pending += count
    if pending:
        pending.sort()
        max_chars = len(str(pending[-1][0]))
        for n, signal_name in pending:
            click.echo('{} of type "{}"'.format(str(n).rjust(max_chars), signal_name))
    click.echo(25 * '-')
    click.echo('Total pending: {} '.format(total_pending))
