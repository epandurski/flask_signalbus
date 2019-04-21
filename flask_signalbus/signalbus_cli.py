import sys
import logging
import click
from flask.cli import with_appcontext
from flask import current_app
from . import SignalBus


@click.group()
def signalbus():
    """Perform SignalBus operations."""


@signalbus.command()
@with_appcontext
@click.option('-e', '--exclude', multiple=True, help='Do not flush signals with the specified name.')
@click.option('-w', '--wait', type=float, help='The number of seconds "flush" will wait after'
              ' obtaining the list of pending signals, to allow concurrent senders to complete.'
              ' The default is %s seconds.' % SignalBus.flush.__defaults__[1])
@click.argument('signal_names', nargs=-1)
def flush(signal_names, exclude, wait):
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
        models_to_flush = [m for m in models_to_flush if m.__name__ in signal_names]
    else:
        wrong_signal_names = exclude - {m.__name__ for m in models_to_flush}
    for name in wrong_signal_names:
        click.echo('Warning: A signal with name "{}" does not exist.'.format(name))
    models_to_flush = [m for m in models_to_flush if m.__name__ not in exclude]
    logger = logging.getLogger(__name__)
    try:
        if wait is not None:
            signal_count = signalbus.flush(models_to_flush, wait=max(0.0, wait))
        else:
            signal_count = signalbus.flush(models_to_flush)
    except Exception:
        logger.exception('Caught error while sending pending signals.')
        sys.exit(1)
    if signal_count == 1:
        logger.warning('%i signal has been successfully processed.', signal_count)
    elif signal_count > 1:
        logger.warning('%i signals have been successfully processed.', signal_count)


@signalbus.command()
@with_appcontext
def flushmany():
    """Send a potentially huge number of pending signals over the message bus.

    This command assumes that the number of pending signals might be
    huge, so that they might not fit into memory. However, it is not
    very smart in handling concurrent senders. It is mostly useful
    when recovering from long periods of disconnectedness from the
    message bus.

    """

    signalbus = current_app.extensions['signalbus']
    signal_count = signalbus.flushmany()
    logger = logging.getLogger(__name__)
    if signal_count == 1:
        logger.warning('%i signal has been successfully processed.', signal_count)
    elif signal_count > 1:
        logger.warning('%i signals have been successfully processed.', signal_count)


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
