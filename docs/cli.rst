.. _command-line-interface:

Command Line Interface
======================

Flask-SignalBus will register a group of Flask CLI commands, starting
with the prefix ``signalbus``. To see all available commands, use::

    $ flask signalbus --help

To flush pending signals which have failed to auto-flush, use::

    $ flask signalbus flush

To send a potentially huge number of pending signals, use::

    $ flask signalbus flushmany

To send all pending signals in predictable order, use::

    $ flask signalbus flushordered

For each of these commands, you can specify the exact type of signals
on which to operate.
