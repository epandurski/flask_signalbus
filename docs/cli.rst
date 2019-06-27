.. _command-line-interface:

Command Line Interface
======================

Flask-SignalBus will register a group of Flask CLI commands, starting
with the prefix ``signalbus``. To see all available commands, use::

    $ flask signalbus --help

To send pending signals over the message bus, use the following
command::

    $ flask signalbus flush


