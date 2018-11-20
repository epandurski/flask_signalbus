.. _command-line-interface:

Command Line Interface
======================

Flask-SignalBus will register a group of Flask command-line interface
commands, starging with the prefix ``signalbus``. To see all available
commands, use::

    $ flask signalbus --help

To send all pending signals over the message bus, use the following
command::

    $ flask signalbus flush


