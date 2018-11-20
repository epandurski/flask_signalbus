Command Line Interface
======================

.. module:: flask_signalbus


Flask-SignalBus will register a group of Flask CLI commands, starging
with the prefix ``signalbus``. To see all available commands, use::

    $ flask signalbus --help

To send all pending signals over the message bus, use the following
command::

    $ flask signalbus flush


