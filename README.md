Flask-SignalBus
===============

**Flask-SignalBus** adds to Flask-SQLAlchemy the capability to
*atomically* send messages (signals) over a message bus.

The processing of each message involves three steps:

  1. The message is recorded in the SQL database as a row in a table.

  2. The message is sent over the message bus (RabbitMQ for example).

  3. Message's corresponding table row is deleted.

Normally, the sending of the recorded messages (steps 2 and 3) is done
automatically after each transaction commit, but when needed, it can
also be triggered explicitly with a method call, or through the Flask
command line interface.


You can read the docs [here](https://flask-signalbus.readthedocs.io/en/latest/).
