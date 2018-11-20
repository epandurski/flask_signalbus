.. Flask-SignalBus documentation master file, created by
   sphinx-quickstart on Mon Nov 19 17:05:28 2018.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to Flask-SignalBus's documentation!
===========================================

**Flask-SignalBus** adds to Flask-SQLAlchemy the capability to
atomically send messages (signals) over a message bus.

The processing of each message involves three steps:

  1. The message is recorded in the SQL database as a row in a table.

  2. The message is sent over the message bus (RabbitMQ for example).

  3. Message's corresponding table row is deleted.

Normally, the sending of the recorded messages (steps 2 and 3) is done
automatically after each transaction commit, but it can also be
triggered explicitly with a `method call <SignalBus.flush>`, or
through the Flask `command-line-interface`.


Installation
````````````

You can install Flask-SignalBus with :command:`pip`::

    $ pip install Flask-SignalBus


Usage
`````

Each type of message (signal) that we plan to send over the message
bus should have a corresponding database model class defined. For
example::

  from flask import Flask
  from flask_sqlalchemy import SQLAlchemy
  from flask_signalbus import SignalBus

  app = Flask(__name__)
  db = SQLAlchemy(app)
  signalbus = SignalBus(db)

  class MySignal(db.Model):
      id = db.Column(db.Integer, primary_key=True, autoincrement=True)
      message_text = db.Column(db.Text, nullable=False)

      def send_signalbus_message(self):
          # Write some code here, that sends
          # the message over the message bus!
          pass

Here, ``MySignal`` represent the particular type of message that we
will be sending over the message bus. Now each time we add a new
object of this type to ``db.session``, Flask-SignalBus will take note
of that, and when the database transaction is committed, will call the
``MySignal.send_signalbus_message`` method, and delete the
corresponding row in the database table. All this will happen
automatically, so we only need to add our message to the database
session as part the database transaction::

  # Insert/delete/update some database rows here!

  # Add our message to the database session:
  msg = MySignal(message_text='Message in a Bottle')
  db.session.add(msg)

  # Insert/delete/update some more database rows here!

  db.commit()


Pending Signals
```````````````

If our program is terminated after a message has been recorded in the
SQL database, but before it has been sent over the message bus, the
row representing the message will remain in the database. We call this
a *pending signal*.

In order to guarantee that all pending signals are processed in time,
even when the application that generates them does not work, it is
recommended that pending signals are flushed periodically, separately
from the application that generates them. (You can do this as part of
a ``cron`` job, for example. See `command-line-interface`.)


Using Application Factory Pattern
`````````````````````````````````

To use Falsk-SignalBus with the application factory pattern, you
should subclass the `flask_sqlalchemy.SQLAlchemy` class, adding the
`SignalBusMixin` mixin. For example::

  from flask import Flask
  from flask_sqlalchemy import SQLAlchemy
  from flask_signalbus import SignalBusMixin

  class CustomSQLAlchemy(SignalBusMixin, SQLAlchemy):
      pass

  app = Flask(__name__)
  db = CustomSQLAlchemy(app)

Note that `SignalBusMixin` should always come before
`flask_sqlalchemy.SQLAlchemy`.


Transaction Isolation Level
```````````````````````````

TODO


Contents:

.. toctree::
   :maxdepth: 2

   cli
   api
