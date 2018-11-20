.. Flask-SignalBus documentation master file, created by
   sphinx-quickstart on Mon Nov 19 17:05:28 2018.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to Flask-SignalBus's documentation!
===========================================

**Flask-SignalBus** adds to Flask-SQLAlchemy the capability to
*atomically* send messages (signals) over a message bus.

The processing of each message involves three steps:

  1. The message is recorded in the SQL database as a row in a table.

  2. The message is sent over the message bus (RabbitMQ for example).

  3. Message's corresponding table row is deleted.

Normally, the sending of the recorded messages (steps 2 and 3) is done
automatically after each transaction commit, but when needed, it can
also be triggered explicitly with a `method call <SignalBus.flush>`,
or through the Flask `command-line-interface`.


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

Here, ``MySignal`` represent one particular type of message that we
will be sending over the message bus. Now, each time we add a new
object of type ``MySignal`` to ``db.session``, Flask-SignalBus will
take note of that, and finally, when the database transaction is
committed, it will call the ``MySignal.send_signalbus_message``
method, and delete the corresponding row from the database table. All
this will happen automatically, so that the only thing we need to do
as a part of the database transaction, is to add our message to
``db.session``::

  # =========== Our transaction begins here. ===========

  # We may insert/delete/update some database rows here!!!

  # Here we add our message to the database session:
  db.session.add(MySignal(message_text='Message in a Bottle'))

  # We may insert/delete/update some database rows here too!!!

  db.commit()
  
  # Our transaction is committed. The message has been sent
  # over the message bus. The corresponding row in the
  # database table has been deleted. Auto-magically!

Within one database transaction we can add many messages (signals) of
many different types. As long as they have a
``send_signalbus_message`` method defined, they all will be processed
and sent automatically.


Pending Signals
```````````````

If, for any reason, our program is terminated right after a message
has been recorded in the SQL database, but before it has been sent
over the message bus, the row representing the message will remain in
the database. We call this a *pending signal*.

In order to guarantee that all pending signals are processed and sent
in time, even when the application that generates them is down, it is
recommended that pending signals are flushed periodically,
independently from the application that generates them. This can be
done as part of a ``cron`` job, for example. (See
`command-line-interface`.)


Application Factory Pattern
```````````````````````````

If you want to use the application factory pattern with
Flask-SignalBus, you should subclass the
:class:`~flask_sqlalchemy.SQLAlchemy` class, adding the
`SignalBusMixin` mixin to it. For example::

  from flask_sqlalchemy import SQLAlchemy
  from flask_signalbus import SignalBusMixin

  class CustomSQLAlchemy(SignalBusMixin, SQLAlchemy):
      pass

  db = CustomSQLAlchemy()

Note that `SignalBusMixin` should always come before
:class:`~flask_sqlalchemy.SQLAlchemy`.


Message Ordering, Message Duplication
`````````````````````````````````````

Flask-SignalBus does not give guarantees about the order in which the
messages will be sent over the message bus. Also, sometimes a single
message can be sent more than once. Keep this in mind while designing
your system.


Transaction Isolation Level
```````````````````````````

It is recommended that you use at least ``REPEATABLE_READ``
transaction isolation level with Flask-SignalBus. ``READ COMMITTED``
should be OK too, but in that case you might, more frequently than
necessary, see a single message sent more than once over the message
bus.


Contents:

.. toctree::
   :maxdepth: 2

   cli
   api
