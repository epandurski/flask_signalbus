.. Flask-SignalBus documentation master file, created by
   sphinx-quickstart on Mon Nov 19 17:05:28 2018.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to Flask-SignalBus's documentation!
===========================================

Release v\ |release|. (:doc:`cli`, :doc:`api`, `Source Code`_)

.. _Source Code: https://github.com/epandurski/flask_signalbus

.. image:: https://badge.fury.io/py/Flask-SignalBus.svg
   :target: https://badge.fury.io/py/Flask-SignalBus

.. image:: https://img.shields.io/badge/License-MIT-yellow.svg
   :target: https://opensource.org/licenses/MIT

.. image:: https://circleci.com/gh/epandurski/flask_signalbus.svg?style=shield
   :target: https://circleci.com/gh/epandurski/flask_signalbus

.. _Flask-SQLAlchemy: http://flask-sqlalchemy.pocoo.org/


**Flask-SignalBus** adds to `Flask-SQLAlchemy`_ the capability to
*atomically* send messages (signals) over a message bus.

The Problem
```````````

In microservices, the temptation to do distributed transactions pops
up all the time.

*Distributed transaction*:
  any situation where a single event results in the mutation of two
  separate sources of data which cannot be committed atomically

One practical and popular solution is to pick one of the services to
be the primary handler for some particular event. This service will
handle the original event with a single commit, and then take
responsibility for asynchronously communicating the secondary effects
to other services via a message bus of some sort (`RabbitMQ`_,
`Kafka`_, etc.).

Thus, the processing of each "distributed" event involves three steps:

  1. As part of the original event transaction, one or more messages
     are recorded in the SQL database of the primary handler service
     (as rows in tables).

  2. The messages are sent over the message bus.

  3. Messages' corresponding table rows are deleted.

*Flask-SignalBus* automates this process and make is less error prone.
It can automatically send the recorded messages after each transaction
commit (steps 2 and 3). Also, the sending of the recorded messages can
be triggered explicitly with a `method call <SignalBus.flush>`, or
through the Flask `command-line-interface`.


.. _RabbitMQ: http://www.rabbitmq.com/
.. _Kafka: http://kafka.apache.org/


Installation
````````````

You can install Flask-SignalBus with :command:`pip`::

    $ pip install Flask-SignalBus


Usage
`````

Each type of message (signal) that we plan to send over the message
bus should have its own database model class defined. For example::

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

Here, ``MySignal`` represent one particular type of message that we
will be sending over the message bus.


Auto-flushing
`````````````

Each time we add a new object of type ``MySignal`` to ``db.session``,
Flask-SignalBus will take note of that, and finally, when the database
transaction is committed, it will call the
``MySignal.send_signalbus_message`` method, and delete the
corresponding row from the database table. All this will happen
automatically, so that the only thing we need to do as a part of the
database transaction, is to add our message to ``db.session``::

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
and sent automatically (flushed).

This *auto-flushing* behavior can be disabled if it is not desired. In
this case, the sending of the recorded messages need to be triggered
explicitly.


Pending Signals
```````````````

When auto-flushing is disabled, or when the program has stopped before
the message had been sent over the message bus, the row representing
the message will remain in the database for some time. We call this a
*pending signal*.

To make sure that pending signals are processed in time, even when the
application that generated them is off-line, it is recommended that
pending signals are flushed periodically, independently from the
application that generates them. This can be done in a ``cron`` job
for example. (See `command-line-interface`.)


Application Factory Pattern
```````````````````````````

If you want to use the Flask application factory pattern with
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

Normally, Flask-SignalBus does not give guarantees about the order in
which the messages are sent over the message bus. Also, sometimes a
single message can be sent more than once. Keep that in mind while
designing your system.

When you want to guarantee that messages are sent in a particular
order, you should disable *auto-flushing*, define a
``signalbus_order_by`` attribute on the model class, and always use
the ``flushordered`` CLI command to flush the messages
explicitly. (Messages can still be sent more than once, though.)


Transaction Management Utilities
````````````````````````````````

As a bonus, **Flask-SignalBus** offers some utilities for transaction
management. See :class:`~flask_signalbus.AtomicProceduresMixin` for
details.


Contents:

.. toctree::
   :maxdepth: 2

   cli
   api
   rabbitmq
