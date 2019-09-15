API Reference
=============

.. module:: flask_signalbus


.. _signal-model:

Signal Model
````````````

A *signal model* is an otherwise normal database model class (a
subclass of ``db.Model``), which however has a
``send_signalbus_message`` method defined. For example::

  from flask import Flask
  from flask_sqlalchemy import SQLAlchemy

  app = Flask(__name__)
  db = SQLAlchemy(app)

  class MySignal(db.Model):
      id = db.Column(db.Integer, primary_key=True, autoincrement=True)
      message_text = db.Column(db.Text, nullable=False)
      signalbus_autoflush = False
      signalbus_order_by = (id, db.desc(message_text))

      def send_signalbus_message(self):
          # Send the message to the message bus.
          print(MySignal.__marshmallow_schema__.dumps(self))

- The ``send_signalbus_message`` method should be implemented in such
  a way that when it returns, the message is guaranteed to be
  successfully sent and stored by the broker. Normally, this means
  that an acknowledge has been received for the message from the
  broker.

- The signal model class **may** have a ``send_signalbus_messages``
  *class method* which accepts one positional argument: an iterable of
  instances of the class. The method should be implemented in such a
  way that when it returns, all messages for the passed instances
  are guaranteed to be successfully sent and stored by the broker.
  Implementing a ``send_signalbus_messages`` class method can greatly
  improve performance, because message brokers are usually optimized
  to process messages in batches much more efficiently.

- The signal model class **may** have a ``signalbus_burst_count``
  integer attribute defined, which determines how many individual
  signals can be sent and deleted at once, as a part of one database
  transaction. This can greatly improve performace in some cases when
  auto-flushing is disabled, especially when the
  ``send_signalbus_messages`` class method is implemented
  efficiently. If not defined, it defaults to ``1``.

- The signal model class **may** have a ``signalbus_autoflush``
  boolean attribute defined, which determines if signals of that type
  will be automatically sent over the message bus after each
  transaction commit. If not defined, it defaults to `True`.

- The signal model class **may** have a ``signalbus_order_by`` tuple
  attribute defined, which determines the order in which signals will
  be send over the network by the ``flushordered`` CLI command. If not
  defined, signals will not be ordered.

- *Flask-SignalBus* will automatically (after
  :func:`sqlalchemy.orm.configure_mappers` is invoked) add a bunch of
  attributes on each signal model class.  These are useful when
  serializing instances, before sending them over the network:

  ``__marshmallow__``
    An auto-generated `Marshmallow`_ schema class for serializing and
    deserializing instances of the model class. It is a subclass of
    `marshmallow.Schema`. This attribute will be automatically added
    to all model classes (signal or non-signal). If the
    ``__marshmallow__`` attribute happens to be defined in the model
    class, it **will not** be overridden.

  ``__marshmallow_schema__``
    An instance of the class referred by the ``__marshmallow__``
    attribute.


.. _Marshmallow: http://marshmallow.readthedocs.io/en/latest/


Classes
```````

.. autoclass:: SignalBus
   :members:


Mixins
``````

.. autoclass:: SignalBusMixin
   :members:


.. autoclass:: AtomicProceduresMixin
   :members:


.. autoclass:: flask_signalbus.atomic._ModelUtilitiesMixin
   :members:


Exceptions
``````````

.. autoclass:: DBSerializationError
   :members:
