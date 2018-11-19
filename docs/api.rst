API Reference
=============

.. module:: flask_signalbus


Signal Models
`````````````

A *signal model* is an otherwise normal database model class (a
subclass of ``db.Model``), which however has a
``send_signalbus_message`` method defined. For example::

  from flask import Flask
  from flask_sqlalchemy import SQLAlchemy

  app = Flask(__name__)
  db = SQLAlchemy(app)

  class MySignal(db.Model):
      id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
      message = db.Column(db.Text, nullable=False)
      signalbus_burst_count = 10

      def send_signalbus_message(self):
          """Send the message to the enterprise message bus."""

In addition, the signal model class may have a
``signalbus_burst_count`` integer attribute defined, which determines
how many individual signals will be deleted at once, as a part of one
database transaction. This might be useful in some cases, to improve
performace. If not defined, it defaults to ``1``.


Classes
```````

.. autoclass:: SignalBus
   :members:


Mixins
``````

.. autoclass:: SignalBusMixin
   :members:
