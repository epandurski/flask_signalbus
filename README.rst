Flask-SignalBus
===============

**Flask-SignalBus** adds to Flask-SQLAlchemy the capability to
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
to other services via a message bus of some sort (RabbitMQ, Kafka,
etc.).

Thus, the processing of each "distributed" event involves three steps:

  1. As part of the original event transaction, one or more messages
     are recorded in the SQL database of the primary handler service
     (as rows in tables).

  2. The messages are sent over the message bus.

  3. Messages' corresponding table rows are deleted.

*Flask-SignalBus* automates this process and make is less error prone.
It automatically sends the recorded messages after each transaction
commit (steps 2 and 3). Also, when needed, the sending of the recorded
messages can be triggered explicitly with a method call, or through
the Flask command line interface.

You can read the docs `here`_.


.. _here: https://flask-signalbus.readthedocs.io/en/latest/
