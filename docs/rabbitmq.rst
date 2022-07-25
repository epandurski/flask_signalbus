``rabbitmq`` module
===================

.. module:: flask_signalbus.rabbitmq

.. _Pika: https://pika.readthedocs.io/


This module contains utilities for processing RabbitMQ messages. It
requires `Pika`_ to be installed.


Classes
```````

.. autoclass:: Publisher
   :members:

.. autoclass:: Message
   :members:

.. autoclass:: MessageProperties
   :members:

.. autoclass:: Consumer
   :members:


Exceptions
``````````

.. autoclass:: DeliveryError
   :members:
   :show-inheritance:

.. autoclass:: ConnectionError
   :members:
   :show-inheritance:

.. autoclass:: TimeoutError
   :members:
   :show-inheritance:

.. autoclass:: TerminatedConsumtion
   :members:
   :show-inheritance:
