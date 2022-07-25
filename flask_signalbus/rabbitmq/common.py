import logging

try:
    import pika
    _BasicProperties = pika.BasicProperties
except ImportError as e:
    # This module can not work without `pika` installed, but at least
    # we can allow Sphinx to successfully import the module.
    logger = logging.getLogger(__name__)
    logger.error(exc_info=e)
    _BasicProperties = object


class MessageProperties(_BasicProperties):
    """Basic message properties

    This is an alias for :class:`pika.BasicProperties`.
    """
