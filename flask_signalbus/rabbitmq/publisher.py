import logging
import re
from threading import local
from typing import Iterable, Optional, NamedTuple

_LOGGER = logging.getLogger(__name__)
_RE_BASIC_ACK = re.compile('^basic.ack$', re.IGNORECASE)

try:
    import pika
    _BasicProperties = pika.BasicProperties
except ImportError as e:
    # This module can not work without `pika` installed, but at least
    # we can allow Sphinx to successfully import the module.
    _LOGGER.error(e)
    _BasicProperties = object


class MessageProperties(_BasicProperties):
    """Basic message properties

    This is an alias for :class:`pika.BasicProperties`.
    """


class Message(NamedTuple):
    """A `typing.NamedTuple` representing a RabbitMQ message to be send

    :param exchange: RabbitMQ exchange name
    :param routing_key: RabbitMQ routing key
    :param body: The message's body
    :param properties: Message properties (see :class:`pika.BasicProperties`)
    :param mandatory: If `True`, requires the message to be added to
      at least one queue.
    """

    exchange: str
    routing_key: str
    body: bytes
    properties: Optional[MessageProperties] = None
    mandatory: bool = False


class DeliverySet:
    def __init__(self, start_tag, message_count):
        self.start_tag = start_tag
        self.confirmed_array = [False for _ in range(start_tag, start_tag + message_count)]
        self.confirmed_count = 0
        self.min_unconfirmed_tag = start_tag

    def confirm(self, delivery_tag, multiple=False):
        confirmed_array = self.confirmed_array
        confirmed_count = 0
        start_tag = self.start_tag
        idx = delivery_tag - start_tag
        if multiple:
            for i in range(self.min_unconfirmed_tag - start_tag, idx + 1):
                if i >= 0 and confirmed_array[i] is False:
                    confirmed_array[i] = True
                    confirmed_count += 1
            next_tag = delivery_tag + 1
            if self.min_unconfirmed_tag < next_tag:
                self.min_unconfirmed_tag = next_tag
        else:
            if idx >= 0 and confirmed_array[idx] is False:
                confirmed_array[idx] = True
                confirmed_count += 1

        self.confirmed_count += confirmed_count
        return confirmed_count > 0

    @property
    def all_confirmed(self):
        return self.confirmed_count == len(self.confirmed_array)


class DeliveryError(Exception):
    """A failed attempt to deliver messages."""


class ConnectionError(DeliveryError):
    """Can not connect to the server."""


class TimeoutError(DeliveryError):
    """The attempt to deliver messages has timed out."""


class Publisher:
    """A RabbitMQ message publisher

    Each instance maintains a separate RabbitMQ connection in every
    thread. If a connection has not been used for longer than the
    heartbeat interval set for the connection, it will be
    automatically closed. A new connection will be open when needed.

    :param app: Optional Flask app object. If not provided `init_app`
      must be called later, providing the Flask app object.
    :param url_config_key: Optional configuration key for the
      RabbitMQ's connection URL

    Example::

        from flask import Flask
        from flask_sqlalchemy import SQLAlchemy
        from flask_signalbus import rabbitmq

        app = Flask(__name__)

        headers = {'header1': 'value1', 'header2': 'value2'}
        properties = rabbitmq.MessageProperties(
            delivery_mode=2,  # This makes the message persistent!
            app_id='example-publisher',
            content_type='application/json',
            headers=headers,
        )
        m1 = rabbitmq.Message(exchange='', routing_key='test',
                              body='Message 1', properties=properties)
        m2 = rabbitmq.Message(exchange='', routing_key='test',
                              body='Message 2', properties=properties,
                              mandatory=True)

        mq = rabbitmq.Publisher(app)
        mq.publish_messages([m1, m2])

    """

    def __init__(self, app=None, *, url_config_key='SIGNALBUS_RABBITMQ_URL'):
        self._state = local()
        self._url_config_key = url_config_key
        if app is not None:
            self.init_app(app)

    def init_app(self, app):
        """Bind the instance to a Flask app object.

        :param app: A Flask app object
        """
        self.app = app
        self._url = app.config[self._url_config_key]
        self._kill_connection()

    @property
    def _channel(self):
        """The channel for the current thread."""
        return getattr(self._state, 'channel', None)

    @_channel.setter
    def _channel(self, new_channel):
        state = self._state
        old_channel = getattr(state, 'channel', None)
        if new_channel is not old_channel:
            if not (old_channel is None or old_channel.is_closed or old_channel.is_closing):
                old_channel.close()
            state.channel = new_channel
            state.message_number = 0

    def _get_connection(self):
        """This method returns the RabbitMQ connection for the current thread.
        If there are no connection, a new one will be created.
        """
        connection = getattr(self._state, 'connection', None)
        if connection is None:
            _LOGGER.info('Connecting to %s', self._url)

            # Tries to connect to RabbitMQ, returning the connection
            # handle. When the connection is established, the
            # _on_connection_open method will be invoked by pika.
            connection = self._state.connection = pika.SelectConnection(
                pika.URLParameters(self._url),
                on_open_callback=self._on_connection_open,
                on_open_error_callback=self._on_connection_open_error,
                on_close_callback=self._on_connection_closed,
            )
        return connection

    def _kill_connection(self):
        """This method closes the RabbitMQ connection for the current thread,
        and throws away the connection object. If there is a channel
        corresponding to the connection, it will be closed and throw
        away too.
        """
        self._channel = None
        connection = getattr(self._state, 'connection', None)
        if connection is not None:
            if not (connection.is_closed or connection.is_closing):
                connection.close()
            self._state.connection = None

    def _on_connection_open(self, connection):
        """This method is called by pika once the connection to RabbitMQ has
        been established. It passes the handle to the connection
        object.
        """
        _LOGGER.info('Connection opened')
        _LOGGER.info('Creating a new channel')

        # This will open a new channel with RabbitMQ by issuing the
        # Channel.Open RPC command. When RabbitMQ confirms the channel
        # is open by sending the Channel.OpenOK RPC reply, the
        # _on_channel_open method will be invoked.
        connection.channel(on_open_callback=self._on_channel_open)

    def _on_connection_open_error(self, troubled_connection, err):
        """This method is called by pika if the connection to RabbitMQ can not
        be established. In this case, we will throw away the
        connection object.
        """
        _LOGGER.error('Connection open failed: %s', err)
        troubled_connection.ioloop.stop()
        if getattr(self._state, 'connection', None) is troubled_connection:
            self._kill_connection()
        self._state.error = ConnectionError(err)

    def _on_connection_closed(self, closed_connection, reason):
        """This method is invoked by pika when the connection to RabbitMQ is
        closed unexpectedly. In this case, we will throw away the
        connection object.
        """
        _LOGGER.info('Connection closed: %s', reason)
        closed_connection.ioloop.stop()
        if getattr(self._state, 'connection', None) is closed_connection:
            self._kill_connection()

    def _on_channel_open(self, channel):
        """This method is invoked by pika when the channel has been opened.
        We use the passed channel object to immediately send the
        queued messages (asynchronously, no blocking).
        """
        _LOGGER.info('Channel opened')
        self._channel = channel
        channel.add_on_close_callback(self._on_channel_closed)
        channel.add_on_return_callback(self._on_return_callback)

        # Send the Confirm.Select RPC method to RabbitMQ to enable
        # delivery confirmations on the channel. The only way to turn
        # this off is to close the channel and create a new one.
        # When the message is confirmed from RabbitMQ, the
        # _on_delivery_confirmation method will be invoked passing in a
        # Basic.Ack or Basic.Nack method from RabbitMQ that will
        # indicate which messages it is confirming or rejecting.
        _LOGGER.info('Issuing Confirm.Select RPC command')
        channel.confirm_delivery(self._on_delivery_confirmation)

        assert channel.is_open
        self._send_messages(channel)

    def _on_channel_closed(self, closed_channel, reason):
        """This method is invoked by pika when RabbitMQ unexpectedly closes
        the channel.  Channels are usually closed if you attempt to do
        something that violates the protocol, such as re-declare an
        exchange or queue with different parameters. In this case, we
        will close the connection and throw away the connection
        object.
        """
        _LOGGER.info('Channel %i was closed: %s', closed_channel, reason)
        if self._channel is closed_channel:
            self._kill_connection()

    def _on_return_callback(self, channel, method, properties, body):
        """This method is invoked by pika when basic_publish is sent a message
        that has been rejected and returned by the server. For
        example, this happens when a message with a `mandatory=True`
        flag can not be routed to any queue.
        """
        self._state.returned_messages = True

    def _on_delivery_confirmation(self, method_frame):
        """This method is invoked by pika when RabbitMQ responds to a
        Basic.Publish RPC command, passing in either a Basic.Ack or
        Basic.Nack frame with the delivery tag of the message that was
        published. The delivery tag is an integer counter indicating
        the message number that was sent on the channel via
        Basic.Publish.

        NOTE: When possible, RabbitMQ sends one delivery confirmation
        for multiple messages. In this case
        `method_frame.method.multiple` will be `True`, meaning that
        this delivery confirmation applies to all messages whose tag
        is less or equal than `method_frame.method.delivery_tag`.
        """
        state = self._state
        connection = getattr(state, 'connection', None)
        if connection is None:
            _LOGGER.warning(
                'A message delivery confirmation will be ignored because a connection '
                'object is not available. This should happen very rarely, or never.')
            return

        method = method_frame.method
        medhod_name = method.NAME
        tag = method.delivery_tag
        multiple = method.multiple
        _LOGGER.debug('Received %s for delivery tag: %i (multiple: %s)',
                      medhod_name, tag, multiple)

        if not _RE_BASIC_ACK.match(medhod_name):
            state.received_nack = True

        pending = state.pending_deliveries
        pending.confirm(tag, multiple)
        if pending.all_confirmed:
            if state.received_nack:
                state.error = DeliveryError('received nack')
            elif state.returned_messages:
                state.error = DeliveryError('returned messages')
            else:
                state.error = None
            connection.ioloop.stop()

    def _send_messages(self, channel):
        """This method publishes messages to RabbitMQ, creating a set of
        pending unconfirmed deliveries. This set will be used to check
        for delivery confirmations in the _on_delivery_confirmations
        method.
        """
        state = self._state
        messages = state.messages

        n = len(messages)
        state.pending_deliveries = DeliverySet(state.message_number + 1, n)
        state.received_nack = False
        state.returned_messages = False
        state.message_number += n
        for m in messages:
            channel.basic_publish(m.exchange, m.routing_key, m.body, m.properties, m.mandatory)
        _LOGGER.debug('Published %i messages', len(messages))

    def publish_messages(
            self,
            messages: Iterable[Message],
            *,
            timeout: Optional[int] = None,
            allow_retry: bool = True,
    ):
        """Publishes messages, waiting for delivery confirmations.

        This method will block until a confirmation from the RabbitMQ
        broker has been received for each of the messages.

        :param messages: The messages to publish
        :param timeout: Optional timeout in seconds

        """
        message_list = messages if isinstance(messages, list) else list(messages)
        if len(message_list) == 0:
            return

        state = self._state
        state.messages = message_list
        state.error = TimeoutError()

        channel = self._channel
        if channel is not None and channel.is_open:
            self._send_messages(channel)

        connection = self._get_connection()
        if timeout is not None:
            connection.ioloop.call_later(timeout, connection.ioloop.stop)

        try:
            connection.ioloop.start()
        except KeyboardInterrupt:
            if not (connection.is_closed or connection.is_closing):
                connection.close()
            if not connection.is_closed:
                # Start the loop again until the connection is fully
                # closed. The loop should stop on its own.
                connection.ioloop.start()
            raise

        error = getattr(state, 'error', None)

        # If at the end of the ioloop the connection is closed, most
        # probably the connection has been closed by the server. In
        # this case, we should retry with a fresh connection, but only
        # once.
        if allow_retry and connection.is_closed and not isinstance(error, ConnectionError):
            _LOGGER.debug('Re-executing publish_messages()')
            return self.publish_messages(message_list, timeout=timeout, allow_retry=False)

        if error is not None:
            raise error
