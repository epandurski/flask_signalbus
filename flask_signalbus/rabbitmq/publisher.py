import logging
import pika
import collections
import re
from threading import local

_LOGGER = logging.getLogger(__name__)
_RE_BASIC_ACK = re.compile('^basic.ack$', re.IGNORECASE)

Message = collections.namedtuple('Message', ['body', 'properties'])
MessageProperties = pika.BasicProperties


class DeliveryError(Exception):
    """A failed attempt to deliver messages."""


class ConnectionError(DeliveryError):
    """Can not connect to the server."""


class TimeoutError(DeliveryError):
    """The attempt to deliver messages has timed out."""


class Publisher(object):
    def __init__(self, app=None, *, url_config_key='SIGNALBUS_RABBITMQ_URL'):
        self._state = local()
        self._url_config_key = url_config_key
        if app is not None:
            self.init_app(app)

    def init_app(self, app):
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
        delivery_tag = method.delivery_tag
        multiple = method.multiple
        _LOGGER.debug('Received %s for delivery tag: %i (multiple: %s)',
                      medhod_name, delivery_tag, multiple)

        if not _RE_BASIC_ACK.match(medhod_name):
            state.received_nack = True

        this_is_the_last_delivery = self._mark_as_confirmed(delivery_tag, multiple)
        if this_is_the_last_delivery:
            if state.received_nack:
                state.error = DeliveryError('received nack')
            else:
                state.error = None
            connection.ioloop.stop()

    def _mark_as_confirmed(self, delivery_tag, ack_multiple):
        """This method tries to find pending unconfirmed deliveries, and mark
        them as confirmed. It returns `True` if at least one
        unconfirmed delivery has been confirmed, and there are no
        pending unconfirmed deliveries left. Otherwise, it returns
        `False`.
        """
        confirmed = False
        pending_deliveries = getattr(self._state, 'pending_deliveries', None)

        def mark(t):
            nonlocal confirmed
            try:
                pending_deliveries.remove(t)
                confirmed = True
            except KeyError:
                pass

        if pending_deliveries is not None:
            if ack_multiple:
                for tag in list(pending_deliveries):
                    if tag <= delivery_tag:
                        mark(tag)
            else:
                mark(delivery_tag)

        return confirmed and not pending_deliveries

    def _send_messages(self, channel):
        """This method publishes messages to RabbitMQ, creating a set of
        pending unconfirmed deliveries. This set will be used to check
        for delivery confirmations in the _on_delivery_confirmations
        method.
        """
        state = self._state
        exchange = state.exchange
        routing_key = state.routing_key
        messages = state.messages
        old_message_number = state.message_number
        new_message_number = old_message_number + len(messages)
        state.pending_deliveries = set(range(old_message_number + 1, new_message_number + 1))
        state.received_nack = False
        state.message_number = new_message_number
        for m in messages:
            channel.basic_publish(exchange, routing_key, m[0], m[1])
        _LOGGER.debug('Published %i messages', len(messages))

    def publish_messages(self, messages, exchange, routing_key, *, timeout=None, allow_retry=True):
        """Publishes messages to RabbitMQ with delivery confirmation. This
        method blocks until a confirmation from the broker has been
        received for each of the messages.

        Example:

        >>> headers = {u'header1': u'value1', u'header2': u'value2'}
        >>> properties = MessageProperties(
        ....    app_id='example-publisher',
        ...     content_type='application/json',
        ...     headers=headers)
        >>> message = Message(u'Example message', properties)
        >>> publisher.publish_messages('exchange_name', 'routing_key', [message])
        """
        message_list = messages if isinstance(messages, list) else list(messages)
        if len(message_list) == 0:
            return

        state = self._state
        state.messages = message_list
        state.exchange = exchange
        state.routing_key = routing_key
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
            return self.publish_messages(message_list, exchange, routing_key,
                                         timeout=timeout, allow_retry=False)

        if error is not None:
            raise error
