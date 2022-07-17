# -*- coding: utf-8 -*-
# pylint: disable=C0111,C0103,R0205

import logging
import pika
from threading import local

LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) '
              '-35s %(lineno) -5d: %(message)s')
LOGGER = logging.getLogger(__name__)


class RabbitmqPublisher(object):
    """This is an example publisher that will handle unexpected interactions
    with RabbitMQ such as channel and connection closures.

    If RabbitMQ closes the connection, it will reopen it. You should
    look at the output, as there are limited reasons why the connection may
    be closed, which usually are tied to permission related issues or
    socket timeouts.

    It uses delivery confirmations and illustrates one way to keep track of
    messages that have been sent and if they've been confirmed by RabbitMQ.

    """

    def __init__(self, app):
        self._state = local()
        if app is not None:
            self.init_app(app)

    def init_app(self, app):
        self.app = app
        self._url = app.config['FLASK_SIGNALBUS_AMQP_URL']
        self._kill_connection()

    @property
    def _channel(self):
        """The channel for the current thread. This property may change
        without notice.
        """
        return self._state.get('channel')

    @_channel.setter
    def _channel(self, new_channel):
        state = self._state
        old_channel = state.get('channel')
        if new_channel is not old_channel:
            if not (old_channel is None or old_channel.is_closed or old_channel.is_closing):
                old_channel.close()
            state.channel = new_channel
            state.deliveries = None
            state.delivery_error = None
            state.message_number = 0

    def _get_connection(self):
        """Return the RabbitMQ connection for the current thread."""
        connection = self._state.get('connection')
        if connection is None:
            LOGGER.info('Connecting to %s', self._url)

            # Tries to connect to RabbitMQ, returning the connection
            # handle.  When the connection is established, the
            # _on_connection_open method will be invoked by pika.
            connection = self._state.connection = pika.SelectConnection(
                pika.URLParameters(self._url),
                on_open_callback=self._on_connection_open,
                on_open_error_callback=self._on_connection_open_error,
                on_close_callback=self._on_connection_closed,
            )
        return connection

    def _kill_connection(self):
        self._channel = None
        connection = self._state.get('connection')
        if connection is not None:
            if not (connection.is_closed or connection.is_closing):
                connection.close()
            self._state.connection = None

    def _on_connection_open(self, connection):
        """This method is called by pika once the connection to RabbitMQ has
        been established. It passes the handle to the connection object in
        case we need it, but in this case, we'll just mark it unused.
        """
        LOGGER.info('Connection opened')
        LOGGER.info('Creating a new channel')

        # This will open a new channel with RabbitMQ by issuing the
        # Channel.Open RPC command. When RabbitMQ confirms the channel
        # is open by sending the Channel.OpenOK RPC reply, the
        # _on_channel_open method will be invoked.
        connection.channel(on_open_callback=self._on_channel_open)

    def _on_connection_open_error(self, troubled_connection, err):
        """This method is called by pika if the connection to RabbitMQ
        can't be established.
        """
        LOGGER.error('Connection open failed: %s', err)
        troubled_connection.ioloop.stop()
        if self._get_connection() is troubled_connection:
            self._kill_connection()

    def _on_connection_closed(self, closed_connection, reason):
        """This method is invoked by pika when the connection to RabbitMQ is
        closed unexpectedly. Since it is unexpected, we will reconnect to
        RabbitMQ if it disconnects.
        """
        LOGGER.info('Connection closed: %s', reason)
        closed_connection.ioloop.stop()
        if self._get_connection() is closed_connection:
            self._kill_connection()

    def _on_channel_open(self, channel):
        """This method is invoked by pika when the channel has been opened.
        The channel object is passed in so we can make use of it.
        """
        LOGGER.info('Channel opened')
        self._channel = channel
        channel.add_on_close_callback(self._on_channel_closed)

        # Send the Confirm.Select RPC method to RabbitMQ to enable
        # delivery confirmations on the channel. The only way to turn
        # this off is to close the channel and create a new one.
        # When the message is confirmed from RabbitMQ, the
        # _on_delivery_confirmation method will be invoked passing in a
        # Basic.Ack or Basic.Nack method from RabbitMQ that will
        # indicate which messages it is confirming or rejecting.
        LOGGER.info('Issuing Confirm.Select RPC command')
        channel.confirm_delivery(self._on_delivery_confirmation)

        assert channel.is_open
        self._send_messages(channel)

    def _on_channel_closed(self, closed_channel, reason):
        """Invoked by pika when RabbitMQ unexpectedly closes the channel.
        Channels are usually closed if you attempt to do something that
        violates the protocol, such as re-declare an exchange or queue with
        different parameters. In this case, we'll close the connection
        to shutdown the object.
        """
        LOGGER.warning('Channel %i was closed: %s', closed_channel, reason)
        if self._channel is closed_channel:
            self._kill_connection()

    def _on_delivery_confirmation(self, method_frame):
        """Invoked by pika when RabbitMQ responds to a Basic.Publish RPC
        command, passing in either a Basic.Ack or Basic.Nack frame
        with the delivery tag of the message that was published. The
        delivery tag is an integer counter indicating the message
        number that was sent on the channel via Basic.Publish.
        """
        state = self._state
        confirmation_type = method_frame.method.NAME.split('.')[1].lower()
        ack_multiple = method_frame.method.multiple
        delivery_tag = method_frame.method.delivery_tag

        # TODO: Fix wasteful logging.
        LOGGER.debug('Received %s for delivery tag: %i (multiple: %s)',
                     confirmation_type, delivery_tag, ack_multiple)

        connection = state.get('connection')
        if connection is None:
            LOGGER.warning(
                'No connection is available in RabbitmqPublisher._on_delivery_confirmation(). '
                'This should happen very rarely or never.')
            return

        marked_as_confirmed = self._mark_as_confirmed(delivery_tag, ack_multiple)
        if marked_as_confirmed and confirmation_type != 'ack':
            state.delivery_error = f'received {confirmation_type}'

        if not state.get('deliveries'):
            connection.ioloop.stop()

    def _mark_as_confirmed(self, deliveries, delivery_tag, ack_multiple):
        if ack_multiple:
            for tag in list(deliveries):
                if tag <= delivery_tag:
                    deliveries.discard(tag)
        else:
            deliveries.discard(delivery_tag)

    def _send_messages(self, channel):
        """Publish a message to RabbitMQ, appending a set of deliveries with
        the message number that was sent. This set will be used to
        check for delivery confirmations in the
        _on_delivery_confirmations method.
        """
        state = self._state
        exchange = state.exchange
        routing_key = state.routing_key
        messages = state.messages
        old_message_number = state.message_number
        new_message_number = old_message_number + len(messages)
        state.deliveries = set(range(old_message_number + 1, new_message_number + 1))
        state.delivery_error = None
        state.message_number = new_message_number
        for m in messages:
            channel.basic_publish(exchange, routing_key, m.body, m.properties)
        LOGGER.debug('Published %i messages', len(messages))

        # hdrs = {u'مفتاح': u' قيمة', u'键': u'值', u'キー': u'値'}
        # properties = pika.BasicProperties(app_id='example-publisher',
        #                                   content_type='application/json',
        #                                   headers=hdrs)

        # message = u'مفتاح قيمة 键 值 キー 値'
        # self._channel.basic_publish(self.EXCHANGE, self.ROUTING_KEY,
        #                             json.dumps(message, ensure_ascii=False),
        #                             properties)
        # self._message_number += 1
        # self._deliveries[self._message_number] = True
        # LOGGER.info('Published message # %i', self._message_number)
        # self.schedule_next_message()

    def publish_messages(self, messages, exchange, routing_key, *, allow_retry=True):
        """Publishes messages to RabbitMQ with delivery confirmation. This
        method blocks until a confirmation from the broker has been
        received for each of the messages.
        """
        state = self._state
        state.messages = messages if (type(messages) is list) else list(messages)
        state.exchange = exchange
        state.routing_key = routing_key

        if len(state.messages) == 0:
            return

        channel = self._channel
        if channel is not None and channel.is_open:
            self._send_messages(channel)

        connection = self._get_connection()
        try:
            # TODO: Set a timeout.
            connection.ioloop.start()
        except KeyboardInterrupt:
            if not (connection.is_closed or connection.is_closing):
                connection.close()
            if not connection.is_closed:
                # Start the loop again until the connection is fully
                # closed. The loop should stop on its own.
                connection.ioloop.start()
            raise

        # If at the end of the ioloop the connection is closed, most
        # probably the connection has timed out. In this case, we
        # should retry with a fresh connection, but only once.
        if connection.is_closed and allow_retry:
            return self.publish_messages(state.messages, exchange, routing_key, allow_retry=False)


def main():
    logging.basicConfig(level=logging.DEBUG, format=LOG_FORMAT)

    # Connect to localhost:5672 as guest with the password guest and virtual host "/" (%2F)
    example = RabbitmqPublisher(
        'amqp://guest:guest@localhost:5672/%2F?connection_attempts=3&heartbeat=3600'
    )
    example.run()


if __name__ == '__main__':
    main()
