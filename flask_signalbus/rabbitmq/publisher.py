# -*- coding: utf-8 -*-
# pylint: disable=C0111,C0103,R0205

import logging
import json
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

    @property
    def _connection(self):
        """The :class:`pika.SelectConnection` for the current thread. This
        property may change without notice.
        """
        connection = getattr(self._state, "connection", None)
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

    @_connection.deleter
    def _connection(self):
        self._channel = None
        try:
            connection = self._state.connection
        except AttributeError:
            pass
        else:
            if not (connection.is_closed or connection.is_closing):
                connection.close()
            del self._state.connection

    @property
    def _channel(self):
        """The channel for the current thread. This property may change
        without notice.
        """
        return getattr(self._state, "channel", None)

    @_channel.setter
    def _channel(self, value):
        c = self._channel
        if not (c is None or c.is_closed or c.is_closing):
            LOGGER.info('Closing the channel')
            c.close()
        self._state.channel = value

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
        if self._connection is troubled_connection:
            del self._connection

    def _on_connection_closed(self, closed_connection, reason):
        """This method is invoked by pika when the connection to RabbitMQ is
        closed unexpectedly. Since it is unexpected, we will reconnect to
        RabbitMQ if it disconnects.
        """
        LOGGER.warning('Connection closed: %s', reason)
        closed_connection.ioloop.stop()
        if self._connection is closed_connection:
            del self._connection

    def _on_channel_open(self, channel):
        """This method is invoked by pika when the channel has been opened.
        The channel object is passed in so we can make use of it.
        """
        LOGGER.info('Channel opened')
        self._state.deliveries = set()
        self._state.message_number = 0
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

    def _on_channel_closed(self, closed_channel, reason):
        """Invoked by pika when RabbitMQ unexpectedly closes the channel.
        Channels are usually closed if you attempt to do something that
        violates the protocol, such as re-declare an exchange or queue with
        different parameters. In this case, we'll close the connection
        to shutdown the object.
        """
        LOGGER.warning('Channel %i was closed: %s', closed_channel, reason)
        if self._channel is closed_channel:
            del self._connection

    def _on_delivery_confirmation(self, method_frame):
        """Invoked by pika when RabbitMQ responds to a Basic.Publish RPC
        command, passing in either a Basic.Ack or Basic.Nack frame with
        the delivery tag of the message that was published. The delivery tag
        is an integer counter indicating the message number that was sent
        on the channel via Basic.Publish. Here we're just doing house keeping
        to keep track of stats and remove message numbers that we expect
        a delivery confirmation of from the list used to keep track of messages
        that are pending confirmation.
        """
        confirmation_type = method_frame.method.NAME.split('.')[1].lower()
        ack_multiple = method_frame.method.multiple
        delivery_tag = method_frame.method.delivery_tag

        LOGGER.debug('Received %s for delivery tag: %i (multiple: %s)',
                     confirmation_type, delivery_tag, ack_multiple)

        if confirmation_type == 'ack':
            self._acked += 1
        elif confirmation_type == 'nack':
            self._nacked += 1

        del self._deliveries[delivery_tag]

        if ack_multiple:
            for tmp_tag in list(self._deliveries.keys()):
                if tmp_tag <= delivery_tag:
                    self._acked += 1
                    del self._deliveries[tmp_tag]
        """
        NOTE: at some point you would check self._deliveries for stale
        entries and decide to attempt re-delivery
        """

        LOGGER.info(
            'Published %i messages, %i have yet to be confirmed, '
            '%i were acked and %i were nacked', self._message_number,
            len(self._deliveries), self._acked, self._nacked)

    def example_publish_message_code(self):
        """If the class is not stopping, publish a message to RabbitMQ,
        appending a list of deliveries with the message number that was sent.
        This list will be used to check for delivery confirmations in the
        _on_delivery_confirmations method.

        Once the message has been sent, schedule another message to be sent.
        The main reason I put scheduling in was just so you can get a good idea
        of how the process is flowing by slowing down and speeding up the
        delivery intervals by changing the PUBLISH_INTERVAL constant in the
        class.

        """
        if self._channel is None or not self._channel.is_open:
            return

        hdrs = {u'مفتاح': u' قيمة', u'键': u'值', u'キー': u'値'}
        properties = pika.BasicProperties(app_id='example-publisher',
                                          content_type='application/json',
                                          headers=hdrs)

        message = u'مفتاح قيمة 键 值 キー 値'
        self._channel.basic_publish(self.EXCHANGE, self.ROUTING_KEY,
                                    json.dumps(message, ensure_ascii=False),
                                    properties)
        self._message_number += 1
        self._deliveries[self._message_number] = True
        LOGGER.info('Published message # %i', self._message_number)
        self.schedule_next_message()

    def publish_messages(self, messages):
        """Publishes messages to RabbitMQ with delivery confirmation. This
        method blocks until a confirmation from the broker has been
        received for each of the messages.
        """
        self._deliveries = {}
        self._message_number = 0
        connection = self._connection
        try:
            connection.ioloop.start()
        except KeyboardInterrupt:
            connection.close()
            if not connection.is_closed:
                # Loop until we're fully closed, will stop on its own.
                connection.ioloop.start()


def main():
    logging.basicConfig(level=logging.DEBUG, format=LOG_FORMAT)

    # Connect to localhost:5672 as guest with the password guest and virtual host "/" (%2F)
    example = RabbitmqPublisher(
        'amqp://guest:guest@localhost:5672/%2F?connection_attempts=3&heartbeat=3600'
    )
    example.run()


if __name__ == '__main__':
    main()
