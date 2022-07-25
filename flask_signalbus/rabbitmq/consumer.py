import logging
import threading
import queue
from functools import partial
from .common import MessageProperties

_LOGGER = logging.getLogger(__name__)

try:
    import pika
except ImportError as e:
    # This module can not work without `pika` installed, but at least
    # we can allow Sphinx to successfully import the module.
    _LOGGER.error(exc_info=e)


class _WorkerThread(threading.Thread):
    def __init__(self, consumer):
        super().__init__()
        self.consumer = consumer
        self.add_callback = consumer._connection.add_callback_threadsafe
        self.process_message = consumer.process_message
        self.is_running = False

    def stop(self):
        self.is_running = False

    def run(self):
        self.is_running = True
        app = self.consumer.app
        work_queue = self.consumer._work_queue
        with app.app_context():
            while self.is_running:
                try:
                    args = work_queue.get(timeout=1.0)
                except queue.Empty:
                    continue
                self._process_message(*args)

    def _process_message(self, channel, method, properties, body):
        try:
            ok = self.process_message(body, properties)
        except Exception as e:
            self.consumer._on_error(self.name, e)
            self.stop()
            return

        delivery_tag = method.delivery_tag
        if ok:
            self.add_callback(partial(channel.basic_ack, delivery_tag=delivery_tag))
        else:
            self.add_callback(partial(channel.basic_reject, delivery_tag=delivery_tag, requeue=False))


class TerminatedConsumtion(Exception):
    """The consumption has been terminated."""


class Consumer():
    """A RabbitMQ message consumer

    :param app: Optional Flask app object. If not provided `init_app`
      must be called later, providing the Flask app object.

    :param config_prefix: A prefix for the Flask configuration
      settings for this consumer instance.

    The received messages will be processed by a pool of worker
    threads, created by the consumer instance, after the `start`
    method is called. Each consumer instance maintains a separate
    RabbitMQ connection. If the connection has been closed for some
    reason, the `start` method will throw an exception, and the
    instance will become useless.

    Consumer's parameters are controlled by keys in the
    configuration. For example, if ``config_prefix`` is set to
    ``"RABBITMQ_BROKER"``, the following Flask configuration keys will
    be used:

    * ``RABBITMQ_BROKER_URL`` -- RabbitMQ’s connection URL

    * ``RABBITMQ_BROKER_QUEUE`` -- the name of the RabbitMQ queue to
      consume from

    * ``RABBITMQ_BROKER_THREADS`` -- the number of worker threads in
      the pool

    * ``RABBITMQ_BROKER_PREFETCH_SIZE`` -- Specifies the prefetch
      window size. RabbitMQ will send a message in advance if it is
      equal to or smaller in size than the available prefetch size
      (and also falls into other prefetch limits). The default value
      is zero, meaning “no specific limit”.

    * ``RABBITMQ_BROKER_PREFETCH_COUNT`` -- Specifies a prefetch
      window in terms of whole messages. This field may be used in
      combination with the prefetch-size field. A message will only be
      sent in advance if both prefetch windows allow it. The default
      value is ``1``. Setting a bigger value may give a performance
      improvement.
    """

    def __init__(self, app=None, config_prefix='SIGNALBUS_RABBITMQ'):
        self.config_prefix = config_prefix
        if app is not None:
            self.init_app(app)

    def init_app(self, app):
        """Bind the instance to a Flask app object.

        :param app: A Flask app object
        """
        config = app.config
        prefix = self.config_prefix
        self.app = app
        self.url = config[f'{prefix}_URL']
        self.queue = config[f'{prefix}_QUEUE']
        self.threads = config.get(f'{prefix}_THREADS', 1)
        self.prefetch_size = config.get(f'{prefix}_PREFETCH_SIZE', 0)
        self.prefetch_count = config.get(f'{prefix}_PREFETCH_COUNT', 1)
        assert self.threads >= 1
        self._work_queue = queue.Queue(max(self.prefetch_count // 3, self.threads))

    def start(self):
        """Opens a RabbitMQ connection and starts processing messages until
        one of the following things happen:

        * The connection has been lost.

        * An error has occurred during message processing.

        * The `stop` method has been called on the consumer instance.

        This method blocks and never returns normally. If one of the
        previous things happen, an exception will be thrown.
        """

        self._stopped = False
        self._connection = pika.BlockingConnection(pika.URLParameters(self.url))
        channel = self._connection.channel()
        channel.basic_qos(self.prefetch_size, self.prefetch_count)
        workers = [_WorkerThread(self) for _ in range(self.threads)]
        for worker in workers:
            worker.start()

        try:
            for method, properties, body in channel.consume(self.queue, inactivity_timeout=1.0):
                if method is not None:
                    self._work_queue.put((channel, method, properties, body))
                if self._stopped:
                    break
        except pika.exceptions.ChannelClosed:
            pass

        for worker in workers:
            worker.stop()

        for worker in workers:
            worker.join()

        channel.cancel()
        if self._connection.is_open:
            self._connection.close()

        raise TerminatedConsumtion()

    def process_message(self, body: bytes, properties: MessageProperties) -> bool:
        """This method must be implemented by the sub-classes.

        :param body: message body
        :param properties: message properties

        The method should return `True` if the message has been
        successfully processed, and can be removed from the queue. If
        `False` is returned, this means that the message is malformed,
        and can not be processed. (Usually, malformed messages will be
        send to a "dead letter queue".)
        """

        raise NotImplementedError

    def stop(self, signum=None, frame=None):
        """Orders the consumer to stop.

        This is useful for properly handling process termination. For
        example::

            import signal

            consumer = Consumer(...)  # creates the instance

            signal.signal(signal.SIGINT, consumer.stop)
            signal.signal(signal.SIGTERM, consumer.stop)
            consumer.start()
        """
        self._stopped = True

    def _on_error(self, thread_name, error):
        _LOGGER.error("Exception in thread %s:", thread_name, exc_info=error)
        self.stop()
