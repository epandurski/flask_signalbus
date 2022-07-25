import datetime
import signal
import pytest
import time
import threading
from flask_signalbus import rabbitmq
from flask_signalbus.rabbitmq.publisher import DeliverySet

BODY = datetime.datetime.now().isoformat().encode()
PROPERTIES = rabbitmq.MessageProperties()


@pytest.fixture
def message():
    return rabbitmq.Message(
        body=BODY,
        properties=PROPERTIES,
        exchange='',
        routing_key='test',
    )


@pytest.fixture(params=['direct', 'init_app'])
def publisher(app, request):
    if request.param == 'direct':
        p = rabbitmq.Publisher(app)
    elif request.param == 'init_app':
        p = rabbitmq.Publisher()
        p.init_app(app)
    return p


@pytest.fixture(params=['direct', 'init_app'])
def consumer(app, request):
    class C(rabbitmq.Consumer):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self._messages = []

        def process_message(self, *args):
            self._messages.append(args)
            return True

    if request.param == 'direct':
        p = C(app)
    elif request.param == 'init_app':
        p = C()
        p.init_app(app)
    return p


def test_delivery_set():
    s = DeliverySet(20, 10)  # from 20 to 29
    assert(not s.all_confirmed)
    assert(s.confirm(23) is True)
    assert(s.confirm(23) is False)
    assert(not s.all_confirmed)
    assert(s.confirm(10) is False)
    assert(not s.all_confirmed)
    assert(s.confirm(28, multiple=True) is True)
    assert(s.confirm(24, multiple=False) is False)
    assert(not s.all_confirmed)
    assert(s.confirm(29) is True)
    assert(s.all_confirmed)


@pytest.mark.skip('requires RabbitMQ instance running')
def test_publisher(publisher, message):
    publisher.publish_messages([])
    publisher.publish_messages([message])
    publisher.publish_messages([message, message])
    time.sleep(20)
    publisher.publish_messages([message], timeout=60)


@pytest.mark.skip('requires RabbitMQ instance running')
def test_consumer(consumer, publisher, message):
    def stop_consuming_after_a_while():
        time.sleep(5)
        consumer.stop()

    threading.Thread(target=stop_consuming_after_a_while).start()
    publisher.publish_messages([message, message, message])

    with pytest.raises(rabbitmq.TerminatedConsumtion):
        consumer.start()

    messages = consumer._messages
    assert len(messages) >= 3
    assert messages[-1] == messages[-2] == messages[-3]
    m = messages[-1]
    assert len(m) == 2
    assert m[0] == BODY
    assert m[1] == PROPERTIES


@pytest.mark.skip('requires RabbitMQ instance running')
def test_consumer_process_error(app, publisher, message):
    consumer = rabbitmq.Consumer(app)

    publisher.publish_messages([message])
    with pytest.raises(rabbitmq.TerminatedConsumtion):
        consumer.start()


@pytest.mark.skip('requires RabbitMQ instance running')
def test_consumer_sigterm(app):
    consumer = rabbitmq.Consumer(app)
    signal.signal(signal.SIGINT, consumer.stop)
    signal.signal(signal.SIGTERM, consumer.stop)

    thread_id = threading.get_ident()

    def send_sigterm_after_a_while():
        time.sleep(5)
        signal.pthread_kill(thread_id, signal.SIGTERM)

    threading.Thread(target=send_sigterm_after_a_while).start()

    with pytest.raises(rabbitmq.TerminatedConsumtion):
        consumer.start()
