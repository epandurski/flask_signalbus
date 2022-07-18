import pytest
import time
from flask_signalbus import rabbitmq
from flask_signalbus.rabbitmq.publisher import DeliverySet


@pytest.fixture(params=['direct', 'init_app'])
def publisher(app, request):
    if request.param == 'direct':
        p = rabbitmq.Publisher(app)
    elif request.param == 'init_app':
        p = rabbitmq.Publisher()
        p.init_app(app)
    return p


def test_delivery_set():
    s = DeliverySet(10, 20)  # from 1 to 19
    assert(not s.all_confirmed)
    assert(s.confirm(13) is True)
    assert(s.confirm(13) is False)
    assert(not s.all_confirmed)
    assert(s.confirm(0) is False)
    assert(not s.all_confirmed)
    assert(s.confirm(18, multiple=True) is True)
    assert(s.confirm(14, multiple=False) is False)
    assert(not s.all_confirmed)
    assert(s.confirm(19) is True)
    assert(s.all_confirmed)


@pytest.mark.skip('requires RabbitMQ instance running')
def test_publisher(publisher):
    message = rabbitmq.Message('test message', rabbitmq.MessageProperties())
    publisher.publish_messages([], '', 'test')
    publisher.publish_messages([message], '', 'test')
    publisher.publish_messages([message, message], '', 'test')
    time.sleep(20)
    publisher.publish_messages([message], '', 'test', timeout=60)
