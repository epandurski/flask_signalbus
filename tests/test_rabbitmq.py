import pytest
import time
from flask_signalbus import rabbitmq


@pytest.fixture(params=['direct', 'init_app'])
def publisher(app, request):
    if request.param == 'direct':
        p = rabbitmq.Publisher(app)
    elif request.param == 'init_app':
        p = rabbitmq.Publisher()
        p.init_app(app)
    return p


@pytest.mark.skip('requires RabbitMQ instance running')
def test_publisher(publisher):
    message = rabbitmq.Message('test message', rabbitmq.MessageProperties())
    publisher.publish_messages([], '', 'test')
    publisher.publish_messages([message], '', 'test')
    publisher.publish_messages([message, message], '', 'test')
    time.sleep(20)
    publisher.publish_messages([message], '', 'test', timeout=60)
