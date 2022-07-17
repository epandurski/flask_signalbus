import pytest
import pika
import time
from flask_signalbus.rabbitmq.publisher import RabbitmqPublisher, Message


@pytest.fixture(params=['direct', 'init_app'])
def publisher(app, request):
    if request.param == 'direct':
        p = RabbitmqPublisher(app)
    elif request.param == 'init_app':
        p = RabbitmqPublisher()
        p.init_app(app)
    return p


@pytest.mark.skip('requires RabbitMQ instance running')
def test_publisher(publisher):
    message = Message('test message', pika.BasicProperties())
    publisher.publish_messages([], '', 'test')
    publisher.publish_messages([message], '', 'test')
    publisher.publish_messages([message, message], '', 'test')
    time.sleep(20)
    publisher.publish_messages([message], '', 'test')
