import flask
import pytest
import flask_sqlalchemy as fsa
import flask_signalbus as fsb
from unittest.mock import Mock


@pytest.fixture
def app(request):
    app = flask.Flask(request.module.__name__)
    app.testing = True
    app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///:memory:'
    app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
    return app


@pytest.fixture
def db(app):
    return fsa.SQLAlchemy(app)


@pytest.fixture
def signalbus(app, db):
    return fsb.SignalBus(app, db)


@pytest.fixture
def send_mock():
    return Mock()


@pytest.fixture
def Signal(db, send_mock):
    class Signal(db.Model):
        __tablename__ = 'test_signal'
        id = db.Column(db.Integer, primary_key=True)
        name = db.Column(db.String(60))
        value = db.Column(db.String(60))

        def send_signalbus_message(self):
            send_mock(self.id, self.name, self.value)
            if self.name == 'error':
                raise ValueError(self.value)

    db.create_all()
    yield Signal
    db.drop_all()


@pytest.fixture
def NonSignal(db):
    class NonSignal(db.Model):
        __tablename__ = 'test_non_signal'
        id = db.Column(db.Integer, primary_key=True)

    db.create_all()
    yield NonSignal
    db.drop_all()
