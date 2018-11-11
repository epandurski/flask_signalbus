import flask
import pytest
import flask_sqlalchemy as fsa
import flask_signalbus as fsb
from mock import Mock


class SignalBusAlchemy(fsb.SignalBusMixin, fsa.SQLAlchemy):
    pass


@pytest.fixture
def app(request):
    app = flask.Flask(request.module.__name__)
    app.testing = True
    app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///:memory:'
    app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
    return app


@pytest.fixture(params=['direct', 'mixin_bound', 'mixin_init_app'])
def db(app, request):
    if request.param == 'direct':
        db = fsa.SQLAlchemy(app)
        db.signalbus = fsb.SignalBus(db)
    elif request.param == 'mixin_bound':
        db = SignalBusAlchemy(app)
    elif request.param == 'mixin_init_app':
        db = SignalBusAlchemy()
        db.init_app(app)
        db.app = app

    assert hasattr(db, 'signalbus')
    assert db.get_app()
    return db


@pytest.fixture
def signalbus(app, db):
    return db.signalbus


@pytest.fixture
def signalbus_with_pending_signal(app, db, signalbus, Signal):
    signalbus.autoflush = False
    sig = Signal(name='signal', value='1')
    db.session.add(sig)
    db.session.commit()
    signalbus.autoflush = True
    return signalbus


@pytest.fixture
def signalbus_with_pending_error(app, db, signalbus, Signal):
    signalbus.autoflush = False
    sig = Signal(name='error', value='1')
    db.session.add(sig)
    db.session.commit()
    signalbus.autoflush = True
    return signalbus


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
