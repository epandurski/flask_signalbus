import flask
import pytest
from sqlalchemy.engine import Engine
from sqlalchemy import event
import flask_sqlalchemy as fsa
import flask_signalbus as fsb
from mock import Mock


@event.listens_for(Engine, "connect")
def set_sqlite_pragma(dbapi_connection, connection_record):
    cursor = dbapi_connection.cursor()
    cursor.execute("PRAGMA foreign_keys=ON")
    cursor.close()


class SignalBusAlchemy(fsb.SignalBusMixin, fsa.SQLAlchemy):
    pass


class AtomicSQLAlchemy(fsb.AtomicProceduresMixin, fsa.SQLAlchemy):
    pass


@pytest.fixture
def app(request):
    app = flask.Flask(request.module.__name__)
    app.testing = True
    app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///:memory:'
    app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
    app.config['SQLALCHEMY_RECORD_QUERIES'] = True
    app.config['SIGNALBUS_RABBITMQ_URL'] = '?heartbeat=5'
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
def atomic_db(app):
    db = AtomicSQLAlchemy()
    db.init_app(app)
    db.app = app
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
            properties = getattr(self, 'properties', [])
            assert hasattr(Signal, '__marshmallow__')
            dump = Signal.__marshmallow_schema__.dump(self)
            send_mock(
                self.id,
                self.name,
                self.value,
                {p.name: p.value for p in properties},
                dump.data if hasattr(dump, 'data') else dump,
            )
            if self.name == 'error':
                raise ValueError(self.value)

        signalbus_order_by = (id, db.desc(name))

    db.create_all()
    yield Signal
    db.drop_all()


@pytest.fixture
def SignalSendMany(db, send_mock):
    class SignalSendMany(db.Model):
        __tablename__ = 'test_signal_send_many'
        id = db.Column(db.Integer, primary_key=True)
        value = db.Column(db.String(60))

        signalbus_autoflush = False
        signalbus_burst_count = 100
        signalbus_order_by = (value,)

        def send_signalbus_message(self):
            pass

        @classmethod
        def send_signalbus_messages(cls, instances):
            for instance in instances:
                send_mock(instance.id)

    db.create_all()
    yield SignalSendMany
    db.drop_all()


@pytest.fixture
def SignalProperty(db, send_mock, Signal):
    class SignalProperty(db.Model):
        __tablename__ = 'test_signal_property'
        signal_id = db.Column(db.ForeignKey(Signal.id, ondelete='CASCADE'), primary_key=True)
        name = db.Column(db.String(60), primary_key=True)
        value = db.Column(db.String(60))
        signal = db.relationship(Signal, backref=db.backref("properties", passive_deletes='all'))

    db.create_all()
    yield SignalProperty
    db.drop_all()


@pytest.fixture
def NonSignal(db):
    class NonSignal(db.Model):
        __tablename__ = 'test_non_signal'
        id = db.Column(db.Integer, primary_key=True)

    db.create_all()
    yield NonSignal
    db.drop_all()


@pytest.fixture
def AtomicModel(atomic_db):
    db = atomic_db

    class AtomicModel(db.Model):
        __tablename__ = 'test_atomic_model'
        id = db.Column(db.Integer, primary_key=True)
        name = db.Column(db.String(60))
        value = db.Column(db.String(60))

    db.create_all()
    yield AtomicModel
    db.drop_all()
