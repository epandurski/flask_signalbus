import pytest
from flask_sqlalchemy import SQLAlchemy
from flask_signalbus import SignalBus
from .conftest import SignalBusAlchemy


def test_create_signalbus_alchemy(app):
    assert 'signalbus' not in app.extensions
    db = SignalBusAlchemy(app)
    assert 'signalbus' in app.extensions
    assert len(db.signalbus.get_signal_models()) == 0


def test_create_signalbus_alchemy_init_app(app):
    db = SignalBusAlchemy()
    assert 'signalbus' not in app.extensions
    db.init_app(app)
    assert 'signalbus' in app.extensions
    assert len(db.signalbus.get_signal_models()) == 0


def test_create_signalbus_directly(app):
    assert 'signalbus' not in app.extensions
    db = SQLAlchemy(app)
    signalbus = SignalBus(db)
    assert 'signalbus' in app.extensions
    assert not hasattr(db, 'signalbus')
    assert len(signalbus.get_signal_models()) == 0


def test_create_two_signalbuses_directly(app):
    db = SQLAlchemy(app)
    SignalBus(db)
    with pytest.raises(RuntimeError):
        SignalBus(db)


def test_create_signalbus_directly_no_app():
    db = SQLAlchemy()
    with pytest.raises(RuntimeError):
        SignalBus(db)


def test_flush_signal_model(app, signalbus, Signal):
    assert len(signalbus.get_signal_models()) == 1
    signalbus.flush(Signal)


def test_flush_nonsignal_model(app, signalbus, NonSignal):
    assert len(signalbus.get_signal_models()) == 0
    with pytest.raises(RuntimeError):
        signalbus.flush(NonSignal)


def test_flush_all_signal_models(app, signalbus, Signal, NonSignal):
    assert len(signalbus.get_signal_models()) == 1
    signalbus.flush()


def test_send_signal_success(db, signalbus, send_mock, Signal):
    sig = Signal(name='signal', value='1')
    db.session.add(sig)
    db.session.flush()
    sig_id = sig.id
    send_mock.assert_not_called()
    db.session.commit()
    send_mock.assert_called_once_with(sig_id, 'signal', '1')
    assert Signal.query.count() == 0


def test_send_signal_error(db, signalbus, send_mock, Signal):
    sig = Signal(name='error', value='1')
    db.session.add(sig)
    db.session.commit()
    assert send_mock.call_count == 1
    with pytest.raises(ValueError):
        signalbus.flush()
    assert send_mock.call_count == 2
    assert Signal.query.count() == 1


def test_autoflush_false(db, signalbus, send_mock, Signal):
    db.session.add(Signal(name='signal', value='1'))
    signalbus.autoflush = False
    db.session.commit()
    assert send_mock.call_count == 0
    assert Signal.query.count() == 1
    signalbus.flush()
    assert send_mock.call_count == 1
    assert Signal.query.count() == 0

    db.session.add(Signal(name='signal', value='2'))
    signalbus.autoflush = True
    db.session.commit()
    assert send_mock.call_count == 2
    assert Signal.query.count() == 0


def test_send_nonsignal_model(db, signalbus, send_mock, NonSignal):
    assert len(signalbus.get_signal_models()) == 0
    db.session.add(NonSignal())
    db.session.flush()
    db.session.commit()
    assert NonSignal.query.count() == 1
