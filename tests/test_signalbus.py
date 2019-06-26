import pytest
import flask_sqlalchemy as fsa
from flask_signalbus import SignalBus
from conftest import SignalBusAlchemy
from mock import call


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
    db = fsa.SQLAlchemy(app)
    signalbus = SignalBus(db)
    assert 'signalbus' in app.extensions
    assert not hasattr(db, 'signalbus')
    assert len(signalbus.get_signal_models()) == 0


def test_create_two_signalbuses_directly(app):
    db = fsa.SQLAlchemy(app)
    SignalBus(db)
    with pytest.raises(RuntimeError):
        SignalBus(db)


def test_create_signalbus_directly_no_app():
    db = fsa.SQLAlchemy()
    with pytest.raises(RuntimeError):
        SignalBus(db)


def test_flush_signal_model(app, signalbus, Signal):
    assert len(signalbus.get_signal_models()) == 1
    signalbus.flush([Signal], wait=0.0)


def test_flush_nonsignal_model(app, signalbus, NonSignal):
    assert len(signalbus.get_signal_models()) == 0
    with pytest.raises(RuntimeError):
        signalbus.flush([NonSignal], wait=0.0)


def test_flush_signal_send_many_success(db, signalbus, send_mock, SignalSendMany):
    assert len(signalbus.get_signal_models()) == 1
    sig1 = SignalSendMany(value='b')
    sig2 = SignalSendMany(value='a')
    db.session.add(sig1)
    db.session.add(sig2)
    db.session.flush()
    sig1_id = sig1.id
    sig2_id = sig2.id
    db.session.commit()
    sent_count = signalbus.flushordered([SignalSendMany])
    assert sent_count == 2
    assert send_mock.call_count == 2
    assert send_mock.call_args_list == [call(sig2_id), call(sig1_id)]


def test_flush_all_signal_models(app, signalbus, Signal, NonSignal):
    assert len(signalbus.get_signal_models()) == 1
    signalbus.flush(wait=0.0)


def test_flushmany_signal_model(app, signalbus_with_pending_signal):
    assert signalbus_with_pending_signal.flushmany() == 1


def test_send_signal_success(db, signalbus, send_mock, Signal):
    sig = Signal(name='signal', value='1')
    db.session.add(sig)
    db.session.flush()
    sig_id = sig.id
    send_mock.assert_not_called()
    db.session.commit()
    send_mock.assert_called_once_with(
        sig_id,
        'signal',
        '1',
        {},
        {'id': sig_id, 'name': 'signal', 'value': '1'},
    )
    assert Signal.query.count() == 0


def test_send_signal_error(db, signalbus, send_mock, Signal):
    sig = Signal(name='error', value='1')
    db.session.add(sig)
    db.session.commit()
    assert send_mock.call_count == 1
    with pytest.raises(ValueError):
        signalbus.flush(wait=0.0)
    assert send_mock.call_count == 2
    assert Signal.query.count() == 1


def test_autoflush_false(db, signalbus, send_mock, Signal):
    db.session.add(Signal(name='signal', value='1'))
    signalbus.autoflush = False
    db.session.commit()
    assert send_mock.call_count == 0
    assert Signal.query.count() == 1
    signalbus.flush(wait=0.0)
    assert send_mock.call_count == 1
    assert Signal.query.count() == 0

    db.session.add(Signal(name='signal', value='2'))
    signalbus.autoflush = True
    db.session.commit()
    assert send_mock.call_count == 2
    assert Signal.query.count() == 0


def test_model_autoflush_false(db, signalbus, send_mock, Signal):
    Signal.signalbus_autoflush = False
    db.session.add(Signal(name='signal', value='1'))
    db.session.commit()
    assert send_mock.call_count == 0
    assert Signal.query.count() == 1


def test_send_nonsignal_model(db, signalbus, send_mock, NonSignal):
    assert len(signalbus.get_signal_models()) == 0
    db.session.add(NonSignal())
    db.session.flush()
    db.session.commit()
    assert NonSignal.query.count() == 1
    assert hasattr(NonSignal, '__marshmallow__')


def test_signal_with_props_success(db, send_mock, Signal, SignalProperty):
    sig = Signal(name='signal', value='1')
    sig.properties = [
        SignalProperty(name='first_name', value='John'),
        SignalProperty(name='last_name', value='Doe'),
    ]
    db.session.add(sig)
    db.session.flush()
    assert Signal.query.count() == 1
    assert SignalProperty.query.count() == 2
    sig_id = sig.id
    send_mock.assert_not_called()
    db.session.commit()
    send_mock.assert_called_once()
    args, kwargs = send_mock.call_args
    assert kwargs == {}
    assert len(args) == 5
    assert args[:3] == (sig_id, 'signal', '1')
    props = args[3]
    assert type(props) is dict
    assert len(props) == 2
    assert props['first_name'] == 'John'
    assert props['last_name'] == 'Doe'
    assert Signal.query.count() == 0
    assert SignalProperty.query.count() == 0
    assert args[4]['name'] == 'signal'
    assert args[4]['value'] == '1'


def test_signal_with_props_is_efficient(app, db, Signal, SignalProperty):
    with app.test_request_context():
        sig = Signal(name='signal', value='1')
        sig.properties = [
            SignalProperty(name='first_name', value='John'),
            SignalProperty(name='last_name', value='Doe'),
        ]
        db.session.add(sig)
        db.session.commit()
        all_statements = [q.statement for q in fsa.get_debug_queries()]
        assert not any('SELECT' in s for s in all_statements)


def test_flush_signal_with_props(db, signalbus, send_mock, Signal, SignalProperty):
    signalbus.autoflush = False
    sig = Signal(name='signal', value='1')
    sig.properties = [
        SignalProperty(name='first_name', value='John'),
        SignalProperty(name='last_name', value='Doe'),
    ]
    db.session.add(sig)
    db.session.commit()
    send_mock.assert_not_called()
    signalbus.flush(wait=0.0)
    send_mock.assert_called_once()
    props = send_mock.call_args[0][3]
    assert type(props) is dict
    assert len(props) == 2
    assert props['first_name'] == 'John'
    assert props['last_name'] == 'Doe'
    assert Signal.query.count() == 0
    assert SignalProperty.query.count() == 0
