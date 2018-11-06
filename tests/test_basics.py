import pytest
import flask_signalbus as fsb


def test_init_app(app, db, Signal):
    signalbus = fsb.SignalBus()
    signalbus.init_app(app, db)
    assert len(signalbus.get_signal_models()) == 1
    signalbus.flush(Signal)


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


def test_non_signal_model(db, signalbus, send_mock, NonSignal):
    assert len(signalbus.get_signal_models()) == 0
    db.session.add(NonSignal())
    db.session.flush()
    db.session.commit()
    assert NonSignal.query.count() == 1
