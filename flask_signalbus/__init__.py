"""
Adds to Flask-SQLAlchemy the capability to atomically send
messages (signals) over a message bus.
"""

import time
import logging
import random
from functools import wraps
from sqlalchemy import event, inspect
from sqlalchemy.exc import DBAPIError
from sqlalchemy.sql.expression import and_
from . import cli

ERROR_CODE_ATTRS = ['pgcode', 'sqlstate']
DEADLOCK_ERROR_CODES = ['40001', '40P01']
SIGNALS_TO_FLUSH_SESSION_INFO_KEY = 'flask_signalbus__signals_to_flush'


def get_db_error_code(exception):
    """Return 5-character SQLSTATE code, or '' if not available.

    Currently `psycopg2`, `psycopg2cffi`, and `MySQL Connector` are supported.
    """

    for attr in ERROR_CODE_ATTRS:
        error_code = getattr(exception, attr, '')
        if error_code:
            break
    return error_code


def retry_on_deadlock(session, retries=6, min_wait=0.1, max_wait=10.0):
    """Return function decorator that executes the function again in case of a deadlock."""

    def decorator(action):
        """Function decorator that retries `action` in case of a deadlock."""

        @wraps(action)
        def f(*args, **kwargs):
            num_failures = 0
            while True:
                try:
                    return action(*args, **kwargs)
                except DBAPIError as e:
                    num_failures += 1
                    if num_failures > retries or get_db_error_code(e.orig) not in DEADLOCK_ERROR_CODES:
                        raise
                session.rollback()
                wait_seconds = min(max_wait, min_wait * 2 ** (num_failures - 1))
                time.sleep(wait_seconds)

        return f

    return decorator


def is_serializaion_error(e):
    return isinstance(e, DBAPIError) and get_db_error_code(e.orig) in DEADLOCK_ERROR_CODES


class SignalBusMixin(object):
    """A **mixin class** that can be used to extend the
    `flask_sqlalchemy.SQLAlchemy` class to handle signals.

    For example::

        from flask import Flask
        from flask_sqlalchemy import SQLAlchemy
        from flask_signalbus import SignalBusMixin

        class CustomSQLAlchemy(SignalBusMixin, SQLAlchemy):
            pass

        app = Flask(__name__)
        db = CustomSQLAlchemy(app)
        db.signalbus.flush()

    """

    def init_app(self, app, *args, **kwargs):
        super(SignalBusMixin, self).init_app(app, *args, **kwargs)
        self.signalbus._init_app(app)

    @property
    def signalbus(self):
        """The associated `SignalBus` object."""

        try:
            signalbus = self.__signalbus
        except AttributeError:
            signalbus = self.__signalbus = SignalBus(self, init_app=False)
        return signalbus


class SignalBus(object):
    """Instances of this class automatically send signal messages that
    have been recorded in the SQL database, over a message
    bus. Normally, the sending of the recorded messages (if there are
    any) is done after each transaction commit, but it also can be
    triggered explicitly by a command.

    :param db: The `flask_sqlalchemy.SQLAlchemy` instance

    For example::

        from flask_sqlalchemy import SQLAlchemy
        from flask_signalbus import SignalBus

        app = Flask(__name__)
        db = SQLAlchemy(app)
        signalbus = SignalBus(db)
        signalbus.flush()

    """

    def __init__(self, db, init_app=True):
        self.db = db
        self.signal_session = self.db.create_scoped_session({'expire_on_commit': False})
        self.logger = logging.getLogger(__name__)
        self._autoflush = True
        retry = retry_on_deadlock(self.signal_session, retries=10, max_wait=1.0)
        self._flush_signals_with_retry = retry(self._flush_signals)
        event.listen(self.db.session, 'transient_to_pending', self._transient_to_pending_handler)
        event.listen(self.db.session, 'after_commit', self._after_commit_handler)
        if init_app:
            if db.app is None:
                raise RuntimeError(
                    'No application found. The SQLAlchemy instance passed to'
                    ' SignalBus should be constructed with an application.'
                )
            self._init_app(db.app)

    @property
    def autoflush(self):
        """Setting this property to `False` instructs the `SignalBus` instance
        to not automatically flush pending signals after each
        transaction commit. Setting it back to `True` restores the
        default behavior.

        """

        return self._autoflush

    @autoflush.setter
    def autoflush(self, value):
        self._autoflush = bool(value)

    def get_signal_models(self):
        """Return all signal types in a list.

        :rtype: list(`signal-model`)

        """

        base = self.db.Model
        return [
            cls for cls in base._decl_class_registry.values() if (
                isinstance(cls, type) and
                issubclass(cls, base) and
                hasattr(cls, 'send_signalbus_message')
            )
        ]

    def flush(self, model=None):
        """Send all pending signals over the message bus.

        :param model: If passed, flushes only signals of the specified type.
        :type model: `signal-model` or `None`
        :return: The total number of signals that have been sent

        """

        models_to_flush = [model] if model else self.get_signal_models()
        try:
            return sum(self._flush_signals_with_retry(m) for m in models_to_flush)
        finally:
            self.signal_session.remove()

    def _init_app(self, app):
        if not hasattr(app, 'extensions'):
            app.extensions = {}
        if app.extensions.get('signalbus') not in [None, self]:
            raise RuntimeError('Can not attach more than one SignalBus to one application.')
        app.extensions['signalbus'] = self
        app.cli.add_command(cli.signalbus)

    def _transient_to_pending_handler(self, session, instance):
        model = type(instance)
        if hasattr(model, 'send_signalbus_message'):
            signals_to_flush = session.info.setdefault(SIGNALS_TO_FLUSH_SESSION_INFO_KEY, set())
            signals_to_flush.add(instance)

    def _after_commit_handler(self, session):
        signals_to_flush = session.info.setdefault(SIGNALS_TO_FLUSH_SESSION_INFO_KEY, set())
        if self.autoflush:
            for signal in signals_to_flush:
                model = type(signal)
                m = inspect(model)
                pk_attrs = [m.get_property_by_column(c).class_attribute for c in m.primary_key]
                pk_values = m.primary_key_from_instance(signal)
                clause = and_(*[attr == value for attr, value in zip(pk_attrs, pk_values)])
                try:
                    self.signal_session.query(model).filter(clause).delete()
                    signal.send_signalbus_message()
                except Exception as e:
                    if not is_serializaion_error(e):
                        self.logger.exception('Caught error while flushing %s.', model.__name__)
                    self.signal_session.rollback()
                    break
            self.signal_session.commit()
            self.signal_session.expire_all()
        elif signals_to_flush:
            self.logger.debug('Flushing skipped, "autoflush" is False.')
        signals_to_flush.clear()

    def _flush_signals(self, model):
        if not hasattr(model, 'send_signalbus_message'):
            raise RuntimeError(
                '{} can not be flushed because it does not have a'
                ' "send_signalbus_message" method.'
            )
        self.logger.debug('Flushing %s.', model.__name__)
        burst_count = int(getattr(model, 'signalbus_burst_count', 1))
        signal_count = 0
        signals = self.signal_session.query(model).all()
        self.signal_session.commit()
        if burst_count > 1:
            # Shuffle signals randomly to avoid systematic deadlocks.
            signals = list(signals)
            random.shuffle(signals)
        for signal in signals:
            self.signal_session.delete(signal)
            signal.send_signalbus_message()
            signal_count += 1
            if signal_count % burst_count == 0:
                self.signal_session.commit()
        self.signal_session.commit()
        self.signal_session.expire_all()
        return signal_count
