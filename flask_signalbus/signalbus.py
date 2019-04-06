"""
Adds to Flask-SQLAlchemy the capability to atomically send
messages (signals) over a message bus.
"""

import time
import logging
from sqlalchemy import event, inspect, and_
from sqlalchemy.orm import mapper
from sqlalchemy.exc import DBAPIError
from flask_signalbus.utils import retry_on_deadlock, get_db_error_code, DEADLOCK_ERROR_CODES
from marshmallow_sqlalchemy import ModelSchema, ModelConversionError

__all__ = ['SignalBus', 'SignalBusMixin']


_SIGNALS_TO_FLUSH_SESSION_INFO_KEY = 'flask_signalbus__signals_to_flush'
_FLUSHMANY_LIMIT = 1000


def _raise_error_if_not_signal_model(model):
    if not hasattr(model, 'send_signalbus_message'):
        raise RuntimeError(
            '{} can not be flushed because it does not have a'
            ' "send_signalbus_message" method.'
        )


class SignalBusMixin(object):
    """A **mixin class** that can be used to extend
    :class:`~flask_sqlalchemy.SQLAlchemy` to handle signals.

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
        event.listen(self.db.session, 'after_commit', self._safe_after_commit_handler)
        event.listen(self.db.session, 'after_rollback', self._after_rollback_handler)
        event.listen(mapper, 'after_configured', _setup_schema(db.Model, self.db.session))
        if init_app:
            if db.app is None:
                raise RuntimeError(
                    'No application found. The SQLAlchemy instance passed to'
                    ' SignalBus() should be constructed with an application.'
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
                isinstance(cls, type)
                and issubclass(cls, base)
                and hasattr(cls, 'send_signalbus_message')
            )
        ]

    def flush(self, models=None, wait=3.0):
        """Send all pending signals over the message bus.

        :param models: If passed, flushes only signals of the specified types.
        :type models: list(`signal-model`) or `None`
        :param float wait: The number of seconds the method will wait
            after obtaining the list of pending signals, to allow
            concurrent senders to complete
        :return: The total number of signals that have been sent

        """

        models_to_flush = self.get_signal_models() if models is None else models
        pks_to_flush = {}
        try:
            for model in models_to_flush:
                _raise_error_if_not_signal_model(model)
                m = inspect(model)
                pk_attrs = [m.get_property_by_column(c).class_attribute for c in m.primary_key]
                pks_to_flush[model] = self.signal_session.query(*pk_attrs).all()
            self.signal_session.rollback()
            time.sleep(wait)
            return sum(
                self._flush_signals_with_retry(model, pk_values_set=set(pks_to_flush[model]))
                for model in models_to_flush
            )
        finally:
            self.signal_session.remove()

    def flushmany(self):
        """Send a potentially huge number of pending signals over the message bus.

        This method assumes that the number of pending signals might
        be huge, so that they might not fit into memory. However,
        `SignalBus.flushmany` is not very smart in handling concurrent
        senders. It is mostly useful when recovering from long periods
        of disconnectedness from the message bus.

        :return: The total number of signals that have been sent

        """

        models_to_flush = self.get_signal_models()
        try:
            return sum(self._flushmany_signals(model) for model in models_to_flush)
        finally:
            self.signal_session.remove()

    def _init_app(self, app):
        from . import signalbus_cli

        if not hasattr(app, 'extensions'):
            app.extensions = {}
        if app.extensions.get('signalbus') not in [None, self]:
            raise RuntimeError('Can not attach more than one SignalBus to one application.')
        app.extensions['signalbus'] = self
        app.cli.add_command(signalbus_cli.signalbus)

        @app.teardown_appcontext
        def shutdown_signal_session(response_or_exc):
            self.signal_session.remove()
            return response_or_exc

    def _transient_to_pending_handler(self, session, instance):
        model = type(instance)
        if hasattr(model, 'send_signalbus_message') and getattr(model, 'signalbus_autoflush', True):
            signals_to_flush = session.info.setdefault(_SIGNALS_TO_FLUSH_SESSION_INFO_KEY, set())
            signals_to_flush.add(instance)

    def _after_commit_handler(self, session):
        signals_to_flush = session.info.pop(_SIGNALS_TO_FLUSH_SESSION_INFO_KEY, set())
        if self.autoflush and signals_to_flush:
            signals = [self.signal_session.merge(s, load=False) for s in signals_to_flush]
            for signal in signals:
                try:
                    signal.send_signalbus_message()
                except Exception:
                    self.logger.exception('Caught error while sending %s.', signal)
                    self.signal_session.rollback()
                    return
                self.signal_session.delete(signal)
            self.signal_session.commit()
            self.signal_session.expire_all()

    def _after_rollback_handler(self, session):
        session.info.pop(_SIGNALS_TO_FLUSH_SESSION_INFO_KEY, None)

    def _safe_after_commit_handler(self, session):
        try:
            return self._after_commit_handler(session)
        except DBAPIError as e:
            if get_db_error_code(e.orig) not in DEADLOCK_ERROR_CODES:
                self.logger.exception('Caught database error during autoflush.')
            self.signal_session.rollback()

    def _get_lock_for_update(self, pk_attrs, pk_values):
        clause = and_(*[attr == value for attr, value in zip(pk_attrs, pk_values)])
        query = self.signal_session.query(*pk_attrs).filter(clause).with_for_update()
        return query.one_or_none() is not None

    def _flushmany_signals(self, model):
        self.logger.warning('Flushing %s in "flushmany" mode.', model.__name__)
        sent_count = 0
        while True:
            n = self._flush_signals(model, max_count=_FLUSHMANY_LIMIT)
            sent_count += n
            if n < _FLUSHMANY_LIMIT:
                break
        return sent_count

    def _flush_signals(self, model, pk_values_set=None, max_count=None):
        query = self.signal_session.query(model)
        if max_count is None:
            self.logger.info('Flushing %s.', model.__name__)
        else:
            query = query.limit(max_count)
        signals = query.all()
        self.signal_session.commit()
        burst_count = int(getattr(model, 'signalbus_burst_count', 1))
        sent_count = 0
        m = inspect(model)
        pk_attrs = [m.get_property_by_column(c).class_attribute for c in m.primary_key]
        for signal in signals:
            pk_values = m.primary_key_from_instance(signal)
            if pk_values_set is None or pk_values in pk_values_set:
                if not self._get_lock_for_update(pk_attrs, pk_values):
                    # The row has been deleted by a concurrent sender.
                    continue
                signal.send_signalbus_message()
                self.signal_session.delete(signal)
                sent_count += 1
                if sent_count % burst_count == 0:
                    self.signal_session.commit()
        self.signal_session.commit()
        self.signal_session.expire_all()
        return sent_count


def _setup_schema(Base, session):
    """Create a function which adds `__marshmallow__` attribute to all signal models."""

    def create_schema_class(m):
        if hasattr(m, 'send_signalbus_message'):
            # Signal models should not use the SQLAlchemy session.
            class Meta(object):
                model = m
        else:
            class Meta(object):
                model = m
                sqla_session = session

        schema_class_name = '%sSchema' % m.__name__
        return type(schema_class_name, (ModelSchema,), {'Meta': Meta})

    def setup_schema_fn():
        for model in Base._decl_class_registry.values():
            if hasattr(model, '__tablename__'):
                if model.__name__.endswith("Schema"):
                    raise ModelConversionError(
                        'Unexpected model name: "{}". '
                        'For safety, _setup_schema() can not be used when a '
                        'model class ends with "Schema".'.format(model.__name__)
                    )
                schema_class = getattr(model, '__marshmallow__', None)
                if schema_class is None:
                    schema_class = model.__marshmallow__ = create_schema_class(model)
                if hasattr(model, 'send_signalbus_message'):
                    setattr(model, '__marshmallow_schema__', schema_class())

    return setup_schema_fn
