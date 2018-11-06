import time
import logging
from functools import wraps
from sqlalchemy import event
from sqlalchemy.exc import DBAPIError
from . import cli


logger = logging.getLogger(__name__)


ERROR_CODE_ATTRS = ['pgcode', 'sqlstate']
DEADLOCK_ERROR_CODES = ['40001', '40P01']
MODELS_TO_PROCESS_SESSION_INFO_KEY = 'flask_signalbus__models_to_process'


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


class SignalBus:
    def __init__(self, app=None, db=None):
        self.db = db
        if app is not None and db is not None:
            self.init_app(app, db)

    def init_app(self, app, db=None):
        self.db = db or self.db
        self.signal_session = db.create_scoped_session({'expire_on_commit': False})
        self.retry_on_deadlock = retry_on_deadlock(self.signal_session)
        event.listen(db.session, 'transient_to_pending', self._transient_to_pending_handler)
        event.listen(db.session, 'after_commit', self._after_commit_handler)
        if not hasattr(app, 'extensions'):
            app.extensions = {}
        signalbus_set = app.extensions.setdefault('signalbus', set())
        signalbus_set.add(self)
        app.cli.add_command(cli.signalbus)

    def get_signal_models(self):
        base = self.db.Model
        return [
            cls for cls in base._decl_class_registry.values() if (
                isinstance(cls, type) and
                issubclass(cls, base) and
                hasattr(cls, 'send_signalbus_message')
            )
        ]

    def flush(self, model=None):
        if model:
            try:
                return self.retry_on_deadlock(self._flush_signals)(model)
            except Exception:
                self.signal_session.rollback()
                self.signal_session.close()
                raise
        else:
            signal_count = 0
            for m in self.get_signal_models():
                signal_count += self.flush(m)
            return signal_count

    def _transient_to_pending_handler(self, session, instance):
        model = type(instance)
        if hasattr(model, 'send_signalbus_message'):
            models_to_process = session.info.setdefault(MODELS_TO_PROCESS_SESSION_INFO_KEY, set())
            models_to_process.add(model)

    def _after_commit_handler(self, session):
        models_to_process = session.info.setdefault(MODELS_TO_PROCESS_SESSION_INFO_KEY, set())
        for model in models_to_process:
            try:
                self.flush(model)
            except Exception:
                logger.exception('Caught error while flushing %s.', model.__name__)
        models_to_process.clear()

    def _flush_signals(self, model):
        logger.debug('Flushing %s.', model.__name__)
        signal_count = 0
        for record in self.signal_session.query(model).all():
            self.signal_session.delete(record)
            self.signal_session.flush()
            record.send_signalbus_message()
            self.signal_session.commit()
            signal_count += 1
        self.signal_session.expire_all()
        self.signal_session.commit()
        return signal_count
