import time
import logging
from functools import wraps
from sqlalchemy import event
from sqlalchemy.exc import DBAPIError


logger = logging.getLogger(__name__)


ERROR_CODE_ATTRS = ['pgcode', 'sqlstate']
DEADLOCK_ERROR_CODES = ['40001', '40P01']


def get_db_error_code(exception):
    """Return 5-character SQLSTATE code, or '' if not available.

    Currently `psycopg2`, `psycopg2cffi`, and `MySQL Connector` are supported.
    """

    for attr in ERROR_CODE_ATTRS:
        error_code = getattr(exception, attr, '')
        if error_code:
            break
    return error_code


def retry_on_deadlock(session, retries=6, min_wait=0.1, max_wait=10.0, catch_all_errors=False):
    """Return a function decorator."""

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
                        if catch_all_errors:
                            session.rollback()
                            logger.exception('Caught database error while executing "retry_on_deadlock"')
                            break
                        raise
                session.rollback()
                wait_seconds = min(max_wait, min_wait * 2 ** (num_failures - 1))
                time.sleep(wait_seconds)

        return f

    return decorator


class SignalBus:
    def __init__(self, app, db):
        self.app = app
        self.db = db
        self.signal_session = db.create_scoped_session({'expire_on_commit': False})
        self.retry_on_deadlock = retry_on_deadlock(self.signal_session, catch_all_errors=True)
        event.listen(db.session, 'transient_to_pending', self._transient_to_pending_handler)
        event.listen(db.session, 'after_commit', self._process_models)

    @staticmethod
    def get_set_of_models_to_process(session):
        return session.info.setdefault('flask_signalbus__models_to_process', set())

    def process_signals(self, model):
        return self.retry_on_deadlock(self._process_signals)(model)

    def _transient_to_pending_handler(self, session, instance):
        model = type(instance)
        if hasattr(model, 'send_signalbus_message'):
            models_to_process = self.get_set_of_models_to_process(session)
            models_to_process.add(model)

    def _process_models(self, session):
        models_to_process = self.get_set_of_models_to_process(session)
        for model in models_to_process:
            self.process_signals(model)
        models_to_process.clear()

    def _process_signals(self, model):
        logger.debug('Processing %s records.', model.__name__)
        for record in self.signal_session.query(model).all():
            self.signal_session.delete(record)
            self.signal_session.flush()
            record.send_signalbus_message()
            self.signal_session.commit()
        self.signal_session.expire_all()
        self.signal_session.commit()
