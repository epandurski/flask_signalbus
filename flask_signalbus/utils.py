import logging
import time
from functools import wraps
from sqlalchemy.exc import DBAPIError

__all__ = [
    'DEADLOCK_ERROR_CODES',
    'DBSerializationError',
    'get_db_error_code',
    'retry_on_deadlock',
]


DEADLOCK_ERROR_CODES = ['40001', '40P01']


class DBSerializationError(Exception):
    """The transaction is rolled back due to a race condition."""


def get_db_error_code(exception):
    """Return 5-character SQLSTATE code, or '' if not available.

    Currently `psycopg2`, `psycopg2cffi`, and `MySQL Connector` are supported.
    """

    for attr in ['pgcode', 'sqlstate']:
        error_code = getattr(exception, attr, '')
        if error_code:
            break
    return error_code


def retry_on_deadlock(session, retries=7, min_wait=0.1, max_wait=10.0):
    """Return function decorator that executes the function again in case of a deadlock."""

    def decorator(action):
        """Function decorator that retries `action` in case of a deadlock."""

        @wraps(action)
        def f(*args, **kwargs):
            num_failures = 0
            while True:
                try:
                    return action(*args, **kwargs)
                except (DBAPIError, DBSerializationError) as e:
                    num_failures += 1
                    is_serialization_error = (
                        isinstance(e, DBSerializationError)
                        or get_db_error_code(e.orig) in DEADLOCK_ERROR_CODES
                    )
                    if num_failures > retries or not is_serialization_error:
                        raise
                session.rollback()
                if num_failures > 1:
                    wait_seconds = min(max_wait, min_wait * 2 ** (num_failures - 2))
                    time.sleep(wait_seconds)

        return f

    return decorator


def report_signal_count(signal_count):
    logger = logging.getLogger(__name__)
    if signal_count == 1:
        logger.info('%i signal has been successfully processed.', signal_count)
    elif signal_count > 1:
        logger.info('%i signals have been successfully processed.', signal_count)
