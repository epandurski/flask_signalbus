import time
from functools import wraps
from sqlalchemy.exc import DBAPIError


ERROR_CODE_ATTRS = ['pgcode', 'sqlstate']
DEADLOCK_ERROR_CODES = ['40001', '40P01']


class DBSerializationError(Exception):
    """The transaction is rolled back due to a race condition."""


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
                except (DBAPIError, DBSerializationError) as e:
                    num_failures += 1
                    is_serialization_error = (
                        isinstance(e, DBSerializationError)
                        or get_db_error_code(e.orig) in DEADLOCK_ERROR_CODES
                    )
                    if num_failures > retries or not is_serialization_error:
                        raise
                session.rollback()
                wait_seconds = min(max_wait, min_wait * 2 ** (num_failures - 1))
                time.sleep(wait_seconds)

        return f

    return decorator
