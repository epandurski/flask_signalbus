"""
Adds to Flask-SQLAlchemy simple but powerful utilities for
creating consistent and correct database APIs.
"""

from functools import wraps
from contextlib import contextmanager
from sqlalchemy.sql.expression import and_
from sqlalchemy.inspection import inspect
from sqlalchemy.exc import IntegrityError
from flask_signalbus.utils import DBSerializationError, retry_on_deadlock

__all__ = ['AtomicProceduresMixin']


_ATOMIC_FLAG_SESSION_INFO_KEY = 'flask_signalbus__atomic_flag'


class _ModelUtilitiesMixin(object):
    @classmethod
    def _get_instance(cls, instance_or_pk):
        """Return an instance in `db.session` when given any instance or a primary key."""

        if isinstance(instance_or_pk, cls):
            if instance_or_pk in cls._flask_signalbus_sa.session:
                return instance_or_pk
            instance_or_pk = inspect(cls).primary_key_from_instance(instance_or_pk)
        return cls.query.get(instance_or_pk)

    @classmethod
    def _lock_instance(cls, instance_or_pk, read=False):
        """Return a locked instance in `db.session` when given any instance or a primary key."""

        mapper = inspect(cls)
        pk_attrs = [mapper.get_property_by_column(c).class_attribute for c in mapper.primary_key]
        pk_values = cls._get_pk_values(instance_or_pk)
        clause = and_(*[attr == value for attr, value in zip(pk_attrs, pk_values)])
        return cls.query.filter(clause).with_for_update(read=read).one_or_none()

    @classmethod
    def _get_pk_values(cls, instance_or_pk):
        """Return a primary key as a tuple when given any instance or primary key."""

        if isinstance(instance_or_pk, cls):
            instance_or_pk = inspect(cls).primary_key_from_instance(instance_or_pk)
        return instance_or_pk if isinstance(instance_or_pk, tuple) else (instance_or_pk,)

    @classmethod
    def _conjure_instance(cls, *args, **kwargs):
        """Continuously try to create an instance, flush it to the database, and return it.

        This is useful, for example, when a constructor is defined
        that generates a random primary key, which is not guaranteed
        to be unique.

        Note: This method uses database savepoints to recover after
        unsuccessful database flush. It will not work correctly on
        databases that do not support savepoints. Also, on every
        unsuccessful flush, the transaction will be rolled back to a
        savepoint, which will expire all objects in the session.

        """

        session = cls._flask_signalbus_sa.session
        tries = kwargs.pop('__tries', 50)
        for _ in range(tries):
            instance = cls(*args, **kwargs)
            session.begin_nested()
            session.add(instance)
            try:
                session.commit()
            except IntegrityError:
                session.rollback()
                continue
            return instance
        raise RuntimeError('Can not conjure an instance.')


class AtomicProceduresMixin(object):
    """Adds utility functions to :class:`~flask_sqlalchemy.SQLAlchemy` and the declarative base.

    For example::

      from flask_sqlalchemy import SQLAlchemy
      from flask_signalbus import AtomicProceduresMixin

      class CustomSQLAlchemy(AtomicProceduresMixin, SQLAlchemy):
          pass

      db = CustomSQLAlchemy()

    Note that `AtomicProceduresMixin` should always come before
    :class:`~flask_sqlalchemy.SQLAlchemy`.

    """

    def make_declarative_base(self, model, *args, **kwargs):
        class model(_ModelUtilitiesMixin, model):
            pass
        declarative_base = super(AtomicProceduresMixin, self).make_declarative_base(model, *args, **kwargs)
        declarative_base._flask_signalbus_sa = self
        return declarative_base

    def atomic(self, func):
        """A decorator that wraps a function in an atomic block.

        Example::

          @atomic
          def f():
              write_to_db('a message')
              return 'OK'

          assert f() == 'OK'

        This code defines the function `f`, which is wrapped in an
        atomic block. Wrapping a function in an atomic block gives two
        guarantees:

        1. The database transaction will be automatically committed if
           the function returns normally, and automatically rolled
           back if the function raises unhandled exception.

        2. If a transaction serialization error occurs during the
           execution of the function, the function will be
           re-executed. (This may happen several times.)

        Atomic blocks can be nested, but in this case the outermost
        block takes full control of transaction's life-cycle, and
        inner blocks do nothing.

        """

        @wraps(func)
        def wrapper(*args, **kwargs):
            session = self.session
            session_info = session.info
            if session_info.get(_ATOMIC_FLAG_SESSION_INFO_KEY):
                return func(*args, **kwargs)
            f = retry_on_deadlock(session)(func)
            session_info[_ATOMIC_FLAG_SESSION_INFO_KEY] = True
            try:
                result = f(*args, **kwargs)
                session.flush()
                session.expunge_all()
                session.commit()
                return result
            except Exception:
                session.rollback()
                raise
            finally:
                session_info[_ATOMIC_FLAG_SESSION_INFO_KEY] = False

        return wrapper

    def execute_atomic(self, __func, *args, **kwargs):
        """A decorator that executes a function in an atomic block.

        Example::

          @execute_atomic
          def result():
              write_to_db('a message')
              return 'OK'

          assert result == 'OK'

        This code defines *and executes* the function `result` in an
        atomic block. At the end, the name `result` holds the value
        returned from the function.

        Note: `execute_atomic` can be called with more that one
        argument. The extra arguments will be passed to the function
        given as a first argument. Example::

          result = execute_atomic(write_to_db, 'a message')

        """

        return self.atomic(__func)(*args, **kwargs)

    @contextmanager
    def retry_on_integrity_error(self):
        """Re-raise `IntegrityError` as `DBSerializationError`.

        This is mainly useful to handle race conditions in atomic
        blocks. For example, even if prior to INSERT we verify that there
        is no existing row with the given primary key, we still may get an
        `IntegrityError` if another transaction have insterted it in the
        meantime. But if we do::

          with db.retry_on_integrity_error():
              db.session.add(instance)

        then if the before-mentioned race condition occurs,
        `DBSerializationError` will be raised instead of `IntegrityError`,
        so that the transaction will be retried (by the atomic block), and
        this time our prior-to-INSERT check will correctly detect a
        primary key collision.

        Note: `retry_on_integrity_error` triggers a session flush.

        """

        session = self.session
        assert session.info.get(_ATOMIC_FLAG_SESSION_INFO_KEY), \
            'Calls to "retry_on_integrity_error" must be wrapped in atomic block.'
        session.flush()
        try:
            yield
            session.flush()
        except IntegrityError:
            raise DBSerializationError
