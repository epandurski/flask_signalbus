"""
Adds to Flask-SQLAlchemy simple but powerful utilities for
creating consistent and correct database APIs.
"""

import os
import struct
from functools import wraps
from contextlib import contextmanager
from sqlalchemy.sql.expression import and_
from sqlalchemy.inspection import inspect
from sqlalchemy.exc import IntegrityError
from flask_signalbus.utils import DBSerializationError, retry_on_deadlock

__all__ = ['AtomicProceduresMixin', 'ShardingKeyGenerationMixin']


_ATOMIC_FLAG_SESSION_INFO_KEY = 'flask_signalbus__atomic_flag'


@contextmanager
def _retry_on_integrity_error(session):
    session.flush()
    try:
        yield
        session.flush()
    except IntegrityError:
        raise DBSerializationError


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
        atomic block. Wrapping a function in an atomic block gives us
        two guarantees:

        1. The database transaction will be automatically comited if the
           function returns normally, and automatically rolled back if the
           function raises exception.

        2. If a transaction serialization error occurs during the
           execution of the function, the function will re-executed.
           (This may happen several times.)

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
                session.commit()
                return result
            except Exception:
                session.rollback()
                raise
            finally:
                session_info[_ATOMIC_FLAG_SESSION_INFO_KEY] = False

        return wrapper

    def execute_atomic(self, __func__, *args, **kwargs):
        """A decorator that executes a function in an atomic block.

        Example::

          @execute_atomic
          def result():
              write_to_db('a message')
              return 'OK'

          assert result == 'OK'

        This code defines *and executes* the function `result` in an
        atomic block. At the end, the name `result` holds the value
        returned from the function. Executing functions in an atomic block
        gives us two guarantees:

        1. The database transaction will be automatically comited if the
           function returns normally, and automatically rolled back if the
           function raises exception.

        2. If a transaction serialization error occurs during the
           execution of the function, the function will re-executed.
           (This may happen several times.)

        Note: `execute_atomic` can be called with more that one
        argument. The extra arguments will be passed to the function given
        as a first argument. For example::

          result = execute_atomic(write_to_db, 'a message')

        """

        return self.atomic(__func__)(*args, **kwargs)

    def modification(self, func):
        """Raise assertion error if `func` is called outside of atomic block.

        It is highly recommended to decorate all functions that write
        to the database with `db.modification`. This prevents
        accidental use of a function that writes to the database,
        outside of an atomic block.

        """

        @wraps(func)
        def wrapper(*args, **kwargs):
            assert self.session.info.get(_ATOMIC_FLAG_SESSION_INFO_KEY), \
                'calls to "{}" must be wrapped in "execute_atomic"'.format(func.__name__)
            return func(*args, **kwargs)

        return wrapper

    def retry_on_integrity_error(self):
        """Re-raise `IntegrityError` as `DBSerializationError`.

        This is mainly useful to handle race conditions in atomic
        blocks. For example, even if prior to INSERT we verify that there
        is no existing row with the given primary key, we still may get an
        `IntegrityError` if another transaction have insterted it in the
        meantime. But if we do::

          with retry_on_integrity_error():
              db.session.add(instance)

        then if the before-mentioned race condition occurs,
        `DBSerializationError` will be raised instead of `IntegrityError`,
        so that the transaction will be retried (by the atomic block), and
        this time our prior-to-INSERT check will correctly detect a
        primary key collision.

        Note: `retry_on_integrity_error()` triggers a session flush.
        """

        return self.modification(_retry_on_integrity_error)(self.session)


class ShardingKeyGenerationMixin(object):
    """Adds random sharding key generation functionality to a model.

    The model should be defined as follows::

      class SomeModelName(ShardingKeyGenerationMixin, db.Model):
          sharding_key_value = db.Column(db.BigInteger, primary_key=True, autoincrement=False)
    """

    def __init__(self, sharding_key_value=None):
        modulo = 1 << 63
        if sharding_key_value is None:
            sharding_key_value = struct.unpack('>q', os.urandom(8))[0] % modulo or 1
        assert 0 < sharding_key_value < modulo
        self.sharding_key_value = sharding_key_value

    @classmethod
    def generate(cls, sharding_key_value=None, tries=50):
        """Create a unique instance and return its `sharding_key_value`."""

        session = cls._flask_signalbus_sa.session
        for _ in range(tries):
            instance = cls(sharding_key_value=sharding_key_value)
            session.begin_nested()
            session.add(instance)
            try:
                session.commit()
            except IntegrityError:
                session.rollback()
                continue
            return instance.sharding_key_value
        raise RuntimeError('Can not generate a unique sharding key.')
