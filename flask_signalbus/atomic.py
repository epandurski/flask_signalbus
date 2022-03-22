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
    def get_instance(cls, instance_or_pk, *options):
        """Return a model instance in ``db.session`` or `None`.

        :param instance_or_pk: An instance of this model class, or a
          primary key. A composite primary key can be passed as a
          tuple.
        :param options: Arguments to be passed to
          :meth:`~sqlalchemy.orm.query.Query.options`.

        Example::

          @db.atomic
          def increase_account_balance(account, amount):
              # Here `Account` is a subclass of `db.Model`.
              account = Account.get_instance(account)
              account.balance += amount

          # Now `increase_account_balance` can be
          # called with an account instance:
          increase_account_balance(my_account, 100.00)

          # or with an account primary key (1234):
          increase_account_balance(1234, 100.00)

        """

        if isinstance(instance_or_pk, cls):
            if instance_or_pk in cls._flask_signalbus_sa.session:
                return instance_or_pk
            instance_or_pk = inspect(cls).primary_key_from_instance(instance_or_pk)
        return cls.query.options(*options).get(instance_or_pk)

    @classmethod
    def lock_instance(cls, instance_or_pk, *options, **kw):
        """Return a locked model instance in ``db.session`` or `None`.

        :param instance_or_pk: An instance of this model class, or a
          primary key. A composite primary key can be passed as a
          tuple.
        :param options: Arguments to be passed to
          :meth:`~sqlalchemy.orm.query.Query.options`.
        :param kw: Arguments to be passed to
          :meth:`~sqlalchemy.orm.query.Query.with_for_update`.

        """

        mapper = inspect(cls)
        pk_attrs = [mapper.get_property_by_column(c).class_attribute for c in mapper.primary_key]
        pk_values = cls.get_pk_values(instance_or_pk)
        clause = and_(*[attr == value for attr, value in zip(pk_attrs, pk_values)])
        return cls.query.options(*options).filter(clause).with_for_update(**kw).one_or_none()

    @classmethod
    def get_pk_values(cls, instance_or_pk):
        """Return a primary key as a tuple.

        :param instance_or_pk: An instance of this model class, or a
          primary key. A composite primary key can be passed as a
          tuple.

        """

        if isinstance(instance_or_pk, cls):
            cls._flask_signalbus_sa.session.flush()
            instance_or_pk = inspect(cls).primary_key_from_instance(instance_or_pk)
        return instance_or_pk if isinstance(instance_or_pk, tuple) else (instance_or_pk,)


class AtomicProceduresMixin(object):
    """A **mixin class** that adds utility functions to
    :class:`flask_sqlalchemy.SQLAlchemy` and the declarative base.

    For example::

      from flask_sqlalchemy import SQLAlchemy
      from flask_signalbus import AtomicProceduresMixin

      class CustomSQLAlchemy(AtomicProceduresMixin, SQLAlchemy):
          pass

      db = CustomSQLAlchemy()

      # Now `AtomicProceduresMixin` method are available in `db`.

    Note that when subclassing, `AtomicProceduresMixin` should always
    come before :class:`flask_sqlalchemy.SQLAlchemy`. Adding
    `AtomicProceduresMixin` has several useful results:

    1. `AtomicProceduresMixin` method will be available in ``db``.

    2. The classmethods from
       :class:`~flask_signalbus.atomic._ModelUtilitiesMixin` will be
       available in the declarative base class (``db.Model``), and
       therefore in every model class.

    3. Database isolation level will be set to ``REPEATABLE_READ``.

    """

    def apply_driver_hacks(self, app, info, options):
        if info.drivername != 'sqlite' and "isolation_level" not in options:
            options["isolation_level"] = "REPEATABLE_READ"
        return super(AtomicProceduresMixin, self).apply_driver_hacks(app, info, options)

    def make_declarative_base(self, model, *args, **kwargs):
        class model(_ModelUtilitiesMixin, model):
            pass
        declarative_base = super(AtomicProceduresMixin, self).make_declarative_base(model, *args, **kwargs)
        declarative_base._flask_signalbus_sa = self
        return declarative_base

    def atomic(self, func):
        """A decorator that wraps a function in an atomic block.

        Example::

          db = CustomSQLAlchemy()

          @db.atomic
          def f():
              write_to_db('a message')
              return 'OK'

          assert f() == 'OK'

        This code defines the function ``f``, which is wrapped in an
        atomic block. Wrapping a function in an atomic block gives
        several guarantees:

        1. The database transaction will be automatically committed if
           the function returns normally, and automatically rolled
           back if the function raises unhandled exception.

        2. When the transaction is committed, all objects in
           ``db.session`` will be expunged. This means that no lazy
           loading will be performed on them.

        3. If a transaction serialization error occurs during the
           execution of the function, the function will be
           re-executed. (It might be re-executed several times.)

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

            @retry_on_deadlock(session)
            def f(*args, **kwargs):
                r = func(*args, **kwargs)
                session.flush()
                return r

            session_info[_ATOMIC_FLAG_SESSION_INFO_KEY] = True
            try:
                result = f(*args, **kwargs)
                session.expunge_all()
                session.commit()
                return result
            except Exception:
                session.rollback()
                raise
            finally:
                session_info[_ATOMIC_FLAG_SESSION_INFO_KEY] = False

        return wrapper

    def execute_atomic(self, func):
        """A decorator that executes a function in an atomic block (see :meth:`atomic`).

        Example::

          db = CustomSQLAlchemy()

          @db.execute_atomic
          def result():
              write_to_db('a message')
              return 'OK'

          assert result == 'OK'

        This code defines *and executes* the function ``result`` in an
        atomic block. At the end, the name ``result`` holds the value
        returned from the function.
        """

        return self.atomic(func)()

    @contextmanager
    def retry_on_integrity_error(self):
        """Re-raise :class:`~sqlalchemy.exc.IntegrityError` as `DBSerializationError`.

        This is mainly useful to handle race conditions in atomic
        blocks. For example, even if prior to a database INSERT we
        have verified that there is no existing row with the given
        primary key, we still may get an
        :class:`~sqlalchemy.exc.IntegrityError` if another transaction
        inserted a row with this primary key in the meantime. But if
        we do (within an atomic block)::

          with db.retry_on_integrity_error():
              db.session.add(instance)

        then if the before-mentioned race condition occurs,
        `DBSerializationError` will be raised instead of
        :class:`~sqlalchemy.exc.IntegrityError`, so that the
        transaction will be retried (by the atomic block), and the
        second time our prior-to-INSERT check will correctly detect a
        primary key collision.

        Note: :meth:`retry_on_integrity_error` triggers a session
        flush.

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
