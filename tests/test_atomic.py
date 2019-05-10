import pytest
from mock import Mock
from sqlalchemy.orm import defer
from flask_signalbus.utils import DBSerializationError


def test_atomic(atomic_db):
    db = atomic_db
    commit = Mock()
    rollback = Mock()
    db.session.commit = commit
    db.session.rollback = rollback

    @db.atomic
    def f(x):
        return 1 / x

    with pytest.raises(ZeroDivisionError):
        f(0)
    commit.assert_not_called()
    rollback.assert_called_once()
    assert f(1) == 1
    commit.assert_called_once()
    rollback.assert_called_once()


def test_execute_atomic(atomic_db):
    db = atomic_db
    commit = Mock()
    rollback = Mock()
    db.session.commit = commit
    db.session.rollback = rollback
    var = 1

    with pytest.raises(RuntimeError):
        @db.execute_atomic
        def f1():
            raise RuntimeError
    commit.assert_not_called()
    rollback.assert_called_once()

    @db.execute_atomic
    def f2():
        assert var == 1
        return 666
    commit.assert_called_once()
    rollback.assert_called_once()
    assert f2 == 666

    assert db.execute_atomic(lambda: 777) == 777


def test_nested_execute_atomic(atomic_db):
    db = atomic_db
    commit = Mock()
    rollback = Mock()
    db.session.commit = commit
    db.session.rollback = rollback

    @db.execute_atomic
    def f1():
        @db.execute_atomic
        def f2():
            pass

    commit.assert_called_once()
    rollback.assert_not_called()


def test_retry_on_integrity_error(atomic_db, AtomicModel):
    db = atomic_db
    o = AtomicModel(
        id=1,
        name='test',
        value='1',
    )

    with pytest.raises(AssertionError):
        with db.retry_on_integrity_error():
            db.session.merge(o)
    assert len(AtomicModel.query.all()) == 0

    @db.execute_atomic
    def t1():
        with db.retry_on_integrity_error():
            return db.session.merge(o)
    assert len(AtomicModel.query.all()) == 1
    assert t1 not in db.session
    assert t1.name == 'test'
    assert t1.value == '1'

    db.session.expunge_all()
    o.value = '2'
    @db.execute_atomic
    def t2():
        with db.retry_on_integrity_error():
            db.session.merge(o)
    objects = AtomicModel.query.all()
    assert len(objects) == 1
    assert objects[0].value == '2'


# @pytest.mark.skip('too slow')
def test_retry_on_integrity_error_slow(atomic_db, AtomicModel):
    db = atomic_db
    call_list = []
    o = AtomicModel(
        id=1,
        name='test',
        value='1',
    )
    db.session.merge(o)
    db.session.commit()
    db.session.expunge_all()

    with pytest.raises(DBSerializationError):
        @db.execute_atomic
        def t():
            with db.retry_on_integrity_error():
                call_list.append(1)
                db.session.add(o)
    assert len(call_list) > 1


def test_get_instance(atomic_db, AtomicModel):
    db = atomic_db
    o = AtomicModel(id=1, name='test', value='1')
    assert o not in db.session
    assert AtomicModel.get_instance(o) is None
    db.session.add(o)
    assert AtomicModel.get_instance(o) is o
    assert o in db.session
    pk = o.id
    db.session.commit()
    assert AtomicModel.get_instance(pk) in db.session
    assert AtomicModel.get_instance((pk,)) in db.session
    assert AtomicModel.get_instance(o) in db.session


def test_get_instance_with_options(atomic_db, AtomicModel):
    db = atomic_db
    o = AtomicModel(id=1, name='test', value='1')
    assert o not in db.session
    assert AtomicModel.get_instance(o, defer('value')) is None
    db.session.add(o)
    assert AtomicModel.get_instance(o, defer('value')) is o
    assert o in db.session
    pk = o.id
    db.session.commit()
    assert AtomicModel.get_instance(pk, defer('value')) in db.session
    assert AtomicModel.get_instance((pk,), defer('value')) in db.session
    assert AtomicModel.get_instance(o, defer('value')) in db.session


def test_lock_instance(atomic_db, AtomicModel):
    db = atomic_db
    o = AtomicModel(id=1, name='test', value='1')
    assert o not in db.session
    assert AtomicModel.lock_instance(o) is None
    db.session.add(o)
    assert AtomicModel.lock_instance(o) is o
    assert o in db.session
    pk = o.id
    db.session.commit()
    assert AtomicModel.lock_instance(pk) in db.session
    assert AtomicModel.lock_instance((pk,)) in db.session
    assert AtomicModel.lock_instance(o) in db.session
    assert AtomicModel.lock_instance(o, read=True) in db.session


def test_get_pk_values(atomic_db, AtomicModel):
    o = AtomicModel(id=1, name='test', value='1')
    assert AtomicModel.get_pk_values(o) == (o.id,)
    assert AtomicModel.get_pk_values(1) == (1,)
    assert AtomicModel.get_pk_values((1,)) == (1,)
