import pytest
from flask_signalbus import DBSerializationError
from mock import Mock


def test_execute_atomic(atomic_db):
    db = atomic_db
    commit = Mock()
    db.session.commit = commit
    var = 1

    with pytest.raises(RuntimeError):
        @db.execute_atomic
        def f1():
            raise RuntimeError
    commit.assert_not_called()

    with pytest.raises(AssertionError):
        @db.execute_atomic
        def f2():
            @db.execute_atomic
            def recursive():
                pass
    commit.assert_not_called()

    @db.execute_atomic
    def f3():
        assert var == 1
        return 666
    commit.assert_called_once()
    assert f3 == 666

    assert db.execute_atomic(lambda x: x, 777) == 777


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
            db.session.merge(o)
    assert len(AtomicModel.query.all()) == 1

    db.session.expunge_all()
    o.value = '2'
    @db.execute_atomic
    def t2():
        with db.retry_on_integrity_error():
            db.session.merge(o)
    debtors = AtomicModel.query.all()
    assert len(debtors) == 1
    assert debtors[0].value == '2'


@pytest.mark.skip('too slow')
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


def test_create_sharding_key(ShardingKey):
    assert ShardingKey().sharding_key_value
    assert hasattr(ShardingKey, 'generate')
