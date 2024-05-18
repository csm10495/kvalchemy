import time
from datetime import datetime, timedelta
from unittest.mock import patch
from uuid import UUID, uuid4

import pytest

from kvalchemy import KVAlchemy
from kvalchemy.models import KVStore
from kvalchemy.time import db_now


@patch("kvalchemy.client.Base.metadata.create_all")
def test_kvalchemy_init_create_all_true(mock_create_all):
    k = KVAlchemy("sqlite://", create_models=True)
    mock_create_all.assert_called_once_with(k._engine)

    assert k._engine
    assert str(k._engine.url) == "sqlite://"

    assert k._session
    assert k._session_factory


@patch("kvalchemy.client.Base.metadata.create_all")
def test_kvalchemy_init_create_all_false(mock_create_all):
    k = KVAlchemy("sqlite://", create_models=False)
    mock_create_all.assert_not_called()

    assert k._engine
    assert str(k._engine.url) == "sqlite://"
    assert k._session
    assert k._session_factory


def test_session_commit_true_delete_expired_false(kvalchemy, kvstore):
    with kvalchemy.session(delete_expired=False) as session:
        session.add(kvstore)
        session.add(
            KVStore(
                key="key2",
                value="value2",
                tag="",
                expire=(db_now() - timedelta(days=1)),
            )
        )

    with kvalchemy.session() as session:
        result = session.query(KVStore).filter_by(key="key", tag="").one()
        assert result.value == "value"

        result = session.query(KVStore).filter_by(key="key2", tag="").one()
        assert result.value == "value2"

        assert session.query(KVStore).count() == 2


def test_session_commit_true_delete_expired_true(kvalchemy, kvstore):
    with kvalchemy.session() as session:
        session.add(kvstore)
        session.add(
            KVStore(
                key="key2",
                value="value2",
                tag="",
                expire=(db_now() - timedelta(days=1)),
            )
        )

    with kvalchemy.session() as session:
        result = session.query(KVStore).filter_by(key="key", tag="").one()
        assert result.value == "value"
        assert session.query(KVStore).count() == 1


def test_session_commit_false(kvalchemy, kvstore):
    with kvalchemy.session(commit=False) as session:
        session.add(kvstore)

    with kvalchemy.session() as session:
        result = session.query(KVStore).filter_by(key="key", tag="").one_or_none()
        assert result is None


def test_get_no_default(kvalchemy):
    with pytest.raises(KeyError):
        assert kvalchemy.get("key")


def test_get_default(kvalchemy):
    assert kvalchemy.get("key", "lolcats") == "lolcats"


def test_get_expired_no_default(kvalchemy):
    with kvalchemy.session() as session:
        session.add(
            KVStore(
                key="key", value="value", tag="", expire=(db_now() - timedelta(days=1))
            )
        )

    with pytest.raises(KeyError):
        kvalchemy.get("key")


def test_get_expired_default(kvalchemy):
    with kvalchemy.session() as session:
        session.add(
            KVStore(
                key="key", value="value", tag="", expire=(db_now() - timedelta(days=1))
            )
        )

        assert kvalchemy.get("key", "lolcats") == "lolcats"


def test_set_get(kvalchemy):
    kvalchemy.set("key", "value2")
    assert kvalchemy.get("key") == "value2"

    assert kvalchemy.get("key", return_expiration=True) == ("value2", None)


def test_set_get_with_tag(kvalchemy):
    kvalchemy.set("key", "value2", tag="tag")
    kvalchemy.set("key", "value3")
    assert kvalchemy.get("key") == "value3"
    assert kvalchemy.get("key", tag="tag") == "value2"


def test_set_get_with_expire(kvalchemy):
    kvalchemy.set("key", "value", expire=0)
    time.sleep(0.1)
    assert kvalchemy.get("key", "default") == "default"

    kvalchemy.set("key", "value", expire=datetime(9999, 2, 2))
    assert kvalchemy.get("key", return_expiration=True) == (
        "value",
        datetime(9999, 2, 2),
    )


def test_set_get_with_multiple_sets(kvalchemy):
    kvalchemy.set("key", "value")
    kvalchemy.set("key", "value2")
    assert kvalchemy.get("key", "default") == "value2"


def test_iter(kvalchemy):
    kvalchemy.set("z", "value", expire=-10)
    kvalchemy.set("a", "value")
    kvalchemy.set("b", "value2")
    kvalchemy.set("b", "value3", tag="tag")

    items = list(kvalchemy)
    assert len(items) == 3

    assert items[0].key == "a"
    assert items[0].value == "value"
    assert items[0].tag == ""

    assert items[1].key == "b"
    assert items[1].value == "value2"
    assert items[1].tag == ""

    assert items[2].key == "b"
    assert items[2].value == "value3"
    assert items[2].tag == "tag"


def test_len(kvalchemy):
    assert len(kvalchemy) == 0

    kvalchemy.set("key", "value")
    assert len(kvalchemy) == 1

    kvalchemy.set("key", "value2")
    assert len(kvalchemy) == 1

    kvalchemy.set("key2", "value")
    assert len(kvalchemy) == 2

    kvalchemy.set("key2", "value", expire=-10)
    assert len(kvalchemy) == 1


def test_delete(kvalchemy):
    kvalchemy.set("key", "value")
    kvalchemy.delete("key")
    assert len(kvalchemy) == 0

    kvalchemy.set("key", "value", "tag")
    assert len(kvalchemy) == 1

    kvalchemy.delete("key", "tag")
    assert len(kvalchemy) == 0


def test_pop(kvalchemy):
    kvalchemy.set("key", "value")
    assert kvalchemy.pop("key") == "value"
    assert len(kvalchemy) == 0

    kvalchemy.set("key", "value2")
    assert kvalchemy.pop("key2", "default") == "default"
    assert len(kvalchemy) == 1
    assert kvalchemy.pop("key") == "value2"

    assert kvalchemy.pop("key", "default") == "default"
    with pytest.raises(KeyError):
        kvalchemy.pop("key")

    kvalchemy.set("key", "value2", "tag1")
    assert kvalchemy.pop("key", tag="tag1") == "value2"


def test_clear(kvalchemy):
    kvalchemy.set("key", "value")
    kvalchemy.set("key", "value2", "tag1")
    assert len(kvalchemy) == 2

    kvalchemy.clear()
    assert len(kvalchemy) == 0


def test_get_proxy(kvalchemy):
    proxy = kvalchemy.get_proxy("key", "default", "tag")
    assert proxy.kva is kvalchemy
    assert proxy.key == "key"
    assert proxy.default == "default"
    assert proxy.tag == "tag"


def test_delete_tag(kvalchemy):
    kvalchemy.set("key", "value", "tag")
    kvalchemy.set("key2", "value2", "tag")
    assert len(kvalchemy) == 2
    kvalchemy.set("key", "value", "tag2")
    assert len(kvalchemy) == 3

    assert kvalchemy.delete_tag("tag") == 2
    assert len(kvalchemy) == 1
    assert kvalchemy.delete_tag("tag2") == 1
    assert len(kvalchemy) == 0
    assert kvalchemy.delete_tag("tag3") == 0


def test_memoize_simple(kvalchemy):
    global ret_val

    # with parenthesis
    ret_val = True

    @kvalchemy.memoize()
    def f():
        global ret_val
        return ret_val

    assert f() == True
    ret_val = False
    assert f() == True
    f.cache_clear()
    assert f() == False

    # no parenthesis
    ret_val = True

    @kvalchemy.memoize
    def x():
        global ret_val
        return ret_val

    assert x() == True
    ret_val = False
    assert x() == True
    x.cache_clear()
    assert x() == False

    ret_val = True

    @kvalchemy.memoize(expire=0.001)
    def y():
        global ret_val
        return ret_val

    assert y() == True
    ret_val = False
    time.sleep(0.1)
    assert y() == False


def test_memoize_expire_if(kvalchemy):
    @kvalchemy.memoize(expire_if=True)
    def x():
        return uuid4()

    assert len({x() for _ in range(3)}) == 3

    global counter
    counter = 0

    def expire_if():
        global counter
        counter += 1
        return counter < 4

    @kvalchemy.memoize(expire_if=expire_if)
    def y():
        return uuid4()

    assert len({y() for _ in range(10)}) == 3


def test_memoize_skip_saving_to_cache_if(kvalchemy):
    @kvalchemy.memoize(skip_saving_to_cache_if=True)
    def x():
        return uuid4()

    assert len({x() for _ in range(3)}) == 3

    global counter
    counter = 0

    def skip_saving_to_cache_if(val):
        assert isinstance(val, UUID)
        global counter
        counter += 1
        return counter < 4

    @kvalchemy.memoize(skip_saving_to_cache_if=skip_saving_to_cache_if)
    def y():
        return uuid4()

    assert len({y() for _ in range(10)}) == 4
