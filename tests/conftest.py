import pytest

from kvalchemy import KVAlchemy, KVStore


@pytest.fixture(scope="function")
def kvalchemy():
    return KVAlchemy("sqlite:///:memory:")


@pytest.fixture(scope="function")
def kvstore():
    yield KVStore(
        key="key",
        value="value",
        tag="",
    )
