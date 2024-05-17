from datetime import datetime

import pytest

from kvalchemy.proxy import Proxy
from kvalchemy.values import ENOVAL


def test_proxy(kvalchemy):
    proxy = Proxy(kvalchemy, key="key", default="d", tag="tag")
    assert proxy.kva is kvalchemy
    assert proxy.key == "key"
    assert proxy.default == "d"
    assert proxy.tag == "tag"

    proxy.set("value")
    assert proxy.get() == "value"

    proxy.set("value", -1)

    assert proxy.get() == "d"
    assert proxy.get(return_expiration=True) == ("d", None)
    assert proxy.get("d2") == "d2"

    proxy.set("v2")
    assert proxy.get() == "v2"
    assert proxy.get(return_expiration=True) == ("v2", None)
    assert kvalchemy.get("key", tag="tag") == "v2"

    proxy.delete()
    assert proxy.get() == "d"

    proxy.default = ENOVAL
    with pytest.raises(KeyError):
        proxy.get()

    proxy.set("v3", datetime(9999, 1, 1))
    assert proxy.get() == "v3"
    assert proxy.get(return_expiration=True) == ("v3", datetime(9999, 1, 1))
