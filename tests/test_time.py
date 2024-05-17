from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import pytest

from kvalchemy.time import db_now, to_expire


def test_db_now():
    now = db_now()
    assert now
    assert now.tzinfo is None


@patch("kvalchemy.time.db_now", return_value=datetime(2021, 1, 1))
def test_to_expire(mock_db_now):
    assert to_expire(None) is None
    assert to_expire(1) == datetime(2021, 1, 1, 0, 0, 1)
    assert to_expire(3.0) == datetime(2021, 1, 1, 0, 0, 3)
    assert to_expire(timedelta(hours=5)) == datetime(2021, 1, 1, 5, 0, 0)
    assert to_expire(datetime(2023, 1, 2)) == datetime(2023, 1, 2)

    with pytest.raises(ValueError):
        to_expire(datetime(2023, 1, 2, tzinfo=timezone.utc))

    with pytest.raises(TypeError):
        to_expire([])
