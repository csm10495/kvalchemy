"""
Home to time related utilities.
"""
from datetime import datetime, timedelta, timezone
from typing import Optional, Union

ExpirationType = Union[None, int, float, timedelta, datetime]


def db_now() -> datetime:
    """
    Returns a datetime without timezone information, but the current time in UTC.
    This is the format of datetime that should be stored in the database.
    """
    return datetime.now(timezone.utc).replace(tzinfo=None)


def to_expire(exp: ExpirationType) -> Optional[datetime]:
    """
    Converts the given expiration value to a db-compatible datetime (or None).

    If exp is a float or int, it is treated as the number of seconds from now.
    If exp is a timedelta, it is treated as the duration from now.
    If exp is a datetime, it is treated as the exact expiration time.
        This must be in UTC and naive (no timezone information).
    If exp is None, it is treated as no expiration.
    """
    if exp is None:
        return None
    elif isinstance(exp, (float, int)):
        return db_now() + timedelta(seconds=exp)
    elif isinstance(exp, timedelta):
        return db_now() + exp
    elif isinstance(exp, datetime):
        if exp.tzinfo:
            raise ValueError("exp must be naive datetime (representing UTC)")
        return exp
    else:
        raise TypeError("exp must be None, int, float, timedelta, or datetime")
