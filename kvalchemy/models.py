"""
Home to models for KVAlchemy.
"""
from datetime import datetime

from sqlalchemy import Column, ColumnElement, UniqueConstraint, or_
from sqlalchemy.dialects.mysql import LONGBLOB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy.types import LargeBinary, PickleType, Unicode

from kvalchemy.time import db_now

KEY_MAX_LENGTH = 256
TAG_MAX_LENGTH = 256


class Base(DeclarativeBase):
    """
    The base class for all models.
    """

    pass


class ValueMixIn:
    """
    A mixin used to correspond with an object with a value attribute
    """

    value = Column(
        "value", PickleType(impl=LargeBinary().with_variant(LONGBLOB, "mysql"))
    )

    def __init__(self, value):
        self.value = value


class KVStore(Base, ValueMixIn):
    """
    The table for storing key-value pairs.
    """

    __tablename__ = "kvstore"

    __table_args__ = (UniqueConstraint("key", "tag", name="key_tag_unique"),)

    key: Mapped[str] = Column(Unicode(KEY_MAX_LENGTH), primary_key=True)
    tag: Mapped[str] = Column(Unicode(TAG_MAX_LENGTH), primary_key=True)

    # Naive datetime (though expected to be UTC)
    expire: Mapped[datetime] = mapped_column(nullable=True)

    @classmethod
    def non_expired_filter(cls) -> ColumnElement[bool]:
        """
        A filter that can be used to find all non expired key-value pairs.
        """
        return or_(KVStore.expire == None, KVStore.expire > db_now())
