"""
Home to models for KVAlchemy.
"""
from datetime import datetime

from sqlalchemy import Column, ColumnElement, UniqueConstraint, or_
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy.types import PickleType

from kvalchemy.time import db_now


class Base(DeclarativeBase):
    """
    The base class for all models.
    """

    pass


class ValueMixIn:
    """
    A mixin used to correspond with an object with a value attribute
    """

    value = Column("value", PickleType)

    def __init__(self, value):
        self.value = value


class KVStore(Base, ValueMixIn):
    """
    The table for storing key-value pairs.
    """

    __tablename__ = "kvstore"

    __table_args__ = (UniqueConstraint("key", "tag", name="key_tag_unique"),)

    key: Mapped[str] = mapped_column(primary_key=True)
    tag: Mapped[str] = mapped_column(primary_key=True)

    # Naive datetime (though expected to be UTC)
    expire: Mapped[datetime] = mapped_column(nullable=True)

    @classmethod
    def non_expired_filter(cls) -> ColumnElement[bool]:
        """
        A filter that can be used to find all non expired key-value pairs.
        """
        return or_(KVStore.expire == None, KVStore.expire > db_now())