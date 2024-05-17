"""
Home to the proxy class for KVAlchemy.
"""
from typing import TYPE_CHECKING, Any

from kvalchemy.time import ExpirationType
from kvalchemy.values import ENOVAL

if TYPE_CHECKING:
    from kvalchemy.client import KVAlchemy  # pragma: no cover


class Proxy:
    """
    A proxy object for get/setting a specific item in the key-value store.
    """

    def __init__(
        self, kva: "KVAlchemy", key: str, default: Any = ENOVAL, tag: str = ""
    ):
        """
        Initializes the proxy object.

        Takes in a KVAlchemy instance, the key, and optionally a default value and tag.
        """
        self.kva = kva
        self.key = key
        self.default = default
        self.tag = tag

    def set(self, value: Any, expire: ExpirationType = None):
        """
        Sets the value (and expiration) of the key/tag combo.
        """
        self.kva.set(self.key, value, tag=self.tag, expire=expire)

    def get(self, default=ENOVAL, return_expiration: bool = False) -> Any:
        """
        Gets the value of the key/tag combo.

        If the default is given, it will be used first if there is no
        corresponding value. If it is not given, will instead use the class's default.
        If neither default is given, may raise a KeyError if not found.

        If return_expiration is True, will return the (value, (expiration datetime or None)) as a tuple
        """
        # The default may have been overloaded by the proxy initializer.
        # Precedence is default for this func, then default for this class.
        if default != ENOVAL:
            default_to_use = default
        elif self.default != ENOVAL:
            default_to_use = self.default
        else:
            default_to_use = ENOVAL

        return self.kva.get(
            self.key,
            default=default_to_use,
            tag=self.tag,
            return_expiration=return_expiration,
        )

    def delete(self):
        """
        Deletes the key/tag combo from the store.
        """
        self.kva.delete(self.key, tag=self.tag)
