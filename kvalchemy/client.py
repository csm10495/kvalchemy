"""
Home to the KVAlchemy client.
"""
import contextlib
import logging
from typing import Any, Callable, Iterable, Union

from sqlalchemy import create_engine
from sqlalchemy.orm import Session, scoped_session, sessionmaker

from kvalchemy.models import Base, KVStore, ValueMixIn
from kvalchemy.proxy import Proxy
from kvalchemy.time import ExpirationType, to_expire
from kvalchemy.values import ENOVAL

log = logging.getLogger(__name__)

MEMOIZE_TAG = "__memoize__"


class KVAlchemy:
    """
    Client for working with the key-value store.
    """

    def __init__(self, url: str, create_models: bool = True) -> None:
        """
        Initializes the KVAlchemy client.

        Takes in the sqlalchemy url to connect to the database, along
        with an option to ensure the necessary db models are created.
        """
        self.url = url
        self._engine = create_engine(url)

        if create_models:
            Base.metadata.create_all(self._engine)

        self._session_factory = sessionmaker(bind=self._engine)
        self._session = scoped_session(self._session_factory)

    def __iter__(self) -> Iterable[str]:
        """
        Returns an iterable of all non-expired keys in the store.
        """
        with self.session(commit=False) as session:
            query = (
                session.query(KVStore)
                .filter(KVStore.non_expired_filter())
                .order_by(KVStore.key.asc(), KVStore.tag.asc())
            )
            for key in query.all():
                yield key

    def __len__(self) -> int:
        """
        Returns the number of non-expired key-value pairs in the store.
        """
        count = 0
        for _ in self:
            count += 1
        return count

    @contextlib.contextmanager
    def session(
        self, commit: bool = True, delete_expired: bool = True
    ) -> Iterable[Session]:
        """
        Contextmanager to obtain a temp session with the underlying database.

        If commit is True, the session will be committed after the block.
        If delete_expired is True, any expired keys will be deleted before exiting the block.
            Note that delete_expired only applies if commit is True.
        """
        with self._session() as session:
            yield session

            if commit:
                if delete_expired:
                    session.query(KVStore).filter(
                        ~KVStore.non_expired_filter()
                    ).delete()

                session.commit()

    def get(
        self,
        key: str,
        default: Any = ENOVAL,
        tag: str = "",
        return_expiration: bool = False,
    ) -> Any:
        """
        Retrieves the value for the given key and tag.

        If the key/tag combo is not found (or expired), and a default is provided, the
        default value is returned. If no default is provided, a KeyError is raised.

        If return_expiration is True, will return the (value, (expiration datetime or None)) as a tuple
        """
        with self.session() as session:
            query = (
                session.query(KVStore)
                .filter(KVStore.non_expired_filter())
                .filter_by(key=key, tag=tag)
            )
            result = query.one_or_none()

            if result is None:
                result = ValueMixIn(default)

            if result.value is ENOVAL:
                raise KeyError(f"key: {key}, tag: {tag}")

            if return_expiration:
                return result.value, getattr(result, "expire", None)

            return result.value

    def set(
        self, key: str, value: Any, tag: str = "", expire: ExpirationType = None
    ) -> None:
        """
        Sets the given key/tag combo to the value provided.

        If expire is provided, it must be something that can be processed by
        the to_expire function in kvalchemy.time.
        """
        with self.session() as session:
            session.merge(
                KVStore(key=key, value=value, tag=tag, expire=to_expire(expire))
            )

    def delete(self, key: str, tag: str = "") -> None:
        """
        Deletes the given key/tag combo from the store.
        """
        with self.session() as session:
            query = (
                session.query(KVStore)
                .filter(KVStore.non_expired_filter())
                .filter_by(key=key, tag=tag)
            )
            result = query.one_or_none()

            if result is not None:
                session.delete(result)

    def pop(self, key: str, default: Any = ENOVAL, tag: str = "") -> None:
        """
        Pops the given key/tag combo from the store.

        If the key/tag combo is not found (or expired), and a default is provided, the
        default value is returned. If no default is provided, a KeyError is raised.
        """
        sentinel = object()
        value = self.get(key, sentinel, tag)

        if value is sentinel:
            if default is ENOVAL:
                raise KeyError(f"key: {key}, tag: {tag}")
            else:
                value = default

        self.delete(key, tag)

        return value

    def clear(self) -> None:
        """
        Clears all key-value pairs from the store.
        """
        with self.session() as session:
            session.query(KVStore).delete()

    def get_proxy(self, key: str, default: Any = ENOVAL, tag: str = "") -> Proxy:
        """
        Returns a Proxy object for the given key, tag, default.
        """
        return Proxy(self, key, default, tag)

    def _delete_startswith(self, key: str, tag: str):
        """
        Deletes all keys that start with the given key but fall under the given tag.
        """
        with self.session() as session:
            session.query(KVStore).filter(
                KVStore.key.startswith(key), KVStore.tag == tag
            ).delete()

    def memoize(
        self,
        expire: ExpirationType = None,
        expire_if: Union[bool, Callable] = False,
        skip_saving_to_cache_if: Union[bool, Callable] = False,
    ):
        """
        A decorator to memoize the results of a function into the key-value store.

        expire allows us to specify a ttl to expire this memoization on.

        expire_if allows a bool or callable, if callable it will be called (and should return a bool).
            If it is True, we'll expire the cache and allow the func to be called.
            If it is False, the cache will be hit per the existing ttl.

        skip_saving_to_cache_if allows a bool or callable, if callable it will be called with a single param of the value about to be returned
            (and should return a bool). This is only checked if the underlying function is otherwised called.
            If it is True, we will not save this value as memoized (so next call with hit the function again).
            If it is False, we will save the value to the cache as per normal.
        """
        if callable(expire):
            # we've been called like:
            # @memoize
            # without () at the end
            func = expire
            expire = None
        else:
            func = None

        def inner(func):
            base_key = f"memoize.{func.__module__}_{func.__qualname__}_{expire!s}"

            def wrapper(*args, **kwargs):
                key = f"{base_key}_{args!s}_{kwargs!s}"

                # if you overwrite inner.expire_if then the callable would only get evaluated once
                # ... so don't do that.
                expire_if = inner.expire_if

                if callable(expire_if):
                    expire_if = expire_if()

                try_cache = not bool(expire_if)

                NO_RESULT = object()
                result = NO_RESULT
                if try_cache:
                    try:
                        result = self.get(key, tag=MEMOIZE_TAG)
                    except KeyError:
                        pass
                else:
                    log.debug(f"expire_if is forcing us to ignore cache: {key}")

                if result == NO_RESULT:
                    result = func(*args, **kwargs)

                    skip_saving_to_cache = False
                    if callable(skip_saving_to_cache_if):
                        if skip_saving_to_cache_if(result):
                            skip_saving_to_cache = True
                    elif skip_saving_to_cache_if:
                        skip_saving_to_cache = skip_saving_to_cache_if

                    if skip_saving_to_cache:
                        log.debug(
                            f"skip_saving_to_cache_if is forcing us to not save to cache the value for key: {key}"
                        )
                    else:
                        self.set(key, result, tag=MEMOIZE_TAG, expire=expire)

                return result

            wrapper.cache_clear = lambda: self._delete_startswith(base_key, MEMOIZE_TAG)
            return wrapper

        inner.expire_if = expire_if

        if func:
            return inner(func)
        else:
            return inner
