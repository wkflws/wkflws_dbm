"""A dirty cache module using the standard library.

This module aims to be a simple drop-in replacement for memcache style cache
libraries. If your project grows to any significant size you should consider using a
real cache stack.
"""
import asyncio
from dbm import gnu as dbm
import os
import pickle
from threading import Lock
import time
from typing import Any, Optional

_WRITE_LOCK = Lock()

epoch = lambda: int(time.time())  # noqa


class CacheError(Exception):
    """Raised during generic caching errors."""

    pass


class CacheKeyNotFoundError(CacheError):
    """Raised when the requested key is not found in the cache."""

    pass


async def get(
    db_path: str,
    key: str,
    default: Optional[Any] = None,
    raise_on_miss: bool = False,
    deserialize_func=pickle.loads,
) -> Any:
    """Retrieve the value stored under ``key`` from the cache.

    Args:
        db_path: The full path of the database to read.
        key: The key the value is stored under.
        default: The default value to return if key is not found.
        raise_on_miss: Raise an exception on error instead of returning default. The
            cache serializes Python objects so it's entirely possible to retrieve an
            object that matches the default value and assume it was a miss.
        deserialize_func: The function to use when deserializing the cached
            value. This function must accept bytes as it's only input.
            *Default is pickle.loads*

    Raises:
        CacheKeyNotFoundError: is raised if there is a cache miss and ``raise_on_miss``
            is ``True``

    Returns: The cached object or ``default``
    """

    def read_value(_k: str) -> Optional[Any]:
        if not os.path.exists(db_path):
            return None
        # Read-only, fast mode (writes not synchronized but no writing is done)
        with dbm.open(db_path, "rf") as db:
            return db.get(_k)

    loop = asyncio.get_event_loop()

    value = await loop.run_in_executor(None, read_value, key)

    if value is None and raise_on_miss:
        raise CacheKeyNotFoundError(f"Key not found in cache: {key}")
    elif value is None:
        return default

    payload = deserialize_func(value)

    if payload.get("exp", 0) and epoch() >= payload["exp"]:
        return default

    return payload["v"] if "v" in payload else payload


async def set(
    db_path: str,
    key: str,
    value: Any,
    expiry_secs: int = 300,
    serialize_func=pickle.dumps,
):
    """Set a value in the cache stored under ``key``.

    .. note::

       This function uses a lock to prevent multiple writers.

    Args:
        db_path: The full path of the database to read.
        key: The key to store the value under.
        value: The value to store.
        expiry_secs: The number of seconds the value is valid for.
        serialize_func: The function to use to serialize ``value`` before storing in the
            cache. This function must accept any object as it's only input and return
            bytes. *Default is pickle.dumps*

    Raises:
        CacheError: There was an error while storing the value.
    """

    def write_value(_v):
        # create if it doesn't exist, sync mode to write immediately
        with dbm.open(db_path, "cs") as db:
            try:
                db[key] = _v
            except Exception as e:
                raise CacheError(f"Error writing data to cache: {e}")

    loop = asyncio.get_event_loop()
    try:
        payload = {
            "exp": epoch() + expiry_secs,
            "v": value,
        }
        serialized_value = serialize_func(payload)
    except Exception as e:
        raise CacheError(f"Error serializing object: {e}")

    with _WRITE_LOCK:
        await loop.run_in_executor(None, write_value, serialized_value)


async def list(db_path: str, keys_only: bool = False, deserialize_func=pickle.loads):
    """Pretty print out the contents of the cache.

    Args:
        db_path: The full path of the database to read.
        keys_only: Don't deserialize and display the raw values.
        deserialize_func: The function to use when deserializing the cached
            value. This function must accept bytes as it's only input.
            *Default is pickle.loads*

    """
    from pprint import pprint

    def list_db():
        # read only in fast mode
        with dbm.open(db_path, "rf") as db:
            k = db.firstkey()
            while k is not None:
                if keys_only:
                    print(k)
                else:
                    v = deserialize_func(db[k])
                    print(k)
                    pprint(v)

                k = db.nextkey(k)

    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, list_db)


async def clean(
    db_path: str,
    older_than_ts: int = None,
    compact: bool = False,
    deserialize_func=pickle.loads,
):
    """Clean the cache of expired values.

    I believe there is a global lock on the actual database so in theory this should be
    safe to run in a separate process. The GNU dbm should prevent any accidents.

    Args:
        db_path: The full path of the database to read.
        older_than_ts: Remove entries that are set to expire before this value.
            *Default is now*
        compact: Also compact the database. This essentially defrags the database so it
            should help reclaim space and improve seek speed after many records have
            been removed. *Default is False*
        deserialize_func: The function to use when deserializing the cached
            value. This function must accept bytes as it's only input. Used to compare
            expiration. *Default is pickle.loads*
    """
    if not older_than_ts:
        older_than_ts = epoch()

    def clean_db():
        # create if it doesn't exist, sync mode to write immediately
        with dbm.open(db_path, "cs") as db:
            k = db.firstkey()
            while k is not None:
                v = deserialize_func(db[k])
                if v.get("exp", 0) and older_than_ts > v["exp"]:
                    del db[k]
                k = db.nextkey(k)

            if compact:
                db.reorganize()

    loop = asyncio.get_event_loop()
    with _WRITE_LOCK:
        await loop.run_in_executor(None, clean_db)
