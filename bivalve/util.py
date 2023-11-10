# --------------------------------------------------------------------
# util.py
#
# Author: Lain Musgrove (lain.proliant@gmail.com)
# Date: Saturday February 11, 2023
#
# Distributed under terms of the MIT license.
# --------------------------------------------------------------------

import asyncio
import inspect
import json
import time
from typing import Any, Sequence


# --------------------------------------------------------------------
class Borg:
    _shared_state: dict[str, Any] = {}

    def __init__(self):
        self.__dict__ = self._shared_state


# --------------------------------------------------------------------
async def async_wrap(f, *args, **kwargs):
    """
    From xeno.utils

    Wraps a normal function in a coroutine.  If the given function
    is already a coroutine function, we simply await it.
    """

    if not asyncio.iscoroutinefunction(f):
        return f(*args, **kwargs)
    return await f(*args, **kwargs)


# --------------------------------------------------------------------
def is_iterable(obj: Any) -> bool:
    """Determine if the given object is an iterable sequence other than a string or byte array."""
    return (
        isinstance(obj, Sequence)
        and not isinstance(obj, (str, bytes, bytearray))
        or inspect.isgenerator(obj)
    )


# --------------------------------------------------------------------
def str_escape(s: str) -> str:
    """
    Use the JSON library to escape a string for use as a literal.
    """

    return json.dumps(s).strip('"')


# --------------------------------------------------------------------
def get_millis() -> int:
    """
    Get the number of milliseconds since the UNIX Epoch.
    Useful for performance timing.
    """
    return time.time_ns() // 1000000


# --------------------------------------------------------------------
def new_future() -> asyncio.Future:
    """
    Creates a future with the current event loop.
    """
    loop = asyncio.get_event_loop()
    return loop.create_future()
